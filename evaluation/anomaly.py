"""Held-out synthetic sequence evaluation of the shipped IsolationForest.

No broker or database is used here. Run pipeline tests separately before making
claims about alert delivery, maintenance dispatch, or service latency.
"""
import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from common import ROOT, provenance, sha256, write_json

sys.path.insert(0, str(ROOT / "edge-analyzer"))
from model_utils import FEATURE_ORDER, MODEL_METADATA, train_model


def generate_sequences(config):
    rows = []
    for seed in config["evaluationSeeds"]:
        for kind in ("healthy", "overheat", "vibration"):
            # Identical healthy component within a seed isolates fault injection.
            rng = np.random.RandomState(seed)
            sequence_id = f"{seed}-{kind}"
            for second in range(0, config["sequenceSeconds"], config["sampleIntervalSeconds"]):
                active = (second // 30) % 2 == 1
                ranges = [(86, 90), (0.4, 0.6), (112, 120)] if active else [(80, 85), (0.1, 0.35), (100, 110)]
                values = [rng.uniform(lower, upper) for lower, upper in ranges]
                faulty = kind != "healthy" and second >= config["faultOnsetSeconds"]
                ramp = min(1, (second - config["faultOnsetSeconds"] + 1) / config["faultRampSeconds"]) if faulty else 0
                if kind == "overheat":
                    values[0] += config["overheatTemperatureIncrease"] * ramp
                    values[2] += config["overheatEnergyIncrease"] * ramp
                elif kind == "vibration":
                    values[1] += config["vibrationIncrease"] * ramp
                    values[2] += config["vibrationEnergyIncrease"] * ramp
                values = [round(values[0], 1), round(values[1], 2), round(values[2])]
                rows.append({
                    "sequenceId": sequence_id, "seed": seed, "kind": kind,
                    "simulatedSeconds": second, "faultPresent": faulty,
                    "faultOnsetSeconds": config["faultOnsetSeconds"] if kind != "healthy" else None,
                    "active": active, **dict(zip(FEATURE_ORDER, values)),
                })
    return rows


def classification_metrics(labels, predictions):
    labels = np.asarray(labels, dtype=bool)
    predictions = np.asarray(predictions, dtype=bool)
    tp = int(np.sum(labels & predictions))
    fp = int(np.sum(~labels & predictions))
    fn = int(np.sum(labels & ~predictions))
    tn = int(np.sum(~labels & ~predictions))
    return {
        "truePositive": tp, "falsePositive": fp, "falseNegative": fn, "trueNegative": tn,
        "precision": tp / (tp + fp) if tp + fp else None,
        "recall": tp / (tp + fn) if tp + fn else None,
        "falseAlarmRate": fp / (fp + tn) if fp + tn else None,
    }


def summarize(rows, predictions):
    metrics = classification_metrics([row["faultPresent"] for row in rows], predictions)
    delays = []
    for sequence in sorted({row["sequenceId"] for row in rows if row["kind"] != "healthy"}):
        selected = [(row, bool(prediction)) for row, prediction in zip(rows, predictions) if row["sequenceId"] == sequence]
        onset = selected[0][0]["faultOnsetSeconds"]
        detected = next((row["simulatedSeconds"] for row, prediction in selected if row["faultPresent"] and prediction), None)
        delays.append({"sequenceId": sequence, "delaySeconds": detected - onset if detected is not None else None})
    detected_delays = [item["delaySeconds"] for item in delays if item["delaySeconds"] is not None]
    metrics["episodes"] = delays
    metrics["missedEpisodes"] = sum(item["delaySeconds"] is None for item in delays)
    metrics["medianDetectionDelaySecondsAmongDetected"] = float(np.median(detected_delays)) if detected_delays else None
    metrics["p95DetectionDelaySecondsAmongDetected"] = float(np.percentile(detected_delays, 95)) if detected_delays else None
    metrics["bySequenceKind"] = {}
    metrics["bySeed"] = {}
    for field, output in (("kind", "bySequenceKind"), ("seed", "bySeed")):
        for key in sorted({row[field] for row in rows}):
            indices = [index for index, row in enumerate(rows) if row[field] == key]
            metrics[output][str(key)] = classification_metrics([rows[index]["faultPresent"] for index in indices], [predictions[index] for index in indices])
    return metrics


def evaluate(config_path, output_dir, saved_inputs=None):
    started = datetime.now(timezone.utc).isoformat()
    timer = time.perf_counter()
    config = json.loads(config_path.read_text(encoding="utf-8"))["anomaly"]
    if config["trainingSeed"] in config["evaluationSeeds"]:
        raise ValueError("Training and evaluation seeds must be disjoint")
    rows = [json.loads(line) for line in saved_inputs.read_text(encoding="utf-8").splitlines() if line] if saved_inputs else generate_sequences(config)
    samples = np.array([[row[name] for name in FEATURE_ORDER] for row in rows])
    model = train_model(config["trainingSeed"])
    scores = -model.decision_function(samples)
    forest_predictions = scores > 0
    threshold_predictions = np.array([any(row[name] < bounds[0] or row[name] > bounds[1] for name, bounds in config["thresholds"].items()) for row in rows])
    output_dir.mkdir(parents=True, exist_ok=True)
    inputs_path = output_dir / "anomaly-inputs.jsonl"
    inputs_path.write_text("".join(json.dumps(row, allow_nan=False) + "\n" for row in rows), encoding="utf-8")
    predictions_path = output_dir / "anomaly-predictions.jsonl"
    predictions_path.write_text("".join(json.dumps({
        "sequenceId": row["sequenceId"], "simulatedSeconds": row["simulatedSeconds"],
        "anomalyScore": float(score), "isolationForest": bool(forest), "threshold": bool(threshold),
    }) + "\n" for row, score, forest, threshold in zip(rows, scores, forest_predictions, threshold_predictions)), encoding="utf-8")
    results = {
        "schemaVersion": 1,
        "experiment": "Offline classifier evaluation on held-out labeled synthetic sequences",
        "config": config,
        "model": MODEL_METADATA,
        "classBalance": {"samples": len(rows), "healthy": sum(not row["faultPresent"] for row in rows), "faulty": sum(row["faultPresent"] for row in rows), "sequences": len({row["sequenceId"] for row in rows})},
        "isolationForest": summarize(rows, forest_predictions),
        "threshold": summarize(rows, threshold_predictions),
        "inputsSha256": sha256(inputs_path), "predictionsSha256": sha256(predictions_path),
        "provenance": provenance(["edge-analyzer/model_utils.py", "evaluation/anomaly.py", "evaluation/common.py", "evaluation/config.json"], started, time.perf_counter() - timer),
        "limitations": [
            "Synthetic labels mean a fault was injected, including the gradual ramp before values leave normal ranges.",
            "Thresholds were fixed from training ranges before evaluation; no thresholds or model parameters were tuned on held-out seeds.",
            "Healthy samples mirror simulator idle/active marginal ranges; correlated real sensors, drift, noise, sensor failure and real fault prevalence are not represented.",
            "Detection delay is simulated seconds after injection, not advance failure prediction or network delivery latency.",
            "Repeated observations in a sequence are dependent. Seed summaries show variation, not independent-sample confidence intervals.",
            "This offline experiment does not test MQTT, MongoDB, maintenance dispatch, repairs, or browser presentation.",
        ],
    }
    write_json(output_dir / "anomaly-results.json", results)
    for name in ("isolationForest", "threshold"):
        print(name, json.dumps({key: value for key, value in results[name].items() if key not in ("episodes", "bySequenceKind", "bySeed")}))
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "evaluation/config.json")
    parser.add_argument("--output", type=Path, default=ROOT / "evaluation/results")
    parser.add_argument("--inputs", type=Path, help="Replay saved JSONL samples instead of generating new samples")
    args = parser.parse_args()
    evaluate(args.config, args.output, args.inputs)

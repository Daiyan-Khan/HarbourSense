"""Replay saved inputs and verify deterministic measurements, excluding run time."""
import contextlib
import io
import json
import math
import tempfile
from pathlib import Path

from common import ROOT, sha256, source_sha256
from anomaly import evaluate as anomaly
from routing import evaluate as routing


def verify_artifact_hashes(directory, report, fields):
    for filename, hash_field in fields.items():
        if sha256(directory / filename) != report[hash_field]:
            raise AssertionError(f"Saved evidence artifact changed: {filename}")


def artifact_values(path):
    content = path.read_text(encoding="utf-8")
    if path.suffix == ".jsonl":
        return [json.loads(line) for line in content.splitlines() if line]
    return json.loads(content)


def verify_prediction_values(actual, expected):
    """Allow only measured cross-platform roundoff in classifier scores.

    Linux and Windows with the same pinned numerical packages differed in 8 of
    2700 scores by at most 1.11e-16. Keep an absolute 1e-15 bound; every label,
    schema/type, identifier, timestamp, aggregate metric and raw hash is exact.
    """
    if type(actual) is not list or type(expected) is not list or len(actual) != len(expected):
        raise AssertionError("Replayed prediction count or schema differs")
    for index, (observed, saved) in enumerate(zip(actual, expected)):
        if type(observed) is not dict or type(saved) is not dict or observed.keys() != saved.keys():
            raise AssertionError(f"Replayed prediction schema differs at row {index}")
        for field, value in saved.items():
            candidate = observed[field]
            if type(candidate) is not type(value):
                raise AssertionError(f"Replayed prediction type differs at row {index}: {field}")
            if field == "anomalyScore":
                if (type(value) is not float or not math.isfinite(value) or not math.isfinite(candidate)
                        or not math.isclose(candidate, value, rel_tol=0.0, abs_tol=1e-15)):
                    raise AssertionError(f"Replayed anomaly score differs at row {index}")
            elif candidate != value:
                raise AssertionError(f"Replayed prediction value differs at row {index}: {field}")


def verify(results=ROOT / "evaluation/results"):
    expected_anomaly = json.loads((results / "anomaly-results.json").read_text())
    expected_routing = json.loads((results / "routing-results.json").read_text())
    verify_artifact_hashes(results, expected_anomaly, {
        "anomaly-inputs.jsonl": "inputsSha256", "anomaly-predictions.jsonl": "predictionsSha256"})
    verify_artifact_hashes(results, expected_routing, {
        "routing-inputs.json": "inputsSha256", "routing-events.jsonl": "eventsSha256"})
    for result in (expected_anomaly, expected_routing):
        for source, expected_hash in result["provenance"]["sourceSha256"].items():
            if source_sha256(ROOT / source) != expected_hash:
                raise AssertionError(f"Source changed since saved evaluation: {source}. Regenerate results before publication.")
    with tempfile.TemporaryDirectory(prefix="harboursense-evaluation-") as directory:
        output = Path(directory)
        with contextlib.redirect_stdout(io.StringIO()):
            actual_anomaly = anomaly(ROOT / "evaluation/config.json", output, results / "anomaly-inputs.jsonl")
            actual_routing = routing(ROOT / "evaluation/config.json", output, results / "routing-inputs.json")
        for key in ("classBalance", "model", "isolationForest", "threshold"):
            if actual_anomaly[key] != expected_anomaly[key]:
                raise AssertionError(f"Anomaly replay mismatch: {key}")
        for key in ("summaries", "runs"):
            if actual_routing[key] != expected_routing[key]:
                raise AssertionError(f"Routing replay mismatch: {key}")
        # Saved files are checked byte-for-byte above. A fresh run may serialize
        # with the operating system's different newline, so compare all decoded
        # values here without weakening the committed artifact integrity check.
        for filename in ("anomaly-inputs.jsonl", "anomaly-predictions.jsonl", "routing-inputs.json", "routing-events.jsonl"):
            if filename == "anomaly-predictions.jsonl":
                verify_prediction_values(artifact_values(output / filename), artifact_values(results / filename))
                continue
            if artifact_values(output / filename) != artifact_values(results / filename):
                raise AssertionError(f"Replayed evidence values differ: {filename}")
    print("Committed evidence artifacts match their raw-byte fingerprints.")
    print("Saved anomaly inputs reproduce exact labels/metrics and scores within 1e-15 absolute roundoff.")
    print("Saved routing graph/workloads reproduce all events and outcomes for both strategies.")
    print("Evaluated source hashes match the current source files.")


if __name__ == "__main__":
    verify()

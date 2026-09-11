"""Draw the corrected report figures from saved HarbourSense measurements.

Reproduce with Python 3.12, matplotlib==3.10.3 and numpy==1.26.4:
    python generate_figures.py --results /path/to/HarbourSense/evaluation/results
Outputs are PDF (vector) plus PNG previews. No simulation/model is rerun here;
use evaluation/verify.py to reproduce the underlying measurements.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import numpy as np

BLUE = "#176493"
ORANGE = "#b75b17"
INK = "#253346"
GRID = "#dfe5eb"


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def default_results():
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "evaluation" / "results"
        if candidate.is_dir():
            return candidate
    raise RuntimeError("Provide --results pointing to the committed evaluation/results directory")


def style_axis(ax, grid="y"):
    ax.spines[["top", "right"]].set_visible(False)
    ax.spines[["bottom", "left"]].set_color("#8b97a5")
    ax.tick_params(length=3, color="#8b97a5")
    ax.set_axisbelow(True)
    ax.grid(axis=grid, color=GRID, linewidth=.6)


def save(fig, name, output):
    fig.savefig(output / f"{name}.pdf", bbox_inches="tight", pad_inches=.06,
                metadata={"Creator": "HarbourSense measured-results figure generator",
                          "CreationDate": None, "ModDate": None})
    fig.savefig(output / f"{name}.png", dpi=300, bbox_inches="tight", pad_inches=.06)
    plt.close(fig)


def classification(report, output):
    fig, axes = plt.subplots(1, 3, figsize=(7.16, 2.45))
    metrics = [("precision", "(a) Precision", "Higher is better", 110),
               ("recall", "(b) Recall", "Higher is better", 110),
               ("falseAlarmRate", "(c) False-alarm rate", "Lower is better", 3.3)]
    for ax, (key, title, direction, upper) in zip(axes, metrics):
        values = [100 * report[model][key] for model in ("isolationForest", "threshold")]
        bars = ax.bar([0, 1], values, width=.58, color=[BLUE, ORANGE], zorder=3)
        for bar, value in zip(bars, values):
            ax.text(bar.get_x()+bar.get_width()/2, value+upper*.025,
                    f"{value:.2f}%", ha="center", va="bottom", fontsize=8.7, fontweight="bold")
        ax.set_xticks([0, 1], ["Isolation\nForest", "Range\nthresholds"])
        ax.set_ylim(0, upper)
        ax.set_ylabel("Percent")
        ax.set_title(title, loc="left", pad=20, fontweight="bold")
        ax.text(0, 1.03, direction, transform=ax.transAxes, fontsize=8, color="#627180")
        if key != "falseAlarmRate":
            ax.set_yticks([0, 25, 50, 75, 100])
        else:
            ax.set_yticks([0, 1, 2, 3])
        style_axis(ax)
    fig.subplots_adjust(left=.075, right=.99, bottom=.20, top=.78, wspace=.55)
    save(fig, "anomaly-classification", output)


def detection_delay(report, output):
    models = ("isolationForest", "threshold")
    episodes = [{item["sequenceId"]: item["delaySeconds"] for item in report[model]["episodes"]}
                for model in models]
    assert episodes[0].keys() == episodes[1].keys()
    ids = list(episodes[0])
    assert all(episodes[i][key] is not None for key in ids for i in range(2)), "Plot missed episodes explicitly before reuse"
    fig, ax = plt.subplots(figsize=(3.5, 3.6))
    rows = np.arange(len(ids))
    for row, key in zip(rows, ids):
        ax.plot([episodes[0][key], episodes[1][key]], [row, row], color="#b9c3cc", lw=1.0, zorder=2)
    for values, color, marker, label in zip(episodes, [BLUE, ORANGE], ["s", "o"], ["Isolation Forest", "Range thresholds"]):
        ax.scatter([values[key] for key in ids], rows, color=color, marker=marker, s=28, label=label, zorder=3)
    ax.set_yticks(rows, [key.replace("-", " / ") for key in ids])
    ax.set_ylim(len(ids)-.45, -.65)
    ax.set_xlim(0, 36)
    ax.set_xticks([0, 10, 20, 30])
    ax.set_xlabel("Detection delay (simulated s)", fontsize=8.2)
    ax.set_title("Paired fault episodes", loc="left", fontweight="bold", pad=31)
    ax.legend(loc="lower left", bbox_to_anchor=(-.01, 1.025), frameon=False,
              ncol=2, fontsize=7.7, handletextpad=.25, columnspacing=.6)
    style_axis(ax, "x")
    fig.subplots_adjust(left=.33, right=.98, bottom=.15, top=.81)
    save(fig, "anomaly-detection-delay", output)


def routing_outcomes(report, output):
    seeds = report["config"]["seeds"]
    by_pair = {(run["seed"], run["strategy"]): run for run in report["runs"]}
    assert len(by_pair) == 2*len(seeds)
    fig, (left, right) = plt.subplots(1, 2, figsize=(7.16, 3.25))
    width = .34
    for strategy, shift, color, label in [("weighted", -width/2, BLUE, "Weighted"), ("bfs", width/2, ORANGE, "BFS")]:
        runs = [by_pair[seed, strategy] for seed in seeds]
        complete = [run["completed"] for run in runs]
        unfinished = [run["unfinished"] for run in runs]
        failed = [run["failed"] for run in runs]
        assert all(value == 0 for value in failed), "Add failure segments if future data contain failures"
        assert all(c+u+f == report["config"]["shipments"] for c, u, f in zip(complete, unfinished, failed))
        locations = np.arange(len(seeds))+shift
        left.bar(locations, complete, width, color=color, zorder=3)
        left.bar(locations, unfinished, width, bottom=complete, facecolor="#f4f6f8", edgecolor=color,
                 hatch="////", linewidth=.7, zorder=3)
        for x, value in zip(locations, complete):
            left.text(x, value-4, str(value), color="white", fontsize=7.6,
                      ha="center", va="center", fontweight="bold")
        times = np.sort([job["deliverySeconds"] for run in runs for job in run["jobs"] if job["status"] == "completed"])
        assert len(times) == sum(complete)
        right.step(np.r_[0, times], np.r_[0, 100*np.arange(1, len(times)+1)/len(times)], where="post",
                   color=color, linewidth=1.6, linestyle="-" if strategy == "weighted" else "--",
                   label=f"{label} (n={len(times)})")
    left.set_xticks(np.arange(len(seeds)), [str(seed) for seed in seeds])
    left.set_ylim(0, 63)
    left.set_yticks([0, 15, 30, 45, 60])
    left.set_ylabel("Shipments per run (60 requested)")
    left.set_xlabel("Paired workload seed")
    left.set_title("(a) Completion and unfinished work", loc="left", fontsize=9.0, fontweight="bold")
    left.legend(handles=[Patch(facecolor=BLUE,label="Weighted"),Patch(facecolor=ORANGE,label="BFS"),
                         Patch(facecolor="#f4f6f8",edgecolor="#687687",hatch="////",label="Unfinished")],
                loc="upper left", bbox_to_anchor=(-.01, -.24), ncol=3, fontsize=7.6,
                frameon=False, handletextpad=.35, columnspacing=.75)
    right.set_xlim(left=0)
    right.set_ylim(0, 102)
    right.set_yticks([0, 25, 50, 75, 100])
    right.set_ylabel("Completed deliveries at or below time (%)")
    right.set_xlabel("Arrival-to-delivery time (simulated s)")
    right.set_title("(b) Pooled completed-job distribution", loc="left", fontsize=9.0, fontweight="bold")
    right.legend(loc="lower right", frameon=False, fontsize=8.0)
    style_axis(left)
    style_axis(right)
    fig.subplots_adjust(left=.075, right=.99, bottom=.27, top=.89, wspace=.35)
    save(fig, "routing-outcomes", output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path)
    parser.add_argument("--output", type=Path, default=Path(__file__).resolve().parent)
    args = parser.parse_args()
    results, output = args.results or default_results(), args.output
    output.mkdir(parents=True, exist_ok=True)
    anomaly = json.loads((results / "anomaly-results.json").read_text(encoding="utf-8"))
    routing = json.loads((results / "routing-results.json").read_text(encoding="utf-8"))
    for filename, report, field in [("anomaly-inputs.jsonl", anomaly, "inputsSha256"),
                                  ("anomaly-predictions.jsonl", anomaly, "predictionsSha256"),
                                  ("routing-inputs.json", routing, "inputsSha256"),
                                  ("routing-events.jsonl", routing, "eventsSha256")]:
        assert sha256(results / filename) == report[field], f"Saved evidence mismatch: {filename}"
    for method in ("isolationForest", "threshold"):
        row = anomaly[method]
        tp, fp, fn, tn = (row[key] for key in ("truePositive", "falsePositive", "falseNegative", "trueNegative"))
        for measured, recomputed in [(row["precision"], tp/(tp+fp)), (row["recall"], tp/(tp+fn)), (row["falseAlarmRate"], fp/(fp+tn))]:
            assert math.isclose(measured, recomputed, rel_tol=0, abs_tol=1e-15)
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 8.5, "axes.labelsize": 8.5,
                         "axes.titlesize": 9.2, "text.color": INK, "axes.labelcolor": INK,
                         "xtick.color": INK, "ytick.color": INK, "axes.linewidth": .6,
                         "pdf.fonttype": 42, "ps.fonttype": 42, "figure.facecolor": "white"})
    classification(anomaly, output)
    detection_delay(anomaly, output)
    routing_outcomes(routing, output)
    manifest = {"schemaVersion": 1, "purpose": "Corrected public report figures from committed offline synthetic experiments",
                "pythonPackages": {"matplotlib": matplotlib.__version__, "numpy": np.__version__},
                "inputs": {name: sha256(results/name) for name in ("anomaly-results.json", "routing-results.json")},
                "generatorSha256": sha256(Path(__file__)),
                "figures": {name: sha256(output/name) for name in [f"{stem}.{ext}" for stem in
                    ("anomaly-classification", "anomaly-detection-delay", "routing-outcomes") for ext in ("pdf", "png")]}}
    (output/"figure-provenance.json").write_text(json.dumps(manifest,indent=2)+"\n",encoding="utf-8")
    print("Generated three measured-results figures (PDF and PNG).")


if __name__ == "__main__":
    main()

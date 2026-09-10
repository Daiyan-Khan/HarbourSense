# HarbourSense evaluation evidence

These are measured **offline synthetic experiments**, generated from the committed inputs and scripts. They do not measure the full deployed port, API latency, real sensor reliability, or advance failure prediction. The anomaly experiment favors simple thresholds. The routing experiment favors congestion-aware routing within the stated transport model.

Measured on 2026-09-10. Source fingerprints, exact package versions, wall-clock execution durations, and machine information are in the result JSON files. Regenerate this page with `python evaluation/report.py`; do not edit result numbers by hand.

## Reproduce

From the repository root, with Python 3.12:

```sh
python -m venv evaluation/.venv
# Windows PowerShell:
.\evaluation\.venv\Scripts\Activate.ps1
# macOS/Linux instead: source evaluation/.venv/bin/activate
python -m pip install -r evaluation/requirements.txt
python -m unittest discover -s evaluation -v
python evaluation/anomaly.py
python evaluation/routing.py
python evaluation/verify.py
python evaluation/report.py
```

`verify.py` first verifies each saved input/prediction/event file against its raw-byte fingerprint. The repository attributes preserve those evidence bytes on checkout. It then replays the saved inputs in a temporary directory. Labels, counts, schemas, inputs, events and aggregate metrics must match exactly. Only per-sample `anomalyScore` permits absolute roundoff of at most 1e-15: a Linux Python 3.12.14 reproduction with the same pinned numerical packages differed from Windows Python 3.12.7 in 8 of 2,700 scores, by at most 1.11e-16, without changing any prediction. Larger score changes, changed labels and nonfinite scores fail verification. Fresh serialization may use different Windows/Linux newlines; source fingerprints normalize CRLF to LF. Nondeterministic wall-clock duration and generation timestamps are excluded. An unexpected source or saved artifact change fails verification instead of silently treating earlier numbers as current evidence.

To score the exact saved inputs after an intentional implementation change, pass `--inputs evaluation/results/anomaly-inputs.jsonl` to the anomaly script and `--inputs evaluation/results/routing-inputs.json` to the routing script, with a separate `--output` directory. Keep old and new results for comparison.

## Anomaly detection

The existing IsolationForest is unchanged: 100 estimators, contamination 0.01, 10,000 independent uniform synthetic training samples, seed 42. Feature order is motor temperature, vibration, energy use. Its normal training ranges are 80–90, 0.1–0.6, and 100–120 respectively, in simulator units. The baseline alerts when any feature is outside those ranges. Thresholds and model parameters were fixed before evaluating the held-out seeds.

The evaluation seeds are 110, 211, 312, 413 and 514. Each seed supplies healthy, overheat and vibration sequences, each lasting 180 simulated seconds at one sample/second. Healthy values alternate idle/active ranges every 30 seconds, with the simulator's rounding precision. Fault injection begins at second 60 and ramps for 30 seconds. Overheat adds up to 25 temperature units and 20 energy units; vibration fault adds up to 1.2 vibration units and 15 energy units. The healthy random component is paired across sequence kinds for each seed.

Ground truth labels an injected fault from its first ramp sample, including samples whose values remain inside healthy ranges. There are **2700 samples: 1500 healthy and 1200 faulty**, across 15 sequences and 10 fault episodes. The class balance is an experimental choice, not an estimated port failure rate.

| Metric | Existing IsolationForest | Fixed range thresholds |
| --- | ---: | ---: |
| Precision | 94.86% | 100.00% |
| Recall | 44.58% | 92.58% |
| False-alarm rate | 1.93% | 0.00% |
| True positives / false positives | 535 / 29 | 1111 / 0 |
| False negatives / true negatives | 665 / 1471 | 89 / 1500 |
| Median first-detection delay | 13.50 simulated s | 7.50 simulated s |
| P95 first-detection delay | 28.95 simulated s | 9.10 simulated s |
| Undetected fault episodes | 0 / 10 | 0 / 10 |

Precision = true positives / all positive predictions; recall = true positives / all fault-labeled samples; false-alarm rate = false positives / all healthy-labeled samples. Detection delay is the first positive prediction at or after injected onset minus onset. Missed episodes are explicitly counted; their delay is null, not zero. The JSON also includes per-seed/per-kind confusion matrices and every episode delay.

**Interpretation:** detecting every episode at least once does not mean continuously identifying every fault sample. The current forest misses many fault-labeled samples and does not outperform predetermined thresholds on these simple out-of-range faults. Keep the baseline and disclose this outcome; more complex modeling is not evidence of better detection. There is no evaluation of lead time before a future failure.

The API alert now includes the negative of sklearn `decision_function` as `analysis.anomalyScore` (positive means anomaly), its zero threshold, observed values, and comparisons against training ranges. These comparisons describe observations, not causal feature attribution. The legacy maintenance alert type remains for contract compatibility; user-facing text describes anomaly detection on synthetic data.

## Congestion-aware routing

Both strategies call the actual production planner in `python-backend/traffic_analyzer.py`: `SmartRoutePlanner.compute_path` and its existing `_bfs_shortest_path` baseline. They share the saved 25-node graph, the same four vehicles, and the same 60 shipment requests per seed. Five paired seeds use the same identifiers as above. Each run is capped at 600 **simulated** seconds.

The evaluation engine is a separate discrete-event transport model. Vehicles start at E1, pick up from A1 or E1, and deliver to B4, D2 or E5. Requests arrive every seeded 2–6 seconds. An edge normally takes four seconds; each pickup/drop-off takes two seconds. A bidirectional edge permits one moving vehicle at a time with FIFO reservations. Three bottleneck edges (A2–A3, B3–B4 and D1–D2) add 16 seconds per hop during seconds 20–200. The travel-time penalty is independent of the route-scoring function. Weighted routing observes injected delay and queued edge time; BFS ignores those signals. Both recompute at hop boundaries.

The following are the mean ± sample standard deviation **across the five seeds**. Delivery-time rows average each run's median/P95; they are not a pooled distribution.

| Metric per 600 simulated seconds | Weighted strategy | Unweighted BFS |
| --- | ---: | ---: |
| Completed deliveries / 60 | 54.20 ± 3.27 | 47.00 ± 3.39 |
| Unfinished deliveries / 60 | 5.80 ± 3.27 | 13.00 ± 3.39 |
| Failed deliveries / 60 | 0.00 ± 0.00 | 0.00 ± 0.00 |
| Completed / simulated minute | 5.42 ± 0.33 | 4.70 ± 0.34 |
| Median delivery time, simulated s | 205.30 ± 20.13 | 242.10 ± 16.61 |
| P95 delivery time, simulated s | 366.06 ± 21.46 | 390.67 ± 25.40 |
| Changes to remaining route | 78.60 ± 7.37 | 0.00 ± 0.00 |

Delivery time starts when a request arrives and ends at completed drop-off. It includes queueing for the fleet, empty pickup travel, edge waits and handling. Distribution metrics include completed deliveries only; unfinished counts must be read alongside them. A route change is a change to a previously planned remaining route within the same phase. Failed means no valid adjacent path or the 500-hop safety limit; unfinished means the horizon ended before completion. Throughput divides completions by the whole fixed horizon.

**Interpretation:** congestion-aware routing completes more deliveries in this particular model, at the cost of more route changes. The run-level JSON keeps all five seeds, job-level outcomes and paths, including unfinished work. This is not evidence of the same throughput improvement through MQTT, the real simulator or the maintenance/shipment manager.

## Machine and limitations

- OS: Windows-11-10.0.26200-SP0; architecture: AMD64.
- Processor: Intel64 Family 6 Model 154 Stepping 3, GenuineIntel; logical CPUs: 16.
- Python 3.12.7; numpy 1.26.4; scikit-learn 1.6.1; scipy 1.17.1.
- Wall-clock evaluator durations are recorded separately for each experiment. They describe this machine's execution time, not port throughput or HTTP latency.
- Synthetic healthy/fault distributions omit real sensor drift, correlated failures and operational noise. The easy threshold baseline benefits from fault values deliberately leaving known normal ranges.
- Temporal observations are dependent; per-seed variation is provided without pretending every sample is an independent trial.
- Routing uses idealized FIFO edges and handling time. It does not exercise warehouse capacity, crane service, manager ownership, MQTT retransmission, MongoDB, or the browser.
- Broker/database integration, pause/reset isolation, maintenance repair and deployed replay tests must be verified separately. An offline result cannot satisfy those acceptance gates.

## Saved evidence

- [Evaluation configuration](../evaluation/config.json)
- [Anomaly results and provenance](../evaluation/results/anomaly-results.json)
- [Labeled anomaly samples](../evaluation/results/anomaly-inputs.jsonl)
- [All anomaly predictions](../evaluation/results/anomaly-predictions.jsonl)
- [Routing results and provenance](../evaluation/results/routing-results.json)
- [Routing graph and workloads](../evaluation/results/routing-inputs.json)
- [All routing events](../evaluation/results/routing-events.jsonl)

The metric tests cover known confusion matrices, missing detections, deterministic inputs, real planner selection, unfinished work and unreachable routes. Analyzer tests separately cover finite-value validation, score sign, honest explanations, run fencing, duplicate suppression, publication retries and healthy-sample persistence. These are unit tests, not full-stack recovery evidence.

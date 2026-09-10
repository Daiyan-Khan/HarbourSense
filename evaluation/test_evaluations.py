"""Checks for metric definitions and evaluation model accounting."""
import json
import tempfile
import unittest
from pathlib import Path

from common import ROOT, sha256, source_sha256
from anomaly import classification_metrics, generate_sequences, summarize
from routing import simulate
from verify import artifact_values, verify_artifact_hashes, verify_prediction_values


class AnomalyEvaluationTests(unittest.TestCase):
    def test_cross_platform_score_roundoff_keeps_labels_and_schema_exact(self):
        row = {"sequenceId": "110-healthy", "simulatedSeconds": 0,
               "anomalyScore": -0.05002669199979759, "isolationForest": False, "threshold": False}
        verify_prediction_values([{**row, "anomalyScore": -0.05002669199979748}], [row])
        for changed in ({**row, "isolationForest": True}, {**row, "threshold": 0},
                        {**row, "simulatedSeconds": 0.0}, {**row, "extra": None}):
            with self.subTest(changed=changed), self.assertRaises(AssertionError):
                verify_prediction_values([changed], [row])
        with self.assertRaises(AssertionError):
            verify_prediction_values([], [row])

    def test_score_tolerance_rejects_substantive_changes_and_nonfinite_scores(self):
        row = {"anomalyScore": 0.1, "isolationForest": True}
        for score in (0.1 + 1e-12, float("nan"), float("inf"), True):
            with self.subTest(score=score), self.assertRaises(AssertionError):
                verify_prediction_values([{**row, "anomalyScore": score}], [row])

    def test_replay_values_ignore_only_serialization_newlines(self):
        with tempfile.TemporaryDirectory() as directory:
            lf, crlf = Path(directory) / "lf.jsonl", Path(directory) / "crlf.jsonl"
            lf.write_bytes(b'{"score":0.2}\n{"score":-0.1}\n')
            crlf.write_bytes(b'{"score":0.2}\r\n{"score":-0.1}\r\n')
            self.assertEqual(artifact_values(lf), artifact_values(crlf))
            self.assertNotEqual(sha256(lf), sha256(crlf))

    def test_corrupted_saved_evidence_is_rejected_before_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            file = root / "predictions.jsonl"
            file.write_bytes(b'{"anomalous":false}\n')
            metadata = {"hash": sha256(file)}
            verify_artifact_hashes(root, metadata, {file.name: "hash"})
            file.write_bytes(b'{"anomalous":true}\n')
            with self.assertRaisesRegex(AssertionError, "Saved evidence artifact changed"):
                verify_artifact_hashes(root, metadata, {file.name: "hash"})

    def test_source_hashes_survive_checkout_line_endings_but_evidence_is_byte_exact(self):
        with tempfile.TemporaryDirectory() as directory:
            lf, crlf = Path(directory) / "lf.py", Path(directory) / "crlf.py"
            lf.write_bytes(b"first\nsecond\n")
            crlf.write_bytes(b"first\r\nsecond\r\n")
            self.assertEqual(source_sha256(lf), source_sha256(crlf))
            self.assertNotEqual(sha256(lf), sha256(crlf))

    def test_confusion_matrix_and_false_alarm_denominator(self):
        result = classification_metrics([True, True, False, False, False], [True, False, True, False, False])
        self.assertEqual(result["precision"], .5)
        self.assertEqual(result["recall"], .5)
        self.assertAlmostEqual(result["falseAlarmRate"], 1 / 3)

    def test_seeded_fault_labels_include_ramp_and_misses_are_reported(self):
        config = json.loads((ROOT / "evaluation/config.json").read_text())["anomaly"]
        rows = generate_sequences(config)
        self.assertEqual(rows, generate_sequences(config))
        self.assertEqual(len(rows), 2700)
        self.assertEqual(sum(row["faultPresent"] for row in rows), 1200)
        self.assertTrue(all(row["seed"] != config["trainingSeed"] for row in rows))
        summary = summarize(rows, [False] * len(rows))
        self.assertEqual(summary["missedEpisodes"], 10)
        self.assertIsNone(summary["medianDetectionDelaySecondsAmongDetected"])


class RoutingEvaluationTests(unittest.TestCase):
    def setUp(self):
        self.graph = {
            "A1": {"neighbors": {"E": "A2", "S": "B1"}},
            "A2": {"neighbors": {"W": "A1", "S": "B2"}},
            "B1": {"neighbors": {"N": "A1", "E": "B2"}},
            "B2": {"neighbors": {"N": "A2", "W": "B1"}},
        }
        self.config = {"fleetStart": "A1", "fleetSize": 1, "handlingSeconds": 0, "hopSeconds": 4,
                       "horizonSeconds": 100, "bottleneck": {"startSeconds": 0, "endSeconds": 100, "additionalHopSeconds": 20, "edges": [["A1", "A2"]]}}
        self.workload = [{"id": "job", "arrivalSeconds": 0, "origin": "A1", "destination": "A2"}]

    def test_actual_weighted_planner_avoids_independently_slow_edge(self):
        weighted, _ = simulate(self.graph, self.workload, self.config, "weighted")
        baseline, _ = simulate(self.graph, self.workload, self.config, "bfs")
        self.assertEqual(weighted["jobs"][0]["deliverySeconds"], 12)
        self.assertEqual(baseline["jobs"][0]["deliverySeconds"], 24)
        self.assertEqual(weighted["jobs"][0]["hops"], 3)
        self.assertEqual(baseline["jobs"][0]["hops"], 1)

    def test_fixed_horizon_reports_unfinished_instead_of_silently_dropping(self):
        self.config["horizonSeconds"] = 5
        result, _ = simulate(self.graph, self.workload, self.config, "bfs")
        self.assertEqual(result["completed"], 0)
        self.assertEqual(result["unfinished"], 1)
        self.assertEqual(result["failed"], 0)
        self.assertIsNone(result["deliverySecondsAmongCompleted"]["median"])

    def test_unreachable_job_is_explicit_failure(self):
        self.graph["A1"]["neighbors"] = {}
        result, _ = simulate(self.graph, self.workload, self.config, "weighted")
        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["completed"] + result["failed"] + result["unfinished"], len(self.workload))


if __name__ == "__main__":
    unittest.main()

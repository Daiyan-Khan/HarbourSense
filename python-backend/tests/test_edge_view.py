import sys
import unittest
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

from edge_view import (  # noqa: E402
    ASSIGNMENT_COLLECTION,
    RUNTIME_COLLECTION,
    merge_edge_snapshot,
    split_edge_document,
)


class MergeEdgeSnapshotTests(unittest.TestCase):
    def test_runtime_only_idle_device(self):
        runtime = {
            "id": "crane001",
            "type": "crane",
            "currentLocation": "A1",
            "taskPhase": "idle",
            "task": "idle",
            "path": [],
            "progressToNext": 0,
            "stateRevision": 2,
        }
        merged = merge_edge_snapshot(None, runtime)
        self.assertEqual(merged["id"], "crane001")
        self.assertEqual(merged["taskPhase"], "idle")
        self.assertEqual(merged["stateRevision"], 2)
        self.assertIsNone(merged["shipmentId"])

    def test_merges_open_assignment(self):
        runtime = {
            "id": "truck_tempo_1",
            "type": "truck_tempo",
            "currentLocation": "B1",
            "taskPhase": "en_route_start",
            "path": ["B2", "C2"],
            "progressToNext": 40,
            "stateRevision": 5,
        }
        assignment = {
            "id": "truck_tempo_1",
            "shipmentId": "shipment_1",
            "assignedShipment": "shipment_1",
            "pendingPath": ["B1", "B2", "C2", "D1"],
            "routeRevision": 3,
            "assignmentEpoch": 2,
            "startNode": "B1",
            "finalNode": "D1",
            "task": {"shipmentId": "shipment_1", "phase": "transport"},
        }
        merged = merge_edge_snapshot(assignment, runtime)
        self.assertEqual(merged["shipmentId"], "shipment_1")
        self.assertEqual(merged["pendingPath"], ["B1", "B2", "C2", "D1"])
        self.assertEqual(merged["routeRevision"], 3)
        self.assertEqual(merged["progressToNext"], 40)

    def test_split_legacy_document(self):
        legacy = {
            "id": "robot001",
            "type": "robot",
            "currentLocation": "B4",
            "taskPhase": "assigned",
            "shipmentId": "shipment_9",
            "pendingPath": ["B4", "B4"],
            "routeRevision": 1,
            "assignmentEpoch": 1,
            "path": [],
            "progressToNext": 0,
        }
        assignment, runtime = split_edge_document(legacy)
        self.assertEqual(assignment["shipmentId"], "shipment_9")
        self.assertEqual(runtime["type"], "robot")
        self.assertEqual(runtime["taskPhase"], "assigned")
        self.assertNotIn("pendingPath", runtime)

    def test_collection_constants(self):
        self.assertEqual(ASSIGNMENT_COLLECTION, "edgeAssignments")
        self.assertEqual(RUNTIME_COLLECTION, "edgeRuntime")


if __name__ == "__main__":
    unittest.main()

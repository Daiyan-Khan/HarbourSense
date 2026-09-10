import sys
import types
import unittest
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))


def install_dependency_stubs():
    motor_module = types.ModuleType("motor")
    motor_asyncio_module = types.ModuleType("motor.motor_asyncio")
    motor_asyncio_module.AsyncIOMotorClient = object
    motor_module.motor_asyncio = motor_asyncio_module
    sys.modules.setdefault("motor", motor_module)
    sys.modules.setdefault("motor.motor_asyncio", motor_asyncio_module)

    bson_module = types.ModuleType("bson")

    class ObjectId:
        def __str__(self):
            return "stub-object-id"

    bson_module.ObjectId = ObjectId
    sys.modules.setdefault("bson", bson_module)

    aiomqtt_module = types.ModuleType("aiomqtt")
    aiomqtt_module.Client = object
    aiomqtt_module.MqttError = Exception
    sys.modules.setdefault("aiomqtt", aiomqtt_module)

    pymongo_module = types.ModuleType("pymongo")

    class ReturnDocument:
        AFTER = 1
        BEFORE = 0

    pymongo_module.ReturnDocument = ReturnDocument
    sys.modules.setdefault("pymongo", pymongo_module)


install_dependency_stubs()

import port_state


def shipment(shipment_id, status, assigned_edges=None, **extra):
    doc = {
        "id": shipment_id,
        "status": status,
        "assignedEdges": assigned_edges or [],
    }
    doc.update(extra)
    return doc


GRAPH_META = {
    "nodes": {
        "A1": {"type": "dock", "capacity": 8, "currentOccupancy": 2},
        "B4": {"type": "warehouse", "capacity": 35, "currentOccupancy": 10},
    }
}


class NextRequiredPhaseTests(unittest.TestCase):
    def test_arrived_needs_offload(self):
        self.assertEqual(port_state.next_required_phase(shipment("s1", "arrived")), "offload")

    def test_offloaded_needs_transport(self):
        self.assertEqual(port_state.next_required_phase(shipment("s2", "offloaded")), "transport")

    def test_storing_needs_store_load_after_move(self):
        edges = [{"edgeId": "r1", "phase": "store_move", "completedAt": "2026-01-01"}]
        self.assertEqual(port_state.next_required_phase(shipment("s3", "storing", edges)), "store_load")

    def test_delivered_has_no_next_phase(self):
        self.assertIsNone(port_state.next_required_phase(shipment("s4", "delivered")))


class DiagnoseShipmentBlockerTests(unittest.TestCase):
    def test_no_idle_crane_at_dock(self):
        diag = port_state.diagnose_shipment_blocker(
            shipment("s1", "arrived", currentNode="A1"),
            edges=[{"id": "crane1", "type": "crane", "taskPhase": "assigned"}],
            graph_meta=GRAPH_META,
        )
        self.assertEqual(diag["blockerCode"], "NO_IDLE_DEVICE")
        self.assertEqual(diag["nextPhase"], "offload")
        self.assertEqual(diag["idleOfType"], 0)

    def test_at_dock_with_idle_crane(self):
        diag = port_state.diagnose_shipment_blocker(
            shipment("s1", "arrived", currentNode="A1"),
            edges=[{"id": "crane1", "type": "crane", "taskPhase": "idle"}],
            graph_meta=GRAPH_META,
        )
        self.assertEqual(diag["blockerCode"], "AT_DOCK_OK")
        self.assertEqual(diag["idleOfType"], 1)

    def test_awaiting_completion_when_assignment_incomplete(self):
        assigned_edges = [{"edgeId": "crane1", "phase": "offload", "completedAt": None}]
        diag = port_state.diagnose_shipment_blocker(
            shipment("s1", "arrived", assigned_edges, currentNode="A1"),
            edges=[{"id": "crane1", "type": "crane", "taskPhase": "assigned"}],
            graph_meta=GRAPH_META,
        )
        self.assertEqual(diag["blockerCode"], "AWAITING_COMPLETION")

    def test_stale_inflight_when_edge_idle_with_open_assignment(self):
        assigned_edges = [{"edgeId": "truck1", "phase": "transport", "completedAt": None}]
        diag = port_state.diagnose_shipment_blocker(
            shipment("s1", "offloaded", assigned_edges, currentNode="A1", destination="B4"),
            edges=[{
                "id": "truck1",
                "type": "truck_tempo",
                "taskPhase": "idle",
                "shipmentId": "s1",
                "pendingPath": ["B1", "A1", "B4"],
                "path": [],
                "nextNode": None,
            }],
            graph_meta=GRAPH_META,
        )
        self.assertEqual(diag["blockerCode"], "STALE_INFLIGHT")
        self.assertEqual(diag["pendingPathLen"], 3)
        self.assertEqual(diag["pathLen"], 0)

    def test_warehouse_full_blocks_transport(self):
        full_graph = {
            "nodes": {
                "A1": {"type": "dock"},
                "B4": {"type": "warehouse", "capacity": 10, "currentOccupancy": 9},
            }
        }
        diag = port_state.diagnose_shipment_blocker(
            shipment("s1", "offloaded", currentNode="A1", destination="B4"),
            edges=[{"id": "truck1", "type": "truck_tempo", "taskPhase": "idle"}],
            graph_meta=full_graph,
        )
        self.assertEqual(diag["blockerCode"], "WAREHOUSE_FULL")
        self.assertEqual(diag["nextPhase"], "transport")

    def test_stale_no_idle_diagnostic_ignored_when_trucks_available(self):
        diag = port_state.diagnose_shipment_blocker(
            shipment(
                "s2",
                "offloaded",
                currentNode="A1",
                destination="B4",
                workflowDiagnostic={
                    "blockerCode": "NO_IDLE_DEVICE",
                    "blockerMessage": "No idle truck_tempo near A1 (0 idle truck_tempo)",
                },
            ),
            edges=[
                {"id": "truck1", "type": "truck_tempo", "taskPhase": "idle"},
                {"id": "truck2", "type": "truck_tempo", "taskPhase": "idle"},
            ],
            graph_meta=GRAPH_META,
        )
        self.assertEqual(diag["idleOfType"], 2)
        self.assertNotIn("0 idle truck_tempo", diag["blockerMessage"])
        self.assertEqual(diag["blockerCode"], "WAITING_ASSIGNMENT")


class DiagnoseIdleEdgeTests(unittest.TestCase):
    def test_idle_crane_with_pending_offload(self):
        pending_counts = {"pending_arrived": 1}
        diag = port_state.diagnose_idle_edge(
            {"id": "crane1", "type": "crane", "taskPhase": "idle", "currentLocation": "A1"},
            {"s1": shipment("s1", "arrived", currentNode="A1")},
            pending_counts=pending_counts,
        )
        self.assertEqual(diag["idleCode"], "WAITING_MONITOR")

    def test_stale_path_on_idle_edge(self):
        diag = port_state.diagnose_idle_edge(
            {
                "id": "crane1",
                "type": "crane",
                "taskPhase": "idle",
                "path": ["A1", "A2"],
            },
            {},
        )
        self.assertEqual(diag["idleCode"], "STALE_PATH_CLEARED")

    def test_completion_pending_for_assigned_edge(self):
        shipments_by_id = {
            "s1": shipment(
                "s1",
                "arrived",
                [{"edgeId": "crane1", "phase": "offload", "completedAt": None}],
            )
        }
        diag = port_state.diagnose_idle_edge(
            {"id": "crane1", "type": "crane", "taskPhase": "assigned", "shipmentId": "s1"},
            shipments_by_id,
        )
        self.assertEqual(diag["idleCode"], "COMPLETION_PENDING")
        self.assertEqual(diag["shipmentId"], "s1")

    def test_stale_idle_edge_shows_waiting_monitor_not_completion_pending(self):
        shipments_by_id = {
            "s1": shipment(
                "s1",
                "transported",
                [{"edgeId": "truck1", "phase": "transport", "completedAt": None}],
            )
        }
        pending_counts = {"pending_transported": 2}
        diag = port_state.diagnose_idle_edge(
            {
                "id": "robot001",
                "type": "robot",
                "taskPhase": "idle",
                "currentLocation": "E5",
            },
            shipments_by_id,
            pending_counts=pending_counts,
        )
        self.assertEqual(diag["idleCode"], "WAITING_MONITOR")

    def test_store_move_not_blocked_when_warehouse_over_capacity(self):
        over_cap_graph = {
            "nodes": {
                "D2": {"type": "warehouse", "capacity": 25, "currentOccupancy": 63},
            }
        }
        diag = port_state.diagnose_shipment_blocker(
            shipment("s1", "transported", destination="D2", storeQueued=True),
            edges=[{"id": "robot001", "type": "robot", "taskPhase": "idle", "currentLocation": "E5"}],
            graph_meta=over_cap_graph,
        )
        self.assertNotEqual(diag["blockerCode"], "WAREHOUSE_FULL")
        self.assertEqual(diag["nextPhase"], "store_move")

    def test_idle_edge_detects_empty_path_stall(self):
        diag = port_state.diagnose_idle_edge(
            {
                "id": "truck_tempo_1",
                "type": "truck_tempo",
                "taskPhase": "en_route_start",
                "currentLocation": "B4",
                "finalNode": "B4",
                "shipmentId": "s1",
                "path": [],
                "pendingPath": [],
                "workflowLeg": "toPickup",
            },
            {},
        )
        self.assertEqual(diag["idleCode"], "EMPTY_PATH_STALL")
        self.assertEqual(diag["workflowLeg"], "toPickup")

    def test_idle_edge_detects_explicit_stall_code(self):
        diag = port_state.diagnose_idle_edge(
            {
                "id": "truck_tempo_1",
                "type": "truck_tempo",
                "taskPhase": "en_route_start",
                "currentLocation": "B4",
                "finalNode": "A1",
                "stallCode": "PICKUP_LEG_MISSING",
                "workflowLeg": "toPickup",
            },
            {},
        )
        self.assertEqual(diag["idleCode"], "PICKUP_LEG_MISSING")
        self.assertEqual(diag["workflowLeg"], "toPickup")

    def test_idle_edge_detects_mqtt_task_never_applied(self):
        diag = port_state.diagnose_idle_edge(
            {
                "id": "robot001",
                "type": "robot",
                "taskPhase": "idle",
                "shipmentId": "s1",
                "task": {"phase": "store_move", "shipmentId": "s1"},
                "path": [],
                "pendingPath": [],
            },
            {},
        )
        self.assertEqual(diag["idleCode"], "MQTT_TASK_NEVER_APPLIED")

    def test_idle_edge_prefers_active_shipment_over_stale_assignment(self):
        shipments_by_id = {
            "shipment_26": shipment(
                "shipment_26",
                "storing",
                [{"edgeId": "forklift_001", "phase": "store_load", "assignedAt": "2026-01-01T10:00:00"}],
            ),
            "shipment_4": shipment(
                "shipment_4",
                "storing",
                [{"edgeId": "forklift_001", "phase": "store_load", "assignedAt": "2026-06-14T10:19:00"}],
            ),
        }
        diag = port_state.diagnose_idle_edge(
            {
                "id": "forklift_001",
                "type": "forklift",
                "taskPhase": "assigned",
                "currentLocation": "B4",
                "shipmentId": "shipment_4",
                "assignedShipment": "shipment_4",
            },
            shipments_by_id,
        )
        self.assertEqual(diag["idleCode"], "COMPLETION_PENDING")
        self.assertEqual(diag["shipmentId"], "shipment_4")
        self.assertNotIn("shipment_26", diag["idleMessage"])

    def test_idle_edge_stale_ack_only_for_orphaned_not_newest_assignment(self):
        shipments_by_id = {
            "shipment_26": shipment(
                "shipment_26",
                "storing",
                [{"edgeId": "forklift_001", "phase": "store_load", "assignedAt": "2026-01-01T10:00:00"}],
            ),
            "shipment_4": shipment(
                "shipment_4",
                "storing",
                [{"edgeId": "forklift_001", "phase": "store_load", "assignedAt": "2026-06-14T10:19:00"}],
            ),
        }
        diag = port_state.diagnose_idle_edge(
            {
                "id": "forklift_001",
                "type": "forklift",
                "taskPhase": "idle",
                "currentLocation": "B4",
            },
            shipments_by_id,
        )
        self.assertEqual(diag["idleCode"], "STALE_COMPLETION_ACK")
        self.assertEqual(diag["shipmentId"], "shipment_26")
        self.assertNotIn("shipment_4", diag["idleMessage"])


class NoValidPathDiagnosticTests(unittest.TestCase):
    def test_storing_shows_store_load_blocker_not_stale_store_move_assignment(self):
        assigned_edges = [{"edgeId": "robot001", "phase": "store_move", "completedAt": "2026-01-01"}]
        diag = port_state.diagnose_shipment_blocker(
            shipment(
                "shipment_24",
                "storing",
                assigned_edges,
                destination="E5",
                workflowDiagnostic={
                    "nextPhase": "store_move",
                    "blockerCode": "ASSIGNED",
                    "blockerMessage": "Assigned robot001 for store_move",
                },
            ),
            edges=[{"id": "forklift_001", "type": "forklift", "taskPhase": "idle", "currentLocation": "E5"}],
            graph_meta={
                "nodes": {
                    "E5": {"type": "warehouse", "capacity": 35, "currentOccupancy": 10},
                }
            },
        )
        self.assertEqual(diag["nextPhase"], "store_load")
        self.assertNotIn("store_move", diag["blockerMessage"])
        self.assertIn("store_load", diag["blockerMessage"])

    def test_no_valid_path_blocker_message(self):
        diag = port_state.diagnose_shipment_blocker(
            shipment(
                "s1",
                "offloaded",
                currentNode="A1",
                destination="B4",
                workflowDiagnostic={
                    "blockerCode": "NO_VALID_PATH",
                    "blockerMessage": "No graph-adjacent path for truck_tempo (1 idle checked)",
                },
            ),
            edges=[{"id": "truck1", "type": "truck_tempo", "taskPhase": "idle"}],
            graph_meta=GRAPH_META,
        )
        self.assertEqual(diag["blockerCode"], "NO_VALID_PATH")


if __name__ == "__main__":
    unittest.main()

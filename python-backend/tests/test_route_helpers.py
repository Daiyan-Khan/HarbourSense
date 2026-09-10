import unittest
from unittest.mock import AsyncMock, MagicMock

from route_helpers import edge_is_in_transit, next_route_revision, publish_route_command


class RouteHelpersTests(unittest.IsolatedAsyncioTestCase):
    def test_edge_is_in_transit_by_progress(self):
        self.assertTrue(edge_is_in_transit({"progressToNext": 50}))
        self.assertFalse(edge_is_in_transit({"progressToNext": 0}))
        self.assertFalse(edge_is_in_transit({"progressToNext": 100}))

    def test_edge_is_in_transit_ignores_active_hop_at_boundary(self):
        self.assertFalse(edge_is_in_transit({"progressToNext": 0, "activeHop": {"from": "B1", "to": "B2"}}))

    def test_next_route_revision(self):
        self.assertEqual(next_route_revision({"routeRevision": 3}), 4)
        self.assertEqual(next_route_revision({}), 1)

    async def test_publish_route_command(self):
        mqtt = AsyncMock()
        ok = await publish_route_command(mqtt, "truck_1", ["B1", "B2"], 2, eta=1.5)
        self.assertTrue(ok)
        mqtt.publish.assert_awaited_once()
        topic = mqtt.publish.await_args.args[0]
        self.assertEqual(topic, "harboursense/edge/truck_1/route")


if __name__ == "__main__":
    unittest.main()

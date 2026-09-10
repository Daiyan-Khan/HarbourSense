import asyncio
import unittest

from edge_command_queue import enqueue_edge_work, reset_edge_queues_for_tests


class EdgeCommandQueueTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        reset_edge_queues_for_tests()

    async def test_serializes_per_edge(self):
        order = []

        async def first():
            order.append("first-start")
            await asyncio.sleep(0.02)
            order.append("first-end")

        async def second():
            order.append("second")

        await asyncio.gather(
            enqueue_edge_work("edge_a", first),
            enqueue_edge_work("edge_a", second),
        )
        self.assertEqual(order, ["first-start", "first-end", "second"])

    async def test_parallel_different_edges(self):
        order = []

        async def work(label, delay):
            order.append(f"{label}-start")
            await asyncio.sleep(delay)
            order.append(f"{label}-end")

        await asyncio.gather(
            enqueue_edge_work("edge_a", lambda: work("a", 0.03)),
            enqueue_edge_work("edge_b", lambda: work("b", 0.01)),
        )
        self.assertIn("b-end", order)
        self.assertIn("a-end", order)


if __name__ == "__main__":
    unittest.main()

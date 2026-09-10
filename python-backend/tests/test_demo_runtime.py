import asyncio
import copy
import sys
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from demo_runtime import DemoContext, DemoRunEnded, RunDatabase, logical_ms, validate_demo_settings


class Collection:
    def __init__(self, document=None):
        self.document = document
        self.calls = []

    async def find_one(self, query):
        return copy.deepcopy(self.document)

    async def update_one(self, query, update, **kwargs):
        if self.document is None or any(self.document.get(k) != v for k, v in query.items()):
            return types.SimpleNamespace(modified_count=0)
        for key, value in update.get('$set', {}).items():
            self.document[key] = value
        for key, value in update.get('$inc', {}).items():
            self.document[key] = self.document.get(key, 0) + value
        return types.SimpleNamespace(modified_count=1)

    async def insert_one(self, value):
        self.calls.append(value)


class Database:
    name = 'harboursense_demo_test_unit'
    client = None

    def __init__(self, state):
        self.collections = {'_demoControl': Collection(state)}

    def __getitem__(self, name):
        return self.collections.setdefault(name, Collection())


class DemoSafetyTests(unittest.TestCase):
    def settings(self, uri='mongodb://127.0.0.1:27018/harboursense_demo', name='harboursense_demo'):
        return types.SimpleNamespace(uri=uri, database_name=name)

    def test_refuses_cloud_or_shared_database_even_when_demo_flag_is_true(self):
        for settings in [self.settings('mongodb+srv://cluster.example/harboursense_demo'),
                         self.settings('mongodb://remote.example/harboursense_demo'),
                         self.settings('mongodb://localhost/port', 'port'),
                         self.settings('mongodb://username:password@localhost/harboursense_demo')]:
            with self.assertRaises(ValueError):
                validate_demo_settings(settings, {'DEMO_MODE': 'true'})

    def test_requires_explicit_demo_and_local_mqtt(self):
        with self.assertRaises(ValueError):
            validate_demo_settings(self.settings(), {})
        with self.assertRaises(ValueError):
            validate_demo_settings(self.settings(), {'DEMO_MODE': 'true', 'MQTT_MODE': 'aws'})
        validate_demo_settings(self.settings(), {'DEMO_MODE': 'true'})

    def test_logical_clock_pause_and_speed_change_are_continuous(self):
        state = {'status': 'running', 'speed': 1, 'clock': {'baseMs': 500, 'anchorWallMs': 1000}}
        self.assertEqual(logical_ms(state, 1500), 1000)
        state.update(status='paused', clock={'baseMs': 1000, 'anchorWallMs': 1500})
        self.assertEqual(logical_ms(state, 100000), 1000)
        state.update(status='running', speed=4, clock={'baseMs': 1000, 'anchorWallMs': 100000})
        self.assertEqual(logical_ms(state, 100000), 1000)
        self.assertEqual(logical_ms(state, 100250), 2000)


class DemoFencingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.run_id = 'a' * 32
        self.base = Database({'_id': 'active', 'runId': self.run_id, 'status': 'running', 'activeOperations': 0})
        self.context = DemoContext(self.base, self.run_id)

    async def test_old_run_write_is_rejected_after_reset(self):
        self.base['_demoControl'].document['runId'] = 'b' * 32
        with self.assertRaises(DemoRunEnded):
            await self.context.db.shipments.insert_one({'id': 'old'})
        self.assertEqual(self.base[f'run_{self.run_id}_shipments'].calls, [])

    async def test_pause_blocks_writes_until_resume(self):
        self.base['_demoControl'].document['status'] = 'paused'
        task = asyncio.create_task(self.context.db.shipments.insert_one({'id': 'one'}))
        await asyncio.sleep(.08)
        self.assertFalse(task.done())
        self.base['_demoControl'].document['status'] = 'running'
        await asyncio.wait_for(task, .3)
        self.assertEqual(self.base['_demoControl'].document['activeOperations'], 0)

    async def test_operation_counter_is_released_when_cancelled(self):
        entered = asyncio.Event()
        async def operation():
            async with self.context.operation():
                entered.set()
                await asyncio.Event().wait()
        task = asyncio.create_task(operation())
        await entered.wait()
        self.assertEqual(self.base['_demoControl'].document['activeOperations'], 1)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(self.base['_demoControl'].document['activeOperations'], 0)

    def test_namespaces_never_alias_existing_live_collections(self):
        first = RunDatabase(self.base, self.context, guarded=False)
        second = RunDatabase(self.base, DemoContext(self.base, 'b' * 32), guarded=False)
        self.assertIsNot(first.graph, second.graph)
        self.assertIsNot(first.graph, self.base['graph'])


if __name__ == '__main__':
    unittest.main()

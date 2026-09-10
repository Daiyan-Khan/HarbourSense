import asyncio
import copy
import sys
import time
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from demo_service import DemoService


class Control:
    def __init__(self, state=None):
        self.state = state
        self.inserted = []

    async def find_one(self, query):
        return copy.deepcopy(self.state)

    async def update_one(self, query, update, **kwargs):
        self.state.update(update.get('$set', {}))
        for key, value in update.get('$inc', {}).items():
            self.state[key] = self.state.get(key, 0) + value
        return types.SimpleNamespace(modified_count=1)

    async def insert_one(self, document):
        self.inserted.append(document)


class DemoRecordingOrderingTests(unittest.IsolatedAsyncioTestCase):
    async def test_pause_acknowledgement_waits_for_pending_capture(self):
        state = {'_id': 'active', 'runId': 'a' * 32, 'scenarioId': 'normal',
                 'status': 'running', 'speed': 1, 'sequence': 0,
                 'clock': {'baseMs': 0, 'anchorWallMs': time.time() * 1000}}
        control = Control(state)
        service = DemoService.__new__(DemoService)
        service.base = {'_demoControl': control, '_demoCommands': Control()}
        service.lock = asyncio.Lock()
        service.specs = [{'id': 'normal', 'shipments': [{'id': 'one'}], 'timeoutMs': 100000}]
        service.state = AsyncMock(side_effect=lambda: copy.deepcopy(control.state))
        service.wait_quiescent = AsyncMock()
        started = asyncio.Event()
        release = asyncio.Event()
        captured_states = []

        async def record(current):
            started.set()
            await release.wait()
            captured_states.append(copy.deepcopy(current))
            return {'shipments': [], 'edges': []}

        service.record_snapshot = record
        sampler = asyncio.create_task(service.sample_loop())
        pause = None
        try:
            await asyncio.wait_for(started.wait(), 1)
            pause = asyncio.create_task(service.command('pause', {'commandId': 'pause-ordering-1'}))
            await asyncio.sleep(.03)
            self.assertFalse(pause.done(), 'A pause acknowledgement must not overtake an in-flight capture')
            release.set()
            response = await asyncio.wait_for(pause, 1)
            self.assertTrue(response['acknowledged'])
            self.assertEqual(response['state']['status'], 'paused')
            self.assertEqual(captured_states[0]['status'], 'running')
            self.assertEqual(len(service.base['_demoCommands'].inserted), 1)
        finally:
            release.set()
            sampler.cancel()
            if pause:
                pause.cancel()
            await asyncio.gather(sampler, *([pause] if pause else []), return_exceptions=True)


if __name__ == '__main__':
    unittest.main()

import importlib
import sys
import unittest
from datetime import datetime
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
  sys.path.insert(0, str(BACKEND_ROOT))

# Other tests register a stub sensor_analyzer in sys.modules; load the real module here.
if 'sensor_analyzer' in sys.modules and not hasattr(sys.modules['sensor_analyzer'], '_coerce_numeric_reading'):
  del sys.modules['sensor_analyzer']

try:
  sensor_analyzer = importlib.import_module('sensor_analyzer')
  SensorAnalyzer = sensor_analyzer.SensorAnalyzer
  SENSOR_ANALYZER_AVAILABLE = hasattr(SensorAnalyzer, '_coerce_numeric_reading')
except ModuleNotFoundError:
  sensor_analyzer = None
  SensorAnalyzer = None
  SENSOR_ANALYZER_AVAILABLE = False

class InMemoryCursor:
  def __init__(self, docs):
    self._docs = list(docs)
    self._index = 0

  def sort(self, key, direction):
    reverse = direction == -1
    self._docs.sort(key=lambda d: d.get('timestamp'), reverse=reverse)
    return self

  def limit(self, n):
    self._docs = self._docs[:n]
    return self

  def __aiter__(self):
    return self

  async def __anext__(self):
    if self._index >= len(self._docs):
      raise StopAsyncIteration
    doc = self._docs[self._index]
    self._index += 1
    return doc


class InMemoryCollection:
  def __init__(self):
    self.docs = []

  def find(self, query=None):
    return InMemoryCursor(self.docs)

  async def insert_one(self, doc):
    self.docs.append(doc)


class SensorAnalyzerMotionTest(unittest.IsolatedAsyncioTestCase):
  @unittest.skipUnless(SENSOR_ANALYZER_AVAILABLE, 'sensor_analyzer dependencies not installed')
  async def test_motion_string_reading_does_not_crash(self):
    db = type('DB', (), {})()
    db.sensorData = InMemoryCollection()
    db.sensorAlerts = InMemoryCollection()
    analyzer = SensorAnalyzer(db)

    result = await analyzer.detect_anomaly({
      'id': 'motion_1',
      'type': 'motion',
      'node': 'A1',
      'reading': 'detected',
      'timestamp': datetime.now(),
    })
    self.assertIsNone(result)

  @unittest.skipUnless(SENSOR_ANALYZER_AVAILABLE, 'sensor_analyzer dependencies not installed')
  async def test_motion_numeric_reading_accepted(self):
    db = type('DB', (), {})()
    db.sensorData = InMemoryCollection()
    db.sensorAlerts = InMemoryCollection()
    analyzer = SensorAnalyzer(db)

    result = await analyzer.detect_anomaly({
      'id': 'motion_2',
      'type': 'motion',
      'node': 'A1',
      'reading': 1,
      'timestamp': datetime.now(),
    })
    self.assertIsNone(result)

  @unittest.skipUnless(SENSOR_ANALYZER_AVAILABLE, 'sensor_analyzer dependencies not installed')
  def test_coerce_numeric_reading_rejects_legacy_motion_strings(self):
    self.assertIsNone(SensorAnalyzer._coerce_numeric_reading('detected', 'motion'))
    self.assertEqual(SensorAnalyzer._coerce_numeric_reading(1, 'motion'), 1.0)


if __name__ == '__main__':
  unittest.main()

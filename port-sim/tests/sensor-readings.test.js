const test = require('node:test');
const assert = require('node:assert/strict');

const { generateReading, generateSpike, isNumericReadingType } = require('../lib/sensor-readings');

test('motion readings are numeric 0 or 1', () => {
  for (let i = 0; i < 20; i += 1) {
    const reading = generateReading('motion');
    assert.ok(reading === 0 || reading === 1);
    assert.equal(typeof reading, 'number');
  }
});

test('vibration and temperature readings are numbers', () => {
  assert.equal(typeof generateReading('vibration'), 'number');
  assert.equal(typeof generateReading('temperature'), 'number');
});

test('generateSpike returns numeric motion spike as 1', () => {
  assert.equal(generateSpike('motion', 0), 1);
});

test('isNumericReadingType includes motion', () => {
  assert.ok(isNumericReadingType('motion'));
  assert.ok(isNumericReadingType('temperature'));
});

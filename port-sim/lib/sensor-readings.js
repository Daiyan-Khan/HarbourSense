function generateReading(type) {
  switch (type) {
    case 'temperature':
      return Number((Math.random() * 40 - 10).toFixed(2));
    case 'humidity':
      return Number((Math.random() * 100).toFixed(2));
    case 'vibration':
      return Number((Math.random() * 10).toFixed(2));
    case 'occupancy':
      return Math.floor(Math.random() * 101);
    case 'motion':
      return Math.random() < 0.5 ? 0 : 1;
    default:
      return Number((Math.random() * 100).toFixed(2));
  }
}

function generateSpike(type, baseReading) {
  switch (type) {
    case 'temperature':
      return Number((Number(baseReading) + Math.random() * 20 + 10).toFixed(2));
    case 'humidity':
      return Number((Number(baseReading) + Math.random() * 50).toFixed(2));
    case 'vibration':
      return Number((Number(baseReading) + Math.random() * 15 + 5).toFixed(2));
    case 'occupancy':
      return 100;
    case 'motion':
      return 1;
    default:
      return Number((Number(baseReading) * 2).toFixed(2));
  }
}

function isNumericReadingType(type) {
  return ['temperature', 'humidity', 'vibration', 'occupancy', 'motion'].includes(type);
}

module.exports = {
  generateReading,
  generateSpike,
  isNumericReadingType,
};

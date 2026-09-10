/** Monotonic shipment lifecycle rank (shared with backend task_assigner.py). */

const SHIPMENT_STATUS_RANK = Object.freeze({
  arrived: 0,
  offloaded: 1,
  transported: 2,
  transporting: 2,
  storing: 3,
  stored: 4,
  delivered: 5,
});

function shipmentStatusRank(status) {
  if (!status) return 0;
  return SHIPMENT_STATUS_RANK[status] ?? 0;
}

function maxShipmentStatus(current, incoming) {
  const currentRank = shipmentStatusRank(current);
  const incomingRank = shipmentStatusRank(incoming);
  if (incomingRank >= currentRank) {
    return incoming || current;
  }
  return current || incoming;
}

module.exports = {
  SHIPMENT_STATUS_RANK,
  shipmentStatusRank,
  maxShipmentStatus,
};

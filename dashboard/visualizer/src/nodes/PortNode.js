import React, { memo } from 'react';
import { Handle, Position } from 'reactflow';

function getPortTypeClass(nodeType) {
  const t = (nodeType || '').toLowerCase();
  if (t.includes('dock')) return 'port-node--dock';
  if (t.includes('warehouse')) return 'port-node--warehouse';
  if (t.includes('berth')) return 'port-node--berth';
  if (t.includes('gate')) return 'port-node--gate';
  if (t.includes('route') || t.includes('road')) return 'port-node--road';
  return 'port-node--default';
}

function PortNode({ data }) {
  const nodeType = data?.nodeType || 'unknown';
  const label = data?.label || '';
  const typeLabel = ({ route_point: 'Road junction', control_office: 'Control centre', exit_gate: 'Exit gate', dock: 'Dock', berth: 'Berth', warehouse: 'Warehouse' })[nodeType] || nodeType.replace(/_/g, ' ');

  return (
    <div className={`port-node ${getPortTypeClass(nodeType)}`}>
      <Handle type="target" position={Position.Top} style={{ opacity: 0 }} />
      <Handle type="source" position={Position.Bottom} style={{ opacity: 0 }} />
      <div className="port-node__label">{label}</div>
      <div className="port-node__type">{typeLabel}</div>
    </div>
  );
}

export default memo(PortNode);

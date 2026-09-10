import React, { memo } from 'react';

export function getDeviceTypeSvg(type) {
  const t = (type || '').toLowerCase();

  if (t.includes('truck')) {
    return (
      <svg viewBox="0 0 24 24" className="device-marker__svg" aria-hidden="true">
        <rect x="2" y="9" width="13" height="8" rx="1.5" fill="currentColor" />
        <path d="M15 11h3l3 4v2h-6v-6z" fill="currentColor" />
        <circle cx="7" cy="18" r="2" fill="#fff" />
        <circle cx="18" cy="18" r="2" fill="#fff" />
      </svg>
    );
  }

  if (t.includes('crane')) {
    return (
      <svg viewBox="0 0 24 24" className="device-marker__svg" aria-hidden="true">
        <path d="M4 20h16" stroke="currentColor" strokeWidth="2" />
        <path d="M6 20V8l10-4v16" stroke="currentColor" strokeWidth="2" fill="none" />
        <path d="M16 4h4v3h-4z" fill="currentColor" />
        <path d="M6 12h6" stroke="currentColor" strokeWidth="2" />
      </svg>
    );
  }

  if (t.includes('robot') || t.includes('agv')) {
    return (
      <svg viewBox="0 0 24 24" className="device-marker__svg" aria-hidden="true">
        <rect x="5" y="8" width="14" height="10" rx="3" fill="currentColor" />
        <circle cx="9" cy="13" r="1.5" fill="#fff" />
        <circle cx="15" cy="13" r="1.5" fill="#fff" />
        <path d="M12 5v3" stroke="currentColor" strokeWidth="2" />
        <circle cx="12" cy="4" r="1.5" fill="currentColor" />
      </svg>
    );
  }

  if (t.includes('forklift')) {
    return (
      <svg viewBox="0 0 24 24" className="device-marker__svg" aria-hidden="true">
        <rect x="4" y="10" width="10" height="8" rx="1" fill="currentColor" />
        <path d="M14 12h4v6h-2v-4h-2z" fill="currentColor" />
        <circle cx="7" cy="19" r="2" fill="#fff" />
        <circle cx="15" cy="19" r="2" fill="#fff" />
      </svg>
    );
  }

  if (t.includes('conveyor')) {
    return (
      <svg viewBox="0 0 24 24" className="device-marker__svg" aria-hidden="true">
        <rect x="3" y="10" width="18" height="4" rx="1" fill="currentColor" />
        <circle cx="7" cy="12" r="1.5" fill="#fff" />
        <circle cx="12" cy="12" r="1.5" fill="#fff" />
        <circle cx="17" cy="12" r="1.5" fill="#fff" />
      </svg>
    );
  }

  return (
    <svg viewBox="0 0 24 24" className="device-marker__svg" aria-hidden="true">
      <circle cx="12" cy="12" r="7" fill="currentColor" />
      <circle cx="12" cy="12" r="3" fill="#fff" />
    </svg>
  );
}

function getDeviceTypeClass(type) {
  const t = (type || '').toLowerCase();
  if (t.includes('truck')) return 'device-marker--truck';
  if (t.includes('crane')) return 'device-marker--crane';
  if (t.includes('robot') || t.includes('agv')) return 'device-marker--robot';
  if (t.includes('forklift')) return 'device-marker--forklift';
  if (t.includes('conveyor')) return 'device-marker--conveyor';
  return 'device-marker--default';
}

function DeviceMarkerNode({ data }) {
  const device = data?.device;
  if (!device) return null;

  const {
    id,
    type,
    borderColor,
    isMoving,
    statusLabel,
    progressPct,
  } = device;

  const displayProgress = data?.displayProgress ?? progressPct ?? 0;
  const title = `${id} — ${statusLabel}${displayProgress > 0 ? ` (${Math.round(displayProgress)}%)` : ''}`;
  const progressStyle = displayProgress > 0
    ? { '--device-progress': `${displayProgress}%` }
    : undefined;

  const classNames = [
    'device-marker',
    getDeviceTypeClass(type),
    isMoving ? 'device-marker--moving' : '',
    displayProgress > 0 ? 'device-marker--has-progress' : '',
  ].filter(Boolean).join(' ');

  return (
    <div
      className={classNames}
      title={title}
      style={{
        borderColor,
        ...progressStyle,
      }}
      data-device-id={id}
    >
      <div className="device-progress-ring" aria-hidden="true" />
      <div className="device-marker__icon">
        {getDeviceTypeSvg(type)}
      </div>
    </div>
  );
}

export default memo(DeviceMarkerNode);

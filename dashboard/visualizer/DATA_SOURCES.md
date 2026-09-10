# Dashboard data sources

HarbourSense uses the same reducer, map and operation panels for two data adapters.

- **Local:** `npm start` uses the live HTTP/SSE adapter. `REACT_APP_API_BASE_URL` defaults to `http://localhost:8000`. The `/api/demo/state` response identifies an isolated local demo and enables its acknowledged controls. A server without that endpoint still supports read-only live operation.
- **Public recorded simulation:** build with `REACT_APP_DATA_SOURCE=replay` and set `PUBLIC_URL` to the hosting subpath, for example `/HarbourSense`. Only same-site `/replays/*.json` assets are fetched. The live adapter is never started; no API, EventSource, MQTT or database connection is attempted. Each browser owns its clock and playback state.

The public build intentionally reports unavailable recordings if its catalogue is absent. It does not manufacture a substitute scenario. The tiny fixtures in unit tests are test inputs, not published engineering evidence.

## Recording contract (version 1)

`public/replays/index.json` contains:

```json
{"schemaVersion":1,"scenarios":[{"id":"normal","title":"Normal operations","description":"…","file":"normal.json"}]}
```

Each file contains:

```text
schemaVersion: 1
scenario: { id, title, description }
recordedAt: ISO timestamp establishing the recorded clock
durationMs: positive playback duration
initialSnapshot:
  graph: { nodes: graph API response nodes }
  edges: device API response array
  shipments: shipment API response array
  sensors: sensor API response array
  craneTelemetry: crane telemetry API response array, including analysis
  maintenanceHistory: all recorded maintenance alerts, including resolved alerts
  maintenanceTasks: recorded repair assignments and completions
  sensorAlerts: sensor alert API response array
  maintenanceAlerts: maintenance alert API response array
  portState: diagnostics API response object
frames:
  - atMs: ordered time from the recording origin
    snapshot: full snapshot or changed top-level snapshot collections
    events: [{ id, atMs, type, message, deviceId?, shipmentId? }]
provenance: exporter metadata, scenario inputs, revision and capture environment
```

The adapter merges each frame over its previous snapshot. A present collection replaces that collection in full; it is not an individual-device patch. Events need stable IDs and times. Duplicate event IDs are coalesced and future events are hidden. Missing snapshot fields retain their previous value. Filenames are restricted to the replay directory, and schema, duration and frame ordering are validated before rendering.

Playback is sample-and-hold for recorded device positions: the dashboard does not invent intermediate task results. The recorded clock advances only while playing. Speed changes preserve the current position. Reset reconstructs the initial state, timeline and telemetry history; selecting another scenario discards any late download from the former selection. Historical sensor trends come from recorded samples and show unavailable data when the pipeline supplied no matching device readings. The UI does not forecast new arrivals in finite recorded/local demo scenarios.

## Health and metrics

Source and health are separate. Polling or an open SSE socket alone never means telemetry is fresh. Device telemetry is stale after 15 seconds based on its own timestamp; panels have a separate 30-second threshold. A missing telemetry timestamp is labeled **Awaiting telemetry**, and a paused simulation is labeled **Paused**. Graph fetches retry with backoff capped at 15 seconds. SSE reconnects with bounded backoff and reconciles HTTP snapshots even if a stream stays open without updates. Requests have bounded timeouts; pending requests from disposed/reset runs cannot apply to the new view.

Throughput is the count of delivered shipments in the current scenario (or loaded live history), not an invented per-minute rate. Active fleet counts assigned, moving and completing devices. Delays are unavailable unless the source defines a delay flag or marks a shipment delayed; a queue is not silently treated as a missed deadline. Critical alerts include unresolved high/critical severity records. Failed or not-yet-loaded metric sources show an em dash rather than a measured zero.

The device and shipment drawers trap keyboard focus, support Escape and restore the triggering control. Map nodes are keyboard-selectable through React Flow; all devices are also available in the searchable fleet. A dedicated **Fit full port** button resets the viewport. Reduced-motion preference disables animation and display interpolation. The layout supports narrow screens without horizontal page scrolling.

## Checks

```powershell
npm test -- --watchAll=false --runInBand
$env:REACT_APP_DATA_SOURCE = 'replay'
$env:PUBLIC_URL = '/HarbourSense'
npm run build
```

Adapter tests cover pause/speed/reset, visitor isolation, late downloads, no hidden live requests in the rendered replay, graph failure recovery, stream interruption and silent-stream reconciliation, stable control IDs, stale timestamps, missing metrics and drawer keyboard behavior. Repository-level browser tests verify the compiled recording assets and actual layout separately. A successful static build is not evidence that a public site has already been deployed.


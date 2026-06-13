# HarbourSense Startup Guide

This guide covers the local startup paths for the HarbourSense backend, port simulator, dashboard, Mosquitto broker, and edge analyzer.

## Prerequisites

- Docker Desktop with Docker Compose.
- Python 3.10 for the backend when running outside Docker.
- Python 3.9 or newer for the edge analyzer when running outside Docker.
- Node.js 18 and npm for the port simulator and dashboard when running outside Docker.
- A reachable MongoDB instance. Docker Compose starts a local `mongo` service by default; standalone service runs need MongoDB started separately or an external `MONGO_URI`.
- Optional AWS IoT certificates only if you intentionally run with `MQTT_MODE=aws`.

## Environment Setup

From the `HarbourSense/` directory, create a local environment file:

```powershell
Copy-Item .env.example .env
```

Then edit `.env` with local values. Keep placeholders in committed files and never commit live credentials.

Key local defaults:

- `MQTT_MODE=local` uses the Compose Mosquitto service.
- `MONGO_URI=mongodb://mongo:27017/port` uses the Compose MongoDB service from inside containers.
- `MONGO_HOST_PORT=27017` exposes the Compose MongoDB service on `localhost:27017` for local tools.
- `MONGO_SERVER_SELECTION_TIMEOUT_MS=5000` keeps backend MongoDB connection failures bounded during startup and API calls.
- `MQTT_BROKER_HOST=mqtt` is correct for Docker Compose services.
- `MQTT_BROKER_PORT=1883` exposes MQTT on the host.
- `MQTT_WEBSOCKET_PORT=9001` exposes Mosquitto WebSockets on the host.
- `AWS_IOT_PORT=8883` is only used when `MQTT_MODE=aws`.

To use Atlas or another external database, set `MONGO_URI` only in your uncommitted `.env`. Keep credentials out of committed files and verify the URI is reachable from Docker before starting the stack.

Use `MQTT_MODE=aws` only after setting a real AWS IoT endpoint locally and mounting valid certificate, key, and CA files. Do not copy those values into this README.

## Seed Local MongoDB

MongoDB starts empty unless you explicitly seed it. Run the seed once before starting `backend` or `portsim`:

```powershell
docker compose up -d mongo
docker compose --profile seed run --build --rm mongo-seed
```

The seed command loads `port-sim/initialise/test-graph.json` into `graph`, `port-sim/initialise/test-edge.json` into `edgeDevices`, and `port-sim/initialise/sensor.json` into `sensorList` using `MONGO_URI` and `MONGO_DB_NAME`. It upserts by `id`, prunes graph nodes that are not in the canonical 5x5 fixture, and clears `shipments` plus `edgeHistory` when `SEED_RESET_WORKFLOW=true` (Compose default for the seed profile). Re-run seed after changing retention tunables so TTL indexes are (re)applied.

Seed also ensures query indexes on `shipments`, `sensorAlerts`, and `maintenanceAlerts`, plus TTL indexes on time-series collections:

| Collection | TTL field | Default retention | Env override |
| --- | --- | --- | --- |
| `sensorData` | `timestamp` | 7 days | `MONGO_TTL_SENSOR_DATA_DAYS` |
| `trafficData` | `timestamp` | 14 days | `MONGO_TTL_TRAFFIC_DATA_DAYS` |
| `edgeHistory` | `timestamp` | 30 days | `MONGO_TTL_EDGE_HISTORY_DAYS` |

MongoDB removes documents when `timestamp + retention` is in the past. TTL deletion is asynchronous (typically within ~60 seconds of expiry). Set longer values in production `.env` before seeding if you need more history.

If your `.env` points `MONGO_URI` at Atlas or another external database, the same seed command will target that database. Only run it against external MongoDB when you intentionally want to seed that dev database.

## Start Everything With Docker Compose

Run these commands from `HarbourSense/`:

```powershell
docker compose up --build
```

To run in the background:

```powershell
docker compose up --build -d
```

Compose starts:

- `mongo`: MongoDB on port `27017`, persisted in the `mongo_data` volume.
- `mqtt`: Eclipse Mosquitto on ports `1883` and `9001`.
- `backend`: FastAPI on port `8000`; its entrypoint also starts `manager.py`.
- `portsim`: Node.js port simulator running `node port.js`.
- `dashboard`: React dashboard served on port `3000`.
- `edge-analyzer`: predictive-maintenance analyzer connected to MQTT.

For a clean local run after seeding, start the app stack with:

```powershell
docker compose up --build -d backend portsim dashboard
```

## Start Individual Services

These commands are useful for local development. Start MongoDB and an MQTT broker first, or use the Compose services.

### MongoDB

```powershell
docker compose up mongo
```

Inside Compose, services should use `mongodb://mongo:27017/port`. Host-side tools can use `mongodb://localhost:27017/port` while the Compose `mongo` service is running.

### MQTT Broker

```powershell
docker compose up mqtt
```

### Backend

From `HarbourSense/python-backend/`:

```powershell
python -m pip install -r requirements.txt
python manager.py
```

In a second terminal from the same directory:

```powershell
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### Port Simulator

From `HarbourSense/port-sim/`:

```powershell
npm install
$env:MONGO_URI="mongodb://localhost:27017/port"
$env:MQTT_MODE="local"
$env:MQTT_BROKER_HOST="localhost"
node port.js
```

`port.js` and `sensors.js` read `MONGO_URI`, `MONGO_DB_NAME`, `MQTT_MODE`, and broker settings from the environment. Inside Compose, defaults reach the internal `mongo` and `mqtt` services. Set `MQTT_MODE=aws` only with a real `AWS_IOT_ENDPOINT` and mounted certificate files.

Tunable port simulator environment variables (`port.js`):

- `SHIPMENT_INTERVALS_MS`: comma-separated shipment generation delays in milliseconds (default `30000,60000,90000`).
- `CRANE_TELEMETRY_INTERVAL_MS`: publish interval for crane telemetry on `harboursense/telemetry/crane/{craneId}/raw` (default `5000`).
- `SIM_PROGRESS_INTERVAL_MS`: movement progress MQTT publish cadence (default `1000`).
- `SIM_LOOP_IDLE_DELAY_MS`: edge loop sleep when idle (default `5000`).
- `SIM_LOOP_TICK_MS`: edge loop iteration delay (default `1000`).
- `SIM_COMPLETING_DELAY_MS`: pause after task execution in completing phase (default `2000`).
- `SIM_EDGE_MISSING_DELAY_MS`: retry delay when an edge record is missing (default `5000`).

Crane telemetry payloads include `craneId`, `motorTemp`, `vibration`, and `energyUse` for `edge-analyzer.py`. Idle cranes publish lower-range samples; active cranes publish upper-range samples within normal training bounds.

For local development outside Docker, `nodemon` is listed in `port-sim/package.json` for optional file-watch restarts (for example `npx nodemon port.js`). **Production and Compose always use `node port.js`**: the port-sim Dockerfile sets `CMD ["node", "port.js"]`, and `docker-compose.yml` overrides with the same command. `nodemon` is not installed or invoked in container images.

### Dashboard

From `HarbourSense/dashboard/visualizer/`:

```powershell
npm install
npm start
```

The dashboard development server runs on `http://localhost:3000`. Configure the backend host with `REACT_APP_API_BASE_URL` (default `http://localhost:8000`). The UI polls graph, edges, sensors, shipments, and alert panels every 3 seconds.

### Edge Analyzer

From `HarbourSense/edge-analyzer/`:

```powershell
python -m pip install -r requirements.txt
python create_model.py
$env:MQTT_BROKER_HOST="localhost"
$env:MONGO_URI="mongodb://localhost:27017/port"
python edge-analyzer.py
```

Run `python create_model.py` before starting the analyzer if `anomaly_model.pkl` is missing. The analyzer auto-generates `anomaly_model.pkl` on first startup when the file is absent. Set `MONGO_URI` to persist maintenance alerts into `maintenanceAlerts` for the dashboard read API; MQTT publish to `harboursense/alerts/maintenance` still occurs without MongoDB.

Tunable edge-analyzer environment variables:

- `MQTT_BROKER_HOST` / `MQTT_BROKER_PORT`: broker connection (Compose default `mqtt:1883`).
- `MQTT_RECONNECT_DELAY_SECONDS`: reconnect backoff after broker disconnect.
- `MONGO_URI` / `MONGO_DB_NAME`: optional persistence for maintenance alerts.
- `ANOMALY_MODEL_PATH`: override model artifact location.
- `LOG_LEVEL` / `EDGE_ANALYZER_LOG_LEVEL` (Compose): logging verbosity.

## Useful URLs And Ports

- Dashboard: `http://localhost:3000`
- Backend health: `http://localhost:8000/`
- Backend graph API: `http://localhost:8000/api/graph`
- Backend edge API: `http://localhost:8000/api/edges`
- Backend sensor API: `http://localhost:8000/api/sensors`
- Backend shipments API: `http://localhost:8000/api/shipments`
- Backend sensor alerts API: `http://localhost:8000/api/alerts/sensor`
- Backend maintenance alerts API: `http://localhost:8000/api/alerts/maintenance`
- MongoDB: `localhost:27017`
- MQTT: `localhost:1883`
- MQTT WebSockets: `localhost:9001`

## Validation And Smoke Checks

Before starting services:

```powershell
docker compose config
```

After starting Compose:

```powershell
docker compose ps
docker compose logs backend
docker compose logs portsim
```

Check the backend:

```powershell
Invoke-RestMethod http://localhost:8000/
Invoke-RestMethod http://localhost:8000/api/graph
Invoke-RestMethod http://localhost:8000/api/edges
Invoke-RestMethod http://localhost:8000/api/sensors
```

Check seeded MongoDB collections:

```powershell
docker compose exec mongo mongosh --quiet --eval "const db = db.getSiblingDB('port'); printjson({ collections: db.getCollectionNames().sort(), graph: db.graph.countDocuments(), edgeDevices: db.edgeDevices.countDocuments(), sensorList: db.sensorList.countDocuments() })"
```

Focused local checks:

```powershell
python -m unittest discover -s python-backend/tests -p "test_*.py"
```

```powershell
Set-Location port-sim
npm test
```

```powershell
node scripts/smoke-local.mjs
```

Optional shipment progression watch (requires running MongoDB and `MONGO_URI`):

```powershell
$env:MONGO_URI = "mongodb://localhost:27017/port"
node scripts/smoke-local.mjs --watch-shipment 120
```

Re-seed edge devices after updating `port-sim/initialise/test-edge.json` (for example when adding `forklift_001`):

```powershell
docker compose --profile seed run --build --rm mongo-seed
docker compose up -d --force-recreate backend portsim
```

```powershell
Set-Location dashboard/visualizer
$env:REACT_APP_API_BASE_URL = "http://localhost:8000"
npm test -- --watchAll=false
npm run build
```

Dashboard tests and builds require `npm install` first so that `react-scripts` exists locally.

## Common Troubleshooting

- Dashboard command fails with `react-scripts` not found: run `npm install` in `HarbourSense/dashboard/visualizer/`.
- Backend logs `Graph missing or empty in DB`: run `docker compose --profile seed run --build --rm mongo-seed`, then restart `backend`.
- Services cannot reach MongoDB: for Compose defaults, confirm `docker compose ps mongo` shows the local `mongo` service as healthy and `MONGO_URI` is `mongodb://mongo:27017/port`. For Atlas or another external database, confirm the URI in `.env` is reachable from Docker.
- Backend exits with an invalid MongoDB configuration message: replace empty or placeholder `MONGO_URI`/`MONGO_DB_NAME` values in `.env`; if using Atlas, verify the `mongodb+srv://` cluster hostname exists.
- Services report missing AWS IoT values: keep `MQTT_MODE=local` for Compose, or replace AWS values only in your local `.env` and ensure certificate paths are mounted.
- MQTT connection fails in Compose: confirm the `mqtt` service is healthy and `MQTT_BROKER_HOST=mqtt` is used inside containers.
- Edge analyzer exits because `anomaly_model.pkl` is missing: run `python create_model.py` in `HarbourSense/edge-analyzer/`, then restart the service or rebuild the image.
- Certificate or key files are present in local folders: treat them as sensitive. Do not print them in logs, copy them into docs, or commit new cert/key files.
- Port simulator logs `Cannot find module 'mongodb'`: rebuild and recreate the service so the entrypoint can resync dependencies into the named `node_modules` volume: `docker compose build portsim` then `docker compose up -d --force-recreate portsim`.
- Port simulator logs `querySrv ENOTFOUND` or Atlas DNS errors: confirm Compose is passing `MONGO_URI=mongodb://mongo:27017/port` and that `.env` does not override it with an unreachable Atlas URI.
- Legacy one-off scripts under `port-sim/initialise/` now read `MONGO_URI` through the shared runtime config, but they may delete and replace collection contents. Prefer `docker compose --profile seed run --build --rm mongo-seed` for local baseline data.

## Safe Shutdown

Stop foreground Compose with `Ctrl+C`.

For background Compose runs:

```powershell
docker compose down
```

To remove the MongoDB and Mosquitto named volumes as well:

```powershell
docker compose down -v
```

Use `docker compose down -v` only when you are comfortable deleting local database and broker data.

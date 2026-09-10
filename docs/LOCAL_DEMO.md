# Run and verify HarbourSense locally

The supported startup path is the root `npm run demo` command. It runs the complete system in a dedicated Docker Compose project. The static preview is a separate path for reviewing the public recorded demo.

## Prerequisites

- Node.js **24**, including npm.
- Docker Desktop running **Linux containers**, with Docker Compose available. Linux hosts can use a working Docker Engine and Compose installation.
- Free local ports 3000 and 8000, or the explicit overrides below.
- Internet access for the first container-image/dependency download, and enough free disk space for those images.

The images use Node.js 24 and Python 3.12. Host Python is only needed for running Python tests/evaluations outside containers. Do not use the older checkout's Python environment as a fresh-clone prerequisite.

Check the prerequisites from a terminal:

```sh
node --version
docker info
docker compose version
```

If `docker info` fails, resolve Docker Desktop's startup problem first. A Docker CLI installation alone does not mean its engine is running.

## Start the complete local demo

Open the repository directory containing `package.json`, `compose.demo.yml` and this guide's parent README:

```sh
npm run demo
```

The launcher validates configuration and ports, then builds/starts MongoDB, Mosquitto, the API, manager, port simulator, sensor publisher, analyzer and dashboard. It waits for the API/graph, fresh worker heartbeats with broker connections, and the dashboard response. It prints the dashboard URL only after those readiness checks succeed. The first image build can take several minutes; worker readiness is bounded separately after startup.

The current fixture contains 25 graph nodes and 10 devices. On first use the demo API creates an isolated run and seeds its fixture. Re-running startup preserves the current run rather than replacing its data.

| Local address | Use |
| --- | --- |
| [Dashboard](http://localhost:3000) | Controls, map, fleet, shipment journey and diagnostics |
| [Readiness](http://localhost:8000/health/ready) | API/database/fixture readiness |
| [Demo state](http://localhost:8000/api/demo/state) | Scenario state and individual worker health |
| [Graph](http://localhost:8000/api/graph) | Graph fixture returned to the dashboard |

Only the dashboard and API are published to the host, on loopback. MongoDB and MQTT stay on the Compose network. Host port 27017 or 1883 is not required for the supported stack.

### Isolation and configuration

The launcher uses Compose project `harboursense-demo`, [compose.demo.yml](../compose.demo.yml), and the deliberately empty [config/demo.env](../config/demo.env). Service settings explicitly select local MongoDB database `harboursense_demo` and the local broker. The launcher removes inherited MongoDB/MQTT/AWS/demo/Compose overrides before invoking Compose, so a previous terminal's cloud settings cannot select this demo's database.

No `.env` copy/edit or manual seed command is part of this quick start. The older `docker-compose.yml` and machine recovery scripts are not substitutes for the isolated portfolio path. In particular, `.local-runtime` may contain local diagnostic runtimes used while Docker Desktop was unavailable; it is ignored, machine-specific, and is not a portable installation or proof of the Docker acceptance gate.

### Other local ports

If the defaults belong to another application, leave that application alone and choose unused ports:

```sh
npm run demo -- --api-port 8001 --dashboard-port 3001
npm run demo:status -- --api-port 8001 --dashboard-port 3001
```

Use the same overrides with later commands that contact the API. The frontend API URL is a build-time setting, so changing the API port goes through startup/build configuration, not an environment change applied to an already-built page.

## Run a scenario

1. Select **Normal**, **Congestion**, or **Crane fault** in the dashboard.
2. Press **Play scenario** and follow the shipment and event timeline.
3. Select a device to inspect its state, route and available telemetry.
4. Use **Pause scenario** / **Resume scenario**, and select 1×, 2× or 4× speed.
5. Wait for the observed terminal outcome. Use **Reset scenario** to return to the initial state before another run.

The versioned definitions are in [demo/scenarios.json](../demo/scenarios.json). They use seed 314 and the shared fixture. Normal/congestion runs require the shipment to reach delivery. Crane-fault completion additionally requires recorded maintenance activity and a completed repair task. A scenario deadline produces an explicit failure, not a success label. The planned approximately 90-second presentation is a target; use each recording's measured duration rather than assuming it has been achieved.

Pause freezes logical simulation progress while real-time health and reconnect checks continue. Speed changes affect future simulation progress. Controls have stable command IDs; reset creates a new run identity and old-run messages are fenced off. The dashboard shows acknowledged state rather than treating a clicked button as proof that a control succeeded.

## Status, stop and reset

```sh
npm run demo:status
npm run demo:logs
npm run demo:stop
```

Stop requests an acknowledged pause for a running scenario before stopping this Compose project's services. Data remains in the project's MongoDB volume. Start again with `npm run demo`; after an acknowledged pause the scenario stays paused until resumed from the dashboard. If the API cannot acknowledge pause, shutdown prints a warning and still stops the services; inspect the run after restart. This does not establish recovery from forced process termination or power loss; those are separate failure tests.

Reset intentionally replaces the active **owned demo run**, preserving unrelated databases:

```sh
npm run demo:reset -- --yes
```

The API must be running and identify itself as the isolated demo. The `--yes` flag is the explicit reset request; omitting it reports the correct command and does not reset. Reset is not a command for deleting arbitrary MongoDB databases or Docker volumes.

## Record and preview the static demo

The public demo is a recording of the real local processing path. It is interactive playback in each visitor's browser. It does not make backend calls or let visitors create arbitrary new server-side runs.

First start the complete demo and ensure `demo:status` reports healthy workers. Then run:

```sh
npm run demo:record -- --yes
```

The recorder runs normal, congestion and crane-fault scenarios at 4×, resetting only the isolated local demo between them. It waits for worker readiness and each terminal state, obtains snapshots/events from the recording API, and validates the results. Publication inputs are replaced only after all three scenarios succeed. The output directory is `dashboard/visualizer/public/replays/`, including a manifest and SHA-256 hashes.

For a non-default local API port, set `DEMO_RECORD_API` to its explicit loopback HTTP address. PowerShell example:

```powershell
$env:DEMO_RECORD_API = 'http://127.0.0.1:8001'
npm run demo:record -- --yes
```

Build and preview the recorded site:

```sh
npm --prefix dashboard/visualizer ci
npm run demo:build
npm run demo:preview
```

The first frontend dependency install is required; later builds can reuse it. The default preview is [127.0.0.1:4173/HarbourSense/](http://127.0.0.1:4173/HarbourSense/). Stop the preview terminal with Ctrl+C. The build output is `dashboard/visualizer/build`.

`demo:build` explicitly selects replay mode, validates completed synthetic recordings, writes build metadata, and runs the build checks. It uses `/HarbourSense` by default. If the target repository subpath differs, set `DEMO_BASE_PATH` before building; use `/` for a site hosted at its domain root:

```powershell
$env:DEMO_BASE_PATH = '/HarbourSense'
npm run demo:build
```

The preview reads the built metadata rather than guessing its URL. Optional `DEMO_PREVIEW_PORT` selects another local preview port. Changing the preview port does not change the deployment subpath.

The committed screenshots and walkthrough are copied into `build/media/` only when their capture hashes match the recordings. After making new recordings, first build and start the preview, then run `node scripts/capture-portfolio.mjs` from another terminal (root `npm ci` and Playwright Chromium are required). Inspect the generated `docs/media/` files and rebuild to include them. The Pages workflow requires matching media; local preview builds may omit outdated media so this recapture cycle remains possible.

After recording, stop the full stack and keep only the static preview running. Test playback, selection, pause/resume, speed and reset in this condition. In the browser network panel, verify that requests remain on the preview origin and do not call `/api/`, localhost:8000 or another backend. Missing recordings are a real build error; do not replace them with hand-written output labeled as a captured run.

## Free publication and a future personal site

The first public release targets GitHub Pages with the compiled replay build. GitHub Pages hosts static HTML/CSS/JavaScript and is available for public repositories on GitHub Free. The included `github.io` address avoids a new domain purchase. Python, MongoDB, MQTT and persistent workers do not execute on Pages. [GitHub Pages documentation](https://docs.github.com/en/pages/getting-started-with-github-pages/what-is-github-pages)

The public release is available at [daiyan-khan.github.io/HarbourSense](https://daiyan-khan.github.io/HarbourSense/). The repository is configured for the **Publish portfolio demo** GitHub Actions workflow, which uploads only the validated compiled frontend, synthetic recordings and verified media. Run that workflow against `main` to publish a later tested release; building locally alone does not publish. [Custom Pages workflows](https://docs.github.com/en/pages/getting-started-with-github-pages/using-custom-workflows-with-github-pages)

There is no personal website yet. Complete and publish HarbourSense first; later, add its interactive-demo, repository and case-study links to the personal site. Full-backend server hosting is optional future work, not a prerequisite for the free static release.

The [public deployment verification](https://github.com/Daiyan-Khan/HarbourSense/actions/runs/34486542398) passed HTTPS revision/subpath checks, all three scenarios, narrow-screen keyboard access and independent visitor resets from an independent Linux runner. Fresh unauthenticated browsers also passed desktop/laptop/mobile visual checks. A literal physical second-device check with the laptop powered off remains an optional manual confirmation; it has not been performed here.

## Verification

The commands below are available checks. Their existence is not evidence that they passed on a particular revision or environment.

| Check | Command / prerequisite |
| --- | --- |
| Launcher and artifact helpers | `npm run test:tooling` |
| Current-tree credential scan | `npm run check:secrets` |
| Built replay assets and manifest | `npm run check:demo`, after a successful replay build |
| Replay browser journeys | Root `npm ci`, `npx playwright install chromium`, then `npm run test:e2e`, after a replay build |
| Simulator unit/contract checks | `npm --prefix port-sim ci`, then `npm --prefix port-sim test` |
| Dashboard unit checks | `npm --prefix dashboard/visualizer test -- --watchAll=false`, after frontend dependency installation |
| Classifier/routing experiments | Commands in [evaluation.md](evaluation.md) |

Replay browser tests start the local static preview when needed. They cover the three recorded scenarios, inspect/pause/resume/reset, independent browser contexts, narrow viewports and unexpected backend/external requests. Traces, screenshots and video are retained on failure under Playwright's output directories.

To run the real local browser journey against an already healthy full stack, explicitly allow its owned-demo reset. PowerShell example:

```powershell
npm ci
npx playwright install chromium
$env:DEMO_E2E_MODE = 'live'
$env:DEMO_TEST_ALLOW_RESET = '1'
npm run test:e2e
```

Set `DEMO_E2E_URL` when using a non-default dashboard URL. Clear the live-mode environment variables before switching back to replay tests. These browser tests are separate from API restart, broker interruption and database-unavailability experiments; do not claim those recovery cases from a happy-path browser result.

The saved offline evaluation results are measured evidence, with raw inputs and source hashes. [Final main CI](https://github.com/Daiyan-Khan/HarbourSense/actions/runs/34486464953) passed all six jobs, including fresh Docker startup, real broker contracts, fault-injection recovery and the live browser journey. [The verification ledger](verification.md) separates this evidence from native recording capture and public replay tests.

## Troubleshooting

| Symptom | Next action |
| --- | --- |
| Docker is unavailable / its engine does not answer | Open Docker Desktop, select Linux containers and resolve its startup diagnostic; retry `docker info` before `npm run demo` |
| Port 3000 or 8000 is occupied | Use explicit unused port overrides; the launcher leaves the existing process alone |
| Startup does not reach ready | Run `npm run demo:logs` and `npm run demo:status`; identify the failing worker, database or broker before resetting |
| Browser reports connection refused on the API | Confirm the full stack is ready and the dashboard was built for the selected API port; a React page alone does not start Python |
| Replay build reports missing/incomplete recordings | Run the real local recorder successfully before building; partial recording output is not sufficient |
| Static preview returns 404 at `/` | Open the printed URL, including `/HarbourSense/` or the configured build subpath |
| Node version rejected | Use the supported Node.js 24 runtime |
| Dependency/image installation fails for lack of disk space | Free sufficient space, then retry the failed install/build; do not treat a partial install as usable |
| Scenario fails its time budget | Preserve the failure logs and inspect the shipment/maintenance terminal conditions; reset only after identifying the issue |

## Legacy cloud credentials

The local demo uses no AWS IoT credentials. Earlier project work included AWS certificate material; current-file exclusion does not revoke a key or erase Git history. Credential-owner review, revocation/reissue and any necessary history cleanup must be handled before reusing a cloud live setup or publishing affected source history. Do not claim these actions are complete based only on `.gitignore` or removal from the current index.

See [the implementation plan](../PORTFOLIO_PLAN.md) for the remaining release gates and [the case study](CASE_STUDY.md) for the engineering rationale.

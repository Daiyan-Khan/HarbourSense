# Release verification

The checks below separate real service execution, recorded browser playback, and offline experiments. Synthetic demonstrations do not establish production capacity or industrial reliability.

## Local service evidence — 10 September 2026

The final Docker run passed all seven [integration checks](verification/local-integration.json): retrying start, freezing clock and movement on pause, storage and delivery, resetting an active run, and recovering from separate API, MQTT and MongoDB interruptions without duplicate shipments. Recovery **plus shipment completion** took 26.9, 24.9 and 25.0 seconds respectively; these are not isolated reconnection latencies. Each interruption test allows up to 60 wall-clock seconds for worker readiness and a further 150 seconds for shipment completion; exceeding either budget fails the check and preserves evidence.

[CLI stop/start evidence](verification/local-persistence.json) verifies preserved run and shipment identity, paused state and resumed logical time. The test used Docker Desktop 29.1.3, Compose 2.40.3 and Node 24.12.0 on Windows with Linux containers. [Running backend source checks](verification/runtime-source.json) matched the final local files after normalizing line endings.

The Chromium live browser test completed the real shipment journey and reset against that isolated stack. Four separate browser tests passed against the production replay build: all three scenarios plus independent visitor state and narrow-screen keyboard access. Replay requests stayed on the static origin, without API calls.

The unit suites passed locally: 158 backend, 157 simulator (one broker-dependent test skipped in the ordinary unit run), 19 analyzer, 98 dashboard and 7 tooling tests. The broker suite is a separate CI gate. Offline evaluation verification is documented in [the measured report](evaluation.md).

## Recorded scenario evidence

The [manifest](../dashboard/visualizer/public/replays/index.json) hashes three recordings captured through the real local workers. Each contains scenario inputs, source fingerprints, synthetic-data labels and a completed terminal snapshot.

| Scenario | Recorded simulation time | Frames | Outcome |
| --- | --- | --- | --- |
| Normal | 67.689 seconds | 32 | Stored and delivered; all devices idle |
| Congestion | 67.091 seconds | 33 | Route A1 → B1 → B2 → B3 → B4 avoids sensed A2 bottleneck; delivered |
| Crane fault | 88.144 seconds | 43 | Positive anomaly score, completed maintenance response and delivered shipment |

Exact wall-clock scheduling can vary between real service runs. These figures describe the committed recordings, not a latency benchmark.

## Public release gate

[Final main CI passed all six jobs](https://github.com/Daiyan-Khan/HarbourSense/actions/runs/34486464953) on release revision `58aefe24`: backend, simulator/tooling, analyzer/evaluation, real MQTT broker contracts, dashboard/replay browser checks, and fresh-stack startup/integration/live browser checks. This was a clean Ubuntu runner, independently rebuilding and running the complete stack. The earlier [implementation CI](https://github.com/Daiyan-Khan/HarbourSense/actions/runs/34485721207) also passed.

[First Pages publication and public verification passed](https://github.com/Daiyan-Khan/HarbourSense/actions/runs/34486542398) for revision `58aefe240a7a04545d45aeae8ce3679e03db3e00`. A separate Ubuntu runner checked the deployed revision, all three public scenario journeys, visitor isolation and mobile keyboard controls against [the actual HTTPS site](https://daiyan-khan.github.io/HarbourSense/), without access to this laptop's backend. The hosted walkthrough returned HTTP 200 with the expected video type and 7,110,945-byte length.

Fresh unauthenticated Chromium sessions also visually verified the public site at 1440×1000, 1280×800 and 390×844. All 25 map locations rendered and fit; each session made five same-origin GET requests and no API/external requests, with no browser or HTTP errors. [Public visual evidence](verification/public-visual-qa.json) preserves the check details.

An independent cloud runner can demonstrate that playback needs only the published assets. A physical second-device test with the development laptop powered off has not been performed here and must not be reported as completed.

## Practical limits

- Interruption tests are bounded single-host experiments. They do not cover every crash point, network partition or concurrency level.
- A hard-killed worker can leave an operation count unresolved. If a bounded drain cannot complete, the run fails with an explicit reset action; it must not silently discard in-flight work.
- The public site replays recorded model outputs. It does not execute Python, MongoDB, MQTT or new anomaly inference online.
- The full pipeline crane scenario injects a combined temperature/vibration fault. Separate overheating and vibration labels are evaluated in the offline experiment.
- Current published recordings were captured from the native full-repository runtime and include 39 matching source fingerprints. Future recordings made inside the current API container include mounted scenario fingerprints only; the container cannot see sibling simulator/analyzer source trees. Do not interpret those partial hashes as a complete container provenance record.
- Legacy AWS IoT keys remain in earlier Git history. Current source/build exclusions do not revoke them; revoke and reissue them before any cloud reuse.

# Add HarbourSense to a future portfolio

HarbourSense is already public at **[daiyan-khan.github.io/HarbourSense](https://daiyan-khan.github.io/HarbourSense/)**. Bookmark this address now. When the personal website is built, add a project card that opens this same demo; the project does not need to move to the new website's host.

The demo, repository, case study, walkthrough and three screenshot URLs below returned HTTP 200 when checked on 11 September 2026. The personal website is a future task. This guide supplies its project copy, links and integration examples.

## Links to keep and share

| Purpose | Public link |
| --- | --- |
| Interactive demo — best first link for visitors | [Open HarbourSense](https://daiyan-khan.github.io/HarbourSense/) |
| Report viewer and project links | [Project resources](https://daiyan-khan.github.io/HarbourSense/#project-resources) |
| Written project report | [Corrected public edition (PDF)](https://daiyan-khan.github.io/HarbourSense/reports/harboursense-report.pdf) |
| Source code and local setup | [GitHub repository](https://github.com/Daiyan-Khan/HarbourSense) |
| Architecture, decisions and contribution | [Engineering case study](https://github.com/Daiyan-Khan/HarbourSense/blob/main/docs/CASE_STUDY.md) |
| Short video | [88-second walkthrough](https://daiyan-khan.github.io/HarbourSense/media/harboursense-walkthrough.webm) |
| Reproducible experiments | [Measured evaluations](https://github.com/Daiyan-Khan/HarbourSense/blob/main/docs/evaluation.md) |
| Test and deployment evidence | [Release verification](https://github.com/Daiyan-Khan/HarbourSense/blob/main/docs/verification.md) |

For a CV, project list or social profile, use the demo as the primary link and the repository as a supporting link. Visitors can open the demo without a GitHub account, choose a scenario, inspect devices, pause, change speed and reset their own playback. They do not need to install anything or ask for access. For a recruiter who wants the written work, include the corrected public report link alongside the demo; the PDF can be viewed in the portal or downloaded for later reading.

To find it later, bookmark the demo and report links or return to this repository's README. These public links work when the development laptop is off. Keep a downloaded copy of the report as a personal backup. Keep the full `/HarbourSense/` path in copied URLs. A local address such as `localhost:3000` works only on the machine running that local stack and is not the address to share.

### Screenshot assets

Use the overview image for the project card. The other two images fit a gallery or case-study page. These direct image addresses can be used by any website:

| Image | Public asset URL | Suggested alternative text |
| --- | --- | --- |
| Overview | [harboursense-overview.png](https://daiyan-khan.github.io/HarbourSense/media/harboursense-overview.png) | HarbourSense dashboard showing a recorded shipment journey and the port map |
| Crane inspection | [harboursense-crane-detail.png](https://daiyan-khan.github.io/HarbourSense/media/harboursense-crane-detail.png) | Recorded crane telemetry trends, anomaly score and maintenance context |
| Mobile layout | [harboursense-mobile.png](https://daiyan-khan.github.io/HarbourSense/media/harboursense-mobile.png) | HarbourSense recorded scenario on a narrow mobile layout |

The source files and their capture provenance are in [docs/media](media/). Linking to the hosted files keeps the portfolio card small; copying approved images into the future website's own assets is also possible. Recapture them when the dashboard or recordings change substantially.

## Project copy

**Title:** HarbourSense — smart-port simulation

**Short card description:** A solo project connecting a React dashboard to a Python and Node.js port simulation through MQTT and MongoDB. Explore recorded shipment journeys, congestion-aware routing and a crane maintenance response.

**Longer first-person blurb:** I built HarbourSense to make a simulated port's operations inspectable, from task assignment and shipment movement to congestion and crane maintenance. The system combines React, FastAPI, Node.js, MQTT and MongoDB, with synthetic telemetry scored by an IsolationForest model. The public demo replays scenarios captured from the real local pipeline; the repository includes the complete local stack, recovery checks and reproducible comparisons against simpler baselines.

**Role:** Solo project by Daiyan Khan.

**Technology tags:** React, Python, FastAPI, Node.js, MQTT, MongoDB, Docker, scikit-learn, GitHub Actions.

Keep the recorded-simulation description near the demo link. The evaluation uses synthetic data and does not establish industrial fault prediction or production capacity. The [evaluation report](evaluation.md) includes the unfavorable model result: simple thresholds outperform the current IsolationForest on the injected faults. Any numerical portfolio claim should keep its experimental scope and link to that evidence.

## Plain HTML example

This project card works in an ordinary HTML page. Its content and links can also be adapted to React, Next.js, Astro or a website builder.

```html
<article aria-labelledby="harboursense-title">
  <img
    src="https://daiyan-khan.github.io/HarbourSense/media/harboursense-overview.png"
    alt="HarbourSense dashboard showing a recorded shipment journey and the port map"
    loading="lazy"
    style="max-width: 100%; height: auto;"
  >
  <h2 id="harboursense-title">HarbourSense</h2>
  <p>Smart-port simulation · Solo project</p>
  <p>
    Follow shipment journeys, inspect congestion-aware routes and explore
    a crane maintenance response. The public demo plays recorded scenarios
    captured from the complete local system.
  </p>
  <p>React · FastAPI · Node.js · MQTT · MongoDB · Docker</p>
  <nav aria-label="HarbourSense project links">
    <a href="https://daiyan-khan.github.io/HarbourSense/"
       target="_blank" rel="noopener noreferrer">Interactive demo</a>
    <a href="https://github.com/Daiyan-Khan/HarbourSense"
       target="_blank" rel="noopener noreferrer">Source code</a>
    <a href="https://github.com/Daiyan-Khan/HarbourSense/blob/main/docs/CASE_STUDY.md"
       target="_blank" rel="noopener noreferrer">Case study</a>
    <a href="https://daiyan-khan.github.io/HarbourSense/reports/harboursense-report.pdf"
       target="_blank" rel="noopener noreferrer">Report (corrected public edition)</a>
    <a href="https://daiyan-khan.github.io/HarbourSense/media/harboursense-walkthrough.webm"
       target="_blank" rel="noopener noreferrer">Watch walkthrough</a>
  </nav>
</article>
```

A normal link gives the dashboard its full viewport, including its map and detail drawer. Embedding the entire dashboard in an iframe is optional later work. To put the walkthrough directly on a project page, use a video element with visible controls:

```html
<video controls playsinline preload="none"
       poster="https://daiyan-khan.github.io/HarbourSense/media/harboursense-overview.png"
       style="max-width: 100%; height: auto;">
  <source
    src="https://daiyan-khan.github.io/HarbourSense/media/harboursense-walkthrough.webm"
    type="video/webm">
  <a href="https://daiyan-khan.github.io/HarbourSense/media/harboursense-walkthrough.webm">
    Open the HarbourSense walkthrough
  </a>
</video>
```

## Project metadata example

For a site that generates its project cards from data, use this as a starting object. The property names are suggestions, not a dependency on a particular framework. Keep `reportEdition` beside the report link so visitors understand which version they are reading.

```json
{
  "slug": "harboursense",
  "title": "HarbourSense",
  "subtitle": "Smart-port simulation",
  "role": "Solo project — Daiyan Khan",
  "description": "Explore recorded shipment journeys, congestion-aware routing and a crane maintenance response captured from a complete local simulation.",
  "demoMode": "Recorded simulation with browser-local playback",
  "technologies": ["React", "FastAPI", "Python", "Node.js", "MQTT", "MongoDB", "Docker", "scikit-learn"],
  "demoUrl": "https://daiyan-khan.github.io/HarbourSense/",
  "sourceUrl": "https://github.com/Daiyan-Khan/HarbourSense",
  "caseStudyUrl": "https://github.com/Daiyan-Khan/HarbourSense/blob/main/docs/CASE_STUDY.md",
  "evaluationUrl": "https://github.com/Daiyan-Khan/HarbourSense/blob/main/docs/evaluation.md",
  "walkthroughUrl": "https://daiyan-khan.github.io/HarbourSense/media/harboursense-walkthrough.webm",
  "imageUrl": "https://daiyan-khan.github.io/HarbourSense/media/harboursense-overview.png",
  "imageAlt": "HarbourSense dashboard showing a recorded shipment journey and the port map",
  "screenshots": [
    "https://daiyan-khan.github.io/HarbourSense/media/harboursense-overview.png",
    "https://daiyan-khan.github.io/HarbourSense/media/harboursense-crane-detail.png",
    "https://daiyan-khan.github.io/HarbourSense/media/harboursense-mobile.png"
  ],
  "resourcesUrl": "https://daiyan-khan.github.io/HarbourSense/#project-resources",
  "reportUrl": "https://daiyan-khan.github.io/HarbourSense/reports/harboursense-report.pdf",
  "reportEdition": "Corrected public edition"
}
```

## Written project report

The [corrected public edition](https://daiyan-khan.github.io/HarbourSense/reports/harboursense-report.pdf) revises *Scalable IoT Architecture for Smart Port Asset Tracking and Management: The HarbourSense System*. It corrects mismatched figures and captions, updates the implementation description, and replaces unsupported performance claims with the reproducible synthetic evaluations in this repository. Those experiments have their own stated limits; they do not measure industrial deployment or full-stack production capacity.

The original academic source ZIP is preserved unchanged locally. The public PDF is clearly labeled as a corrected edition. The [engineering case study](CASE_STUDY.md) is a separate, shorter explanation of the implementation and decisions.

In the portal, choose **Report & project info**, then **View report**. **Open report** opens the PDF directly, and **Download PDF** saves a copy. Sharing the [project resources link](https://daiyan-khan.github.io/HarbourSense/#project-resources) takes a visitor straight to this section. The direct PDF link also works independently of the dashboard.

### Update the report later

1. Edit the reviewed LaTeX source and figures in [docs/report-source](report-source/). Follow that directory's README to rebuild the PDF, and inspect every page before replacing the published copy. Preserve the original ZIP separately.
2. Put the reviewed PDF at `dashboard/visualizer/public/reports/harboursense-report.pdf`. Keep this filename to preserve shared links.
3. Update the title, edition, description and page count in `dashboard/visualizer/src/projectResources.json` to match the PDF. If a `sha256` is configured, update it too. Keep only the configured PDF in the public reports directory.
4. Follow the preview and publication steps below. The demo build validates the configured PDF and records its hash. The public check verifies the deployed PDF's content type and hash; browser checks exercise its links and viewer.

Changing the report does not require running the backend or regenerating unchanged scenario recordings and walkthrough media.

## Hosting and future access

The GitHub Pages site serves the compiled dashboard, synthetic recordings, media and report PDF. Playback happens in each visitor's browser, so two visitors can reset independently and the development laptop can be off. Python, MongoDB, the MQTT broker and new model inference are available through the local engineering demo; they are not running on the Pages server.

GitHub Pages is available for public repositories on GitHub Free. The existing `github.io` address avoids a domain purchase, and this static design does not need a paid backend. Continued access depends on keeping the repository and Pages publication available; renaming the repository or account can change these links. [GitHub Pages hosting and availability](https://docs.github.com/en/pages/getting-started-with-github-pages/what-is-github-pages)

The future portfolio website can use a different host or domain and simply link to HarbourSense. Its hosting is a separate choice. Building that website does not require changing HarbourSense's working public deployment.

## Edit, preview and publish later

If only the future portfolio card changes, edit and publish that website. If HarbourSense's dashboard, recordings, media or report change, update this repository:

1. Edit the relevant source files. Keep the existing local/replay distinction in the displayed copy. Read [the local demo guide](LOCAL_DEMO.md) for the complete stack and recording commands.
2. With Node.js 24, install the root and dashboard dependencies and build the preview:

   ```sh
   npm ci
   npm --prefix dashboard/visualizer ci
   npm run demo:build
   npm run demo:preview
   ```

   Open the printed `/HarbourSense/` preview URL. Leave that terminal running while inspecting the site. In another terminal, `npx playwright install chromium` followed by `npm run test:e2e` runs the recorded browser journeys.
3. If simulation behavior changed, capture all three scenarios again using the real local system. If recordings or presentation changed, recapture and inspect the screenshots/walkthrough before rebuilding. The [recording and media workflow](LOCAL_DEMO.md#record-and-preview-the-static-demo) describes the order; publication requires matching recording/media hashes.
4. Commit and push the reviewed changes through the repository's normal workflow. Wait for **HarbourSense CI** to pass on the revision intended for release. A local preview or a source push alone does not trigger the manually dispatched Pages publication.
5. Open [GitHub Actions](https://github.com/Daiyan-Khan/HarbourSense/actions), select **Publish portfolio demo**, choose **Run workflow** against `main`, and wait for `build`, `deploy` and `verify-public` to succeed. The final job checks the deployed revision and report PDF, then exercises the public demo from a separate Linux runner.
6. Reopen the public demo, report and media links, refresh the page, and inspect the scenarios and report viewer. Update the future portfolio copy or screenshots if its description changed. Keep the stable demo URL in bookmarks and shared project cards.

For the current successful release and its limits, see [verification.md](verification.md). For architecture and interview preparation, start with [CASE_STUDY.md](CASE_STUDY.md).

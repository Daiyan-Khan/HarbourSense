# HarbourSense report — corrected public edition

This directory contains the editable source for the report linked from the HarbourSense portal. The edition is dated **11 September 2026** and credits **Daiyan Khan** as the sole author.

- [Read the public PDF](https://daiyan-khan.github.io/HarbourSense/reports/harboursense-report.pdf)
- [Open the portal's report section](https://daiyan-khan.github.io/HarbourSense/#project-resources)
- [Portfolio integration and sharing guide](../PORTFOLIO_INTEGRATION.md)

## What changed from the original

The original academic report was supplied as a LaTeX ZIP. Its original ZIP and extracted source remain preserved locally and were not overwritten. The public PDF is explicitly a corrected edition, rather than an unchanged submission.

The revision retains the project problem and architectural explanation, updates the implementation description, and replaces mismatched performance figures and unsupported cloud benchmarks with the repository's reproducible synthetic evaluations. It distinguishes offline comparisons, real local service tests, and static browser replay. It reports the threshold baseline's stronger anomaly-detection results and limits routing claims to the declared workload. References and figure captions were reviewed, and the original author–year bibliography style is retained.

Original archive filename:

`Scalable_IoT_Architecture_for_Smart_Port_Asset_Tracking_and_Management__The_HarbourSense_System.zip`

Original archive SHA-256:

```text
4bf108b1a340942ff139bff892122a06c4753b2836b46f31e51ba4c2ca90c076
```

The archive is not part of the public site. Keep a separate backup of that original alongside the editable public edition.

## Source and evidence

- `main.tex`: report text and current architecture diagram.
- `references.bib`: bibliography; `IEEEtran.cls`: original document class, with its existing license header preserved.
- `figures/`: three vector PDF plots, PNG previews, their generator and package versions, and `figure-provenance.json` with hashes of the saved inputs and outputs.
- [Saved evaluation inputs and results](../../evaluation/results/): the evidence used for the plots.
- [Evaluation method and reproduction](../evaluation.md): experimental setup and limitations.

The PDF publication file is `dashboard/visualizer/public/reports/harboursense-report.pdf`. The portal's `dashboard/visualizer/src/projectResources.json` supplies its title, edition, page count and release hash. The public build and deployment checks verify that hash.

## Rebuild the report

The public PDF was compiled with **Tectonic 0.17.0**. Install the [official Tectonic release](https://github.com/tectonic-typesetting/tectonic/releases/tag/tectonic%400.17.0), then, from this directory:

```sh
mkdir build
tectonic --untrusted --keep-logs --outdir build main.tex
```

Tectonic obtains required TeX packages on its first run. On Windows, use a short local checkout and set TECTONIC_CACHE_DIR to a short directory path if the operating system's path-length limit prevents package or font loading. Existing `build` directories can be reused.

The committed plot PDFs are sufficient for a text-only rebuild. To regenerate the plots, use Python 3.12 and run these commands from the repository root:

```sh
python -m pip install -r docs/report-source/figures/requirements.txt
python docs/report-source/figures/generate_figures.py --results evaluation/results
```

The plot generator verifies the saved raw-input hashes and redraws the plots; it does not rerun the simulation or train a new model. Follow the evaluation guide when changing the measurements themselves.

## Publish an updated edition

1. Edit the report source and edition note. Keep numerical claims tied to saved evidence.
2. Compile the PDF, inspect every rendered page, and check for unresolved references, clipped content, incorrect captions and stale results.
3. Copy the reviewed `build/main.pdf` to `dashboard/visualizer/public/reports/harboursense-report.pdf`. Update its page count and SHA-256 in `dashboard/visualizer/src/projectResources.json`. Use `Get-FileHash -Algorithm SHA256` on Windows or `sha256sum` on Linux.
4. Run `npm run test:tooling`, the dashboard tests, `npm run demo:build`, and `npm run test:e2e` as described in the integration guide. Confirm the report viewer, open link and download link.
5. Commit and push the reviewed source, plots, PDF and metadata. Wait for HarbourSense CI, then run the **Publish portfolio demo** workflow on `main`.
6. Confirm the public PDF opens and the public verification job passes. Its stable URL can remain unchanged in a portfolio, CV or recruiter message.

Generated TeX intermediates and local environments are excluded from source control. The report source supports future editing; identical PDF bytes across different TeX package bundles are not promised.

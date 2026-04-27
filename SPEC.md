# Vet Scraper System

## Purpose
An agentic system for scraping veterinary directory sites with
automated quality auditing and self-correcting improvement loops.

## Architecture
Four independent components, each in its own folder:

1. `scrapers/` — Individual site scrapers. Each produces an
   .xlsx file matching the schema in SCRAPER_CONTRACT.md.
   Adding a new site = adding a new script here.

2. `auditor/` — Phase 1. Reads any scraper output, produces
   a data quality report. No code modification, no web requests.

3. `diagnostic/` — Phase 2. For flagged rows, re-fetches
   source pages and diagnoses scraper selector issues. Read-only.

4. `patcher/` — Phase 3. Proposes code patches to scrapers
   based on diagnostics. Always shows diff before applying.
   Runs test-subset reruns, not full reruns. Iteration cap: 3.

## Workflow
Run scraper → auditor → (if issues) diagnostic → (if actionable)
patcher → test-subset rerun → re-audit. Max 3 patch cycles.

## Principles
- Each phase runs independently. Composable, not monolithic.
- Auditor/diagnostic/patcher are scraper-agnostic — they work
  on any scraper output matching SCRAPER_CONTRACT.md.
- Existing working scrapers (AAEP) are never modified without
  explicit approval and a diff review.
- Full scraper reruns are expensive — avoid unless necessary.
- Acceptable error threshold: 90-95% clean. Do not chase
  perfection — hand remaining edge cases to manual review.

## Environment
Python 3.12, Windows. One .venv at project root.
Libraries: requests, beautifulsoup4, pandas, openpyxl,
selenium, undetected-chromedriver, usaddress.

Use `.venv\Scripts\python.exe` directly for all Python
execution. Do NOT rely on shell activation.

## Status
- [x] AAEP scraper (working, see scrapers/aaep_scraper.py)
- [ ] HorseDVM scraper (to build)
- [ ] Phase 1: Auditor
- [ ] Phase 2: Diagnostic
- [ ] Phase 3: Patcher
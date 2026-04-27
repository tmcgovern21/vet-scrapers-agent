# vet-scrapers-agent

A pipeline for scraping, auditing, and cleaning veterinary practitioner directories. Designed as a system, not a script — separation of concerns between extraction, validation, and post-hoc data correction.

## What this is

Veterinary directory data is fragmented across professional associations, alumni groups, and specialty organizations. Each source publishes its own database with its own schema, quality, and quirks. This project builds toward a unified, audit-clean dataset by treating each source as a separate scraper that conforms to a shared output contract.

Today the pipeline produces clean Excel files. The architecture supports a future consolidation layer (database, API, or dashboard) without requiring scraper changes.

## Architecture

```
scrape (per-source) → audit (universal) → patch (per-source) → ship
```

Three independent modules, each with its own SPEC:

- **scrapers/** — One Python module per source site. Each scraper writes to a shared schema defined in `SCRAPER_CONTRACT.md`. Scrapers handle source-specific extraction logic (API calls, HTML parsing, profile-page enrichment) and produce timestamped `.xlsx` files.
- **auditor/** — Source-agnostic validator. Reads any output that conforms to the contract, runs Tier 1 (presence), Tier 2 (format), and Tier 3 (cross-field consistency) checks. Produces a per-row pass/fail report with a summary score. Decoupled from scrapers so it can audit any output independently.
- **patcher/** — Post-hoc xlsx corrector. Applies deterministic fixes (PO Box parsing, UK postcodes, Canada postal codes, whitespace) and per-source override files for source-side data errors that no scraper can fix. Does not modify source data; produces a `_patched.xlsx` alongside the original.

## What's built today

| Source | Records | Method | Audit clean | Status |
|---|---|---|---|---|
| AAEP (American Association of Equine Practitioners) | ~4,000 | Direct Algolia API + profile-page enrichment | 98.8% | Production |
| HorseDVM | 225 practices | Static HTML + profile-page enrichment | 98.7% | Production |

Total: roughly 4,300 records across 33+ countries.

## What's planned

- Two more directories queued (AHVMA, Chi University TCVM alumni)
- Auditor v2 enhancements: per-column population coverage stats, parse-completeness checks, `--compare-to` delta mode for measuring patcher impact
- LLM-assisted address parsing for cases that deterministic rules can't handle (Tier 2 of the patcher)
- Cross-source deduplication layer
- Database persistence and query layer (the "consolidation phase")

## Stack

- Python 3.12, requests, BeautifulSoup, pandas, openpyxl, usaddress
- Claude Code as pair programmer; architecture decisions and reviews are mine, implementation is AI-accelerated under review
- GitHub for version control

## Project structure

`vet-scrapers-agent/`

- `scrapers/` — Per-source scrapers (`aaep_scraper.py`, `horsedvm_scraper.py`, `overrides/`, `SCRAPER_CONTRACT.md`)
- `auditor/` — Universal validator (`aliases.py`, `checks.py`, `loader.py`, `run.py`, `SPEC.md`)
- `patcher/` — Post-hoc xlsx fixes (`patches.py`, `run.py`, `SPEC.md`)
- `outputs/` — Generated xlsx files (gitignored)

## Design notes

A few decisions worth flagging for anyone reading this code:

1. **Auditor is decoupled from scrapers.** Scrapers don't call the auditor; the auditor reads any conforming output independently. This means audit results become a feedback signal that can drive both scraper fixes (when patterns of failure suggest a code bug) and source-data overrides (when patterns suggest the source itself is wrong).
2. **Patcher exists because some failures aren't scraper bugs.** When a source mistags a Canadian practice as American, the scraper has no way to detect or correct that. The override system captures these source-side errors as data, not code.
3. **Investigation precedes implementation.** Each scraper starts with a structured investigation phase: site type, pagination, field inventory, recommended approach. Code is only written after the architecture is approved. This catches design problems early — for example, switching AAEP from browser automation to direct API calls based on what the investigation found.

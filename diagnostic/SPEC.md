# Phase 2: Diagnostic

## Goal
For rows flagged by the auditor, re-fetch source pages and
diagnose WHY the scraper produced bad data. Read-only.
Does not modify scraper code.

## Input
- Audit report from Phase 1 (audit_reports/*.xlsx)
- Original scraper source (scrapers/*.py) for reference

## Process
For each flagged row (cap at top 50 by default):
1. Fetch the Profile URL (with polite rate limiting, 1+ sec
   delay, respect robots.txt)
2. Save raw HTML to debug_html/{source_id}.html
3. Read the scraper source to understand what selectors
   it uses for the flagged columns
4. Extract what the scraper WOULD extract from the saved HTML
5. Compare to what the scraper ACTUALLY extracted (from the
   original .xlsx)
6. Characterize the mismatch:
   - "Selector returned wrong element" (selector too broad/narrow)
   - "Selector returned nothing" (selector missing, page
     structure differs)
   - "Page structure differs from expected" (layout variant)
   - "Source data is genuinely bad" (page itself has the issue)

## Output
Report at audit_reports/{source}_diagnostic_{YYYYMMDD_HHMM}.xlsx

Sheets:
1. Row Diagnoses — one row per diagnosed row:
   - Source ID, Profile URL
   - Columns with issues
   - Diagnosis category (per list above)
   - Expected (what page has)
   - Actual (what scraper got)
   - Selector/regex the scraper used
2. Pattern Summary — grouped by diagnosis category:
   counts + suggested fix direction for each pattern
3. Not Scraper's Fault — rows where the source page itself
   has bad data; flag for manual review, not code fix

## Guardrails
- Rate limit: 1 req/sec minimum
- Max 50 rows per diagnostic run (configurable)
- Cache raw HTML in debug_html/ — do not re-fetch if
  already present
- Never modify scraper code

## CLI
.venv\Scripts\python.exe diagnostic\run.py --audit audit_reports\aaep_audit_X.xlsx --max 50
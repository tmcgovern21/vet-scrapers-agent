# Phase 1: Auditor

## Goal
Read any scraper .xlsx and produce a data quality report.
Handles variance in column names, sheet names, and extended
fields via the alias layer in SCRAPER_CONTRACT.md.

Treat sparse data as informational, not as error.
A column being 30% populated is a fact, not a defect.

## Input
Path to an .xlsx file. Auditor auto-detects the main
data sheet (heuristic: sheet with most rows that has
recognizable columns).

## Load-time normalization
1. Detect main data sheet
2. Detect whether first data row is actually headers
   (see "Header-row bug detection" below)
3. Normalize column names using COLUMN_ALIASES in
   SCRAPER_CONTRACT.md
4. Classify each column as Tier 1, Tier 2, or Tier 3
5. Convert empty-string/NaN/None consistently to "" for
   downstream checks

## Header-row bug detection
If the first data row contains values that look like
column headers (e.g., "Name", "Email", "Phone" appearing
as row values), flag this and STOP. Report:

    LIKELY HEADER-ROW BUG: The first data row appears
    to contain column headers. This typically happens
    when the scraper wrote a title line above the
    real header. Fix the scraper or re-load with
    skiprows=1.

This was seen in the Chi University output.

## Checks

### Tier 1 (fail = real problem)
- Source Site: non-empty for all rows
- Source ID: non-empty AND unique
- Profile URL: valid URL format, non-empty
- Name: non-empty, no digits, no street-type words
  (Street, Ave, Road, Suite, Unit, PO Box), length 3-100

### Tier 2 (fail = likely scraper bug)
Applied only to POPULATED values. Empty is fine.

Contact fields:
- Phone / Phone 2: matches phone regex (US + intl)
- Email: matches email regex
- Website: valid URL AND is NOT a google.com/maps URL
  (cross-contamination check — seen in IVAS output)
- Google Maps URL: contains maps.google or google.com/maps

Identity:
- Credentials: no digits, reasonable length

Address:
- Full Address Raw: contains digits, not placeholder text
  ("View Location", "Map", "See profile", etc.)
- Address Line 1: contains digits (street number),
  does not contain phone numbers or URLs
- City: no digits, no URLs, length reasonable
- State: valid US state code / Canadian province / or
  recognizable international region
- Zip: matches postal patterns (US 5-digit, US+4,
  Canadian, UK, etc.)
- Country: valid country name or 2-letter code

Practice:
- Practice / Company: does NOT look like an address
  (no "PO Box", no leading street numbers) —
  cross-contamination check (seen in IVAS output)

Geo:
- Latitude: -90 to 90 numeric
- Longitude: -180 to 180 numeric

### Tier 3 (informational only)
- Fill rate, distinct count, top 5 values
- If column name hints at type (contains "URL",
  "Email", "Phone", "Date"), apply that check

### Cross-column
- Dead rows (all Tier 1+2 empty)
- Duplicate Source IDs
- Duplicate (Name + Practice + State) likely-dupes
- Address parse consistency: if Full Address Raw is
  populated but parsed fields (Line 1, City, State)
  are empty, flag "parse failed" for that row

## Output
audit_reports/{input_filename}_audit_{YYYYMMDD_HHMM}.xlsx

Sheets:
1. Summary
   - Scraper name (Source Site value)
   - Total rows
   - Overall score (% rows with zero flagged issues)
   - Tier 1 check pass rates
   - Tier 2 check pass rates (per check)
   - Tier 3 columns present (informational list)
   - Header-row bug: yes/no
   - Address parse success rate
   - Override reporting (if overrides file exists):
     - Total override count
     - Override rate (% of rows)
     - Top 3 override patterns (grouped by 'reason')
     - Warning if override rate > 5%
2. Column Details — per column: tier, fill rate,
   distinct count, top values, check results
3. Flagged Rows — every row failing ≥1 check:
   row index, Source ID, Profile URL,
   checks failed (comma list), problem values
4. Tier 3 Fields — extended columns seen, with
   cross-site presence note if comparing multiple audits
5. Suggestions — plain-text diagnoses of patterns
   seen (e.g., "15 rows have Website values that are
   Google Maps URLs — Website selector is likely
   matching the wrong element")

## CLI
.venv\Scripts\python.exe auditor\run.py --input outputs\aaep_20260423.xlsx

## Out of scope
- No scraper code modification
- No web requests (other than optional sampled HEAD
  checks, off by default)
- No promotion of Tier 3 → Tier 2 (manual decision)
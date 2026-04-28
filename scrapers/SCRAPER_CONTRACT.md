# Scraper Output Contract

All scrapers in this folder produce .xlsx output following
the structure below. Auditor, diagnostic, and patcher rely
on this contract.

## Philosophy
- Goal is MAXIMUM data capture, not minimum.
- If a site exposes a field, the scraper captures it.
- Sparse fields are expected and fine — not a defect.
- Scrapers add extended columns freely. The auditor treats
  unknown columns as informational.
- Column names vary across scrapers. Canonical names are
  suggested, not enforced. The auditor normalizes via a
  mapping layer.

## Column tiers

### Tier 1: Core (every scraper must include)
These MUST exist in every output, even if empty.
- Source Site     — scraper name, e.g. "AAEP", "IVAS"
- Source ID       — site-specific unique ID
- Profile URL     — direct link to the person's page
- Name            — full name of the practitioner
                    (may include credentials for sites that
                    don't separate them)

#### AAHA exception (hospital-level data)
AAHA records are practice-level, not practitioner-level — each row is an
accredited hospital, not a vet. The Name and Credentials fields are blank
by design; the hospital name lives in Practice / Company. The AAHA detail
page does expose a list of veterinarians per hospital, but per the
hospital-level contract for AAHA we keep it as a Tier-3 `Veterinarians`
column (semicolon-delimited) rather than pivoting to per-vet rows. The
auditor treats empty Name as informational (not a Tier 1 failure) when
Source Site == "AAHA". A single physical hospital may also appear with
two distinct recnos when it holds both a General Practice and a Referral
accreditation — these are NOT duplicates and should NOT be deduped by
address.

### Tier 2: Standard (include if the site provides)
Canonical names in parentheses. Synonyms are accepted —
the auditor maps synonyms to canonical via COLUMN_ALIASES
below.

- Credentials                    (syn: Qualifications, Title)
- Practice / Company             (syn: Practice Name,
                                  Practice / Clinic)
- Specialties                    (syn: Specialty, Services,
                                  Species Treated)
- Email
- Phone                          (syn: Business Phone)
- Phone 2                        (syn: Cell Phone,
                                  Secondary Phone)
- Website
- Google Maps URL
- Latitude
- Longitude

Address fields (include BOTH raw and parsed if possible):
- Full Address Raw               (syn: Address)
- Address Line 1                 (syn: Street Address)
- Address Line 2
- City
- State                          (state/province code)
- Zip                            (syn: ZIP, Postal Code)
- Country

Social media (site-dependent):
- Facebook
- Instagram
- LinkedIn URL
- Twitter URL

Bio (site-dependent):
- About / Description            (syn: Bio, Summary)

### Tier 3: Extended (site-specific, append at end)
Any field the site exposes that isn't in Tier 1-2.
Examples seen so far:
- Certifications                 (Chi)
- Extra Info                     (Chi)
- About / Description            (AHVMA)
Other examples to expect: Years of Experience, Education,
Languages, Accepting New Patients, Hours, etc.

Place Tier 3 columns AFTER all Tier 1-2 columns.

## Column aliases (auditor uses this to normalize)
The auditor treats the following as equivalent:

```
COLUMN_ALIASES = {
    "Name":              ["Name / Credentials", "Contact Name"],
    "Credentials":       ["Qualifications", "Title"],
    "Practice / Company":["Practice Name", "Practice / Clinic"],
    "Specialties":       ["Specialty", "Services"],
    "Phone":             ["Business Phone", "Phone 1"],
    "Phone 2":           ["Cell Phone", "Secondary Phone"],
    "Zip":               ["ZIP", "Postal Code"],
    "Full Address Raw":  ["Address", "Full Address"],
    "Address Line 1":    ["Street Address", "Street"],
    "About / Description":["Bio", "Summary", "Description"],
    "Source ID":         ["Algolia ID", "ID"],
}
```

Scrapers may use either canonical or alias names. The
auditor normalizes before running checks.

## Sheet structure

Every scraper's .xlsx output MUST contain these four sheets, in
this order:

1. **Members** — all records (full global dataset). This is the
   primary data sheet; the auditor treats it as authoritative.
2. **US Only** — filtered to records where Country is US / USA /
   "United States" (or State is a US 2-letter code when Country is
   blank). Same columns as Members.
3. **International** — filtered to records that are NOT US (the
   complement of US Only). Same columns as Members.
4. **Summary** — fill rates, metadata, new-field notices. The
   auditor also reads this for cross-reference.

Additional sheets (e.g., "By State", "Stats") are optional and
appended after Summary.

The auditor auto-detects the main data sheet by name, preferring
"Members"; if absent it falls back to the sheet with the most
recognized columns. The US Only / International sheets are
informational convenience views for humans — the auditor does NOT
re-audit them.

Scrapers must scrape GLOBALLY — never filter by country during the
fetch phase. The US Only / International split happens at export
time, not at scrape time.

## Output location
outputs/{source_site_lowercase}_{YYYYMMDD}.xlsx
e.g. outputs/aaep_20260423.xlsx

## Empty-value convention
Empty string "" preferred, but NaN/None also accepted.
Auditor normalizes on load.

## Address parsing
Scrapers should ATTEMPT to parse addresses into components
(Address Line 1, City, State, Zip, Country) when the site
provides them structured.

If parsing is uncertain, populate Full Address Raw only and
leave parsed fields empty. The auditor flags low-parse-rate
addresses for review.

Suggested libraries: usaddress (US), pyap (international).

## New field notification
When a scraper encounters fields not in Tier 1-2, it MUST:

1. Add the field as a Tier 3 column.
2. Print a summary at end of run:

   NEW FIELDS DETECTED in {site}:
   - "Bio" (847/1200 rows, 71%)
   - "Years of Experience" (1104/1200 rows, 92%)

3. Write the same summary to:
   outputs/{site}_{date}_new_fields.txt

User decides whether to promote to Tier 2.

## Adding a new scraper
1. Create scrapers/{sitename}_scraper.py
2. Produce output per this contract
3. On first run, review new-field notices
4. Promote universal fields to Tier 2 by editing
   this document

## Address Overrides

Addresses are the one field where general-code fixes
frequently fail due to international variance, site-specific
quirks, and malformed source data. To handle this without
polluting scraper code with special cases, the system
supports PER-ROW OVERRIDES scoped to address fields only.

### Scope
Overrides apply ONLY to these columns:
- Address Line 1
- Address Line 2
- City
- State
- Zip
- Country

Overrides do NOT apply to Name, Phone, Website, etc.
Those get general-code fixes via the patcher.

### Storage
One file per scraper, tracked in git:
scrapers/overrides/{sitename}_address_overrides.json

### Schema
Keyed on Source ID (the guaranteed-unique column).

```
{
  "{source_id}": {
    "reason": "human-readable explanation",
    "address_line_1": "...",
    "address_line_2": "...",
    "city": "...",
    "state": "...",
    "zip": "...",
    "country": "...",
    "reviewed_by": "thomas",
    "reviewed_date": "YYYY-MM-DD"
  }
}
```

Only include fields being overridden. Empty-string means
"set this field to empty." Omitted fields are left as
whatever the scraper produced.

### Application
Overrides are applied POST-scrape, not during scraping:
1. Scraper produces outputs/{site}_{date}.xlsx
2. Override-apply step reads the overrides file
3. Produces outputs/{site}_{date}_final.xlsx with
   overrides merged in
4. Auditor runs on the _final.xlsx

The raw un-overridden output is preserved for comparison.

### Hard cap
If overrides exceed 5% of total rows for a given scraper,
the system reports this as an alert:

    OVERRIDE LIMIT EXCEEDED: {site} has {N} overrides
    covering {X}% of rows. This suggests the general
    parser needs improvement, not more overrides.
    Review the override patterns in {file} and consider
    updating the scraper's address parsing logic.

### Audit reporting
The auditor reads the overrides file (if present) and
reports:
- Total overrides in use
- Override rate (% of rows)
- Pattern clustering (e.g., "18 overrides are for rows
  where Full Address Raw starts with the practice name —
  consider general fix for this pattern")
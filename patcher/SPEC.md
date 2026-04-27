# Phase 3: Patcher

## Goal
Based on auditor and diagnostic output, propose fixes to
scraper code OR to a per-row overrides file. Always
diff-review before applying. Run test-subset reruns to
verify. Max 3 iterations per scraper per cycle.

The patcher is the only component that modifies scraper
code or overrides. Auditor and diagnostic are read-only.

## Input
- Diagnostic report from Phase 2 (audit_reports/*_diagnostic_*.xlsx)
- Audit report from Phase 1 (audit_reports/*_audit_*.xlsx)
- Scraper source code (scrapers/*.py)
- Override file (if exists): scrapers/overrides/{site}_address_overrides.json
- Original scraper output for the subset rerun baseline

## Patcher triggers
The patcher runs in response to one of three signals:

1. Diagnostic report flags scraper bugs that appear in
   multiple rows with a consistent pattern → propose
   general code fix to the scraper.

2. Audit report flags override rate above 5% OR override
   pattern clustering (≥10 overrides with same 'reason') →
   propose general code fix to retire overrides
   (see "Override → General Fix Promotion" below).

3. Diagnostic report flags a true one-off that general
   code can't solve cleanly, AND the affected column is
   an address field → propose a new override entry
   (address fields only), requires user approval before
   writing to the overrides file.

## Process

### 1. Classify each problem pattern
Group diagnostic findings by signature. For each pattern,
determine scope:
- How many rows share this diagnosis?
- Which column(s) are affected?
- Is the problem in scraper logic or source data?
- Have previous fix attempts for this pattern failed?

### 2. Apply decision logic
For each pattern:

    if source_data_is_bad_not_scraper:
        flag_for_manual_review()
        skip_this_pattern()

    elif pattern_affects >= 5 rows AND general_fix_viable:
        propose_general_code_fix()

    elif pattern_affects < 5 rows AND column in ADDRESS_FIELDS:
        propose_override_entry()

    elif general_fix_attempted_twice_and_failed:
        fall_back_to_overrides()  # only for ADDRESS_FIELDS

    else:
        flag_for_manual_review()

    ADDRESS_FIELDS = [
        "Address Line 1", "Address Line 2",
        "City", "State", "Zip", "Country"
    ]

### 3. General code fix path
If proposing a general fix:
a. Read the relevant scraper code
b. Identify the function/selector/regex to change
c. Write proposed diff
d. Show diff to user — REQUIRE APPROVAL
e. On approval:
   i.   Git commit current scraper (auto-rollback point)
   ii.  Apply the patch
   iii. Run scraper on the SUBSET of originally-flagged
        rows PLUS ~20 known-good rows as regression check
   iv.  Re-audit the subset output
   v.   Report: issues fixed? regressions introduced?
f. If regressions: auto-revert patch, log failure reason
g. If fix worked: leave applied, move to next pattern
h. If fix didn't work: count as one attempt, try different
   approach OR fall back to override path

### 4. Override entry path
If proposing an override entry:
a. Extract Source ID, the affected address fields, and
   the correct values (from diagnostic's "Expected" data)
b. Show user the proposed override entry:

        Proposed override for AAEP Source ID 65159:
          reason: "International address, parser stuffed
                   practice name into Address Line 1"
          address_line_1: ""
          city: "Dubai"
          state: "Dubai"
          zip: "123455"
          country: "UAE"

c. REQUIRE APPROVAL before writing
d. On approval, append to:
   scrapers/overrides/{site}_address_overrides.json
e. Git commit the override file (versioned history)

### 5. Iterate
Continue until:
- All patterns processed, OR
- 3 patch cycles completed (hard cap), OR
- Overall quality score reaches 92% threshold

Report final state: patches applied, overrides added,
patterns skipped, regressions avoided.

## Override → General Fix Promotion

When the auditor reports override rate > 5% OR when
override clustering shows ≥10 overrides with the same
'reason' field, the patcher enters PROMOTION MODE:

1. Read scrapers/overrides/{site}_address_overrides.json
2. Group entries by 'reason' field
3. For each cluster of ≥10 similar overrides:
   a. Analyze what transformation converts the raw
      scraper output to the override value (look at the
      original Full Address Raw vs. the override fields)
   b. If a clean transformation is extractable:
      - Propose general code change implementing it
      - Show diff + list of overrides that would be
        retired
      - REQUIRE APPROVAL
   c. On approval:
      - Apply code fix
      - Re-run scraper on affected rows
      - Compare new output to override values
      - If match: delete those overrides from the JSON
        (they're now handled by general code)
      - If mismatch: revert code fix, keep overrides
4. If no clean transformation extractable for a cluster:
   Report: "These {N} overrides don't share a common
   transformation I can extract cleanly. Keeping them
   as overrides." — this is a valid outcome.

This mechanism is how the system learns from accumulated
overrides and upgrades the general parser over time.
Overrides are short-term memory; scraper code is
long-term memory; promotion moves patterns from one to
the other.

## Guardrails (hard — never bypass)

- Iteration cap: 3 patch cycles per scraper per run
- Mandatory diff review (no auto-apply, ever)
- Auto-git-commit before every code change (rollback
  insurance)
- Regression test on known-good subset is MANDATORY
  before accepting a general code fix
- Never trigger a full scraper rerun — subset reruns
  only (20-100 rows max per verification)
- If overall qu
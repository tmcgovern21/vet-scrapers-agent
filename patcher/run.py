"""
Phase 3 (Stage 1) patcher: deterministic regex fixes + per-source overrides.

CLI: python patcher/run.py --input outputs/{site}_{date}.xlsx [--source {site}]

Reads all sheets from the input. Patches the Members sheet through the
pipeline. Re-derives US Only / International from the patched Members so
overrides that move rows across groups stay consistent. The original
Summary sheet is preserved as-is (informational; audit gives current
stats). A "Patch Log" sheet listing every change is appended.
"""

import argparse
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from patches import (
    fix_whitespace,
    fix_address_parentheticals,
    fix_po_box_line1,
    fix_uk_postcode,
    fix_canada_province_postal,
    fix_uk_address_split,
    fix_canada_address_split,
    fix_generic_intl_address_split,
    apply_overrides,
    post_override_us_parse,
    US_STATE_CODES,
    US_COUNTRY_NAMES,
)

PATCH_PIPELINE = [
    fix_whitespace,
    fix_address_parentheticals,        # must run before any address parsing
    fix_po_box_line1,
    fix_uk_postcode,
    fix_uk_address_split,
    fix_generic_intl_address_split,
    apply_overrides,                   # flips Country for mistagged rows
    # Canada and US fixes run AFTER overrides so newly-flipped rows get
    # the right country-specific parsing (e.g. source 376 was tagged US,
    # override flips it to Canada; fix_canada_* must run post-flip to
    # extract the postal code from the address).
    fix_canada_province_postal,
    fix_canada_address_split,
    post_override_us_parse,
]


def derive_source(filename: str) -> str:
    """'horsedvm_2026-04-27.xlsx' -> 'horsedvm'"""
    return Path(filename).stem.split("_")[0].lower()


def is_us_row(row) -> bool:
    c = str(row.get("Country", "")).strip().lower()
    if c in US_COUNTRY_NAMES:
        return True
    if not c:
        return str(row.get("State", "")).strip().upper() in US_STATE_CODES
    return False


def write_patched(out_path, sheets, log_df):
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for sheet_name, sheet_df in sheets.items():
            sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)
            ws = writer.sheets[sheet_name]
            ws.freeze_panes = "A2"
            for col in ws.columns:
                max_len = max(
                    (len(str(c.value)) for c in col if c.value is not None),
                    default=10,
                )
                ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 60)
        log_df.to_excel(writer, sheet_name="Patch Log", index=False)
        ws = writer.sheets["Patch Log"]
        ws.freeze_panes = "A2"
        for col in ws.columns:
            max_len = max(
                (len(str(c.value)) for c in col if c.value is not None),
                default=10,
            )
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 60)


def main():
    ap = argparse.ArgumentParser(
        description="Apply deterministic patches + per-source overrides to a scraper xlsx"
    )
    ap.add_argument("--input", required=True)
    ap.add_argument("--source", default=None,
                    help="Source name for override lookup. "
                         "Defaults to the first underscore-token of the filename.")
    args = ap.parse_args()

    src = (args.source or derive_source(args.input)).lower()
    inp = Path(args.input)
    out = inp.parent / f"{inp.stem}_patched{inp.suffix}"

    print(f"\n{'='*55}")
    print(f"  Patcher (Stage 1: deterministic + overrides)")
    print(f"  Input:  {inp}")
    print(f"  Output: {out}")
    print(f"  Source: {src}")
    print(f"{'='*55}\n")

    if not inp.exists():
        print(f"ERROR: input not found: {inp}")
        sys.exit(1)

    sheets = pd.read_excel(inp, sheet_name=None)
    if "Members" not in sheets:
        print("ERROR: input has no 'Members' sheet")
        sys.exit(1)

    members = sheets["Members"].fillna("")

    all_changes = []
    for fn in PATCH_PIPELINE:
        members, changes = fn(members, source=src)
        all_changes.extend(changes)
        print(f"  {fn.__name__:32}  {len(changes)} change(s)")

    # Re-derive US Only / International from the patched Members so the
    # split sheets stay consistent with overrides that flip Country.
    us_mask = members.apply(is_us_row, axis=1)
    us_df   = members[us_mask].reset_index(drop=True)
    intl_df = members[~us_mask].reset_index(drop=True)

    sheets["Members"] = members
    if "US Only" in sheets:
        sheets["US Only"] = us_df
    if "International" in sheets:
        sheets["International"] = intl_df
    # Summary preserved as-is (auditor only re-reads Members).

    log_df = pd.DataFrame(
        all_changes, columns=["source_id", "field", "before", "after", "fix_name"]
    )

    write_patched(out, sheets, log_df)

    print()
    print(f"Total changes: {len(all_changes)}")
    print(f"Wrote {out}")
    if all_changes:
        ctr = Counter(c["fix_name"] for c in all_changes)
        print()
        print("Changes by fix:")
        for k, v in ctr.most_common():
            print(f"  {k:32}  {v}")
    print()


if __name__ == "__main__":
    main()

"""
Deterministic patches applied to a scraper's xlsx output.

Each patch function takes (df, source=None) and returns (df, changes), where
changes is a list of dicts with keys: source_id, field, before, after, fix_name.

Patches operate on the scraper's literal column names (no alias normalization).
"""

import json
import re
from pathlib import Path

import pandas as pd

OVERRIDES_DIR = Path(__file__).parent.parent / "scrapers" / "overrides"


# UK postcodes: 1-2 letters + digit + optional letter/digit, then digit + 2 letters.
UK_POSTCODE_RE = re.compile(
    r"\b([A-Z]{1,2}\d[A-Z\d]?)\s*(\d[A-Z]{2})\b",
    re.IGNORECASE,
)

CA_PROVINCE_CODES = frozenset({
    "AB", "BC", "MB", "NB", "NL", "NS", "NT",
    "NU", "ON", "PE", "QC", "SK", "YT",
})
CA_PROVINCE_RE = re.compile(r"\b(" + "|".join(CA_PROVINCE_CODES) + r")\b")

# Canadian postal: A1A 1A1 (D F I O Q U excluded by Canada Post).
CA_POSTAL_RE = re.compile(
    r"\b([ABCEGHJ-NPRSTVXY]\d[A-Z])\s*(\d[A-Z]\d)\b",
    re.IGNORECASE,
)

PO_BOX_RE = re.compile(r"^\s*(P\.?\s*O\.?\s*Box\s+\S+)", re.IGNORECASE)


def _record(changes, source_id, field, before, after, fix_name):
    if str(before) == str(after):
        return
    changes.append({
        "source_id": str(source_id),
        "field":     field,
        "before":    "" if before is None else str(before),
        "after":     "" if after is None else str(after),
        "fix_name":  fix_name,
    })


def _get(row, col, default=""):
    if col not in row.index:
        return default
    v = row[col]
    if pd.isna(v):
        return default
    return v


def fix_whitespace(df, source=None):
    """Collapse runs of whitespace in Full Address Raw and About / Description."""
    changes = []
    targets = [c for c in ("Full Address Raw", "About / Description")
               if c in df.columns]
    for col in targets:
        for i in df.index:
            val = df.at[i, col]
            if not isinstance(val, str) or not val:
                continue
            cleaned = re.sub(r"\s+", " ", val).strip()
            if cleaned != val:
                sid = _get(df.loc[i], "Source ID")
                _record(changes, sid, col, val, cleaned, "fix_whitespace")
                df.at[i, col] = cleaned
    return df, changes


def fix_po_box_line1(df, source=None):
    """If Address Line 1 empty and Full Address Raw starts with PO Box,
    extract the PO Box span (through next comma)."""
    changes = []
    if "Address Line 1" not in df.columns or "Full Address Raw" not in df.columns:
        return df, changes
    for i in df.index:
        line1 = _get(df.loc[i], "Address Line 1")
        full  = _get(df.loc[i], "Full Address Raw")
        if line1 or not full:
            continue
        if not PO_BOX_RE.match(full):
            continue
        before_comma = full.split(",", 1)[0].strip()
        candidate = before_comma if PO_BOX_RE.match(before_comma) \
                    else PO_BOX_RE.match(full).group(1)
        sid = _get(df.loc[i], "Source ID")
        _record(changes, sid, "Address Line 1", "", candidate, "fix_po_box_line1")
        df.at[i, "Address Line 1"] = candidate
    return df, changes


def fix_uk_postcode(df, source=None):
    """UK rows with empty Zip: extract postcode from Full Address Raw."""
    changes = []
    if "Country" not in df.columns or "Zip" not in df.columns:
        return df, changes
    for i in df.index:
        if _get(df.loc[i], "Country") != "United Kingdom":
            continue
        if _get(df.loc[i], "Zip"):
            continue
        full = _get(df.loc[i], "Full Address Raw")
        if not full:
            continue
        m = UK_POSTCODE_RE.search(full)
        if not m:
            continue
        postcode = (m.group(1) + " " + m.group(2)).upper()
        sid = _get(df.loc[i], "Source ID")
        _record(changes, sid, "Zip", "", postcode, "fix_uk_postcode")
        df.at[i, "Zip"] = postcode
    return df, changes


def fix_canada_province_postal(df, source=None):
    """Canada rows: fill empty State from province code; empty Zip from postal."""
    changes = []
    if "Country" not in df.columns:
        return df, changes
    for i in df.index:
        if _get(df.loc[i], "Country") != "Canada":
            continue
        full = _get(df.loc[i], "Full Address Raw")
        if not full:
            continue
        if "State" in df.columns and not _get(df.loc[i], "State"):
            mp = CA_PROVINCE_RE.search(full)
            if mp:
                code = mp.group(1).upper()
                sid = _get(df.loc[i], "Source ID")
                _record(changes, sid, "State", "", code, "fix_canada_province_postal")
                df.at[i, "State"] = code
        if "Zip" in df.columns and not _get(df.loc[i], "Zip"):
            mz = CA_POSTAL_RE.search(full)
            if mz:
                postal = (mz.group(1) + " " + mz.group(2)).upper()
                sid = _get(df.loc[i], "Source ID")
                _record(changes, sid, "Zip", "", postal, "fix_canada_province_postal")
                df.at[i, "Zip"] = postal
    return df, changes


def apply_overrides(df, source=None):
    """Apply per-Source-ID overrides from scrapers/overrides/{source}_overrides.json.
    Keys starting with '_' (like '_reason') are skipped — they're metadata."""
    changes = []
    if not source:
        return df, changes
    path = OVERRIDES_DIR / f"{source}_overrides.json"
    if not path.exists():
        return df, changes
    with open(path, "r", encoding="utf-8") as f:
        overrides = json.load(f)
    if "Source ID" not in df.columns:
        return df, changes
    sid_str = df["Source ID"].astype(str)
    for sid, fields in overrides.items():
        mask = sid_str == str(sid)
        if not mask.any():
            continue
        idx = df.index[mask][0]
        for field, new_val in fields.items():
            if field.startswith("_"):
                continue
            if field not in df.columns:
                continue
            before = _get(df.loc[idx], field)
            if str(before) != str(new_val):
                _record(changes, sid, field, before, new_val, "apply_overrides")
                df.at[idx, field] = new_val
    return df, changes

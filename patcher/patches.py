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

try:
    import usaddress
    HAS_USADDRESS = True
except ImportError:
    HAS_USADDRESS = False

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


# US validation — used by post_override_us_parse and shared with run.py.
US_STATE_CODES = frozenset([
    "AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL",
    "IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE",
    "NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD",
    "TN","TX","UT","VT","VA","WA","WV","WI","WY",
    "AS","GU","MP","PR","VI",
])
US_COUNTRY_NAMES = {
    "us", "usa", "u.s.", "u.s.a.",
    "united states", "united states of america",
}


# UK ceremonial counties + Welsh principal areas (single-word).
UK_COUNTIES = frozenset({
    "Bedfordshire","Berkshire","Buckinghamshire","Cambridgeshire","Cheshire",
    "Cornwall","Cumbria","Derbyshire","Devon","Dorset","Durham","Essex",
    "Gloucestershire","Hampshire","Herefordshire","Hertfordshire","Kent",
    "Lancashire","Leicestershire","Lincolnshire","Norfolk","Northamptonshire",
    "Northumberland","Nottinghamshire","Oxfordshire","Rutland","Shropshire",
    "Somerset","Staffordshire","Suffolk","Surrey","Warwickshire","Wiltshire",
    "Worcestershire","Merseyside","Bristol",
    "Anglesey","Cardiff","Carmarthenshire","Ceredigion","Conwy","Denbighshire",
    "Flintshire","Gwynedd","Monmouthshire","Pembrokeshire","Powys","Swansea",
    "Wrexham",
    "Aberdeenshire","Angus","Fife","Highland","Lanarkshire","Moray",
})
# Multi-word — checked first (longest prefix wins).
UK_COUNTIES_MULTIWORD = (
    "North Yorkshire","South Yorkshire","West Yorkshire","East Yorkshire",
    "West Midlands","East Midlands","Tyne and Wear","East Sussex","West Sussex",
    "South Glamorgan","West Glamorgan","Mid Glamorgan","Vale of Glamorgan",
    "Greater London","Greater Manchester","Isle of Wight","Isle of Man",
    "County Durham","County Antrim","County Down","County Armagh",
    "County Tyrone","County Fermanagh","County Londonderry",
)

# Canadian province name → 2-letter code.
CA_PROVINCE_NAMES = {
    "alberta":"AB","british columbia":"BC","manitoba":"MB","new brunswick":"NB",
    "newfoundland":"NL","newfoundland and labrador":"NL","nova scotia":"NS",
    "northwest territories":"NT","nunavut":"NU","ontario":"ON",
    "prince edward island":"PE","quebec":"QC","saskatchewan":"SK","yukon":"YT",
}

# Countries that use the simple "Line 1 = first segment" heuristic.
INTL_COUNTRIES_FOR_GENERIC = {
    "Belgium","Sweden","Finland","South Africa","Switzerland",
    "Netherlands","Germany","France","Italy","Spain","Portugal",
    "Norway","Denmark","Ireland","Austria",
}


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


def fix_address_parentheticals(df, source=None):
    """Strip '(...)' content from Full Address Raw before any address parsing.
    usaddress chokes on parentheticals (e.g. row 387: 'Parsons Rd (Old US 219)')."""
    changes = []
    if "Full Address Raw" not in df.columns:
        return df, changes
    for i in df.index:
        val = df.at[i, "Full Address Raw"]
        if not isinstance(val, str) or "(" not in val or ")" not in val:
            continue
        cleaned = re.sub(r"\s*\([^)]*\)\s*", " ", val)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        cleaned = re.sub(r"\s+,", ",", cleaned)
        if cleaned != val:
            sid = _get(df.loc[i], "Source ID")
            _record(changes, sid, "Full Address Raw", val, cleaned,
                    "fix_address_parentheticals")
            df.at[i, "Full Address Raw"] = cleaned
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


_CA_POSTAL_VALID_RE = re.compile(
    r"^[ABCEGHJ-NPRSTVXY]\d[A-Z]\s?\d[A-Z]\d$", re.IGNORECASE,
)


def fix_canada_province_postal(df, source=None):
    """Canada rows: fill empty State from province code; fill OR replace Zip
    from postal pattern. Replaces any existing Zip that doesn't match the
    canonical Canadian postal format (the scraper's usaddress run on
    US-tagged-but-actually-Canadian addresses can produce garbage like
    '0N0' from 'R0G 0N0' — see source 376)."""
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
        if "Zip" in df.columns:
            mz = CA_POSTAL_RE.search(full)
            if mz:
                postal = (mz.group(1) + " " + mz.group(2)).upper()
                current = str(_get(df.loc[i], "Zip")).strip()
                # Replace if empty OR not a valid Canadian postal (scraper
                # garbage like "0N0" should be overwritten with "R0G 0N0").
                if not _CA_POSTAL_VALID_RE.match(current.upper()):
                    sid = _get(df.loc[i], "Source ID")
                    _record(changes, sid, "Zip", current, postal,
                            "fix_canada_province_postal")
                    df.at[i, "Zip"] = postal
    return df, changes


def _split_uk_postcode_segment(text):
    """Given the last comma-segment with the postcode already stripped,
    return (city, county). county = trailing word(s) matching UK_COUNTIES."""
    if not text:
        return "", ""
    text = text.strip()
    for cty in UK_COUNTIES_MULTIWORD:  # multi-word first (longest match)
        if text == cty or text.endswith(" " + cty):
            return text[: len(text) - len(cty)].strip(), cty
    parts = text.split()
    if parts and parts[-1] in UK_COUNTIES:
        return " ".join(parts[:-1]), parts[-1]
    return text, ""


def fix_uk_address_split(df, source=None):
    """UK rows: Line 1 = first comma-segment. From the last segment with
    postcode stripped, derive City and (optional) State (UK county).
    Falls back to second-to-last segment for City if the last segment is
    county-only (e.g. 'Fourways, ..., Stroud, Gloucestershire GL6 7PH')."""
    changes = []
    if "Country" not in df.columns or "Full Address Raw" not in df.columns:
        return df, changes
    for i in df.index:
        if _get(df.loc[i], "Country") != "United Kingdom":
            continue
        full = _get(df.loc[i], "Full Address Raw")
        if not full:
            continue
        segments = [s.strip() for s in full.split(",") if s.strip()]
        if not segments:
            continue
        sid = _get(df.loc[i], "Source ID")

        if "Address Line 1" in df.columns and not _get(df.loc[i], "Address Line 1"):
            _record(changes, sid, "Address Line 1", "", segments[0],
                    "fix_uk_address_split")
            df.at[i, "Address Line 1"] = segments[0]

        # Strip postcode from last segment, then split city/county.
        last_no_pc = UK_POSTCODE_RE.sub("", segments[-1]).strip()
        last_no_pc = re.sub(r"\s+", " ", last_no_pc)
        city, state = _split_uk_postcode_segment(last_no_pc)

        # If the last segment was county-only, take City from second-to-last —
        # but only if it's not the same string we used for Line 1.
        if not city and len(segments) >= 2 and segments[-2] != segments[0]:
            city = segments[-2]
        elif not city and len(segments) >= 3:
            city = segments[-2]

        if city and "City" in df.columns and not _get(df.loc[i], "City"):
            _record(changes, sid, "City", "", city, "fix_uk_address_split")
            df.at[i, "City"] = city
        if state and "State" in df.columns and not _get(df.loc[i], "State"):
            _record(changes, sid, "State", "", state, "fix_uk_address_split")
            df.at[i, "State"] = state
    return df, changes


def fix_canada_address_split(df, source=None):
    """Canada rows: Line 1 = first segment, City = middle segment (or last
    segment minus province+postal). Convert full province names to 2-letter
    codes. State and Zip already populated by fix_canada_province_postal
    where a 2-letter code or postal is present in the source."""
    changes = []
    if "Country" not in df.columns or "Full Address Raw" not in df.columns:
        return df, changes
    for i in df.index:
        if _get(df.loc[i], "Country") != "Canada":
            continue
        full = _get(df.loc[i], "Full Address Raw")
        if not full:
            continue
        segments = [s.strip() for s in full.split(",") if s.strip()]
        if not segments:
            continue
        sid = _get(df.loc[i], "Source ID")

        if "Address Line 1" in df.columns and not _get(df.loc[i], "Address Line 1"):
            _record(changes, sid, "Address Line 1", "", segments[0],
                    "fix_canada_address_split")
            df.at[i, "Address Line 1"] = segments[0]

        if "City" in df.columns and not _get(df.loc[i], "City"):
            city = ""
            if len(segments) >= 3:
                city = segments[1]
            elif len(segments) == 2:
                last = CA_POSTAL_RE.sub("", segments[1])
                last = CA_PROVINCE_RE.sub("", last)
                low = last.lower()
                for name in sorted(CA_PROVINCE_NAMES, key=len, reverse=True):
                    if low.endswith(" " + name) or low == name:
                        last = last[: len(last) - len(name)]
                        break
                    if low.startswith(name + " ") or low == name:
                        last = last[len(name):]
                        break
                city = re.sub(r"\s+", " ", last).strip()
            if city:
                _record(changes, sid, "City", "", city,
                        "fix_canada_address_split")
                df.at[i, "City"] = city

        if "State" in df.columns and not _get(df.loc[i], "State"):
            last_low = segments[-1].lower()
            for name, code in sorted(
                CA_PROVINCE_NAMES.items(), key=lambda kv: -len(kv[0])
            ):
                if name in last_low:
                    _record(changes, sid, "State", "", code,
                            "fix_canada_address_split")
                    df.at[i, "State"] = code
                    break
    return df, changes


def fix_generic_intl_address_split(df, source=None):
    """Belgium / Sweden / Finland / South Africa / Switzerland (and similar):
    extract Address Line 1 from the first comma-separated segment.
    City/State left as-is (often unparseable without country-specific logic).
    Per spec — keep this function deliberately narrow."""
    changes = []
    if ("Country" not in df.columns or "Full Address Raw" not in df.columns
            or "Address Line 1" not in df.columns):
        return df, changes
    for i in df.index:
        if _get(df.loc[i], "Country") not in INTL_COUNTRIES_FOR_GENERIC:
            continue
        if _get(df.loc[i], "Address Line 1"):
            continue
        full = _get(df.loc[i], "Full Address Raw")
        if not full:
            continue
        segments = [s.strip() for s in full.split(",") if s.strip()]
        if not segments:
            continue
        sid = _get(df.loc[i], "Source ID")
        _record(changes, sid, "Address Line 1", "", segments[0],
                "fix_generic_intl_address_split")
        df.at[i, "Address Line 1"] = segments[0]
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


def post_override_us_parse(df, source=None):
    """Re-run usaddress on US rows whose Address Line 1 is empty.
    Catches: rows whose Country was overridden to US (overrides flip Country
    but don't touch parsed fields), and rows where the original scraper-time
    parse failed (e.g. parentheticals — now stripped by
    fix_address_parentheticals earlier in the pipeline)."""
    changes = []
    if not HAS_USADDRESS:
        return df, changes
    if ("Country" not in df.columns or "Full Address Raw" not in df.columns
            or "Address Line 1" not in df.columns):
        return df, changes
    for i in df.index:
        country = str(_get(df.loc[i], "Country")).strip().lower()
        if country not in US_COUNTRY_NAMES:
            continue
        # Gate: re-parse if ANY US address field is empty. (Gating only on
        # Line 1 misses the Canada→US override case where the Canadian
        # splitter populated Line 1+City but Zip stayed empty — row 363.)
        needs_fill = any(
            not _get(df.loc[i], c)
            for c in ("Address Line 1", "City", "State", "Zip")
            if c in df.columns
        )
        if not needs_fill:
            continue
        full = _get(df.loc[i], "Full Address Raw")
        if not full:
            continue
        try:
            tagged, _ = usaddress.tag(full)
        except Exception:
            continue

        sid = _get(df.loc[i], "Source ID")

        street_parts = []
        for k in ("AddressNumber", "StreetNamePreDirectional", "StreetName",
                  "StreetNamePostType", "StreetNamePostDirectional",
                  "OccupancyType", "OccupancyIdentifier"):
            v = tagged.get(k, "")
            if v:
                street_parts.append(v)
        if street_parts:
            line1 = " ".join(street_parts)
            _record(changes, sid, "Address Line 1", "", line1,
                    "post_override_us_parse")
            df.at[i, "Address Line 1"] = line1

        if "City" in df.columns and not _get(df.loc[i], "City"):
            city = tagged.get("PlaceName", "")
            if city:
                _record(changes, sid, "City", "", city,
                        "post_override_us_parse")
                df.at[i, "City"] = city

        if "State" in df.columns and not _get(df.loc[i], "State"):
            cand = (tagged.get("StateName", "") or "").strip().upper()
            if cand in US_STATE_CODES:
                _record(changes, sid, "State", "", cand,
                        "post_override_us_parse")
                df.at[i, "State"] = cand

        if "Zip" in df.columns and not _get(df.loc[i], "Zip"):
            zipc = tagged.get("ZipCode", "")
            if zipc:
                _record(changes, sid, "Zip", "", zipc,
                        "post_override_us_parse")
                df.at[i, "Zip"] = zipc

    return df, changes

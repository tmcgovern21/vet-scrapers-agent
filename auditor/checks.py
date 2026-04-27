"""Data-quality checks for scraper output.

Each check returns a CheckResult with a full-length pass_mask. Rows
where a check is not applicable (cell empty / column missing) pass
vacuously, so masks can be unioned without special-casing.

tier convention:
  1, 2, 3 — per-column, matches SCRAPER_CONTRACT.md tiers
  0       — cross-column (dead rows, likely dupes, parse consistency)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable

import pandas as pd

from auditor.aliases import TIER_1, TIER_2
from auditor.loader import LoadMeta


# -------- Data model --------

@dataclass
class CheckResult:
    check_name: str
    column: str                     # "" for cross-column checks
    tier: int
    pass_mask: pd.Series            # bool, len == len(df)
    problem_values: dict[int, object] = field(default_factory=dict)


# -------- Regexes & constants --------

_URL_RE   = re.compile(r"^https?://\S+$", re.IGNORECASE)
_EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")

_US_ZIP_RE      = re.compile(r"^\d{5}(-\d{4})?$")
_CA_POSTAL_RE   = re.compile(r"^[A-Z]\d[A-Z] ?\d[A-Z]\d$", re.IGNORECASE)
_UK_POSTAL_RE   = re.compile(r"^[A-Z]{1,2}\d[A-Z\d]? ?\d[A-Z]{2}$", re.IGNORECASE)
_GENERIC_POSTAL = re.compile(r"^[A-Z0-9\- ]{3,10}$", re.IGNORECASE)

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2})?")

_STREET_WORD_RE = re.compile(
    r"\b(Street|St\.?|Ave\.?|Avenue|Road|Rd\.?|Suite|Ste\.?|Unit|"
    r"Drive|Dr\.?|Blvd\.?|Boulevard|Highway|Hwy\.?|Apt\.?|"
    r"P\.?\s*O\.?\s*Box|PO\s*Box)\b",
    re.IGNORECASE,
)

_PLACEHOLDER_ADDRESS_SUBS = (
    "view location", "view vet", "see profile",
    "see map", "view map", "location map",
)

_GOOGLE_MAPS_SUBS = ("maps.google", "google.com/maps")

_US_STATE_CODES = frozenset([
    "AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL",
    "IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE",
    "NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD",
    "TN","TX","UT","VT","VA","WA","WV","WI","WY",
    "AS","GU","MP","PR","VI",
])
_US_STATE_NAMES = frozenset([
    "alabama","alaska","arizona","arkansas","california","colorado",
    "connecticut","delaware","district of columbia","florida","georgia",
    "hawaii","idaho","illinois","indiana","iowa","kansas","kentucky",
    "louisiana","maine","maryland","massachusetts","michigan","minnesota",
    "mississippi","missouri","montana","nebraska","nevada","new hampshire",
    "new jersey","new mexico","new york","north carolina","north dakota",
    "ohio","oklahoma","oregon","pennsylvania","rhode island",
    "south carolina","south dakota","tennessee","texas","utah","vermont",
    "virginia","washington","west virginia","wisconsin","wyoming",
    "american samoa","guam","northern mariana islands","puerto rico",
    "virgin islands","us virgin islands",
])
_CA_PROVINCE_CODES = frozenset([
    "AB","BC","MB","NB","NL","NS","NT","NU","ON","PE","QC","SK","YT",
])
_CA_PROVINCE_NAMES = frozenset([
    "alberta","british columbia","manitoba","new brunswick","newfoundland",
    "newfoundland and labrador","nova scotia","northwest territories",
    "nunavut","ontario","prince edward island","quebec","saskatchewan",
    "yukon",
])

_COUNTRY_NAMES = frozenset([
    "united states","united states of america","usa","us","u.s.","u.s.a.",
    "canada","mexico","united kingdom","uk","england","scotland","wales",
    "ireland","australia","new zealand","germany","france","italy","spain",
    "portugal","netherlands","belgium","switzerland","austria","sweden",
    "norway","denmark","finland","iceland","poland","czech republic",
    "hungary","romania","greece","turkey","russia","ukraine","china",
    "japan","south korea","korea","taiwan","singapore","malaysia",
    "thailand","vietnam","philippines","indonesia","india","pakistan",
    "bangladesh","sri lanka","israel","saudi arabia",
    "united arab emirates","uae","egypt","south africa","nigeria","kenya",
    "morocco","brazil","argentina","chile","colombia","peru","ecuador",
    "venezuela",
])
_COUNTRY_CODES = frozenset([
    "US","CA","MX","GB","UK","IE","AU","NZ","DE","FR","IT","ES","PT","NL",
    "BE","CH","AT","SE","NO","DK","FI","IS","PL","CZ","HU","RO","GR","TR",
    "RU","UA","CN","JP","KR","TW","SG","MY","TH","VN","PH","ID","IN","PK",
    "BD","LK","IL","SA","AE","EG","ZA","NG","KE","MA","BR","AR","CL","CO",
    "PE","EC","VE",
])

_US_COUNTRY_SYNONYMS = frozenset([
    "us", "usa", "u.s.", "u.s.a.",
    "united states", "united states of america",
])


def _is_non_us_country(v) -> bool:
    """True when Country is recognizably non-US. Empty / unknown values
    return False (treated as US-like, so US-strict checks still apply)."""
    txt = "" if pd.isna(v) else str(v).strip()
    if not txt:
        return False
    if txt.lower() in _US_COUNTRY_SYNONYMS or txt.upper() == "US":
        return False
    return txt.upper() in _COUNTRY_CODES or txt.lower() in _COUNTRY_NAMES


# -------- Helpers --------

def _is_populated(s: pd.Series) -> pd.Series:
    """True where a cell has real content (non-NaN, non-empty string)."""
    def _one(v):
        if pd.isna(v):
            return False
        if isinstance(v, str) and not v.strip():
            return False
        return True
    return s.apply(_one)


def _as_str(v) -> str:
    if pd.isna(v):
        return ""
    return str(v).strip()


def _problem_dict(s: pd.Series, mask: pd.Series) -> dict[int, object]:
    return {int(i): s.loc[i] for i in s.index[~mask]}


def _always_ok(index) -> pd.Series:
    return pd.Series(True, index=index)


# -------- Tier 1 checks --------

def _missing_column_result(check: str, col: str, tier: int, index) -> CheckResult:
    mask = pd.Series(False, index=index)
    return CheckResult(
        check, col, tier, mask,
        {int(i): "<column missing>" for i in index},
    )


def check_source_site_nonempty(df: pd.DataFrame) -> CheckResult:
    if "Source Site" not in df.columns:
        return _missing_column_result("source_site_nonempty", "Source Site", 1, df.index)
    s = df["Source Site"]
    mask = _is_populated(s)
    return CheckResult("source_site_nonempty", "Source Site", 1, mask, _problem_dict(s, mask))


def check_source_id_nonempty(df: pd.DataFrame) -> CheckResult:
    if "Source ID" not in df.columns:
        return _missing_column_result("source_id_nonempty", "Source ID", 1, df.index)
    s = df["Source ID"]
    mask = _is_populated(s)
    return CheckResult("source_id_nonempty", "Source ID", 1, mask, _problem_dict(s, mask))


def check_source_id_unique(df: pd.DataFrame) -> CheckResult:
    if "Source ID" not in df.columns:
        return CheckResult("source_id_unique", "Source ID", 1, _always_ok(df.index), {})
    s = df["Source ID"]
    pop = _is_populated(s)
    # Duplicates are only meaningful among populated values (empty Source IDs
    # are already flagged by source_id_nonempty).
    str_vals = s.astype(str).str.strip()
    counts = str_vals[pop].value_counts()
    dupe_values = set(counts[counts > 1].index)
    is_dup = pop & str_vals.isin(dupe_values)
    mask = ~is_dup
    return CheckResult("source_id_unique", "Source ID", 1, mask, _problem_dict(s, mask))


def check_profile_url_nonempty(df: pd.DataFrame) -> CheckResult:
    if "Profile URL" not in df.columns:
        return _missing_column_result("profile_url_nonempty", "Profile URL", 1, df.index)
    s = df["Profile URL"]
    mask = _is_populated(s)
    return CheckResult("profile_url_nonempty", "Profile URL", 1, mask, _problem_dict(s, mask))


def check_profile_url_format(df: pd.DataFrame) -> CheckResult:
    if "Profile URL" not in df.columns:
        return CheckResult("profile_url_format", "Profile URL", 1, _always_ok(df.index), {})
    s = df["Profile URL"]
    pop = _is_populated(s)
    mask = pd.Series(True, index=s.index)
    if pop.any():
        mask.loc[pop] = s.loc[pop].apply(lambda v: bool(_URL_RE.match(_as_str(v))))
    return CheckResult("profile_url_format", "Profile URL", 1, mask, _problem_dict(s, mask))


def check_name_nonempty(df: pd.DataFrame) -> CheckResult:
    if "Name" not in df.columns:
        return _missing_column_result("name_nonempty", "Name", 1, df.index)
    s = df["Name"]
    mask = _is_populated(s)
    return CheckResult("name_nonempty", "Name", 1, mask, _problem_dict(s, mask))


def check_name_valid(df: pd.DataFrame) -> CheckResult:
    """Populated Name must: have no digits; have no street-words beyond
    the first whitespace token (so 'Dr. Smith' passes); be 3-100 chars."""
    if "Name" not in df.columns:
        return CheckResult("name_valid", "Name", 1, _always_ok(df.index), {})
    s = df["Name"]
    pop = _is_populated(s)

    def _ok(v) -> bool:
        txt = _as_str(v)
        if not (3 <= len(txt) <= 100):
            return False
        if any(ch.isdigit() for ch in txt):
            return False
        first_space = txt.find(" ")
        first_token_end = len(txt) if first_space < 0 else first_space
        for m in _STREET_WORD_RE.finditer(txt):
            if m.end() > first_token_end:
                return False
        return True

    mask = pd.Series(True, index=s.index)
    if pop.any():
        mask.loc[pop] = s.loc[pop].apply(_ok)
    return CheckResult("name_valid", "Name", 1, mask, _problem_dict(s, mask))


# -------- Tier 2 string checks (given populated subset, return bool Series) --------

def _check_phone(pop: pd.Series) -> pd.Series:
    def _ok(v) -> bool:
        digits = re.sub(r"\D", "", _as_str(v))
        # Reject 15+ digits — no real phone has that many.
        return 7 <= len(digits) <= 14
    return pop.apply(_ok)


def _check_email(pop: pd.Series) -> pd.Series:
    return pop.apply(lambda v: bool(_EMAIL_RE.match(_as_str(v))))


def _check_website_not_maps(pop: pd.Series) -> pd.Series:
    def _ok(v) -> bool:
        txt = _as_str(v)
        if not _URL_RE.match(txt):
            return False
        low = txt.lower()
        return not any(sub in low for sub in _GOOGLE_MAPS_SUBS)
    return pop.apply(_ok)


def _check_google_maps_url(pop: pd.Series) -> pd.Series:
    return pop.apply(
        lambda v: any(sub in _as_str(v).lower() for sub in _GOOGLE_MAPS_SUBS)
    )


def _check_credentials(pop: pd.Series) -> pd.Series:
    def _ok(v) -> bool:
        txt = _as_str(v)
        if not (1 <= len(txt) <= 200):
            return False
        return not any(ch.isdigit() for ch in txt)
    return pop.apply(_ok)


def _check_full_address_raw(pop: pd.Series) -> pd.Series:
    def _ok(v) -> bool:
        txt = _as_str(v)
        low = txt.lower()
        if any(sub in low for sub in _PLACEHOLDER_ADDRESS_SUBS):
            return False
        return any(ch.isdigit() for ch in txt)
    return pop.apply(_ok)


def _check_address_line_1(pop: pd.Series, country: pd.Series | None = None) -> pd.Series:
    """Address Line 1 must not be a URL or a phone-shaped value.
    For US (or unknown) rows, require at least one digit (street number).
    For non-US rows, accept any non-empty content — many UK property
    names ('Breadstone', 'Butchers Lane', 'Duffryn Bach Farm') have no
    street number, and demanding one is a US-bias bug, not a quality
    standard."""
    def _shared_fail(v) -> bool:
        txt = _as_str(v)
        low = txt.lower()
        if "http://" in low or "https://" in low:
            return True
        if re.search(r"[()+]", txt) and len(re.sub(r"\D", "", txt)) >= 7:
            return True
        return False

    if country is None:
        # No Country context — fall back to US-strict.
        def _us_ok(v) -> bool:
            return (not _shared_fail(v)) and any(ch.isdigit() for ch in _as_str(v))
        return pop.apply(_us_ok)

    out = pd.Series(False, index=pop.index)
    for idx in pop.index:
        v = pop.loc[idx]
        if _shared_fail(v):
            out.loc[idx] = False
            continue
        if _is_non_us_country(country.loc[idx]):
            out.loc[idx] = bool(_as_str(v))
        else:
            out.loc[idx] = any(ch.isdigit() for ch in _as_str(v))
    return out


def _check_city(pop: pd.Series) -> pd.Series:
    def _ok(v) -> bool:
        txt = _as_str(v)
        if any(ch.isdigit() for ch in txt):
            return False
        low = txt.lower()
        if "http://" in low or "https://" in low:
            return False
        return 1 <= len(txt) <= 80
    return pop.apply(_ok)


def _check_state(pop: pd.Series) -> pd.Series:
    def _ok(v) -> bool:
        txt = _as_str(v)
        if not txt:
            return False
        if txt.upper() in _US_STATE_CODES or txt.upper() in _CA_PROVINCE_CODES:
            return True
        if txt.lower() in _US_STATE_NAMES or txt.lower() in _CA_PROVINCE_NAMES:
            return True
        if any(ch.isdigit() for ch in txt):
            return False
        return 2 <= len(txt) <= 40 and bool(re.match(r"^[A-Za-z\s\-\.']+$", txt))
    return pop.apply(_ok)


def _check_zip(pop: pd.Series) -> pd.Series:
    def _ok(v) -> bool:
        txt = _as_str(v)
        return bool(
            _US_ZIP_RE.match(txt)
            or _CA_POSTAL_RE.match(txt)
            or _UK_POSTAL_RE.match(txt)
            or _GENERIC_POSTAL.match(txt)
        )
    return pop.apply(_ok)


def _check_country(pop: pd.Series) -> pd.Series:
    def _ok(v) -> bool:
        txt = _as_str(v)
        if txt.upper() in _COUNTRY_CODES or txt.lower() in _COUNTRY_NAMES:
            return True
        if any(ch.isdigit() for ch in txt):
            return False
        return 2 <= len(txt) <= 50 and bool(re.match(r"^[A-Za-z\s\-\.']+$", txt))
    return pop.apply(_ok)


def _check_practice_not_address(pop: pd.Series) -> pd.Series:
    """Practice / Company should not look like an address.

    Catches three patterns: PO Box prefix; leading street number
    followed by a street-type word in the next few tokens; trailing
    5-digit zip (the 'City, State 12345' pattern seen in IVAS row 2).
    """
    def _ok(v) -> bool:
        txt = _as_str(v)
        if re.search(r"\bP\.?\s*O\.?\s*Box\b", txt, re.IGNORECASE):
            return False
        tokens = txt.split()
        if tokens and tokens[0].isdigit():
            tail = " ".join(tokens[1:5])
            if _STREET_WORD_RE.search(tail):
                return False
        if re.search(r"\b\d{5}(-\d{4})?\s*$", txt):
            return False
        return True
    return pop.apply(_ok)


_TIER_2_STRING_CHECKS: dict[str, Callable[[pd.Series], pd.Series]] = {
    "Phone":              _check_phone,
    "Phone 2":            _check_phone,
    "Email":              _check_email,
    "Website":            _check_website_not_maps,
    "Google Maps URL":    _check_google_maps_url,
    "Credentials":        _check_credentials,
    "Full Address Raw":   _check_full_address_raw,
    "Address Line 1":     _check_address_line_1,
    "City":               _check_city,
    "State":              _check_state,
    "Zip":                _check_zip,
    "Country":            _check_country,
    "Practice / Company": _check_practice_not_address,
}

_TIER_2_CHECK_NAMES: dict[str, str] = {
    "Phone":              "phone_format",
    "Phone 2":            "phone_format",
    "Email":              "email_format",
    "Website":            "website_not_maps",
    "Google Maps URL":    "google_maps_url_format",
    "Credentials":        "credentials_valid",
    "Full Address Raw":   "full_address_raw_valid",
    "Address Line 1":     "address_line_1_valid",
    "City":               "city_valid",
    "State":              "state_valid",
    "Zip":                "zip_valid",
    "Country":            "country_valid",
    "Practice / Company": "practice_not_address",
}


def _apply_tier2_string(
    column: str,
    s: pd.Series,
    check_fn: Callable[..., pd.Series],
    df: pd.DataFrame | None = None,
) -> CheckResult:
    pop = _is_populated(s)
    mask = pd.Series(True, index=s.index)
    if pop.any():
        pop_idx = s.index[pop]
        # Address Line 1 is Country-aware: UK property names lack street
        # numbers and shouldn't fail the US-style "must contain a digit" rule.
        if column == "Address Line 1" and df is not None and "Country" in df.columns:
            mask.loc[pop_idx] = check_fn(
                s.loc[pop_idx], country=df["Country"].loc[pop_idx]
            )
        else:
            mask.loc[pop_idx] = check_fn(s.loc[pop_idx])
    return CheckResult(
        _TIER_2_CHECK_NAMES[column], column, 2, mask, _problem_dict(s, mask)
    )


def _apply_latlng(column: str, s: pd.Series, lo: float, hi: float) -> CheckResult:
    coerced = pd.to_numeric(s, errors="coerce")
    pop = ~coerced.isna()
    mask = pd.Series(True, index=s.index)
    if pop.any():
        mask.loc[pop] = coerced.loc[pop].between(lo, hi)
    check_name = "latitude_range" if column == "Latitude" else "longitude_range"
    return CheckResult(check_name, column, 2, mask, _problem_dict(s, mask))


# -------- Tier 3 name-hint checks --------

def _tier3_name_hints(column: str, s: pd.Series) -> list[CheckResult]:
    low = column.lower()
    pop = _is_populated(s)

    def _run(name_suffix: str, test: Callable[[str], bool]) -> CheckResult:
        mask = pd.Series(True, index=s.index)
        if pop.any():
            mask.loc[pop] = s.loc[pop].apply(lambda v: test(_as_str(v)))
        return CheckResult(
            f"{column}:{name_suffix}", column, 3, mask, _problem_dict(s, mask)
        )

    results: list[CheckResult] = []
    if "url" in low:
        results.append(_run("url_format", lambda t: bool(_URL_RE.match(t))))
    if "email" in low:
        results.append(_run("email_format", lambda t: bool(_EMAIL_RE.match(t))))
    if "phone" in low:
        def _phone_ok(t: str) -> bool:
            d = re.sub(r"\D", "", t)
            return 7 <= len(d) <= 14
        results.append(_run("phone_format", _phone_ok))
    if "date" in low:
        results.append(_run("date_format", lambda t: bool(_ISO_DATE_RE.match(t))))
    return results


# -------- Cross-column checks (tier=0) --------

def check_dead_rows(df: pd.DataFrame) -> CheckResult:
    relevant = [c for c in df.columns if c in TIER_1 or c in TIER_2]
    if not relevant:
        return CheckResult("dead_rows", "", 0, _always_ok(df.index), {})
    populated_any = pd.Series(False, index=df.index)
    for c in relevant:
        sub = df[c]
        if isinstance(sub, pd.DataFrame):  # duplicate column name
            continue
        populated_any = populated_any | _is_populated(sub)
    mask = populated_any
    pv = {int(i): "<all Tier 1+2 empty>" for i in df.index[~mask]}
    return CheckResult("dead_rows", "", 0, mask, pv)


def check_likely_dupes(df: pd.DataFrame) -> CheckResult:
    required = ["Name", "Practice / Company", "State"]
    if not all(c in df.columns for c in required):
        return CheckResult("likely_dupes", "", 0, _always_ok(df.index), {})
    for c in required:
        if isinstance(df[c], pd.DataFrame):
            return CheckResult("likely_dupes", "", 0, _always_ok(df.index), {})

    pop = pd.Series(True, index=df.index)
    for c in required:
        pop = pop & _is_populated(df[c])

    def _s(c: str) -> pd.Series:
        return df[c].astype(str).str.strip().str.lower()

    tuples = pd.Series(
        list(zip(_s("Name"), _s("Practice / Company"), _s("State"))),
        index=df.index,
    )
    tuples_pop = tuples.where(pop, other=None)
    counts = tuples_pop[pop].value_counts()
    dupe_tuples = set(counts[counts > 1].index)
    is_dup = pop & tuples_pop.isin(dupe_tuples)
    mask = ~is_dup
    pv = {int(i): f"dup of {tuples_pop.loc[i]}" for i in df.index[is_dup]}
    return CheckResult("likely_dupes", "", 0, mask, pv)


def check_address_parse_consistency(df: pd.DataFrame) -> CheckResult:
    if "Full Address Raw" not in df.columns:
        return CheckResult(
            "address_parse_consistency", "", 0, _always_ok(df.index), {}
        )
    far = df["Full Address Raw"]
    if isinstance(far, pd.DataFrame):
        return CheckResult(
            "address_parse_consistency", "", 0, _always_ok(df.index), {}
        )
    parsed_cols = [
        c for c in ("Address Line 1", "City", "State") if c in df.columns
    ]
    if not parsed_cols:
        return CheckResult(
            "address_parse_consistency", "", 0, _always_ok(df.index), {}
        )

    far_pop = _is_populated(far)
    any_parsed = pd.Series(False, index=df.index)
    for c in parsed_cols:
        sub = df[c]
        if isinstance(sub, pd.DataFrame):
            continue
        any_parsed = any_parsed | _is_populated(sub)

    fail = far_pop & (~any_parsed)
    mask = ~fail
    pv = {int(i): _as_str(far.loc[i]) for i in df.index[fail]}
    return CheckResult("address_parse_consistency", "", 0, mask, pv)


# -------- Top-level orchestration --------

TIER_1_CHECKS: list[Callable[[pd.DataFrame], CheckResult]] = [
    check_source_site_nonempty,
    check_source_id_nonempty,
    check_source_id_unique,
    check_profile_url_nonempty,
    check_profile_url_format,
    check_name_nonempty,
    check_name_valid,
]

CROSS_COLUMN_CHECKS: list[Callable[[pd.DataFrame], CheckResult]] = [
    check_dead_rows,
    check_likely_dupes,
    check_address_parse_consistency,
]


def run_all_checks(df: pd.DataFrame, meta: LoadMeta) -> list[CheckResult]:
    results: list[CheckResult] = []

    for fn in TIER_1_CHECKS:
        results.append(fn(df))

    for col, check_fn in _TIER_2_STRING_CHECKS.items():
        if col not in df.columns or col in meta.duplicate_normalized:
            continue
        sub = df[col]
        if isinstance(sub, pd.DataFrame):
            continue
        results.append(_apply_tier2_string(col, sub, check_fn, df))

    if "Latitude" in df.columns and "Latitude" not in meta.duplicate_normalized:
        results.append(_apply_latlng("Latitude", df["Latitude"], -90.0, 90.0))
    if "Longitude" in df.columns and "Longitude" not in meta.duplicate_normalized:
        results.append(_apply_latlng("Longitude", df["Longitude"], -180.0, 180.0))

    for col in df.columns:
        if col in TIER_1 or col in TIER_2 or col in meta.duplicate_normalized:
            continue
        sub = df[col]
        if isinstance(sub, pd.DataFrame):
            continue
        results.extend(_tier3_name_hints(col, sub))

    for fn in CROSS_COLUMN_CHECKS:
        results.append(fn(df))

    return results

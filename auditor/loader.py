"""Load a scraper .xlsx into a normalized DataFrame.

Responsibilities:
1. Pick the main data sheet (skip Summary / By State / Stats).
2. Detect the Chi-style header-row bug (title row above real header).
3. Normalize column names via COLUMN_ALIASES.
4. Normalize empty cells in string columns (NaN / whitespace -> "").
   Numeric columns keep their dtype and their NaN.
5. Return (df, LoadMeta).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd

from auditor.aliases import KNOWN_COLUMN_NAMES, TIER_1, normalize

# Sheets that are never the main data sheet (match case-insensitively).
_METADATA_SHEET_NAMES = {"summary", "by state", "stats"}

# Verbatim from auditor/SPEC.md §"Header-row bug detection". Do not edit.
HEADER_ROW_BUG_MESSAGE = (
    "LIKELY HEADER-ROW BUG: The first data row appears\n"
    "to contain column headers. This typically happens\n"
    "when the scraper wrote a title line above the\n"
    "real header. Fix the scraper or re-load with\n"
    "skiprows=1."
)

# How many recognized column names a data row must contain before the
# detector declares a header-row bug. 3 is enough to reject a single
# legitimate row whose Name happens to be "Email" or similar.
_HEADER_ROW_HIT_THRESHOLD = 3


class HeaderRowBug(Exception):
    """Raised when the first data row looks like column headers.

    run.py catches this, prints HEADER_ROW_BUG_MESSAGE, exits code 2.
    """


@dataclass
class LoadMeta:
    input_path: Path
    sheet_picked: str
    original_columns: list[str]
    normalized_columns: list[str]
    skiprows: int
    row_count: int
    source_site: Optional[str] = None
    duplicate_normalized: list[str] = field(default_factory=list)


def pick_main_sheet(
    xlsx_path: Path, sheet_override: Optional[str] = None
) -> str:
    """Return the name of the sheet to audit.

    Skip metadata sheets (Summary / By State / Stats), then among the
    remainder rank by (# of Tier 1 columns in the header row, row
    count) and pick the top. A --sheet override bypasses the heuristic.
    """
    xl = pd.ExcelFile(xlsx_path)
    if sheet_override is not None:
        if sheet_override not in xl.sheet_names:
            raise ValueError(
                f"--sheet {sheet_override!r} not found in "
                f"{xlsx_path.name}. Available: {xl.sheet_names}"
            )
        return sheet_override

    candidates = [
        s for s in xl.sheet_names
        if s.strip().lower() not in _METADATA_SHEET_NAMES
    ]
    if not candidates:
        candidates = list(xl.sheet_names)

    def rank(sheet: str) -> tuple[int, int]:
        header = _peek_header(xlsx_path, sheet)
        tier1_hits = sum(
            1 for h in header if normalize(str(h)) in TIER_1
        )
        return (tier1_hits, _row_count(xlsx_path, sheet))

    candidates.sort(key=rank, reverse=True)
    return candidates[0]


def _row_count(xlsx_path: Path, sheet: str) -> int:
    df = pd.read_excel(xlsx_path, sheet_name=sheet, header=None)
    return len(df)


def _peek_header(xlsx_path: Path, sheet: str) -> list:
    df = pd.read_excel(xlsx_path, sheet_name=sheet, header=None, nrows=1)
    if df.empty:
        return []
    return list(df.iloc[0].values)


def _count_header_like(row_values: list) -> int:
    """How many cells in the row are recognized column names."""
    hits = 0
    for v in row_values:
        if not isinstance(v, str):
            continue
        if v.strip().lower() in KNOWN_COLUMN_NAMES:
            hits += 1
    return hits


def load(
    xlsx_path: Path,
    sheet: Optional[str] = None,
    skiprows: int = 0,
) -> tuple[pd.DataFrame, LoadMeta]:
    """Load a scraper .xlsx and return (normalized df, LoadMeta)."""
    xlsx_path = Path(xlsx_path)
    sheet_name = pick_main_sheet(xlsx_path, sheet)

    # pandas: header=N means row N is the header; rows 0..N-1 are dropped.
    df = pd.read_excel(
        xlsx_path,
        sheet_name=sheet_name,
        header=skiprows,
    )

    # Header-row bug check (only meaningful when the user did not
    # already pass --skiprows as a manual recovery).
    if skiprows == 0 and not df.empty:
        first_data_row = list(df.iloc[0].values)
        if _count_header_like(first_data_row) >= _HEADER_ROW_HIT_THRESHOLD:
            raise HeaderRowBug(HEADER_ROW_BUG_MESSAGE)

    original_columns = [str(c) for c in df.columns]
    df.columns = [normalize(c) for c in df.columns]
    normalized_columns = list(df.columns)

    # Flag, but do not fix, collisions caused by aliasing (e.g., a
    # scraper that has both "Address" and "Full Address Raw").
    seen: dict[str, int] = {}
    for c in normalized_columns:
        seen[c] = seen.get(c, 0) + 1
    duplicates = [c for c, n in seen.items() if n > 1]

    # Empty normalization for STRING columns only: NaN -> "",
    # whitespace-only -> "". Numeric columns (Latitude, Longitude,
    # or any numeric Tier 3 column we haven't anticipated) keep
    # their dtype and their NaN — checks.py handles NaN natively.
    def _normalize_strings_only(col: pd.Series) -> pd.Series:
        if col.dtype != object:
            return col
        col = col.where(pd.notna(col), "")
        return col.map(
            lambda v: "" if isinstance(v, str) and not v.strip() else v
        )

    df = df.apply(_normalize_strings_only)

    source_site = None
    if "Source Site" in df.columns and "Source Site" not in duplicates:
        # Guard pd.notna in case Source Site is all-NaN and pandas
        # inferred it as float64 — then values would be nan, not "".
        non_empty = [
            v for v in df["Source Site"].tolist()
            if pd.notna(v) and v != ""
        ]
        if non_empty:
            source_site = str(non_empty[0])

    meta = LoadMeta(
        input_path=xlsx_path,
        sheet_picked=sheet_name,
        original_columns=original_columns,
        normalized_columns=normalized_columns,
        skiprows=skiprows,
        row_count=len(df),
        source_site=source_site,
        duplicate_normalized=duplicates,
    )
    return df, meta

"""CLI entry point for the auditor (Phase 1, text-only output).

    .venv\\Scripts\\python.exe auditor\\run.py --input outputs\\aaep_members.xlsx

Exit codes:
  0  — report written
  1  — unexpected error
  2  — header-row bug detected (Chi-style title row above real header)
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make `from auditor.X import ...` work when this file is invoked as a
# script (python auditor/run.py ...). Without this, only auditor/ is on
# sys.path, not the project root.
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import argparse  # noqa: E402

import pandas as pd  # noqa: E402

from auditor.aliases import TIER_1, TIER_2  # noqa: E402
from auditor.checks import CheckResult, run_all_checks  # noqa: E402
from auditor.loader import HeaderRowBug, LoadMeta, load  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Audit a scraper .xlsx output.")
    p.add_argument("--input", required=True, help="Path to scraper .xlsx")
    p.add_argument("--sheet", default=None, help="Override sheet auto-detect")
    p.add_argument(
        "--skiprows", type=int, default=0,
        help="Rows to skip before header (recovery for header-row bug)",
    )
    args = p.parse_args(argv)

    try:
        df, meta = load(Path(args.input), sheet=args.sheet, skiprows=args.skiprows)
    except HeaderRowBug as e:
        print(str(e))
        return 2

    results = run_all_checks(df, meta)
    _print_report(df, meta, results)
    return 0


# -------- Report rendering --------

_RULE = "=" * 72
_SUBRULE = "-" * 72


def _print_report(df: pd.DataFrame, meta: LoadMeta, results: list[CheckResult]) -> None:
    _print_header(meta)
    t1 = [r for r in results if r.tier == 1]
    t2 = [r for r in results if r.tier == 2]
    t3 = [r for r in results if r.tier == 3]
    cross = [r for r in results if r.tier == 0]

    _print_section("TIER 1", t1, meta.row_count)
    _print_section("TIER 2", t2, meta.row_count)
    _print_tier3_section(df, meta, t3)
    _print_cross_section(cross, meta.row_count)
    _print_footer(df, results)


def _print_header(meta: LoadMeta) -> None:
    print(_RULE)
    print(f"AUDITOR REPORT: {meta.input_path.name}")
    print(_RULE)
    print(f"Sheet:        {meta.sheet_picked}")
    print(f"Rows:         {meta.row_count}")
    print(f"Source Site:  {meta.source_site or '<none populated>'}")
    if meta.duplicate_normalized:
        print(f"Duplicates:   {', '.join(meta.duplicate_normalized)}")
    else:
        print("Duplicates:   none")
    print()


def _print_section(label: str, results: list[CheckResult], n_rows: int) -> None:
    print(f"{_SUBRULE}")
    print(f"{label}")
    print(f"{_SUBRULE}")
    if not results:
        print("  (none)")
        print()
        return
    for r in results:
        _print_check_line(r, n_rows, indent="  ")
    print()


def _print_check_line(r: CheckResult, n_rows: int, indent: str) -> None:
    fails = int((~r.pass_mask).sum())
    passes = n_rows - fails
    status = "OK  " if fails == 0 else "FAIL"
    tag = r.check_name + (f" [{r.column}]" if r.column else "")
    print(f"{indent}[{status}] {tag}: {passes}/{n_rows} pass, {fails} fail")
    if fails > 0:
        for row_idx, val in list(r.problem_values.items())[:3]:
            val_str = _truncate(repr(val), 60)
            print(f"{indent}       row {row_idx}: {val_str}")


def _print_tier3_section(
    df: pd.DataFrame, meta: LoadMeta, t3_results: list[CheckResult]
) -> None:
    print(_SUBRULE)
    print("TIER 3 (informational)")
    print(_SUBRULE)
    t3_cols = [
        c for c in df.columns
        if c not in TIER_1
        and c not in TIER_2
        and c not in meta.duplicate_normalized
    ]
    if not t3_cols:
        print("  (no Tier 3 columns)")
    else:
        for c in t3_cols:
            col_data = df[c]
            if isinstance(col_data, pd.DataFrame):
                continue
            if col_data.dtype == object:
                populated = col_data.apply(
                    lambda v: pd.notna(v)
                    and not (isinstance(v, str) and not v.strip())
                )
            else:
                populated = col_data.notna()
            pop_count = int(populated.sum())
            n = len(df)
            rate = (100.0 * pop_count / n) if n else 0.0
            print(f"  {c}: {pop_count}/{n} populated ({rate:.0f}%)")
    if t3_results:
        print("  name-hint checks:")
        for r in t3_results:
            _print_check_line(r, len(r.pass_mask), indent="    ")
    print()


def _print_cross_section(cross: list[CheckResult], n_rows: int) -> None:
    print(_SUBRULE)
    print("CROSS-COLUMN")
    print(_SUBRULE)
    if not cross:
        print("  (none)")
        print()
        return
    for r in cross:
        fails = int((~r.pass_mask).sum())
        print(f"  {r.check_name}: {fails} flagged / {n_rows}")
        if fails > 0:
            for row_idx, val in list(r.problem_values.items())[:3]:
                val_str = _truncate(repr(val), 60)
                print(f"         row {row_idx}: {val_str}")
    print()


def _print_footer(df: pd.DataFrame, results: list[CheckResult]) -> None:
    n = len(df)
    print(_RULE)
    if n == 0:
        print("Overall score: n/a (empty)")
        return
    scoring = [r for r in results if r.tier in (1, 2)]
    if not scoring:
        print("Overall score: n/a (no scoring checks ran)")
        return
    combined = pd.Series(True, index=df.index)
    for r in scoring:
        combined = combined & r.pass_mask
    clean = int(combined.sum())
    pct = 100.0 * clean / n
    print(f"Overall score: {clean}/{n} rows clean ({pct:.1f}%)")
    print("  (clean = zero Tier 1/2 failures)")


def _truncate(s: str, n: int) -> str:
    if len(s) <= n:
        return s
    return s[: n - 1] + "..."


if __name__ == "__main__":
    raise SystemExit(main())

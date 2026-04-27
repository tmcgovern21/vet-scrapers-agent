"""
AAEP Member Directory Scraper — Algolia API + Profile Detail Edition
=====================================================================
Phase 1: Pulls ALL members globally via geographic bounding box grid queries
          Saves progress to a checkpoint file so crashes can be resumed.
Phase 2: Visits each profile URL to grab full address, phone, Google Maps link

Requirements:
    pip install requests pandas openpyxl beautifulsoup4

Usage:
    python aaep_scraper.py                    # Scrape ALL members worldwide
    python aaep_scraper.py --state MA         # Filter by state after fetch
    python aaep_scraper.py --output vets.xlsx # Custom filename
    python aaep_scraper.py --no-profiles      # Skip profile scraping (faster)
    python aaep_scraper.py --resume           # Resume from checkpoint file
"""

import argparse
import time
import sys
import re
import json
from dataclasses import dataclass, asdict
from typing import Optional
from urllib.parse import urlencode
from pathlib import Path

import requests
from bs4 import BeautifulSoup
import pandas as pd

# ─────────────────────────────────────────────────
# ALGOLIA CONFIG
# ─────────────────────────────────────────────────
APP_ID  = "T39P2JKYVW"
API_KEY = "10728665a771d26521605acca9439c68"
INDEX   = "PRODUCTION_directory"

QUERY_URL = f"https://{APP_ID}-dsn.algolia.net/1/indexes/*/queries"

ALGOLIA_HEADERS = {
    "x-algolia-application-id": APP_ID,
    "x-algolia-api-key":        API_KEY,
    "content-type":             "application/x-www-form-urlencoded",
    "Origin":                   "https://aaep.org",
    "Referer":                  "https://aaep.org/",
    "User-Agent":               "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Accept":                   "*/*",
    "Accept-Language":          "en-US,en;q=0.9",
    "Sec-Fetch-Dest":           "empty",
    "Sec-Fetch-Mode":           "cors",
    "Sec-Fetch-Site":           "cross-site",
}

PROFILE_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

HITS_PER_PAGE    = 100
RATE_LIMIT       = 3.5   # seconds between every Algolia page request
BBOX_PAUSE       = 6.0   # extra pause between bounding box regions
PROFILE_DELAY    = 0.75  # seconds between profile page requests
CHECKPOINT_FILE  = "aaep_checkpoint.json"
PROFILE_CHECKPOINT_FILE     = "aaep_profiles_checkpoint.json"
PROFILE_CHECKPOINT_INTERVAL = 100
SOURCE_SITE                 = "AAEP"

ATTRS = ("objectID,name,permalink,directory_credentials,"
         "directory_company_name,directory_state,specialty,directory_hide")

# Global bounding boxes — format: (label, latNE, lngNE, latSW, lngSW)
GLOBAL_BBOXES = [
    # North America — Western US
    ("NA-NW",         50.0, -110.0,  37.0, -125.0),
    ("NA-NC",         50.0,  -95.0,  37.0, -110.0),
    # North America — Eastern US split into two halves at -80 longitude
    ("NE-North-W",    50.0,  -80.0,  37.0,  -95.0),
    ("NE-North-E",    50.0,  -65.0,  37.0,  -80.0),
    # Southern US
    ("NA-SW",         37.0, -105.0,  24.0, -125.0),
    ("NA-SC",         37.0,  -90.0,  24.0, -105.0),
    ("NA-SE",         37.0,  -65.0,  24.0,  -90.0),
    # Alaska & Hawaii
    ("Alaska",        72.0, -130.0,  54.0, -170.0),
    ("Hawaii",        23.0, -154.0,  18.0, -162.0),
    # Caribbean / Puerto Rico
    ("Caribbean",     24.0,  -60.0,  14.0,  -85.0),
    # Canada
    ("Canada-W",      60.0, -100.0,  48.0, -140.0),
    ("Canada-E",      60.0,  -52.0,  42.0, -100.0),
    # Mexico & Central America
    ("Mexico-CA",     24.0,  -77.0,   7.0, -118.0),
    # South America
    ("SA-North",       8.0,  -34.0, -10.0,  -82.0),
    ("SA-South",     -10.0,  -34.0, -56.0,  -82.0),
    # Europe
    ("UK-Ireland",    59.0,   -1.0,  49.0,  -11.0),
    ("EU-West",       52.0,   10.0,  36.0,   -1.0),
    ("EU-East",       57.0,   40.0,  36.0,   10.0),
    ("Scandinavia",   71.0,   32.0,  54.0,    4.0),
    # Oceania
    ("Australia",     -9.0,  154.0, -44.0,  113.0),
    ("NZ",           -34.0,  178.0, -47.0,  166.0),
    # Asia & Middle East
    ("Asia-W",        42.0,   60.0,  12.0,   26.0),
    ("Asia-E",        53.0,  145.0,  20.0,   60.0),
    ("Japan",         45.0,  146.0,  30.0,  129.0),
    # Africa
    ("Africa",        38.0,   52.0, -35.0,  -18.0),
]

BBOX_LABELS = [b[0] for b in GLOBAL_BBOXES]


# ─────────────────────────────────────────────────
# CHECKPOINT
# ─────────────────────────────────────────────────
def save_checkpoint(completed_labels: list[str], all_hits: list[dict]) -> None:
    data = {
        "completed": completed_labels,
        "hits":      all_hits,
    }
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f)
    print(f"  [checkpoint saved: {len(all_hits)} records, "
          f"{len(completed_labels)}/{len(GLOBAL_BBOXES)} regions done]")


def load_checkpoint() -> tuple[list[str], list[dict]]:
    p = Path(CHECKPOINT_FILE)
    if not p.exists():
        return [], []
    with open(p, "r", encoding="utf-8") as f:
        data = json.load(f)
    completed = data.get("completed", [])
    hits      = data.get("hits", [])
    print(f"  Loaded checkpoint: {len(hits)} records, "
          f"{len(completed)} regions already done: {completed}")
    return completed, hits


# Phase 2 checkpoint: profile_url -> enriched-fields dict. Always loaded
# if present (keyed by URL, so safe to auto-resume). Delete the file
# manually to force a fresh Phase 2 run.
def save_profile_checkpoint(enriched: dict) -> None:
    with open(PROFILE_CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(enriched, f)


def load_profile_checkpoint() -> dict:
    p = Path(PROFILE_CHECKPOINT_FILE)
    if not p.exists():
        return {}
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


# ─────────────────────────────────────────────────
# DATA MODEL
# ─────────────────────────────────────────────────
@dataclass
class Member:
    name:            str = ""
    credentials:     str = ""
    company:         str = ""
    state:           str = ""
    specialties:     str = ""
    profile_url:     str = ""
    object_id:       str = ""
    full_address:    str = ""
    phone:           str = ""
    google_maps_url: str = ""
    # Populated from profile page CSS classes (.address-*).
    address_line_1:  str = ""
    city:            str = ""
    zip_code:        str = ""
    country:         str = ""

    @classmethod
    def from_hit(cls, hit: dict) -> "Member":
        specs = hit.get("specialty", [])
        if isinstance(specs, list):
            specs = ", ".join(sorted(specs))
        return cls(
            name        = hit.get("name", "").strip(),
            credentials = hit.get("directory_credentials", "").strip(),
            company     = hit.get("directory_company_name", "").strip(),
            state       = hit.get("directory_state", "").strip(),
            specialties = specs,
            profile_url = hit.get("permalink", "").strip(),
            object_id   = str(hit.get("objectID", "")),
        )


# ─────────────────────────────────────────────────
# ALGOLIA API
# ─────────────────────────────────────────────────
def post_query(bbox: Optional[str] = None, page: int = 0) -> dict:
    params = {
        "filters":              "directory_hide:false",
        "hitsPerPage":          HITS_PER_PAGE,
        "page":                 page,
        "attributesToRetrieve": ATTRS,
    }
    if bbox:
        params["insideBoundingBox"] = bbox

    body = {
        "requests": [
            {"indexName": INDEX, "params": urlencode(params)}
        ]
    }

    for attempt in range(10):
        try:
            resp = requests.post(QUERY_URL, headers=ALGOLIA_HEADERS,
                                 json=body, timeout=30)
            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                print(f"  Rate limited — waiting {wait}s before retry {attempt+1}/10...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()["results"][0]

        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            wait = 20 * (attempt + 1)
            print(f"  Network error ({type(e).__name__}) — "
                  f"waiting {wait}s before retry {attempt+1}/10...")
            time.sleep(wait)
            continue

    raise RuntimeError("All retries exhausted — check your internet connection.")


def fetch_bbox(label: str, bbox_str: str, seen_ids: set) -> list[dict]:
    hits = []
    page = 0
    while True:
        data     = post_query(bbox=bbox_str, page=page)
        batch    = data.get("hits", [])
        nb_hits  = data.get("nbHits", 0)
        nb_pages = data.get("nbPages", 1)

        new = []
        for h in batch:
            oid = h.get("objectID")
            if oid not in seen_ids:
                seen_ids.add(oid)
                new.append(h)
        hits.extend(new)

        print(f"  [{label}] page {page+1:>2}/{nb_pages}  "
              f"nbHits={nb_hits}  +{len(new)} new  (total seen: {len(seen_ids)})")

        if nb_hits > 1000:
            print(f"  *** WARNING: {label} has {nb_hits} hits — "
                  f"only first 1000 accessible. Consider splitting this box.")

        if page + 1 >= nb_pages or not batch:
            break
        page += 1
        time.sleep(RATE_LIMIT)

    return hits


def scrape_algolia_all(resume: bool = False) -> list[dict]:
    # Load checkpoint if resuming
    completed_labels: list[str] = []
    all_hits: list[dict] = []
    seen_ids: set = set()

    if resume:
        completed_labels, all_hits = load_checkpoint()
        seen_ids = {h["objectID"] for h in all_hits}

    probe = post_query(bbox=None, page=0)
    total = probe.get("nbHits", 0)
    remaining = [b for b in GLOBAL_BBOXES if b[0] not in completed_labels]

    print(f"Total members in index (global): {total}")
    print(f"Regions to query: {len(remaining)} "
          f"({'resuming' if completed_labels else 'fresh start'})\n")

    for label, latNE, lngNE, latSW, lngSW in remaining:
        bbox_str = f"{latNE},{lngNE},{latSW},{lngSW}"
        hits = fetch_bbox(label, bbox_str, seen_ids)
        all_hits.extend(hits)
        completed_labels.append(label)
        print(f"  -> {label}: {len(hits)} new  (running total: {len(all_hits)})\n")

        # Save checkpoint after every region
        save_checkpoint(completed_labels, all_hits)
        time.sleep(BBOX_PAUSE)

    print(f"Geo sweep complete: {len(all_hits)} unique members collected.")
    if total > len(seen_ids):
        print(f"Note: ~{total - len(seen_ids)} records may lack geo coordinates.")

    # Clean up checkpoint on successful completion
    p = Path(CHECKPOINT_FILE)
    if p.exists():
        p.unlink()
        print("  Checkpoint file removed (run complete).")

    return all_hits


# ─────────────────────────────────────────────────
# PROFILE PAGE SCRAPER
# ─────────────────────────────────────────────────
def scrape_profile(url: str) -> dict:
    result = {
        "full_address":   "", "phone": "", "google_maps_url": "",
        "address_line_1": "", "city": "", "zip_code": "", "country": "",
    }
    if not url:
        return result
    try:
        resp = requests.get(url, headers=PROFILE_HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception:
        return result

    soup = BeautifulSoup(resp.text, "html.parser")

    # ── Structured fields (AAEP profile template CSS classes) ──
    def _text_of(cls: str) -> str:
        el = soup.find(class_=cls)
        return el.get_text(" ", strip=True) if el else ""

    result["full_address"]   = _text_of("formatted-address")
    result["address_line_1"] = _text_of("address-address")
    result["city"]           = _text_of("address-city")
    result["zip_code"]       = _text_of("address-zip")
    result["country"]        = _text_of("address-country")

    phone_el = soup.find(class_="dialable-phone")
    if phone_el:
        result["phone"] = phone_el.get_text(" ", strip=True)

    # ── Fallbacks (defensive against AAEP redesigns) ──
    if not result["full_address"]:
        addr_parts = []
        for tag in soup.select(".directory-location, .member-location, "
                               "[class*='location'], [class*='address']"):
            text = tag.get_text(" ", strip=True)
            if text and len(text) > 5:
                addr_parts.append(text)
                break
        if not addr_parts:
            for heading in soup.find_all(["h2", "h3", "h4", "strong", "p"]):
                if "location" in heading.get_text().lower():
                    parent = heading.find_parent()
                    if parent:
                        lines = [l.strip() for l in parent.get_text("\n").splitlines()
                                 if l.strip() and "location" not in l.lower()]
                        if lines:
                            addr_parts.append(" | ".join(lines))
                    break
        if not addr_parts:
            zip_match = re.search(
                r'([^\n]+\n[^\n]*\b\d{5}(?:-\d{4})?\b[^\n]*)',
                soup.get_text("\n")
            )
            if zip_match:
                addr_parts.append(zip_match.group(1).replace("\n", ", ").strip())
        if addr_parts:
            result["full_address"] = addr_parts[0]

    if not result["phone"]:
        phone_match = re.search(
            r'\(?\d{3}\)?[\s.\-]\d{3}[\s.\-]\d{4}', soup.get_text()
        )
        if phone_match:
            result["phone"] = phone_match.group(0).strip()

    # ── Google Maps URL ──
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "maps.google" in href or "google.com/maps" in href:
            result["google_maps_url"] = href
            break
    if not result["google_maps_url"]:
        for iframe in soup.find_all("iframe", src=True):
            src = iframe["src"]
            if "maps.google" in src or "google.com/maps" in src:
                result["google_maps_url"] = src
                break
    if not result["google_maps_url"]:
        m = re.search(
            r'(https://(?:maps\.google\.com|www\.google\.com/maps)[^\s\'"<>]+)',
            resp.text
        )
        if m:
            result["google_maps_url"] = m.group(1)

    return result


def enrich_profiles(members: list[Member]) -> None:
    total = len(members)

    # Auto-load Phase 2 checkpoint if present. Keyed by profile_url,
    # so it's always safe to reuse. Delete the file manually to force
    # a fresh Phase 2 run.
    cache = load_profile_checkpoint()
    if cache:
        print(f"  Phase 2 checkpoint: {len(cache)} profiles previously enriched (auto-resuming).")

    print(f"\nScraping {total} profile pages (structured address + phone + maps)...\n")

    uncached_total = sum(
        1 for m in members if m.profile_url and m.profile_url not in cache
    )
    start = time.time()
    processed = 0
    scraped_this_run = 0

    for m in members:
        if not m.profile_url:
            continue
        if m.profile_url in cache:
            d = cache[m.profile_url]
        else:
            d = scrape_profile(m.profile_url)
            cache[m.profile_url] = d
            scraped_this_run += 1
            time.sleep(PROFILE_DELAY)

        m.full_address    = d.get("full_address", "")
        m.phone           = d.get("phone", "")
        m.google_maps_url = d.get("google_maps_url", "")
        m.address_line_1  = d.get("address_line_1", "")
        m.city            = d.get("city", "")
        m.zip_code        = d.get("zip_code", "")
        m.country         = d.get("country", "")
        processed += 1

        if processed % PROFILE_CHECKPOINT_INTERVAL == 0 or processed == total:
            save_profile_checkpoint(cache)
            elapsed = time.time() - start
            if scraped_this_run > 0 and elapsed > 0:
                per = elapsed / scraped_this_run
                remaining_uncached = uncached_total - scraped_this_run
                eta_min = int(remaining_uncached * per / 60)
                eta_str = f"~{eta_min} min remaining"
            else:
                eta_str = "(all cached)"
            print(
                f"  Profile {processed}/{total} "
                f"({100*processed/total:.0f}%) -- "
                f"{int(elapsed/60)} min elapsed, {eta_str} "
                f"[cache size: {len(cache)}]"
            )

    p = Path(PROFILE_CHECKPOINT_FILE)
    if p.exists():
        p.unlink()
        print("  Phase 2 checkpoint removed (all profiles enriched).")


# ─────────────────────────────────────────────────
# EXPORT
# ─────────────────────────────────────────────────
def export(members: list[Member], output_file: str) -> None:
    if not members:
        print("No members to export.")
        return

    df = pd.DataFrame([asdict(m) for m in members])
    df.rename(columns={
        "name":            "Name",
        "credentials":     "Credentials",
        "company":         "Practice / Company",
        "state":           "State",
        "specialties":     "Specialties",
        "full_address":    "Full Address Raw",
        "address_line_1":  "Address Line 1",
        "city":            "City",
        "zip_code":        "Zip",
        "country":         "Country",
        "phone":           "Phone",
        "google_maps_url": "Google Maps URL",
        "profile_url":     "Profile URL",
        "object_id":       "Algolia ID",
    }, inplace=True)

    # Source Site is Tier 1 per SCRAPER_CONTRACT.md — constant for this scraper.
    df.insert(0, "Source Site", SOURCE_SITE)

    col_order = [
        "Source Site", "Name", "Credentials", "Practice / Company",
        "Specialties", "Full Address Raw", "Address Line 1", "City",
        "State", "Zip", "Country", "Phone", "Google Maps URL",
        "Profile URL", "Algolia ID",
    ]
    df = df[[c for c in col_order if c in df.columns]]
    df.sort_values(["Country", "State", "Name"], inplace=True, ignore_index=True)

    # Partition into US / International for the split sheets.
    us_codes = frozenset([
        "AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL",
        "IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE",
        "NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD",
        "TN","TX","UT","VT","VA","WA","WV","WI","WY",
        "AS","GU","MP","PR","VI",
    ])
    us_country_names = {
        "us", "usa", "u.s.", "u.s.a.",
        "united states", "united states of america",
    }

    def _is_us(row) -> bool:
        c = str(row.get("Country", "")).strip().lower()
        if c in us_country_names:
            return True
        if not c:
            return str(row.get("State", "")).strip().upper() in us_codes
        return False

    us_mask  = df.apply(_is_us, axis=1)
    us_df    = df[us_mask].reset_index(drop=True)
    intl_df  = df[~us_mask].reset_index(drop=True)

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        for sheet_name, data in [
            ("Members",       df),
            ("US Only",       us_df),
            ("International", intl_df),
        ]:
            data.to_excel(writer, index=False, sheet_name=sheet_name)
            ws = writer.sheets[sheet_name]
            ws.freeze_panes = "A2"
            for col in ws.columns:
                max_len = max(
                    (len(str(c.value)) for c in col if c.value), default=10
                )
                ws.column_dimensions[col[0].column_letter].width = min(
                    max_len + 2, 60
                )

        # Summary sheet last, per SCRAPER_CONTRACT.md sheet order.
        ws2 = writer.book.create_sheet("Summary")
        rows = [
            ("Total Members",              len(df)),
            ("US Members",                 len(us_df)),
            ("International Members",      len(intl_df)),
            ("Countries Covered",          df["Country"].replace("", pd.NA).nunique()),
            ("States/Regions Covered",     df["State"].replace("", pd.NA).nunique()),
            ("Members with Specialties",   int((df["Specialties"] != "").sum())),
            ("Members with Practice Name", int((df["Practice / Company"] != "").sum())),
            ("Members with Phone",         int((df["Phone"] != "").sum())),
            ("Members with Full Address",  int((df["Full Address Raw"] != "").sum())),
            ("Members with Parsed City",   int((df["City"] != "").sum())),
            ("Members with Maps Link",     int((df["Google Maps URL"] != "").sum())),
        ]
        for i, (label, val) in enumerate(rows, 1):
            ws2[f"A{i}"] = label
            ws2[f"B{i}"] = val
        hdr_row = len(rows) + 2
        ws2[f"A{hdr_row}"] = "State"
        ws2[f"B{hdr_row}"] = "Count"
        for i, (state, cnt) in enumerate(
            df["State"].value_counts().items(), start=hdr_row + 1
        ):
            ws2[f"A{i}"] = state
            ws2[f"B{i}"] = cnt
        ws2.column_dimensions["A"].width = 30
        ws2.column_dimensions["B"].width = 10

    print(f"  [OK] Excel -> {output_file}")


# ─────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Scrape AAEP member directory (all countries) via Algolia geo-grid + profiles",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--state",       help="Filter by state/region code after fetch (e.g. MA)")
    parser.add_argument("--specialty",   help="Filter by specialty keyword after fetch")
    parser.add_argument("--output",      default="aaep_members.xlsx")
    parser.add_argument("--no-profiles", action="store_true",
                        help="Skip profile page scraping (faster, less data)")
    parser.add_argument("--resume",      action="store_true",
                        help="Resume from checkpoint file (aaep_checkpoint.json)")
    parser.add_argument("--limit",       type=int, default=None,
                        help="DEBUG: cap to first N members after Phase 1 "
                             "(applied after --state/--specialty filters)")
    parser.add_argument("--intl-only",   action="store_true",
                        help="DEBUG: keep only non-US members (Algolia's "
                             "state field is populated only for US rows)")
    args = parser.parse_args()

    print(f"\n{'='*55}")
    print(f"  AAEP Member Directory Scraper")
    print(f"  All Members | Geo-Grid + Profile Detail")
    print(f"  Index: {INDEX}")
    print(f"{'='*55}\n")
    print(f"Output  : {args.output}")
    print(f"Profiles: {'disabled' if args.no_profiles else 'enabled (address/phone/maps)'}")
    if args.resume:
        print(f"Mode    : RESUMING from {CHECKPOINT_FILE}\n")
    else:
        print()

    # ── Phase 1: Collect all via bounding boxes ──
    try:
        raw_hits = scrape_algolia_all(resume=args.resume)
    except (requests.HTTPError, RuntimeError) as e:
        print(f"\nERROR: {e}")
        print(f"Re-run with --resume to continue from where it stopped.")
        sys.exit(1)

    members = [Member.from_hit(h) for h in raw_hits]
    print(f"\nTotal members collected: {len(members)}")

    if args.state:
        members = [m for m in members if m.state == args.state.upper()]
        print(f"After --state {args.state.upper()}: {len(members)}")
    if args.specialty:
        members = [m for m in members if args.specialty.lower() in m.specialties.lower()]
        print(f"After --specialty filter: {len(members)}")
    if args.intl_only:
        before = len(members)
        members = [m for m in members if not m.state]
        print(f"After --intl-only: {len(members)} (was {before})")
    if args.limit is not None and args.limit > 0 and len(members) > args.limit:
        # --limit layers cleanly on top of --state/--specialty/--resume/
        # --no-profiles: it just caps the member list before Phase 2. The
        # exported file will contain exactly this subset.
        print(f"\nDEBUG MODE: limiting to {args.limit} profiles (was {len(members)})")
        members = members[:args.limit]

    # ── Phase 2: Profile pages ──
    if not args.no_profiles and members:
        enrich_profiles(members)

    print()
    export(members, args.output)
    print(f"\n{'='*55}")
    print("  Done!")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    main()
"""
CIVT Veterinary Referral Directory Scraper
==========================================
Scrapes practitioners listed at https://civtedu.org/directory and
their individual profile pages. Output conforms to SCRAPER_CONTRACT.md.

Source: College of Integrative Veterinary Therapies (CIVT).

Phase 1: Fetch listing pages 1..N at /directory?page=N. The site's
         pager visually shows only 7 pages but the back-end clamps any
         page > last to the last page silently. We probe pages until
         the slug list repeats.
Phase 2: Visits each profile page (~140 requests at 0.5s rate limit)
         to capture phone, email, website, structured contact info,
         country/region, services, species, and lat/lng. Checkpoint
         auto-loads if present.

Each row represents one practitioner (the h4 person name is canonical
as Name; the h1 practice name is captured as Practice / Company).

Requirements:
    pip install requests beautifulsoup4 pandas openpyxl usaddress

Usage:
    python civt_scraper.py
    python civt_scraper.py --output custom.xlsx
    python civt_scraper.py --no-profiles   # skip Phase 2 (faster, sparse)
    python civt_scraper.py --limit 20      # debug
"""

import argparse
import json
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from urllib.parse import unquote

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    import usaddress
    HAS_USADDRESS = True
except ImportError:
    HAS_USADDRESS = False


# ─────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────
SOURCE_SITE     = "CIVT"
BASE_URL        = "https://civtedu.org"
LISTING_URL     = "https://civtedu.org/directory"
PROFILE_DELAY   = 0.5
LIST_PAGE_DELAY = 0.5
MAX_LIST_PAGES  = 25  # safety cap; back-end clamps so loop will exit earlier
PROFILE_CHECKPOINT_FILE     = "civt_profiles_checkpoint.json"
PROFILE_CHECKPOINT_INTERVAL = 25

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/146.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

US_STATE_CODES = frozenset([
    "AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL",
    "IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE",
    "NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD",
    "TN","TX","UT","VT","VA","WA","WV","WI","WY",
    "AS","GU","MP","PR","VI",
])

# US state full name → 2-letter code (for the structured state field returned
# by the profile page, which uses full names like "Ohio").
US_STATE_NAME_TO_CODE = {
    "Alabama":"AL","Alaska":"AK","Arizona":"AZ","Arkansas":"AR","California":"CA",
    "Colorado":"CO","Connecticut":"CT","Delaware":"DE","District of Columbia":"DC",
    "Florida":"FL","Georgia":"GA","Hawaii":"HI","Idaho":"ID","Illinois":"IL",
    "Indiana":"IN","Iowa":"IA","Kansas":"KS","Kentucky":"KY","Louisiana":"LA",
    "Maine":"ME","Maryland":"MD","Massachusetts":"MA","Michigan":"MI","Minnesota":"MN",
    "Mississippi":"MS","Missouri":"MO","Montana":"MT","Nebraska":"NE","Nevada":"NV",
    "New Hampshire":"NH","New Jersey":"NJ","New Mexico":"NM","New York":"NY",
    "North Carolina":"NC","North Dakota":"ND","Ohio":"OH","Oklahoma":"OK","Oregon":"OR",
    "Pennsylvania":"PA","Rhode Island":"RI","South Carolina":"SC","South Dakota":"SD",
    "Tennessee":"TN","Texas":"TX","Utah":"UT","Vermont":"VT","Virginia":"VA",
    "Washington":"WA","West Virginia":"WV","Wisconsin":"WI","Wyoming":"WY",
    "American Samoa":"AS","Guam":"GU","Northern Mariana Islands":"MP",
    "Puerto Rico":"PR","U.S. Virgin Islands":"VI","Virgin Islands":"VI",
}

US_COUNTRY_NAMES = {
    "us", "usa", "u.s.", "u.s.a.",
    "united states", "united states of america",
}


# ─────────────────────────────────────────────────
# DATA MODEL
# ─────────────────────────────────────────────────
@dataclass
class Practitioner:
    source_id:        str = ""
    profile_url:      str = ""
    name:             str = ""        # h4 person name
    practice:         str = ""        # h1 practice/company
    credentials:      str = ""
    specialties:      str = ""        # Services Provided
    phone:            str = ""
    email:            str = ""
    website:          str = ""
    full_address_raw: str = ""
    address_line_1:   str = ""
    city:             str = ""
    state:            str = ""        # 2-letter for US, full name otherwise
    zip_code:         str = ""
    country:          str = ""
    # Tier 3
    latitude:         str = ""
    longitude:        str = ""
    google_maps_url:  str = ""
    species_treated:  str = ""
    country_iso:      str = ""        # 2-letter ISO from flag svg


# ─────────────────────────────────────────────────
# CHECKPOINT
# ─────────────────────────────────────────────────
def load_profile_checkpoint() -> dict:
    p = Path(PROFILE_CHECKPOINT_FILE)
    if not p.exists():
        return {}
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def save_profile_checkpoint(cache: dict) -> None:
    with open(PROFILE_CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False)


# ─────────────────────────────────────────────────
# PHASE 1 — LISTING PAGES
# ─────────────────────────────────────────────────
def parse_listing_card(card) -> Practitioner | None:
    """Pull whatever the list page exposes; profile fetch overrides later."""
    p = Practitioner()

    a = card.select_one('a.btn-primary[href*="/directory/"]')
    if not a:
        return None
    href = a.get("href", "").strip()
    if not href:
        return None
    if not href.startswith("http"):
        href = BASE_URL + href
    p.profile_url = href
    # Source ID = last URL segment (slug)
    p.source_id = href.rstrip("/").rsplit("/", 1)[-1]

    h4 = card.find("h4")
    if h4:
        p.name = h4.get_text(strip=True)

    body = card.select_one(".card-body")
    if body:
        strong = body.find("strong")
        if strong:
            p.practice = strong.get_text(strip=True)
        # Credentials = first .text-muted inside the d-flex name block
        flex = body.select_one(".d-flex.flex-column")
        if flex:
            cred = flex.select_one(".text-muted")
            if cred:
                p.credentials = cred.get_text(strip=True)
        # Specialties on list page: .text-muted that is a DIRECT child of card-body
        # (the credentials one is nested inside d-flex.flex-column)
        for child in body.find_all("div", class_="text-muted", recursive=False):
            txt = child.get_text(" ", strip=True)
            if txt:
                p.specialties = txt
                break

    # Maps URL from the Directions button
    dir_a = card.find("a", href=re.compile(r"maps/dir"))
    if dir_a:
        p.google_maps_url = dir_a["href"]
        m = re.search(r"destination=([\d.\-]+)%2C([\d.\-]+)", dir_a["href"])
        if m:
            p.latitude  = m.group(1)
            p.longitude = m.group(2)

    return p


def fetch_listings() -> list[Practitioner]:
    print(f"Fetching listing pages from {LISTING_URL}")
    practitioners: list[Practitioner] = []
    seen_first_slug = None

    for page in range(1, MAX_LIST_PAGES + 1):
        url = f"{LISTING_URL}?page={page}"
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select(".one_directory")
        if not cards:
            print(f"  Page {page}: empty -- stopping.")
            break

        page_first = None
        page_items: list[Practitioner] = []
        for c in cards:
            pr = parse_listing_card(c)
            if pr:
                page_items.append(pr)
                if page_first is None:
                    page_first = pr.source_id

        # Detect the back-end's "clamp to last" behavior: if page>1 and the
        # first slug matches the previous page's first slug, we're past the end.
        if page > 1 and page_first == seen_first_slug:
            print(f"  Page {page}: duplicate of page {page-1} -- end of results.")
            break

        seen_first_slug = page_first
        practitioners.extend(page_items)
        print(f"  Page {page}: {len(page_items)} listings (running total: {len(practitioners)})")
        time.sleep(LIST_PAGE_DELAY)

    print(f"  Found {len(practitioners)} listings across {page-1} pages.\n")
    return practitioners


# ─────────────────────────────────────────────────
# PHASE 2 — PROFILE PAGES
# ─────────────────────────────────────────────────
def _label_to_value(soup: BeautifulSoup, label: str) -> str:
    """Find <div.text-muted><small>{label}</small></div> and return the
    text of its next sibling <div>."""
    for tm in soup.find_all("div", class_="text-muted"):
        small = tm.find("small")
        if small and small.get_text(strip=True) == label:
            sib = tm.find_next_sibling("div")
            if sib:
                return sib.get_text(" ", strip=True)
    return ""


def scrape_profile(url: str) -> dict:
    out = {
        "name": "", "practice": "", "credentials": "",
        "specialties": "", "species_treated": "",
        "phone": "", "email": "", "website": "",
        "full_address_raw": "", "address_line_2": "",
        "state": "", "country": "", "country_iso": "",
        "latitude": "", "longitude": "", "google_maps_url": "",
    }
    if not url:
        return out

    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        r.encoding = "utf-8"
    except Exception as e:
        print(f"  [warn] profile fetch failed: {url} -- {e}")
        return out

    soup = BeautifulSoup(r.text, "html.parser")

    # The profile body lives in the FIRST .col-12.col-lg-6 (left col with
    # name/practice/region) plus the contact card on the right. Anchor on h1.
    h1 = soup.find("h1", class_="section_header")
    if h1:
        out["practice"] = h1.get_text(strip=True)

    h4 = soup.find("h4", class_="fw-normal")
    if h4:
        out["name"] = h4.get_text(strip=True)
        # Credentials = the next div sibling after h4 within the same column
        cred = h4.find_next_sibling("div")
        if cred:
            out["credentials"] = cred.get_text(strip=True)

    # Country flag block
    flag = soup.select_one('img.rounded-circle[src*="countries"]')
    if flag:
        out["country"] = (flag.get("alt") or "").strip()
        m = re.search(r"/countries/1x1/([a-z]{2})\.svg", flag.get("src", ""))
        if m:
            out["country_iso"] = m.group(1).upper()
        # The state/region is the div.me-2 RIGHT AFTER the flag inside the same
        # flex container. Walk up to the parent flex, then grab div.me-2.
        flex_parent = flag.find_parent("div")
        if flex_parent:
            me2 = flex_parent.find("div", class_="me-2")
            if me2:
                state = me2.get_text(strip=True).rstrip(",").strip()
                out["state"] = state

    # Services / Species
    out["specialties"]     = _label_to_value(soup, "Services Provided")
    out["species_treated"] = _label_to_value(soup, "Species Treated")

    # Contact panel rows
    for div in soup.select("div.d-flex.flex-row.py-2.align-items-center"):
        i = div.find("i")
        if not i:
            continue
        cls = " ".join(i.get("class", []))
        a = div.find("a")
        if "fa-phone" in cls and a:
            out["phone"] = a.get_text(strip=True)
        elif "fa-envelope" in cls and a:
            out["email"] = a.get_text(strip=True)
        elif "fa-globe" in cls and a:
            out["website"] = (a.get("href") or a.get_text(strip=True)).strip()
        elif "fa-map-marker" in cls:
            text_div = div.find("div", class_="ms-3")
            if text_div:
                # The address has <br>-separated lines. The second line is the
                # region/country echo (e.g. "Ohio, United States of America").
                # Keep only the first line(s) above the duplicate.
                for br in text_div.find_all("br"):
                    br.replace_with("\n")
                lines = [ln.strip() for ln in text_div.get_text("\n").splitlines() if ln.strip()]
                # Detect duplicate-region echo and drop trailing line(s) that
                # match the structured state/country we already have
                if lines and out["country"]:
                    last = lines[-1].lower()
                    cl = out["country"].lower()
                    sl = (out["state"] or "").lower()
                    if cl in last and (not sl or sl in last):
                        lines = lines[:-1]
                out["full_address_raw"] = ", ".join(lines)

    # Lat/Lng + Maps URL from Get Directions
    dir_a = soup.find("a", string=re.compile(r"Get Directions", re.I))
    if not dir_a:
        dir_a = soup.find("a", href=re.compile(r"maps/dir"))
    if dir_a and dir_a.get("href"):
        out["google_maps_url"] = dir_a["href"]
        m = re.search(r"destination=([\d.\-]+)%2C([\d.\-]+)", dir_a["href"])
        if m:
            out["latitude"]  = m.group(1)
            out["longitude"] = m.group(2)

    return out


def enrich_profiles(practitioners: list[Practitioner]) -> None:
    cache = load_profile_checkpoint()
    if cache:
        print(f"  Phase 2 checkpoint: {len(cache)} profiles previously enriched (auto-resuming).")

    total = len(practitioners)
    print(f"\nScraping {total} profile pages (rate: {PROFILE_DELAY}s)...\n")

    uncached = sum(1 for p in practitioners
                   if p.profile_url and p.profile_url not in cache)
    start = time.time()
    processed, fetched = 0, 0

    for p in practitioners:
        if not p.profile_url:
            continue

        if p.profile_url in cache:
            d = cache[p.profile_url]
        else:
            d = scrape_profile(p.profile_url)
            cache[p.profile_url] = d
            fetched += 1
            time.sleep(PROFILE_DELAY)

        # Profile is authoritative when present; otherwise keep listing values.
        if d.get("name"):        p.name        = d["name"]
        if d.get("practice"):    p.practice    = d["practice"]
        if d.get("credentials"): p.credentials = d["credentials"]
        if d.get("specialties"): p.specialties = d["specialties"]
        p.species_treated = d.get("species_treated", "")
        p.phone           = d.get("phone", p.phone)
        p.email           = d.get("email", p.email)
        p.website         = d.get("website", p.website)
        if d.get("full_address_raw"):
            p.full_address_raw = d["full_address_raw"]
        if d.get("state"):
            p.state = d["state"]
        if d.get("country"):
            p.country = d["country"]
        if d.get("country_iso"):
            p.country_iso = d["country_iso"]
        if d.get("latitude"):
            p.latitude = d["latitude"]
            p.longitude = d["longitude"]
        if d.get("google_maps_url"):
            p.google_maps_url = d["google_maps_url"]

        processed += 1
        if processed % PROFILE_CHECKPOINT_INTERVAL == 0 or processed == total:
            save_profile_checkpoint(cache)
            elapsed = time.time() - start
            if fetched > 0 and elapsed > 0:
                per = elapsed / fetched
                remaining = uncached - fetched
                eta = int(remaining * per / 60)
                eta_str = f"~{eta} min remaining" if remaining > 0 else "(done)"
            else:
                eta_str = "(all cached)"
            print(f"  Profile {processed}/{total} "
                  f"({100*processed/total:.0f}%) -- "
                  f"{int(elapsed/60)}m{int(elapsed)%60}s elapsed, {eta_str} "
                  f"[cache: {len(cache)}]")

    cp = Path(PROFILE_CHECKPOINT_FILE)
    if cp.exists():
        cp.unlink()
        print("  Phase 2 checkpoint removed (all profiles enriched).")


# ─────────────────────────────────────────────────
# ADDRESS PARSING
# ─────────────────────────────────────────────────
def normalize_us_state(p: Practitioner) -> None:
    """Convert full US state name to 2-letter code (ISO already in country_iso)."""
    if p.country_iso == "US" and p.state and p.state not in US_STATE_CODES:
        code = US_STATE_NAME_TO_CODE.get(p.state)
        if code:
            p.state = code


def parse_us_address(p: Practitioner) -> None:
    if p.country_iso != "US" or not p.full_address_raw:
        return
    if not HAS_USADDRESS:
        return
    try:
        tagged, _ = usaddress.tag(p.full_address_raw)
    except Exception:
        return

    street_parts = []
    for k in ("AddressNumber", "StreetNamePreDirectional", "StreetName",
              "StreetNamePostType", "StreetNamePostDirectional",
              "OccupancyType", "OccupancyIdentifier"):
        v = tagged.get(k, "")
        if v:
            street_parts.append(v)
    if street_parts:
        p.address_line_1 = " ".join(street_parts)
    if not p.city:
        p.city = tagged.get("PlaceName", "") or ""
    # State already canonicalized by normalize_us_state(); only replace if missing
    if not p.state:
        cand = (tagged.get("StateName", "") or "").strip().upper()
        if cand in US_STATE_CODES:
            p.state = cand
    if not p.zip_code:
        p.zip_code = tagged.get("ZipCode", "") or ""


# ─────────────────────────────────────────────────
# EXPORT
# ─────────────────────────────────────────────────
def export(practitioners: list[Practitioner], output_file: str) -> None:
    if not practitioners:
        print("No practitioners to export.")
        return

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame([asdict(p) for p in practitioners])

    df.rename(columns={
        "source_id":         "Source ID",
        "profile_url":       "Profile URL",
        "name":              "Name",
        "practice":          "Practice / Company",
        "credentials":       "Credentials",
        "specialties":       "Specialties",
        "phone":             "Phone",
        "email":             "Email",
        "website":           "Website",
        "full_address_raw":  "Full Address Raw",
        "address_line_1":    "Address Line 1",
        "city":              "City",
        "state":             "State",
        "zip_code":          "Zip",
        "country":           "Country",
        "latitude":          "Latitude",
        "longitude":         "Longitude",
        "google_maps_url":   "Google Maps URL",
        "species_treated":   "Species Treated",
        "country_iso":       "Country ISO",
    }, inplace=True)

    df.insert(0, "Source Site", SOURCE_SITE)

    col_order = [
        # Tier 1
        "Source Site", "Source ID", "Profile URL", "Name",
        # Tier 2
        "Practice / Company", "Credentials", "Specialties",
        "Phone", "Email", "Website",
        "Full Address Raw", "Address Line 1", "City", "State", "Zip", "Country",
        # Tier 3
        "Latitude", "Longitude", "Google Maps URL",
        "Species Treated", "Country ISO",
    ]
    df = df[[c for c in col_order if c in df.columns]]
    df.sort_values(["Country", "State", "Name"], inplace=True, ignore_index=True)

    def _is_us(row) -> bool:
        c = str(row.get("Country", "")).strip().lower()
        if c in US_COUNTRY_NAMES:
            return True
        iso = str(row.get("Country ISO", "")).strip().upper()
        if iso == "US":
            return True
        if not c:
            return str(row.get("State", "")).strip().upper() in US_STATE_CODES
        return False

    us_mask = df.apply(_is_us, axis=1)
    us_df   = df[us_mask].reset_index(drop=True)
    intl_df = df[~us_mask].reset_index(drop=True)

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        for sheet, data in [
            ("Members",       df),
            ("US Only",       us_df),
            ("International", intl_df),
        ]:
            data.to_excel(writer, index=False, sheet_name=sheet)
            ws = writer.sheets[sheet]
            ws.freeze_panes = "A2"
            for col in ws.columns:
                max_len = max(
                    (len(str(c.value)) for c in col if c.value), default=10
                )
                ws.column_dimensions[col[0].column_letter].width = min(
                    max_len + 2, 60
                )

        ws2 = writer.book.create_sheet("Summary")
        rows = [
            ("Source Site",                SOURCE_SITE),
            ("Note",                       "CIVT = College of Integrative "
                                           "Veterinary Therapies. Each row is a "
                                           "practitioner; Practice/Company is the "
                                           "associated practice."),
            ("", ""),
            ("Total Practitioners",        len(df)),
            ("US Practitioners",           len(us_df)),
            ("International Practitioners",len(intl_df)),
            ("Countries Covered",          df["Country"].replace("", pd.NA).nunique()),
            ("States Covered",             df["State"].replace("", pd.NA).nunique()),
            ("", ""),
            ("With Phone",                 int((df["Phone"] != "").sum())),
            ("With Email",                 int((df["Email"] != "").sum())),
            ("With Website",               int((df["Website"] != "").sum())),
            ("With Specialties",           int((df["Specialties"] != "").sum())),
            ("With Species Treated",       int((df["Species Treated"] != "").sum())),
            ("With Credentials",           int((df["Credentials"] != "").sum())),
            ("With Full Address",          int((df["Full Address Raw"] != "").sum())),
            ("With Parsed City (US)",      int((df["City"] != "").sum())),
            ("With Latitude",              int((df["Latitude"] != "").sum())),
            ("With Longitude",             int((df["Longitude"] != "").sum())),
        ]
        for i, (label, val) in enumerate(rows, 1):
            ws2[f"A{i}"] = label
            ws2[f"B{i}"] = val

        hdr_row = len(rows) + 2
        ws2[f"A{hdr_row}"] = "Country"
        ws2[f"B{hdr_row}"] = "Count"
        for i, (cnt_label, cnt) in enumerate(
            df["Country"].replace("", "(unknown)").value_counts().items(),
            start=hdr_row + 1
        ):
            ws2[f"A{i}"] = cnt_label
            ws2[f"B{i}"] = cnt

        ws2.column_dimensions["A"].width = 32
        ws2.column_dimensions["B"].width = 60

    print(f"  [OK] Excel -> {output_file}")


# ─────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────
def main():
    today = date.today().isoformat()
    default_out = f"outputs/civt_{today}.xlsx"

    ap = argparse.ArgumentParser(
        description="Scrape civtedu.org veterinary referral directory",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--output", default=default_out)
    ap.add_argument("--no-profiles", action="store_true",
                    help="Skip Phase 2 (faster, less data)")
    ap.add_argument("--limit", type=int, default=None,
                    help="DEBUG: cap to first N practitioners")
    args = ap.parse_args()

    print(f"\n{'='*55}")
    print(f"  CIVT Veterinary Referral Directory Scraper")
    print(f"  Source: {LISTING_URL}")
    print(f"{'='*55}\n")
    print(f"Output  : {args.output}")
    print(f"Profiles: {'disabled' if args.no_profiles else f'enabled (rate {PROFILE_DELAY}s)'}")
    if not HAS_USADDRESS:
        print("Note    : usaddress not installed -- US addresses won't be parsed.")
    print()

    try:
        practitioners = fetch_listings()
    except requests.HTTPError as e:
        print(f"\nERROR fetching listing pages: {e}")
        sys.exit(1)

    if args.limit is not None and args.limit > 0 and len(practitioners) > args.limit:
        print(f"DEBUG MODE: limiting to {args.limit} practitioners (was {len(practitioners)})")
        practitioners = practitioners[:args.limit]

    if not args.no_profiles and practitioners:
        enrich_profiles(practitioners)

    # State + address parsing (after profiles, so country_iso is set)
    for p in practitioners:
        normalize_us_state(p)
    if HAS_USADDRESS:
        for p in practitioners:
            parse_us_address(p)

    print()
    export(practitioners, args.output)
    print(f"\n{'='*55}")
    print(f"  Done! {len(practitioners)} practitioners written.")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    main()

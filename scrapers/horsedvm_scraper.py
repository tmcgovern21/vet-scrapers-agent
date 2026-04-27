"""
HorseDVM Equine Vet Directory Scraper
======================================
Scrapes practices listed at https://horsedvm.com/equine-vets.php and
their individual profile pages. Output conforms to SCRAPER_CONTRACT.md.

Phase 1: Single fetch of the index page; parses all <li> listings.
Phase 2: Visits each profile page (~225 requests at 0.75s rate limit)
         to capture phone, email, website, social links, description.
         Checkpoint auto-loads if present.

Note: This site lists veterinary PRACTICES/CLINICS, not individual
practitioners. Each row represents one practice. The Name and
Practice/Company columns are populated with the same value.

Requirements:
    pip install requests beautifulsoup4 pandas openpyxl usaddress

Usage:
    python horsedvm_scraper.py
    python horsedvm_scraper.py --output custom.xlsx
    python horsedvm_scraper.py --no-profiles   # skip Phase 2 (faster)
    python horsedvm_scraper.py --limit 10      # debug
"""

import argparse
import json
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

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
SOURCE_SITE     = "HorseDVM"
LISTING_URL     = "https://horsedvm.com/equine-vets.php"
PROFILE_DELAY   = 0.75
PROFILE_CHECKPOINT_FILE     = "horsedvm_profiles_checkpoint.json"
PROFILE_CHECKPOINT_INTERVAL = 25
DESCRIPTION_MAX_CHARS       = 1000  # truncate at export only

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/146.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Country IDs from the site's #selectCountry filter list
COUNTRY_MAP = {
    "22":  "Belgium",
    "40":  "Canada",
    "75":  "Finland",
    "205": "South Africa",
    "215": "Sweden",
    "216": "Switzerland",
    "235": "United Kingdom",
    "236": "United States",
}

# Practice type tokens — from #selectType plus 'hospital' which appears
# on listings but isn't in the filter UI.
PRACTICE_TYPES = {"referral", "university", "ambulatory", "hospital"}

# Special-equipment tokens. Includes filter-UI ids (#selectSpecial) plus
# extras seen on listings but not in the filter UI.
SPECIAL_EQUIPMENT = {
    "CTscan":       "CT Scan",
    "HStreadmill":  "High Speed Treadmill",
    "HBOT":         "Hyperbaric Oxygen Therapy",
    "inLab":        "Inhouse Lab",
    "MRI":          "MRI",
    "PET":          "PET Scan",
    "recoverypool": "Surgical Recovery Pool",
    "watertread":   "Underwater Treadmill",
    "bonescan":     "Bone Scan",
    "EKG":          "EKG",
    "ECG":          "ECG",
    "aquatread":    "Aquatic Treadmill",
    "flexiNeb":     "FlexiNeb",
    "therma":       "Thermography",
    "pool":         "Pool",
}
# Case-insensitive lookup (source HTML mixes cases, e.g. CTscan vs ctscan)
_EQ_LOWER = {k.lower(): v for k, v in SPECIAL_EQUIPMENT.items()}

# US state/territory 2-letter codes (used to validate usaddress output —
# the lib will happily tag "Manitoba R0G" as StateName for a Canadian
# address that the source mislabeled with countryId-236).
US_STATE_CODES = frozenset([
    "AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL",
    "IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE",
    "NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD",
    "TN","TX","UT","VT","VA","WA","WV","WI","WY",
    "AS","GU","MP","PR","VI",
])


# ─────────────────────────────────────────────────
# DATA MODEL
# ─────────────────────────────────────────────────
@dataclass
class Practice:
    source_id:         str = ""
    profile_url:       str = ""
    name:              str = ""
    full_address_raw:  str = ""
    address_line_1:    str = ""
    city:              str = ""
    state:             str = ""
    zip_code:          str = ""
    country:           str = ""
    phone:             str = ""
    email:             str = ""
    website:           str = ""
    facebook:          str = ""
    instagram:         str = ""
    twitter:           str = ""
    description:       str = ""
    logo_url:          str = ""
    practice_type:     str = ""
    special_equipment: str = ""


# ─────────────────────────────────────────────────
# CHECKPOINT (Phase 2 only)
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
# PHASE 1 — LISTING PAGE
# ─────────────────────────────────────────────────
def _equipment_for(class_token: str) -> str:
    return _EQ_LOWER.get(class_token.lower(), "")


def parse_listing(li) -> Practice:
    p = Practice()

    # Source ID: id="professional-{N}"
    el_id = li.get("id", "")
    if el_id.startswith("professional-"):
        p.source_id = el_id.split("-", 1)[1]

    classes = li.get("class", [])
    cls_str = " ".join(classes)

    # Country: extract digits only (handles "countryId-236MRI" concat bug)
    m = re.search(r"countryId-(\d+)", cls_str)
    if m:
        p.country = COUNTRY_MAP.get(m.group(1), "")

    # State: state-XX (empty for non-US — class is just "state-")
    for c in classes:
        sm = re.match(r"^state-([A-Z]{2})$", c)
        if sm:
            p.state = sm.group(1)
            break

    # Practice types (multiple possible)
    types = sorted({c for c in classes if c in PRACTICE_TYPES})
    p.practice_type = ", ".join(types)

    # Special equipment — handle two cases:
    #   (a) standalone class token (e.g. "MRI" as its own class)
    #   (b) concatenated to countryId (e.g. "countryId-236MRI" — source bug)
    found_eq = set()
    for c in classes:
        hit = _equipment_for(c)
        if hit:
            found_eq.add(hit)
        cm = re.match(r"countryId-\d+(.+)", c)
        if cm:
            hit = _equipment_for(cm.group(1))
            if hit:
                found_eq.add(hit)
    p.special_equipment = ", ".join(sorted(found_eq))

    # Practice name + profile URL
    a = li.select_one("a.practiceNameStyle")
    if a:
        p.name = a.get_text(strip=True)
        p.profile_url = a.get("href", "").strip()

    # Logo URL
    img = li.select_one("img.prof_logo")
    if img:
        p.logo_url = img.get("src", "").strip()

    # Listing-page address (fallback if profile page fails)
    addr_el = li.select_one(".pull-right.text-right") or li.select_one(".text-right")
    if addr_el:
        for br in addr_el.find_all("br"):
            br.replace_with(", ")
        text = addr_el.get_text(" ", strip=True)
        text = re.sub(r"\s+,", ",", text)
        text = re.sub(r"\s{2,}", " ", text)
        p.full_address_raw = text

    return p


def fetch_listings() -> list[Practice]:
    print(f"Fetching listing page: {LISTING_URL}")
    r = requests.get(LISTING_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    r.encoding = "utf-8"
    soup = BeautifulSoup(r.text, "html.parser")
    items = soup.select("li.professional-item")
    print(f"  Found {len(items)} listings.\n")
    return [parse_listing(li) for li in items]


# ─────────────────────────────────────────────────
# PHASE 2 — PROFILE PAGE
# ─────────────────────────────────────────────────
def scrape_profile(url: str) -> dict:
    out = {
        "full_address_raw": "", "phone": "", "email": "", "website": "",
        "facebook": "", "instagram": "", "twitter": "", "description": "",
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

    # Address block: street(s) + city/state/zip + phone + email all in <address>
    addr = soup.find("address")
    if addr:
        # Email — pull mailto link out of the tree first
        a_mail = addr.find("a", href=re.compile(r"^mailto:"))
        if a_mail:
            out["email"] = a_mail["href"].replace("mailto:", "").strip()
            a_mail.decompose()

        # Split into <br>-delimited segments
        segments, cur = [], []
        for node in addr.children:
            if getattr(node, "name", None) == "br":
                segments.append("".join(cur).strip())
                cur = []
            else:
                if hasattr(node, "get_text"):
                    cur.append(node.get_text(" ", strip=True))
                else:
                    cur.append(str(node))
        if cur:
            segments.append("".join(cur).strip())

        # Phone is the segment that begins with "P:" (after the <abbr> text).
        # Colon is REQUIRED — without it, US cities starting with "P"
        # (Phoenix, Prescott, Petaluma, etc.) match and clobber the address.
        phone_segs = [s for s in segments if re.match(r"^\s*P\s*:\s*", s)]
        addr_segs  = [s for s in segments
                      if s and not re.match(r"^\s*P\s*:\s*", s)]
        if phone_segs:
            out["phone"] = re.sub(r"^\s*P\s*:\s*", "", phone_segs[0]).strip()
        out["full_address_raw"] = ", ".join(s for s in addr_segs if s)

    # Social row — match by anchor title attribute
    social_ul = soup.find("ul", id="socialRow")
    if social_ul:
        for a in social_ul.find_all("a", href=True):
            title = (a.get("title") or "").lower()
            href  = a["href"].strip()
            href_lc = href.lower()
            if "homepage" in title:
                # Some source entries store the URL without a scheme
                # (e.g. "www.foo.com"). Prepend https:// for valid URLs.
                if href and not re.match(r"^https?://", href, re.IGNORECASE):
                    href = "https://" + href.lstrip("/")
                out["website"] = href
            elif "facebook" in title or "facebook.com" in href_lc:
                out["facebook"] = href
            elif "instagram" in title or "instagram.com" in href_lc:
                out["instagram"] = href
            elif ("twitter" in title or "twitter.com" in href_lc
                  or "x.com" in href_lc):
                out["twitter"] = href

    # Overview / description
    desc_el = soup.find("div", class_="practiceText")
    if desc_el:
        out["description"] = desc_el.get_text(" ", strip=True)

    return out


def enrich_profiles(practices: list[Practice]) -> None:
    cache = load_profile_checkpoint()
    if cache:
        print(f"  Phase 2 checkpoint: {len(cache)} profiles previously enriched (auto-resuming).")

    total = len(practices)
    print(f"\nScraping {total} profile pages (rate: {PROFILE_DELAY}s)...\n")

    uncached = sum(1 for p in practices
                   if p.profile_url and p.profile_url not in cache)
    start = time.time()
    processed, fetched = 0, 0

    for p in practices:
        if not p.profile_url:
            continue

        if p.profile_url in cache:
            d = cache[p.profile_url]
        else:
            d = scrape_profile(p.profile_url)
            cache[p.profile_url] = d
            fetched += 1
            time.sleep(PROFILE_DELAY)

        if d.get("full_address_raw"):
            p.full_address_raw = d["full_address_raw"]
        p.phone       = d.get("phone", "")
        p.email       = d.get("email", "")
        p.website     = d.get("website", "")
        p.facebook    = d.get("facebook", "")
        p.instagram   = d.get("instagram", "")
        p.twitter     = d.get("twitter", "")
        p.description = d.get("description", "")
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
# US ADDRESS PARSING
# ─────────────────────────────────────────────────
def parse_us_address(p: Practice) -> None:
    if p.country != "United States" or not p.full_address_raw:
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
    if not p.state:
        cand = (tagged.get("StateName", "") or "").strip().upper()
        if cand in US_STATE_CODES:
            p.state = cand
    if not p.zip_code:
        p.zip_code = tagged.get("ZipCode", "") or ""


# ─────────────────────────────────────────────────
# EXPORT
# ─────────────────────────────────────────────────
def _truncate_description(s: str) -> str:
    if not isinstance(s, str) or len(s) <= DESCRIPTION_MAX_CHARS:
        return s or ""
    return s[:DESCRIPTION_MAX_CHARS].rstrip() + " [...]"


def export(practices: list[Practice], output_file: str) -> None:
    if not practices:
        print("No practices to export.")
        return

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame([asdict(p) for p in practices])

    # Truncate descriptions for Excel readability (export-only, per spec).
    df["description"] = df["description"].apply(_truncate_description)

    # Per spec: Name = practice name, Practice/Company = practice name (same)
    df["Practice / Company"] = df["name"]

    df.rename(columns={
        "source_id":         "Source ID",
        "profile_url":       "Profile URL",
        "name":              "Name",
        "full_address_raw":  "Full Address Raw",
        "address_line_1":    "Address Line 1",
        "city":              "City",
        "state":             "State",
        "zip_code":          "Zip",
        "country":           "Country",
        "phone":             "Phone",
        "email":             "Email",
        "website":           "Website",
        "facebook":          "Facebook",
        "instagram":         "Instagram",
        "twitter":           "Twitter URL",
        "description":       "About / Description",
        "logo_url":          "Logo URL",
        "practice_type":     "Practice Type",
        "special_equipment": "Special Equipment",
    }, inplace=True)

    df.insert(0, "Source Site", SOURCE_SITE)

    col_order = [
        # Tier 1
        "Source Site", "Source ID", "Profile URL", "Name",
        # Tier 2
        "Practice / Company",
        "Full Address Raw", "Address Line 1", "City", "State", "Zip", "Country",
        "Phone", "Email", "Website",
        "Facebook", "Instagram", "Twitter URL",
        "About / Description",
        # Tier 3
        "Logo URL", "Practice Type", "Special Equipment",
    ]
    df = df[[c for c in col_order if c in df.columns]]
    df.sort_values(["Country", "State", "Name"], inplace=True, ignore_index=True)

    # US / International split (matches AAEP logic)
    us_codes = US_STATE_CODES
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

        # Summary sheet
        ws2 = writer.book.create_sheet("Summary")
        rows = [
            ("Source Site",             SOURCE_SITE),
            ("Note",                    "This scraper captures veterinary "
                                        "practices, not individual practitioners. "
                                        "Each row represents a practice/clinic."),
            ("", ""),
            ("Total Practices",         len(df)),
            ("US Practices",            len(us_df)),
            ("International Practices", len(intl_df)),
            ("Countries Covered",       df["Country"].replace("", pd.NA).nunique()),
            ("States Covered",          df["State"].replace("", pd.NA).nunique()),
            ("", ""),
            ("With Phone",              int((df["Phone"] != "").sum())),
            ("With Email",              int((df["Email"] != "").sum())),
            ("With Website",            int((df["Website"] != "").sum())),
            ("With Facebook",           int((df["Facebook"] != "").sum())),
            ("With Instagram",          int((df["Instagram"] != "").sum())),
            ("With Twitter",            int((df["Twitter URL"] != "").sum())),
            ("With Description",        int((df["About / Description"] != "").sum())),
            ("With Full Address",       int((df["Full Address Raw"] != "").sum())),
            ("With Parsed City (US)",   int((df["City"] != "").sum())),
            ("With Practice Type",      int((df["Practice Type"] != "").sum())),
            ("With Special Equipment",  int((df["Special Equipment"] != "").sum())),
        ]
        for i, (label, val) in enumerate(rows, 1):
            ws2[f"A{i}"] = label
            ws2[f"B{i}"] = val

        # Country breakdown
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
    default_out = f"outputs/horsedvm_{today}.xlsx"

    ap = argparse.ArgumentParser(
        description="Scrape horsedvm.com equine vet practices",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--output", default=default_out)
    ap.add_argument("--no-profiles", action="store_true",
                    help="Skip Phase 2 (faster, less data)")
    ap.add_argument("--limit", type=int, default=None,
                    help="DEBUG: cap to first N practices")
    args = ap.parse_args()

    print(f"\n{'='*55}")
    print(f"  HorseDVM Equine Vet Directory Scraper")
    print(f"  Source: {LISTING_URL}")
    print(f"{'='*55}\n")
    print(f"Output  : {args.output}")
    print(f"Profiles: {'disabled' if args.no_profiles else f'enabled (rate {PROFILE_DELAY}s)'}")
    if not HAS_USADDRESS:
        print("Note    : usaddress not installed -- US addresses won't be parsed.")
    print()

    # ── Phase 1 ──
    try:
        practices = fetch_listings()
    except requests.HTTPError as e:
        print(f"\nERROR fetching listing page: {e}")
        sys.exit(1)

    if args.limit is not None and args.limit > 0 and len(practices) > args.limit:
        print(f"DEBUG MODE: limiting to {args.limit} practices (was {len(practices)})")
        practices = practices[:args.limit]

    # ── Phase 2 ──
    if not args.no_profiles and practices:
        enrich_profiles(practices)

    # ── US address parsing (post-Phase-2 because it's the better source) ──
    if HAS_USADDRESS:
        for p in practices:
            parse_us_address(p)

    print()
    export(practices, args.output)
    print(f"\n{'='*55}")
    print(f"  Done! {len(practices)} practices written.")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    main()

"""
AAHA Accredited-Hospital Scraper — Selenium-class, two-pass
============================================================
First Selenium-class scraper in this repo. Drives a real Chrome via
undetected-chromedriver; bypasses Cloudflare and satisfies reCAPTCHA v3's
score-bound submit (tokens minted outside the page's own click handler are
silently rejected by the WP backend, so all interaction goes through the
visible UI: autocomplete + pac-item click + Search button click).

Two-pass architecture:
  Pass 1 — Listing sweep: ~270 city queries; cap=30 results/query (the 30
           NEAREST to each search center, regardless of radius). Stores
           per-recno {found_via_query, distance}; keeps the smallest
           distance across overlapping queries.
  Pass 2 — Detail enrichment: GROUP recnos by their Pass-1 found_via query;
           for each group, do ONE search to land on the results page, then
           click each recno-lookup link, parse the detail, driver.back() to
           the results, and repeat. Empirically validated 5/5 success on the
           same Boston recnos that scored 1/5 with re-search-per-recno —
           reCAPTCHA score, once warmed by a successful initial search,
           survives the in-page click + back-button loop.

Same physical hospital may appear with TWO recnos (one for GP accreditation,
one for Referral accreditation) — e.g. recno=8290 ("VCA South Shore #154")
and recno=449624 ("VCA South Shore #154 -Referral") share an address but
are distinct accreditations. We do NOT dedupe by address.

Output: outputs/aaha_{date}.xlsx with sheets Hospitals / US Only /
International / Summary, matching SCRAPER_CONTRACT.md (with the AAHA
exception: Name and Credentials are blank by design — hospital-level data,
no practitioner attached to a row).

Source: https://www.aaha.org/for-pet-parents/find-an-aaha-accredited-animal-hospital-near-me/

Requirements:
    pip install undetected-chromedriver webdriver-manager setuptools \\
                pandas openpyxl beautifulsoup4 usaddress

Usage:
    python -m scrapers.aaha_scraper                       # full two-pass
    python -m scrapers.aaha_scraper --listing-only        # Pass 1 only
    python -m scrapers.aaha_scraper --detail-only         # Pass 2 only
    python -m scrapers.aaha_scraper --detail-only --retry-failed
                                       # re-attempt prior detail_failed rows
    python -m scrapers.aaha_scraper --resume              # continue from checkpoint
    python -m scrapers.aaha_scraper --limit 30            # debug cap

Per-host budget at ~12 s/query: Pass 1 ≈ 60 min for ~270 queries;
Pass 2 ≈ 18-25 hours for ~4,500 recnos (overnight).

Resuming across machines:
    1. Run on scraping computer:  python -m scrapers.aaha_scraper
    2. Transfer outputs/ folder to main computer (xlsx + 3 checkpoint JSONs).
    3. On main computer: audit the xlsx, identify failure patterns.
    4. Make code fixes, commit + push.
    5. On scraping computer: git pull, copy outputs/ back into the project,
       run:  python -m scrapers.aaha_scraper --detail-only --retry-failed
    6. New successes overwrite the "detail_failed" placeholders in the
       checkpoint; "ok" rows are skipped.
    7. Iterate until residual failures are irreducible.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import traceback
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from urllib.parse import quote_plus
from typing import Iterable

import pandas as pd
from bs4 import BeautifulSoup

try:
    import usaddress  # type: ignore
    HAS_USADDRESS = True
except ImportError:
    HAS_USADDRESS = False

import undetected_chromedriver as uc
from selenium.common.exceptions import (
    TimeoutException, WebDriverException, NoSuchElementException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


# ─────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────
SOURCE_SITE = "AAHA"
LOCATOR_URL = (
    "https://www.aaha.org/for-pet-parents/"
    "find-an-aaha-accredited-animal-hospital-near-me/"
)

PER_QUERY_PAUSE     = 3.0   # between queries
RESULTS_WAIT        = 8.0   # after Search click before reading page_source
DETAIL_WAIT         = 8.0   # after recno-lookup click before reading detail
AUTOCOMPLETE_WAIT   = 3.0   # after typing, wait for .pac-item suggestions
SESSION_RECYCLE     = 50    # restart browser every N queries
DETAIL_RETRY_PAUSE  = 10.0  # pause after empty-form failure before retry
CHECKPOINT_INTERVAL = 25
CHROME_VERSION_MAIN = 147

# Checkpoints live alongside the xlsx in outputs/ so the user can transfer a
# single folder between the scraping computer and main computer.
LISTING_CHECKPOINT = "outputs/aaha_listing_checkpoint.json"   # query -> {count}
RECNO_CHECKPOINT   = "outputs/aaha_recno_checkpoint.json"     # recno -> listing dict
DETAIL_CHECKPOINT  = "outputs/aaha_detail_checkpoint.json"    # recno -> detail dict


KILL_BANNER_JS = """
['#usercentrics-cmp-ui','#uc-banner','#CybotCookiebotDialog',
 'aside[data-nosnippet]','aside[keyboard-events]'].forEach(s=>{
    document.querySelectorAll(s).forEach(e => e.remove());
});
"""


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
CA_PROVINCES = frozenset({
    "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU",
    "ON", "PE", "QC", "SK", "YT",
})


# ─────────────────────────────────────────────────
# SEARCH CENTERS (Pass 1)
# ─────────────────────────────────────────────────
# Hand-curated city queries. Each is typed into the locator's autocomplete
# input; the first .pac-item is selected. Density is intentionally higher
# in metro areas because the per-query cap is 30. Total ~275 centers.
SEARCH_CENTERS: list[str] = [
    # NYC metro
    "Manhattan, NY", "Brooklyn, NY", "Bronx, NY", "Queens, NY",
    "Staten Island, NY", "Yonkers, NY", "White Plains, NY", "Hempstead, NY",
    "Newark, NJ", "Jersey City, NJ", "Paterson, NJ", "Stamford, CT",
    "New Haven, CT", "Hartford, CT", "Bridgeport, CT",
    # New England
    "Boston, MA", "Worcester, MA", "Springfield, MA", "Cape Cod, MA",
    "Providence, RI", "Manchester, NH", "Portland, ME", "Burlington, VT",
    "Albany, NY", "Buffalo, NY", "Rochester, NY", "Syracuse, NY",
    "Binghamton, NY", "Ithaca, NY",
    # Mid-Atlantic
    "Philadelphia, PA", "Pittsburgh, PA", "Allentown, PA", "Harrisburg, PA",
    "Erie, PA", "Scranton, PA", "State College, PA",
    "Wilmington, DE", "Dover, DE",
    "Baltimore, MD", "Annapolis, MD", "Frederick, MD", "Salisbury, MD",
    "Washington, DC",
    "Richmond, VA", "Virginia Beach, VA", "Norfolk, VA", "Roanoke, VA",
    "Charlottesville, VA", "Arlington, VA", "Fairfax, VA",
    "Charleston, WV", "Morgantown, WV",
    # South
    "Charlotte, NC", "Raleigh, NC", "Greensboro, NC", "Asheville, NC",
    "Wilmington, NC",
    "Columbia, SC", "Charleston, SC", "Greenville, SC",
    "Atlanta, GA", "Savannah, GA", "Augusta, GA", "Macon, GA", "Athens, GA",
    "Jacksonville, FL", "Miami, FL", "Tampa, FL", "Orlando, FL",
    "Fort Lauderdale, FL", "Tallahassee, FL", "Pensacola, FL",
    "Sarasota, FL", "Fort Myers, FL", "West Palm Beach, FL",
    "Birmingham, AL", "Mobile, AL", "Montgomery, AL", "Huntsville, AL",
    "Jackson, MS", "Hattiesburg, MS",
    "New Orleans, LA", "Baton Rouge, LA", "Shreveport, LA", "Lafayette, LA",
    "Memphis, TN", "Nashville, TN", "Knoxville, TN", "Chattanooga, TN",
    "Lexington, KY", "Louisville, KY",
    "Little Rock, AR", "Fayetteville, AR",
    # Texas + OK
    "Dallas, TX", "Fort Worth, TX", "Houston, TX", "Austin, TX",
    "San Antonio, TX", "El Paso, TX", "Lubbock, TX", "Amarillo, TX",
    "Corpus Christi, TX", "Waco, TX", "Tyler, TX", "Beaumont, TX",
    "McAllen, TX", "Midland, TX",
    "Oklahoma City, OK", "Tulsa, OK",
    # Midwest
    "Chicago, IL", "Naperville, IL", "Rockford, IL", "Peoria, IL",
    "Springfield, IL", "Champaign, IL",
    "Indianapolis, IN", "Fort Wayne, IN", "Evansville, IN", "South Bend, IN",
    "Detroit, MI", "Grand Rapids, MI", "Lansing, MI", "Ann Arbor, MI",
    "Flint, MI", "Kalamazoo, MI", "Traverse City, MI", "Marquette, MI",
    "Cleveland, OH", "Columbus, OH", "Cincinnati, OH", "Akron, OH",
    "Toledo, OH", "Dayton, OH", "Youngstown, OH",
    "Milwaukee, WI", "Madison, WI", "Green Bay, WI", "Eau Claire, WI",
    "Minneapolis, MN", "Saint Paul, MN", "Duluth, MN", "Rochester, MN",
    "Des Moines, IA", "Cedar Rapids, IA", "Davenport, IA", "Sioux City, IA",
    "Saint Louis, MO", "Kansas City, MO", "Springfield, MO", "Columbia, MO",
    # Plains
    "Omaha, NE", "Lincoln, NE",
    "Wichita, KS", "Topeka, KS",
    "Fargo, ND", "Bismarck, ND",
    "Sioux Falls, SD", "Rapid City, SD",
    # Mountain
    "Denver, CO", "Colorado Springs, CO", "Boulder, CO", "Fort Collins, CO",
    "Grand Junction, CO",
    "Albuquerque, NM", "Santa Fe, NM", "Las Cruces, NM",
    "Phoenix, AZ", "Tucson, AZ", "Mesa, AZ", "Flagstaff, AZ", "Yuma, AZ",
    "Salt Lake City, UT", "Provo, UT", "Saint George, UT",
    "Las Vegas, NV", "Reno, NV", "Henderson, NV",
    "Boise, ID", "Idaho Falls, ID", "Coeur d'Alene, ID",
    "Cheyenne, WY", "Casper, WY", "Jackson, WY",
    "Billings, MT", "Missoula, MT", "Bozeman, MT", "Great Falls, MT",
    # West Coast
    "Los Angeles, CA", "Long Beach, CA", "Anaheim, CA", "Santa Ana, CA",
    "Irvine, CA", "Pasadena, CA", "Burbank, CA", "Riverside, CA",
    "San Bernardino, CA", "Bakersfield, CA",
    "San Diego, CA", "Chula Vista, CA", "Oceanside, CA",
    "San Francisco, CA", "Oakland, CA", "San Jose, CA", "Berkeley, CA",
    "Palo Alto, CA", "Santa Rosa, CA", "Fremont, CA",
    "Sacramento, CA", "Stockton, CA", "Modesto, CA", "Fresno, CA",
    "Visalia, CA", "Salinas, CA", "Santa Cruz, CA", "Monterey, CA",
    "Santa Barbara, CA", "Ventura, CA",
    "Eureka, CA", "Redding, CA", "Chico, CA",
    "Portland, OR", "Eugene, OR", "Salem, OR", "Bend, OR", "Medford, OR",
    "Seattle, WA", "Tacoma, WA", "Spokane, WA", "Bellevue, WA",
    "Bellingham, WA", "Olympia, WA", "Yakima, WA", "Tri-Cities, WA",
    "Anchorage, AK", "Fairbanks, AK", "Juneau, AK",
    "Honolulu, HI", "Hilo, HI",
    # Canada
    "Toronto, ON", "Mississauga, ON", "Ottawa, ON", "London, ON",
    "Hamilton, ON", "Kitchener, ON", "Windsor, ON",
    "Montreal, QC", "Quebec City, QC", "Sherbrooke, QC",
    "Vancouver, BC", "Victoria, BC", "Kelowna, BC", "Surrey, BC",
    "Calgary, AB", "Edmonton, AB", "Red Deer, AB",
    "Winnipeg, MB", "Saskatoon, SK", "Regina, SK",
    "Halifax, NS", "Saint John, NB", "Charlottetown, PE",
    "St. John's, NL", "Whitehorse, YT", "Yellowknife, NT",
    # Asia
    "Tokyo, Japan", "Osaka, Japan", "Kyoto, Japan",
    "Sapporo, Japan", "Fukuoka, Japan",
    "Seoul, South Korea", "Busan, South Korea",
]


# ─────────────────────────────────────────────────
# CHECKPOINT IO
# ─────────────────────────────────────────────────
def _load_json(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(path: str, data: dict) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


# ─────────────────────────────────────────────────
# DRIVER LIFECYCLE
# ─────────────────────────────────────────────────
class Driver:
    def __init__(self):
        self.driver = None
        self._start()

    def _start(self):
        opts = uc.ChromeOptions()
        opts.add_argument("--window-size=1280,1080")
        # Visible window required — headless reliably fails CF managed challenge.
        self.driver = uc.Chrome(options=opts, version_main=CHROME_VERSION_MAIN)
        self.driver.set_page_load_timeout(60)
        self.driver.set_script_timeout(45)

    def restart(self):
        try:
            if self.driver:
                self.driver.quit()
        except Exception:
            pass
        time.sleep(2)
        self._start()

    def quit(self):
        try:
            if self.driver:
                self.driver.quit()
        except Exception:
            pass


# ─────────────────────────────────────────────────
# CORE UI HELPERS
# ─────────────────────────────────────────────────
def _wait_for_form(driver):
    WebDriverWait(driver, 45).until(
        EC.presence_of_element_located((By.ID, "hospital-locator-form"))
    )
    time.sleep(4)
    driver.execute_script(KILL_BANNER_JS)


def _do_search(driver, query: str) -> bool:
    """Drive autocomplete + click pac-item + click Search. Return True if
    a results-list rendered (server accepted reCAPTCHA token)."""
    driver.get(LOCATOR_URL)
    _wait_for_form(driver)

    addr = driver.find_element(By.ID, "autocomplete-address")
    addr.click()
    addr.send_keys(query)
    time.sleep(AUTOCOMPLETE_WAIT)

    items = driver.find_elements(By.CSS_SELECTOR, ".pac-item")
    if items:
        try:
            items[0].click()
        except WebDriverException:
            addr.send_keys(Keys.ARROW_DOWN); time.sleep(0.4)
            addr.send_keys(Keys.ENTER)
    else:
        addr.send_keys(Keys.ARROW_DOWN); time.sleep(0.4)
        addr.send_keys(Keys.ENTER)
    time.sleep(1.5)

    driver.execute_script(KILL_BANNER_JS)
    driver.execute_script(
        "document.querySelector('[name=\"radius\"]').value='200';"
    )
    btn = driver.find_element(By.ID, "locator-search")
    btn.click()
    time.sleep(RESULTS_WAIT)

    return "hospitalLocatorResultsList" in driver.page_source


# ─────────────────────────────────────────────────
# PASS 1 — LISTING SWEEP
# ─────────────────────────────────────────────────
LISTING_CARD_RE = re.compile(
    r'<a class="recno-lookup"[^>]+'
    r'href="([^"]+)"[^>]+'
    r'data-recno="(\d+)"[^>]+'
    r'data-hospital="([^"]*)"',
    re.MULTILINE,
)
LOCATIONS_RE = re.compile(
    r"var\s+locations\s*=\s*(\[[\s\S]*?\]);", re.MULTILINE
)


def _parse_distance_miles(s: str) -> float:
    """'0.66 Miles' -> 0.66. Returns inf on parse failure."""
    if not s:
        return float("inf")
    m = re.match(r"\s*([\d.]+)", s)
    return float(m.group(1)) if m else float("inf")


def parse_listing_html(html: str, query: str) -> list[dict]:
    """Combine var locations JSON + recno-lookup card markup into per-recno
    dicts. The locations JSON is index-aligned with the cards."""
    soup = BeautifulSoup(html, "html.parser")
    container = soup.find("div", id="hospitalLocatorResultsList")
    if container is None:
        return []

    cards = []
    for a in container.select("a.recno-lookup"):
        cards.append({
            "recno":    (a.get("data-recno") or "").strip(),
            "hospital": (a.get("data-hospital") or "").strip(),
            "href":     (a.get("href") or "").strip(),
        })

    # Attach address/phone/distance from each card's nearby spans
    for i, card in enumerate(cards):
        # Each card lives in a <div class="col-lg-4 col-md-6 mb-5">; find the
        # one whose first descendant a.recno-lookup matches our recno.
        anchor = container.find("a", attrs={"data-recno": card["recno"]})
        if anchor:
            wrap = anchor.find_parent("div", class_=re.compile(r"col-"))
            if wrap:
                addr = wrap.find("span", class_="hlrp_address")
                if addr:
                    for br in addr.find_all("br"): br.replace_with(", ")
                    card["address"] = re.sub(r"\s+", " ", addr.get_text(" ", strip=True))
                phone = wrap.find("span", class_="hlrp_phone")
                if phone:
                    card["phone"] = phone.get_text(" ", strip=True)
                dist = wrap.find("span", class_="hlrp_distance")
                if dist:
                    card["distance_str"] = dist.get_text(" ", strip=True)
                    card["distance"] = _parse_distance_miles(card["distance_str"])
                for ml in wrap.find_all("a", href=True):
                    if "maps.google" in ml["href"]:
                        card["maps_url"] = ml["href"]; break

    # var locations JSON for lat/lng/icon
    m = LOCATIONS_RE.search(html)
    if m:
        try:
            locs = json.loads(m.group(1))
        except json.JSONDecodeError:
            locs = []
    else:
        locs = []
    while locs and locs[0].get("icon") == "house":
        locs = locs[1:]
    for i in range(min(len(cards), len(locs))):
        loc = locs[i]
        cards[i].setdefault("lat", str(loc.get("lat", "")))
        cards[i].setdefault("lng", str(loc.get("lng", "")))
        cards[i].setdefault("icon", (loc.get("icon") or "").lower())
        # distance from JSON (scientific notation in some rows) — fall back
        if "distance" not in cards[i]:
            try:
                cards[i]["distance"] = float(loc.get("distance", "inf"))
            except Exception:
                cards[i]["distance"] = float("inf")

    for c in cards:
        c["found_via"] = query
    return cards


def listing_sweep(queries: list[str], limit: int | None) -> dict:
    """Execute Pass 1. Returns {recno -> per-recno dict}. Persists progress
    via two checkpoints: LISTING_CHECKPOINT (per-query state) and
    RECNO_CHECKPOINT (per-recno data)."""
    listing_done: dict = _load_json(LISTING_CHECKPOINT)
    recno_data:   dict = _load_json(RECNO_CHECKPOINT)
    if listing_done or recno_data:
        print(f"  Auto-resume: {len(listing_done)} queries done, "
              f"{len(recno_data)} unique recnos.")

    pending = [q for q in queries if q not in listing_done]
    if not pending:
        print("  Pass 1: all queries already complete.")
        return recno_data

    drv = Driver()
    queries_this_session = 0
    empty_streak = 0
    start = time.time()

    try:
        for q in pending:
            if (queries_this_session > 0
                    and queries_this_session % SESSION_RECYCLE == 0):
                print(f"  -- Pass1: recycling browser after {SESSION_RECYCLE} queries --")
                drv.restart()

            try:
                got = _do_search(drv.driver, q)
            except (TimeoutException, WebDriverException) as e:
                print(f"  [warn] Pass1 {q!r}: {type(e).__name__} -- restart driver")
                drv.restart()
                try:
                    got = _do_search(drv.driver, q)
                except Exception as e2:
                    print(f"  [skip] Pass1 {q!r}: {e2}")
                    listing_done[q] = {"err": str(e2)[:200], "count": 0}
                    continue

            cards = parse_listing_html(drv.driver.page_source, q) if got else []
            for c in cards:
                rid = c["recno"]
                if not rid:
                    continue
                prev = recno_data.get(rid)
                # Keep the entry with the smallest distance — that query
                # has the recno closest to its center, lowest "noise" in
                # the result. Used by Pass 2 to replay the strongest query.
                if (prev is None
                        or c.get("distance", float("inf"))
                        < prev.get("distance", float("inf"))):
                    recno_data[rid] = c
            listing_done[q] = {"count": len(cards)}
            queries_this_session += 1
            empty_streak = 0 if cards else (empty_streak + 1)

            elapsed = int(time.time() - start)
            print(f"  [{len(listing_done)}/{len(queries)}] {q!r}: {len(cards)} cards "
                  f"(unique recnos: {len(recno_data)}, "
                  f"elapsed {elapsed//60}m{elapsed%60}s)")

            if len(listing_done) % CHECKPOINT_INTERVAL == 0:
                _save_json(LISTING_CHECKPOINT, listing_done)
                _save_json(RECNO_CHECKPOINT, recno_data)

            if empty_streak >= 5:
                print(f"  -- Pass1: {empty_streak} empty queries in a row, recycling --")
                drv.restart()
                empty_streak = 0

            if limit is not None and len(recno_data) >= limit:
                print(f"  -- --limit {limit} reached, stopping Pass 1 --")
                break

            time.sleep(PER_QUERY_PAUSE)
    finally:
        drv.quit()
        _save_json(LISTING_CHECKPOINT, listing_done)
        _save_json(RECNO_CHECKPOINT, recno_data)
        print(f"\n  Pass 1 complete: {len(listing_done)} queries, "
              f"{len(recno_data)} unique recnos.")

    return recno_data


# ─────────────────────────────────────────────────
# PASS 2 — DETAIL ENRICHMENT
# ─────────────────────────────────────────────────
def parse_detail_html(html: str) -> dict | None:
    """Parse a single-hospital detail page. Returns None if the page is
    actually the empty form (reCAPTCHA score rejected)."""
    soup = BeautifulSoup(html, "html.parser")
    if not soup.find("h2", class_="hldp_hospital_name"):
        return None  # empty form bounce

    out: dict = {}
    h2 = soup.find("h2", class_="hldp_hospital_name")
    out["hospital_name"] = h2.get_text(" ", strip=True)

    # Accreditation year — h4 above the map, no class
    for h4 in soup.find_all("h4"):
        t = h4.get_text(" ", strip=True)
        if "accredited since" in t.lower():
            m = re.search(r"\b(19|20)\d{2}\b", t)
            if m:
                out["accreditation_year"] = m.group(0)
            break

    # Walk every card-deck card (Visit Us, Contact Us, Veterinarians, Species
    # Treated, Specialties, Hospital Hours, Mission, ...).
    cards: dict[str, dict] = {}
    for card in soup.find_all("div", class_="card"):
        header = card.find("header", class_="card-header")
        body   = card.find("div", class_="card-body")
        if not header or not body:
            continue
        title = header.get_text(" ", strip=True)
        for br in body.find_all("br"): br.replace_with("\n")
        text  = body.get_text("\n", strip=True)
        text  = re.sub(r"\n\s*\n+", "\n", text).strip()
        links = [{"text": a.get_text(" ", strip=True), "href": a["href"]}
                 for a in body.find_all("a", href=True)]
        cards[title] = {"text": text, "links": links}

    visit = cards.get("Visit Us") or cards.get("Visit us")
    if visit:
        addr_lines = [
            ln.strip() for ln in visit["text"].split("\n")
            if ln.strip() and "Get Driving" not in ln
        ]
        out["address_lines"]    = addr_lines
        out["full_address_raw"] = ", ".join(
            ln.rstrip(",") for ln in addr_lines if ln
        )
        for L in visit["links"]:
            if "maps.google" in L["href"]:
                out["maps_url"] = L["href"]; break

    contact = cards.get("Contact Us") or cards.get("Contact us")
    if contact:
        for L in contact["links"]:
            href = L["href"]
            if href.startswith("mailto:"):
                out["email"] = href[len("mailto:"):].strip()
                break
        for L in contact["links"]:
            href = L["href"]
            if (href.startswith("http")
                    and "maps.google" not in href
                    and "facebook.com" not in href):
                out["website"] = href; break
        for L in contact["links"]:
            if "facebook.com" in L["href"]:
                out["facebook"] = L["href"]; break
        m = re.search(r"Phone:\s*([^\n]+)", contact["text"])
        if m: out["phone"] = m.group(1).strip()

    vets = cards.get("Veterinarians")
    if vets:
        names = [ln.strip() for ln in vets["text"].split("\n") if ln.strip()]
        if names:
            out["veterinarians"] = "; ".join(names)

    species = cards.get("Species Treated")
    if species:
        items = [ln.strip() for ln in species["text"].split("\n") if ln.strip()]
        out["species_treated"] = ", ".join(items)

    specs = cards.get("Specialties")
    if specs:
        items = [ln.strip() for ln in specs["text"].split("\n") if ln.strip()]
        out["specialties"] = ", ".join(items)

    hours = cards.get("Hospital Hours")
    if hours:
        out["hospital_hours"] = hours["text"]

    mission = cards.get("Mission")
    if mission:
        out["mission"] = mission["text"]

    return out


def _click_recno_lookup(driver, recno: str) -> bool:
    """On a results page, click the recno-lookup link for the given recno.
    Returns True if the click fired (the next page may still bounce empty)."""
    driver.execute_script(KILL_BANNER_JS)
    try:
        link = driver.find_element(
            By.CSS_SELECTOR, f'a.recno-lookup[data-recno="{recno}"]'
        )
    except NoSuchElementException:
        return False
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", link)
    time.sleep(0.5)
    driver.execute_script(KILL_BANNER_JS)
    driver.execute_script("arguments[0].click();", link)
    time.sleep(DETAIL_WAIT)
    return True


def detail_pass(recno_data: dict, limit: int | None,
                retry_failed: bool = False) -> dict:
    """Pass 2: enrich each recno via the locator's recno-lookup detail page.

    Architecture: GROUP recnos by their found_via query, then for each group
    do ONE search and click each recno-lookup link in turn, using
    driver.back() to return to the results between clicks. This empirically
    achieves much higher success than re-searching per recno (the back-button
    probe got 5/5 vs 1/5 for re-search-per-recno against the same recnos).
    The reCAPTCHA score, once established by a successful initial search,
    survives the recno-lookup -> back -> recno-lookup loop.

    With retry_failed=True, any prior 'detail_failed' rows are flipped to
    'pending_retry' so they get re-attempted. 'ok' rows are always skipped.
    """
    detail_data: dict = _load_json(DETAIL_CHECKPOINT)
    if detail_data:
        print(f"  Auto-resume: {len(detail_data)} details already cached.")

    if retry_failed:
        flipped = 0
        for rid, d in detail_data.items():
            if d.get("status") == "detail_failed":
                d["status"] = "pending_retry"
                flipped += 1
        if flipped:
            print(f"  --retry-failed: flipped {flipped} detail_failed -> pending_retry")
            _save_json(DETAIL_CHECKPOINT, detail_data)

    # Bucket pending recnos by their found_via query. This lets us search
    # once per query and click through every needed recno in that result set.
    pending_by_query: dict[str, list[tuple[str, dict]]] = {}
    for rid, listing in recno_data.items():
        if (rid in detail_data
                and detail_data[rid].get("status") not in ("pending_retry",)):
            continue  # already ok, skip
        q = listing.get("found_via") or "Boston, MA"
        pending_by_query.setdefault(q, []).append((rid, listing))

    if limit is not None:
        # Apply the limit globally across all groups (preserves debug-friendly
        # behavior of --limit). Fill groups in deterministic order until cap.
        seen = 0
        capped: dict[str, list] = {}
        for q in sorted(pending_by_query):
            for rid, listing in pending_by_query[q]:
                if seen >= limit: break
                capped.setdefault(q, []).append((rid, listing))
                seen += 1
            if seen >= limit: break
        pending_by_query = capped
        print(f"  Pass 2 limit: {limit} recnos across {len(pending_by_query)} query groups")

    total_pending = sum(len(v) for v in pending_by_query.values())
    if total_pending == 0:
        print("  Pass 2: nothing to do.")
        return detail_data

    drv = Driver()
    clicks_this_session = 0
    start = time.time()
    total_done = 0

    try:
        for q, recnos_in_group in sorted(pending_by_query.items()):
            print(f"\n  >>> query group {q!r} ({len(recnos_in_group)} recnos)")

            # Establish the results page for this query. If the search itself
            # bounces, mark every recno in the group failed and move on.
            if not _ensure_results_page(drv, q):
                print(f"    [search-failed] {q!r} bounced; marking "
                      f"{len(recnos_in_group)} recnos as detail_failed")
                for rid, _ in recnos_in_group:
                    detail_data[rid] = {"status": "detail_failed", "found_via": q}
                    total_done += 1
                _save_json(DETAIL_CHECKPOINT, detail_data)
                continue

            for rid, listing in recnos_in_group:
                # Recycle the browser session (and re-search this query) on
                # the boundary, mid-group.
                if (clicks_this_session > 0
                        and clicks_this_session % SESSION_RECYCLE == 0):
                    print(f"    -- recycling browser after {SESSION_RECYCLE} clicks --")
                    drv.restart()
                    if not _ensure_results_page(drv, q):
                        print(f"    [search-failed after recycle] {q!r}")
                        detail_data[rid] = {"status": "detail_failed", "found_via": q}
                        total_done += 1
                        clicks_this_session += 1
                        continue

                detail = _click_parse_back(drv.driver, rid)

                # Soft retry: one more click attempt without re-searching
                if detail is None:
                    print(f"    [empty] retry recno={rid} (no re-search)")
                    time.sleep(DETAIL_RETRY_PAUSE)
                    # Make sure we're on a results page; if back() ended up
                    # somewhere unexpected, re-search.
                    if "hospitalLocatorResultsList" not in drv.driver.page_source:
                        _ensure_results_page(drv, q)
                    detail = _click_parse_back(drv.driver, rid)

                if detail is None:
                    detail_data[rid] = {"status": "detail_failed", "found_via": q}
                else:
                    detail["status"]    = "ok"
                    detail["found_via"] = q
                    detail_data[rid]    = detail

                clicks_this_session += 1
                total_done += 1
                elapsed = int(time.time() - start)
                ok_total = sum(1 for d in detail_data.values()
                               if d.get("status") == "ok")
                print(f"    [{total_done}/{total_pending}] recno={rid:>7}: "
                      f"{detail_data[rid].get('status'):<14}  "
                      f"({ok_total} ok overall, "
                      f"elapsed {elapsed//60}m{elapsed%60}s)")

                if total_done % CHECKPOINT_INTERVAL == 0:
                    _save_json(DETAIL_CHECKPOINT, detail_data)

                time.sleep(PER_QUERY_PAUSE)
    finally:
        drv.quit()
        _save_json(DETAIL_CHECKPOINT, detail_data)
        ok_total = sum(1 for d in detail_data.values()
                       if d.get("status") == "ok")
        print(f"\n  Pass 2 complete: {ok_total}/{len(detail_data)} enriched.")

    return detail_data


def _ensure_results_page(drv, query: str, max_attempts: int = 3) -> bool:
    """Search until we land on a results-list page. Returns True on success."""
    for attempt in range(1, max_attempts + 1):
        try:
            if _do_search(drv.driver, query):
                return True
        except (TimeoutException, WebDriverException) as e:
            print(f"    [warn] search err ({type(e).__name__}); restarting driver")
            drv.restart()
        if attempt < max_attempts:
            print(f"    [empty-search] {query!r} attempt {attempt}; retrying")
            time.sleep(DETAIL_RETRY_PAUSE)
    return False


def _click_parse_back(driver, recno: str) -> dict | None:
    """Click the recno-lookup link on the current results page, parse the
    detail, then driver.back() to return. Returns parsed dict on success
    (and the driver is left on the results page); returns None on failure
    (in which case the driver is re-pointed at the results page best-effort)."""
    if not _click_recno_lookup(driver, recno):
        return None
    html = driver.page_source
    detail = parse_detail_html(html)
    # Always navigate back so the next click in the loop has a results page.
    try:
        driver.back()
        time.sleep(4)
        driver.execute_script(KILL_BANNER_JS)
    except Exception:
        pass
    return detail


# ─────────────────────────────────────────────────
# ADDRESS NORMALIZATION
# ─────────────────────────────────────────────────
def _normalize_address(full: str) -> tuple[str, str, str, str, str]:
    """Return (line1, city, state, zip, country_iso). Best-effort across
    US/CA/JP/KR. line1 = first comma-segment; the last segment is parsed
    for state + zip."""
    if not full:
        return "", "", "", "", ""

    # Try usaddress for US-format first
    if HAS_USADDRESS:
        try:
            tagged, _ = usaddress.tag(full)
            parts = []
            for k in ("AddressNumber", "StreetNamePreDirectional", "StreetName",
                      "StreetNamePostType", "StreetNamePostDirectional",
                      "OccupancyType", "OccupancyIdentifier"):
                v = tagged.get(k, "")
                if v: parts.append(v)
            line1 = " ".join(parts)
            city  = tagged.get("PlaceName", "") or ""
            state = (tagged.get("StateName", "") or "").strip().upper()
            zipc  = tagged.get("ZipCode", "") or ""
            if state in US_STATE_CODES:
                return line1, city, state, zipc, "US"
        except Exception:
            pass

    # Generic fallback: comma-split
    segments = [s.strip() for s in full.split(",") if s.strip()]
    if not segments:
        return "", "", "", "", ""
    line1 = segments[0]

    # Last segment: try US-state + ZIP first
    last = segments[-1]
    m = re.search(r"\b([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\b", last)
    if m:
        state = m.group(1)
        zipc  = m.group(2)
        city  = segments[-2] if len(segments) >= 3 else ""
        iso = "US" if state in US_STATE_CODES else (
              "CA" if state in CA_PROVINCES else "")
        return line1, city, state, zipc, iso

    # Canadian postal: A1A 1A1
    m = re.search(
        r"\b([ABCEGHJ-NPRSTVXY]\d[A-Z])\s*(\d[A-Z]\d)\b", last, re.IGNORECASE
    )
    if m:
        zipc = (m.group(1) + " " + m.group(2)).upper()
        # Province may also be in last segment, before postal
        prov_match = re.search(r"\b(" + "|".join(CA_PROVINCES) + r")\b", last)
        state = prov_match.group(1) if prov_match else ""
        city  = segments[-2] if len(segments) >= 2 else ""
        return line1, city, state, zipc, "CA"

    # Japanese addresses: city is last comma-segment, ZIP = 〒nnn-nnnn or 7 digits
    return line1, "", "", "", ""


# ─────────────────────────────────────────────────
# COUNTRY INFERENCE
# ─────────────────────────────────────────────────
def _country_from_query(query: str, parsed_state: str, parsed_iso: str) -> tuple[str, str]:
    """(country_name, country_iso) from query suffix or parsed state."""
    q = (query or "").lower()
    if "japan" in q:        return "Japan", "JP"
    if "korea" in q:        return "South Korea", "KR"
    if parsed_iso == "US":  return "United States", "US"
    if parsed_iso == "CA":  return "Canada", "CA"
    tail = q.rsplit(",", 1)[-1].strip().upper()
    if tail in CA_PROVINCES or parsed_state in CA_PROVINCES:
        return "Canada", "CA"
    if parsed_state in US_STATE_CODES:
        return "United States", "US"
    return "United States", "US"  # default for unknown


# ─────────────────────────────────────────────────
# MERGE & EXPORT
# ─────────────────────────────────────────────────
@dataclass
class Hospital:
    source_id:          str = ""
    profile_url:        str = ""
    name:               str = ""        # blank by design
    practice:           str = ""
    credentials:        str = ""        # blank by design
    phone:              str = ""
    email:              str = ""
    website:            str = ""
    full_address_raw:   str = ""
    address_line_1:     str = ""
    city:               str = ""
    state:              str = ""
    zip_code:           str = ""
    country:            str = ""
    country_iso:        str = ""
    latitude:           str = ""
    longitude:          str = ""
    google_maps_url:    str = ""
    # Tier 3
    practice_type:      str = ""
    accreditation_year: str = ""
    veterinarians:      str = ""
    species_treated:    str = ""
    specialties:        str = ""
    hospital_hours:     str = ""
    mission:            str = ""
    facebook:           str = ""
    found_via_query:    str = ""
    detail_status:      str = ""        # ok / detail_failed


def merge_to_hospitals(recno_data: dict, detail_data: dict) -> list[Hospital]:
    """Combine listing + detail dicts into Hospital rows."""
    rows: list[Hospital] = []
    for recno, listing in recno_data.items():
        detail = detail_data.get(recno) or {}
        status = detail.get("status", "")

        # Choose the richer address: detail's, falling back to listing's
        full_addr = detail.get("full_address_raw") or listing.get("address", "")
        line1, city, state, zipc, iso = _normalize_address(full_addr)
        country, iso_final = _country_from_query(
            listing.get("found_via", ""), state, iso or "")

        # icon -> practice type
        ptype = {
            "general":  "General Practice",
            "referral": "Referral Practice",
        }.get((listing.get("icon") or "").lower(), "")

        # Profile URL: AAHA's literal href has a double-slash and unencoded
        # spaces in hospital_name. Build a clean URL ourselves.
        clean_url = (
            f"{LOCATOR_URL}?recno={recno}"
            f"&hospital_name={quote_plus(detail.get('hospital_name') or listing.get('hospital', ''))}"
        )

        h = Hospital(
            source_id          = recno,
            profile_url        = clean_url,
            practice           = (detail.get("hospital_name")
                                  or listing.get("hospital", "")),
            phone              = (detail.get("phone")
                                  or listing.get("phone", "")),
            email              = detail.get("email", ""),
            website            = detail.get("website", ""),
            full_address_raw   = full_addr,
            address_line_1     = line1,
            city               = city,
            state              = state,
            zip_code           = zipc,
            country            = country,
            country_iso        = iso_final,
            latitude           = str(listing.get("lat") or ""),
            longitude          = str(listing.get("lng") or ""),
            google_maps_url    = (detail.get("maps_url")
                                  or listing.get("maps_url", "")),
            practice_type      = ptype,
            accreditation_year = detail.get("accreditation_year", ""),
            veterinarians      = detail.get("veterinarians", ""),
            species_treated    = detail.get("species_treated", ""),
            specialties        = detail.get("specialties", ""),
            hospital_hours     = detail.get("hospital_hours", ""),
            mission            = detail.get("mission", ""),
            facebook           = detail.get("facebook", ""),
            found_via_query    = listing.get("found_via", ""),
            detail_status      = status or "no_detail",
        )
        rows.append(h)
    return rows


def export(hospitals: list[Hospital], output_file: str) -> None:
    if not hospitals:
        print("No hospitals to export.")
        return
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame([asdict(h) for h in hospitals])
    df.rename(columns={
        "source_id":          "Source ID",
        "profile_url":        "Profile URL",
        "name":               "Name",
        "practice":           "Practice / Company",
        "credentials":        "Credentials",
        "phone":              "Phone",
        "email":              "Email",
        "website":            "Website",
        "full_address_raw":   "Full Address Raw",
        "address_line_1":     "Address Line 1",
        "city":               "City",
        "state":              "State",
        "zip_code":           "Zip",
        "country":            "Country",
        "country_iso":        "Country ISO",
        "latitude":           "Latitude",
        "longitude":          "Longitude",
        "google_maps_url":    "Google Maps URL",
        "practice_type":      "Practice Type",
        "accreditation_year": "Accreditation Year",
        "veterinarians":      "Veterinarians",
        "species_treated":    "Species Treated",
        "specialties":        "Specialties",
        "hospital_hours":     "Hospital Hours",
        "mission":            "Mission",
        "facebook":           "Facebook",
        "found_via_query":    "Found Via Query",
        "detail_status":      "Detail Status",
    }, inplace=True)
    df.insert(0, "Source Site", SOURCE_SITE)

    col_order = [
        # Tier 1
        "Source Site", "Source ID", "Profile URL", "Name",
        # Tier 2
        "Practice / Company", "Credentials",
        "Phone", "Email", "Website",
        "Full Address Raw", "Address Line 1", "City", "State", "Zip",
        "Country", "Country ISO",
        "Latitude", "Longitude", "Google Maps URL",
        # Tier 3
        "Practice Type", "Accreditation Year", "Veterinarians",
        "Species Treated", "Specialties", "Hospital Hours", "Mission",
        "Facebook", "Found Via Query", "Detail Status",
    ]
    df = df[[c for c in col_order if c in df.columns]]
    df.sort_values(["Country", "State", "Practice / Company"],
                   inplace=True, ignore_index=True)

    def _is_us(row) -> bool:
        c = str(row.get("Country", "")).strip().lower()
        if c in US_COUNTRY_NAMES: return True
        iso = str(row.get("Country ISO", "")).strip().upper()
        if iso == "US": return True
        if not c:
            return str(row.get("State", "")).strip().upper() in US_STATE_CODES
        return False

    us_mask = df.apply(_is_us, axis=1)
    us_df   = df[us_mask].reset_index(drop=True)
    intl_df = df[~us_mask].reset_index(drop=True)

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        for sheet, data in [
            ("Hospitals",     df),
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
            ("Source Site",                 SOURCE_SITE),
            ("Note",                        "AAHA records are hospital-level. "
                                            "Name and Credentials are blank by design "
                                            "(see SCRAPER_CONTRACT.md AAHA exception). "
                                            "Practice / Company holds the hospital name. "
                                            "Same hospital may appear with two recnos "
                                            "(GP + Referral accreditations)."),
            ("", ""),
            ("Total Hospitals",             len(df)),
            ("US Hospitals",                len(us_df)),
            ("International Hospitals",     len(intl_df)),
            ("Detail Enrichment OK",        int((df["Detail Status"] == "ok").sum())),
            ("Detail Failed/Missing",       int((df["Detail Status"] != "ok").sum())),
            ("Countries Covered",           df["Country"].replace("", pd.NA).nunique()),
            ("States/Provinces Covered",    df["State"].replace("", pd.NA).nunique()),
            ("", ""),
            ("With Phone",                  int((df["Phone"] != "").sum())),
            ("With Email",                  int((df["Email"] != "").sum())),
            ("With Website",                int((df["Website"] != "").sum())),
            ("With Full Address",           int((df["Full Address Raw"] != "").sum())),
            ("With Parsed City",            int((df["City"] != "").sum())),
            ("With Parsed State",           int((df["State"] != "").sum())),
            ("With Parsed Zip",             int((df["Zip"] != "").sum())),
            ("With Latitude",               int((df["Latitude"] != "").sum())),
            ("With Practice Type",          int((df["Practice Type"] != "").sum())),
            ("General Practices",           int((df["Practice Type"] == "General Practice").sum())),
            ("Referral Practices",          int((df["Practice Type"] == "Referral Practice").sum())),
            ("With Accreditation Year",     int((df["Accreditation Year"] != "").sum())),
            ("With Veterinarians",          int((df["Veterinarians"] != "").sum())),
            ("With Species Treated",        int((df["Species Treated"] != "").sum())),
            ("With Specialties",            int((df["Specialties"] != "").sum())),
            ("With Hospital Hours",         int((df["Hospital Hours"] != "").sum())),
            ("With Mission",                int((df["Mission"] != "").sum())),
            ("With Facebook",               int((df["Facebook"] != "").sum())),
        ]
        for i, (label, val) in enumerate(rows, 1):
            ws2[f"A{i}"] = label
            ws2[f"B{i}"] = val
        hdr_row = len(rows) + 2
        ws2[f"A{hdr_row}"] = "State"
        ws2[f"B{hdr_row}"] = "Count"
        for i, (state, cnt) in enumerate(
            df["State"].replace("", "(unknown)").value_counts().items(),
            start=hdr_row + 1
        ):
            ws2[f"A{i}"] = state
            ws2[f"B{i}"] = cnt
        ws2.column_dimensions["A"].width = 32
        ws2.column_dimensions["B"].width = 60

    print(f"  [OK] Excel -> {output_file}")


# ─────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────
def main():
    today = date.today().isoformat()
    default_out = f"outputs/aaha_{today}.xlsx"

    ap = argparse.ArgumentParser(
        description="Scrape AAHA accredited-hospital directory (Selenium-class, two-pass)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--output", default=default_out)
    ap.add_argument("--listing-only", action="store_true",
                    help="Run Pass 1 only (listing sweep)")
    ap.add_argument("--detail-only", action="store_true",
                    help="Run Pass 2 only (detail enrichment); requires "
                         "an existing listing checkpoint")
    ap.add_argument("--resume", action="store_true",
                    help="Resume from checkpoint (auto-resumes regardless; "
                         "this flag is for clarity)")
    ap.add_argument("--retry-failed", action="store_true",
                    help="Pass 2 only: flip prior 'detail_failed' entries "
                         "to 'pending_retry' so they get re-attempted in "
                         "this run. 'ok' rows are still skipped.")
    ap.add_argument("--limit", type=int, default=None,
                    help="DEBUG: stop after N recnos collected (Pass 1) "
                         "or N detail fetches (Pass 2)")
    args = ap.parse_args()

    if args.listing_only and args.detail_only:
        print("ERROR: --listing-only and --detail-only are mutually exclusive.")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  AAHA Accredited-Hospital Scraper (Selenium-class, two-pass)")
    print(f"  Source: {LOCATOR_URL}")
    print(f"{'='*60}\n")
    print(f"Output  : {args.output}")
    print(f"Centers : {len(SEARCH_CENTERS)} Pass-1 search queries")
    print(f"Mode    : "
          f"{'Pass 1 only' if args.listing_only else 'Pass 2 only' if args.detail_only else 'Pass 1 + Pass 2'}")
    if args.limit:
        print(f"Limit   : {args.limit}")
    if not HAS_USADDRESS:
        print("Note    : usaddress not installed; addresses use regex fallback")
    print()

    # Pass 1
    if args.detail_only:
        recno_data = _load_json(RECNO_CHECKPOINT)
        if not recno_data:
            print("ERROR: --detail-only requires an existing listing checkpoint "
                  f"({RECNO_CHECKPOINT}). Run Pass 1 first.")
            sys.exit(1)
    else:
        recno_data = listing_sweep(SEARCH_CENTERS, args.limit)

    if args.listing_only:
        # Still emit a partial xlsx (listing-only data) for inspection
        detail_data = _load_json(DETAIL_CHECKPOINT)
        hospitals = merge_to_hospitals(recno_data, detail_data)
        print(f"\n  Exporting {len(hospitals)} hospitals "
              f"(listing-only, no detail enrichment)...")
        export(hospitals, args.output)
        return

    # Pass 2
    detail_data = detail_pass(recno_data, args.limit, retry_failed=args.retry_failed)

    # Merge + export
    hospitals = merge_to_hospitals(recno_data, detail_data)
    print(f"\n  Exporting {len(hospitals)} hospitals...")
    export(hospitals, args.output)
    print(f"\n{'='*60}")
    print(f"  Done. {len(hospitals)} hospitals -> {args.output}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()

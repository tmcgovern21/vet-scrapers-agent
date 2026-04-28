"""Canonical column vocabulary for the auditor.

Mirror of scrapers/SCRAPER_CONTRACT.md §"Column aliases". Keep in sync.
If you edit COLUMN_ALIASES or TIER_1/TIER_2 here, update the contract
so the document and code agree.
"""

from __future__ import annotations

# Tier 1: every scraper must produce these columns (may be empty per
# row, but the column itself must exist).
TIER_1 = frozenset({
    "Source Site",
    "Source ID",
    "Profile URL",
    "Name",
})

# Tier 2: canonical names. Include when the site provides the data.
TIER_2 = frozenset({
    # Identity
    "Credentials",
    "Practice / Company",
    "Specialties",
    # Contact
    "Email",
    "Phone",
    "Phone 2",
    "Website",
    # Geo
    "Google Maps URL",
    "Latitude",
    "Longitude",
    # Address
    "Full Address Raw",
    "Address Line 1",
    "Address Line 2",
    "City",
    "State",
    "Zip",
    "Country",
    # Social
    "Facebook",
    "Instagram",
    "LinkedIn URL",
    "Twitter URL",
    # Bio
    "About / Description",
})

# Verbatim mirror of the COLUMN_ALIASES block in SCRAPER_CONTRACT.md.
COLUMN_ALIASES: dict[str, list[str]] = {
    "Name":               ["Name / Credentials", "Contact Name"],
    "Credentials":        ["Qualifications", "Title"],
    "Practice / Company": ["Practice Name", "Practice / Clinic"],
    "Specialties":        ["Specialty", "Services"],
    "Phone":              ["Business Phone", "Phone 1"],
    "Phone 2":            ["Cell Phone", "Secondary Phone"],
    "Zip":                ["ZIP", "Postal Code"],
    "Full Address Raw":   ["Address", "Full Address"],
    "Address Line 1":     ["Street Address", "Street"],
    "About / Description":["Bio", "Summary", "Description"],
    "Source ID":          ["Algolia ID", "ID"],
}

# Reverse lookup: any recognized name (lowercased) -> canonical name.
_ALIAS_TO_CANONICAL: dict[str, str] = {}
for _canonical, _aliases in COLUMN_ALIASES.items():
    _ALIAS_TO_CANONICAL[_canonical.lower()] = _canonical
    for _a in _aliases:
        _ALIAS_TO_CANONICAL[_a.lower()] = _canonical
# Tier 1/2 canonicals with no declared aliases still need to self-resolve.
for _c in TIER_1 | TIER_2:
    _ALIAS_TO_CANONICAL.setdefault(_c.lower(), _c)

# Every recognized column name (canonical or alias), lowercased.
# Used by the header-row bug detector to decide when a data row "looks
# like" a header row.
KNOWN_COLUMN_NAMES = frozenset(_ALIAS_TO_CANONICAL.keys())


def normalize(name: str) -> str:
    """Return the canonical column name for `name`, or the trimmed
    original if it is not in the alias table."""
    if name is None:
        return ""
    key = str(name).strip()
    return _ALIAS_TO_CANONICAL.get(key.lower(), key)


def tier_for(canonical: str) -> int:
    """Classify a canonical column name as Tier 1, 2, or 3."""
    if canonical in TIER_1:
        return 1
    if canonical in TIER_2:
        return 2
    return 3

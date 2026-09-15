"""Nebraska child care provider spider.

Source: two public state sources merged in memory --

  * **NRRS** (Nebraska Resource & Referral System, nrrs.ne.gov) -- the
    **spine**: a complete, cookieless JSON API listing ~2,523 distinct
    child-care resources with location, contact, facility type, county,
    ages/hours. No cookie/token/CAPTCHA is required; a bare POST with a JSON
    body returns full data.
  * **Step Up to Quality** (stepuptoquality.ne.gov) -- the **quality
    overlay**: 1,077 quality-rated programs, each carrying a Step rating
    (1-5), coordinates, staff counts, and capacity. The search results page
    embeds the full statewide list as a JS variable
    (``var searchResults = {"center":{...},"providers":[...]};``) even
    though only a handful of cards render visibly.

This is the same structural family as ``louisiana.py`` / ``indiana.py``
(cookieless JSON API) for NRRS, and ``delaware.py`` (buffer an enrichment
source in memory, build an index, then page the spine and emit
already-joined items) for the two-source merge -- see
``tasks/nebraska/nebraska_plan.md`` for the full research + decision log.

Decisions signed off by Ryan (2026-09-15, plan Sec 3) -- do not re-litigate:

  D-1: matching is TIERED, not license-only. NRRS publishes a license number
       on only ~24% of records (600/2,531) even though the same providers
       exist in NRRS with a null license number -- license-only reach is
       ~25%; +exact name+ZIP reaches ~40-50%. Tiers: (1) exact
       license_number, (2) exact normalized(name) + 5-digit ZIP. Fuzzy/token
       matching is intentionally NOT implemented in this build -- it is
       reserved behind an opt-in ``-a fuzzy=1`` flag for a future pass (see
       ``__init__``); passing it today just logs a note and changes nothing.
  D-2: a Step Up record with no NRRS match is emitted as a STANDALONE
       ProviderItem (``ne_stepup_only=True``) rather than being discarded --
       no quality data is dropped. A Step Up record consumed by a match is
       never emitted twice.
  D-3: every NRRS record is included, including the 167 non-licensure
       self-added listings (``ne_from_licensure=False``). NRRS rows are
       deduped on ``id`` only -- name-duplicates are kept (real multi-site
       providers, and licensure + self-added pairs of the same provider).

Crawl shape (plan Sec 4) -- two phases, in-memory join, mirroring
delaware.py's phase-gating:

  Phase A (Step Up): GET the search page, balanced-brace-scan out
  `searchResults.providers` (1,077), GET every provider's detail page,
  buffer parsed records, then build the license/name+zip indexes.

  Phase B (NRRS): only once Phase A's index is ready, page the NRRS search
  (127 pages) and GET every distinct resource's detail; each NRRS detail
  callback builds the joined ProviderItem (match lookup + Step Up fields
  attached on a hit) and yields it immediately -- nothing is held. Once the
  last NRRS detail is done, every un-consumed Step Up record is emitted
  standalone (D-2).

Sequencing rationale (plan Sec 4): the Step Up index must be complete before
any NRRS item is emitted (else a matchable rating is missed), and leftover
Step Up items can only be known after every NRRS item has been processed
(else a provider could be emitted twice).

Counters, not booleans, track both fan-outs (Step Up detail, NRRS detail) --
every request carries BOTH a callback and an errback that decrements the
same counter, because a request with no errback that fails would wedge the
phase transition forever (plan Sec 4/12).

A third source, the Nebraska License Search (nebraska.gov/LISSearch), was
down during research and is deliberately NOT built here (plan Sec 13).
"""

import html
import json
import re

import scrapy

from provider_scrape.items import ProviderItem

NRRS_SEARCH_URL = "https://nrrs.ne.gov/resources/search"
NRRS_DETAIL_URL = "https://nrrs.ne.gov/resource/{id}/12"
NRRS_PROVIDER_URL = "https://nrrs.ne.gov/resource/{id}/12.0"
# parent_category 12 == "Child Care"; child_category 0 == all sub-categories.
# ZIP 68516 (Lincoln) + a 10000-mile radius returns the entire statewide list
# in one response -- this is not a geographic search, just a way to ask
# Step Up's API for "everything".
STEPUP_SEARCH_URL = (
    "https://stepuptoquality.ne.gov/resources-parents-families/provider-search-results/"
    "?address=68516&radius=10000&search-by=address"
)

USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64; rv:153.0) Gecko/20100101 Firefox/153.0"
_NRRS_JSON_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json, text/plain, */*",
    "X-Requested-With": "XMLHttpRequest",
}
NRRS_SEARCH_HEADERS = {**_NRRS_JSON_HEADERS, "Content-Type": "application/json;charset=utf-8"}
NRRS_DETAIL_HEADERS = _NRRS_JSON_HEADERS
STEPUP_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Baselines calibrated live 2026-09-15 (plan Sec 16). closed() warns well
# below these -- a sign the site shape/coverage changed, not a hard fail.
NRRS_DISTINCT_BASELINE = 2523
NRRS_EXPECTED_MIN = 2200
STEPUP_BASELINE = 1077
STEPUP_EXPECTED_MIN = 900

# --------------------------------------------------------------------------- #
# NRRS: license prefix / sub-category vocabulary -> provider_type (Sec 5.1)
# --------------------------------------------------------------------------- #

# The full leading alphabetic run of the license number (not just its first
# two characters) -- this is what disambiguates "FI" from "FII" without any
# ordering trick: re.match(r"[A-Za-z]+", "FII10096") already returns "FII",
# not "FI", because the regex is greedy.
_LICENSE_PREFIX_RE = re.compile(r"^[A-Za-z]+")

LICENSE_PREFIX_TO_TYPE = {
    "CCC": "Child Care Center",
    "FI": "Family Child Care Home I",
    "FII": "Family Child Care Home II",
}

# NRRS's parent "Child Care" category (category_id 12) -- every provider
# carries a row for it; it is never itself a provider sub-category and is
# excluded before reading resourceCategoryCounty for a facility type.
PARENT_CHILD_CARE_CATEGORY_ID = 12

# resourceCategoryCounty[].category.category_name values that are referral/
# registry entries, not providers (e.g. "Child Care Registry - HHS") -- a
# record whose only non-parent sub-categories are these is skipped (Sec 5.1).
NON_PROVIDER_SUBCATEGORIES = {"Child Care Locators"}

# The known child-care provider vocabulary (Sec 5.1's "Observed provider
# sub-category vocabulary" list; the trailing-space variant is matched after
# .strip()). Live-verified (2026-09-15, resource 9670: "Sarpy Community
# YMCA") that some NRRS resources are multi-service community agencies whose
# resourceCategoryCounty rows mix several unrelated registry categories
# (e.g. "Social Development", "Recreation-Children/Youth") in with a
# genuine "Child Care Center" row -- when at least one of a record's
# candidate sub-categories is in this known vocabulary, the tie-break below
# restricts the pick to that subset first, so a real child-care category
# is never shadowed by an unrelated one that merely happened to sort first.
KNOWN_CHILDCARE_SUBCATEGORIES = {
    "Child Care Center",
    "Family Child Care Home I",
    "Family Child Care Home II",
    "Provisional Family Child Care Home I",
    "Provisional Family Child Care Home II",
}

# categoryFieldResponses field ids used (Sec 5, field catalog verified
# against tasks/nebraska/nrrs_detail.json parentCategories[0].fields).
FIELD_AGES_SERVED = 16
FIELD_HOURS = 20
FIELD_USDA_FOOD_PROGRAM = 151
FIELD_CAPACITY = 154
FIELD_TRANSPORTATION = 178
FIELD_MEALS_ONSITE = 183
LANGUAGE_FIELDS = {167: "English", 168: "Spanish", 169: "Vietnamese", 170: "Arabic", 171: "Sign Language"}
FIELD_OTHER_LANGUAGE = 172

_WEEKDAY_ORDER = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")

# --------------------------------------------------------------------------- #
# Step Up: icon filename -> provider_type (used only as a fallback when a
# detail page's h2 is somehow missing; the h2 text itself is preferred).
# --------------------------------------------------------------------------- #

ICON_TO_PROVIDER_TYPE = {
    "marker-child-care-center.svg": "Child Care Center",
    "marker-family-child-care-home.svg": "Family Child Care Home",
    "marker-public-school.svg": "Public School",
    "marker-head-start.svg": "Head Start",
    "marker-preschool.svg": "Preschool",
}

# Step Up's "Age of Children Served" tag-list values -> the common age-group
# flag they set (Sec 7 -- preferred over NRRS's ambiguous-unit numeric range
# whenever a matched/standalone record carries these).
AGE_TAG_TO_FLAG = {"Infants": "infant", "Toddlers": "toddler", "Preschool": "preschool", "School Age": "school"}

# norm_name (Sec 7): tokens stripped before matching. "owned"/"by" as
# separate stopwords removes the "owned by" phrase wherever it occurs
# without needing phrase-level matching.
_NORM_NAME_STOPWORDS = {"llc", "inc", "incorporated", "ltd", "the", "co", "company", "dba", "owned", "by"}
_NORM_NAME_PUNCT_RE = re.compile(r"[^a-z0-9\s]")
_ZIP5_RE = re.compile(r"(\d{5})(?:-?\d{4})?\s*$")

# Official Nebraska county-number system (Neb. Rev. Stat. Sec 60-370, per the
# Nebraska Association of County Officials' published number/county/seat
# table) -- all 93 counties. Douglas (1), Lancaster (2), and Sarpy (59)
# stopped issuing county-numbered plates in 2002 but keep their numbers as
# the canonical registration identifiers used elsewhere (incl. NRRS). Do NOT
# hand-edit from memory -- this is the authoritative source, re-verify
# against it if the anchors below ever look wrong.
NE_COUNTY_NAMES = {
    1: "Douglas",
    2: "Lancaster",
    3: "Gage",
    4: "Custer",
    5: "Dodge",
    6: "Saunders",
    7: "Madison",
    8: "Hall",
    9: "Buffalo",
    10: "Platte",
    11: "Otoe",
    12: "Knox",
    13: "Cedar",
    14: "Adams",
    15: "Lincoln",
    16: "Seward",
    17: "York",
    18: "Dawson",
    19: "Richardson",
    20: "Cass",
    21: "Scotts Bluff",
    22: "Saline",
    23: "Boone",
    24: "Cuming",
    25: "Butler",
    26: "Antelope",
    27: "Wayne",
    28: "Hamilton",
    29: "Washington",
    30: "Clay",
    31: "Burt",
    32: "Thayer",
    33: "Jefferson",
    34: "Fillmore",
    35: "Dixon",
    36: "Holt",
    37: "Phelps",
    38: "Furnas",
    39: "Cheyenne",
    40: "Pierce",
    41: "Polk",
    42: "Nuckolls",
    43: "Colfax",
    44: "Nemaha",
    45: "Webster",
    46: "Merrick",
    47: "Valley",
    48: "Red Willow",
    49: "Howard",
    50: "Franklin",
    51: "Harlan",
    52: "Kearney",
    53: "Stanton",
    54: "Pawnee",
    55: "Thurston",
    56: "Sherman",
    57: "Johnson",
    58: "Nance",
    59: "Sarpy",
    60: "Frontier",
    61: "Sheridan",
    62: "Greeley",
    63: "Boyd",
    64: "Morrill",
    65: "Box Butte",
    66: "Cherry",
    67: "Hitchcock",
    68: "Keith",
    69: "Dawes",
    70: "Dakota",
    71: "Kimball",
    72: "Chase",
    73: "Gosper",
    74: "Perkins",
    75: "Brown",
    76: "Dundy",
    77: "Garden",
    78: "Deuel",
    79: "Hayes",
    80: "Sioux",
    81: "Rock",
    82: "Keya Paha",
    83: "Garfield",
    84: "Wheeler",
    85: "Banner",
    86: "Blaine",
    87: "Logan",
    88: "Loup",
    89: "Thomas",
    90: "McPherson",
    91: "Arthur",
    92: "Grant",
    93: "Hooker",
}


# --------------------------------------------------------------------------- #
# Pure helpers (unit tested directly)
# --------------------------------------------------------------------------- #


def extract_search_results(text):
    """Balanced-brace extraction of Step Up's embedded ``searchResults`` blob.

    The page embeds ``var searchResults = {"center":{...},"providers":[...]};``
    on a single ~480 KB line. A lazy regex (``var searchResults = (\\{.*?\\});``)
    breaks because the JSON itself contains the two-character sequence ``"};``
    inside provider info-window HTML snippets, so it stops early on the wrong
    ``}``. This walks the braces one character at a time, tracking whether the
    scan is inside a JSON string (and honoring backslash escapes) so a ``}``
    embedded in a string is never mistaken for the closing brace (plan Sec
    2.2/12). Returns the parsed dict, or ``None`` if the marker is missing or
    the braces never balance / don't parse as JSON (site redesign).
    """
    marker = "var searchResults = "
    start_idx = text.find(marker)
    if start_idx == -1:
        return None
    start = start_idx + len(marker)
    depth = 0
    in_string = False
    escape = False
    end = None
    for i in range(start, len(text)):
        ch = text[i]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
        else:
            if ch == '"':
                in_string = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break
    if end is None:
        return None
    try:
        return json.loads(text[start:end])
    except (json.JSONDecodeError, ValueError):
        return None


_STRONG_RE = re.compile(r"<strong>(.*?)</strong>", re.S)
_HREF_RE = re.compile(r"href='([^']+)'")


def parse_stepup_search_entry(html_snippet):
    """Pull ``(name, detail_url)`` out of one ``searchResults.providers[].html``
    info-window snippet, e.g. ``"<p><strong>NAME</strong><br>...<a
    href='URL'>Learn More</a></p>"``. Either may be ``None`` if the snippet
    doesn't match the expected shape.
    """
    if not html_snippet:
        return None, None
    name_match = _STRONG_RE.search(html_snippet)
    href_match = _HREF_RE.search(html_snippet)
    name = html.unescape(name_match.group(1)).strip() if name_match else None
    detail_url = href_match.group(1).strip() if href_match else None
    return name or None, detail_url or None


def license_prefix(license_number):
    """The leading alphabetic run of a license number, or None."""
    if not isinstance(license_number, str):
        return None
    match = _LICENSE_PREFIX_RE.match(license_number.strip())
    return match.group(0).upper() if match else None


def derive_provider_type(license_number, resource_category_county, logger=None, resource_id=None):
    """Derive NRRS ``provider_type`` (Sec 5.1). Returns ``(provider_type,
    is_non_provider)`` -- when ``is_non_provider`` is True the record is a
    referral/registry entry (e.g. "Child Care Locators") and must be skipped,
    not emitted.
    """
    prefix = license_prefix(license_number)
    if prefix:
        mapped = LICENSE_PREFIX_TO_TYPE.get(prefix)
        if mapped:
            return mapped, False
        if logger:
            logger.warning(
                "Nebraska: resource %s has an unmapped license prefix %r (license %r) -- "
                "falling back to the sub-category vocabulary",
                resource_id,
                prefix,
                license_number,
            )

    sub_rows = [
        row
        for row in (resource_category_county or [])
        if (row.get("category") or {}).get("id") != PARENT_CHILD_CARE_CATEGORY_ID
    ]
    if not sub_rows:
        # No category data at all beyond the parent "Child Care" row -- not
        # evidence of a non-provider locator entry, just uncategorized.
        return None, False

    provider_names = []
    licensure_names = []
    for row in sub_rows:
        name = ((row.get("category") or {}).get("category_name") or "").strip()
        if not name or name in NON_PROVIDER_SUBCATEGORIES:
            continue
        provider_names.append(name)
        if row.get("is_from_licensure"):
            licensure_names.append(name)

    if not provider_names:
        return None, True  # every non-parent sub-category is a known referral/registry entry

    distinct = list(dict.fromkeys(licensure_names or provider_names))
    # Restrict to the known child-care vocabulary first when at least one
    # candidate is in it (e.g. resource 9670's ['Social Development',
    # 'Recreation', ..., 'Child Care Center'] resolves to 'Child Care
    # Center', not the unrelated registry category that happened to sort
    # first) -- only fall back to the full candidate list when none of them
    # are a recognized child-care term.
    known = [name for name in distinct if name in KNOWN_CHILDCARE_SUBCATEGORIES]
    if known:
        distinct = known
    if len(distinct) > 1 and logger:
        logger.warning(
            "Nebraska: resource %s has ambiguous provider sub-categories %s -- using %r",
            resource_id,
            distinct,
            distinct[0],
        )
    return distinct[0], False


def _strip_provisional(name):
    """Strip a leading "Provisional " status modifier for comparison purposes.

    Live-verified quirk: a record can be licensed under a plain prefix (e.g.
    ``FII10096`` -> provider_type "Family Child Care Home II" via the license
    table) while its OWN resourceCategoryCounty row is tagged with the
    "Provisional " variant of that same category (e.g. resource 73989). The
    two strings name the same underlying facility type -- "Provisional" is a
    license-status modifier, not a different category (cf. Wisconsin's
    "(Probational)" suffix strip in normalization.py) -- so county matching
    compares the stripped form on both sides rather than requiring an exact
    string match that would otherwise leave the county unset on every
    provisional-license record.
    """
    prefix = "Provisional "
    return name[len(prefix) :] if name.startswith(prefix) else name


def derive_county(resource_category_county, provider_type, county_names, logger=None, resource_id=None):
    """Best-effort ``county`` (Sec 5.2): the county_number of the row that is
    both licensure-sourced and tagged with the provider's own facility
    sub-category. resourceCategoryCounty conflates physical + counties-served,
    so this deliberately returns None (not a guess) unless exactly one
    distinct qualifying county number is found.
    """
    if not provider_type:
        return None
    target = _strip_provisional(provider_type)
    numbers = set()
    for row in resource_category_county or []:
        if not row.get("is_from_licensure"):
            continue
        name = ((row.get("category") or {}).get("category_name") or "").strip()
        if _strip_provisional(name) != target:
            continue
        number = row.get("county_number")
        if number is not None:
            numbers.add(number)
    if len(numbers) != 1:
        if len(numbers) > 1 and logger:
            logger.info(
                "Nebraska: resource %s has ambiguous licensure county numbers %s for %r -- county left unset",
                resource_id,
                sorted(numbers),
                provider_type,
            )
        return None
    number = numbers.pop()
    name = county_names.get(number)
    if name is None and logger:
        logger.warning("Nebraska: unmapped NRRS county_number %s on resource %s", number, resource_id)
    return name


def normalize_zip(value):
    """5-digit ZIP from any of NRRS's shapes: plain 5-digit, ZIP+4 with a
    dash, or the 9-digit-no-dash form NRRS actually emits (e.g.
    ``"685160000"`` -> ``"68516"``, Sec 12). Returns None for empty input.
    """
    if not value:
        return None
    digits = re.sub(r"\D", "", str(value))
    if not digits:
        return None
    return digits[:5]


def compose_address(street1, street2, city, state, zip5):
    """Compose a ``"street, city, ST zip"`` address; never invents a missing
    piece."""
    street_line = " ".join(p.strip() for p in (street1, street2) if p and str(p).strip())
    parts = [p for p in (street_line, city) if p]
    tail = " ".join(p for p in (state, zip5) if p)
    if tail:
        parts.append(tail)
    return ", ".join(parts) if parts else None


def field_response(category_field_responses, field_id):
    """The raw ``response`` value for one categoryFieldResponses field id
    (categoryFieldResponses is sparse -- most fields are simply absent).
    """
    entry = (category_field_responses or {}).get(str(field_id))
    if not isinstance(entry, dict):
        return None
    return entry.get("response")


def age_flags_from_nrrs_range(lower, upper):
    """Best-effort infant/toddler/preschool/school flags from NRRS's numeric
    Ages Served range (Sec 12). The unit is genuinely ambiguous in the
    source (a Child Care Center was observed live with a 6-13 range) --
    this treats the numbers as approximate YEARS, the convention used
    elsewhere in this project (e.g. indiana.py), and is intentionally only a
    fallback: callers should prefer Step Up's explicit age tags when a
    matched/standalone Step Up record carries them (Sec 7/12).
    """
    if lower is None and upper is None:
        return {}
    try:
        lo = float(lower) if lower is not None else 0.0
        hi = float(upper) if upper is not None else lo
    except (TypeError, ValueError):
        return {}
    flags = {}
    if lo <= 1:
        flags["infant"] = True
    if lo <= 2 and hi >= 1:
        flags["toddler"] = True
    if lo <= 5 and hi >= 2:
        flags["preschool"] = True
    if hi >= 6:
        flags["school"] = True
    return flags


def format_nrrs_hours(hours_response):
    """Compose an hours string from NRRS's per-weekday ``{open, close}``
    dict. Collapses to ``"Monday-Friday 6:00 am-6:00 pm"`` when every
    populated day shares the same open/close pair (cf. indiana.py's
    ``format_schedule``); otherwise lists each day separately.
    """
    if not isinstance(hours_response, dict):
        return None
    rows = []
    for day in _WEEKDAY_ORDER:
        day_hours = hours_response.get(day)
        if isinstance(day_hours, dict):
            open_t, close_t = day_hours.get("open"), day_hours.get("close")
            if open_t or close_t:
                rows.append((day, open_t, close_t))
    if not rows:
        return None
    times = {(o, c) for _, o, c in rows}
    if len(times) == 1 and len(rows) > 1:
        o, c = rows[0][1], rows[0][2]
        span = f"{rows[0][0]}-{rows[-1][0]}"
        return f"{span} {o}-{c}".strip()
    return "; ".join(f"{day} {o}-{c}".strip() for day, o, c in rows)


def languages_from_responses(category_field_responses):
    """The list of spoken languages from fields 167-172 (Sec 5)."""
    langs = []
    for field_id, label in LANGUAGE_FIELDS.items():
        response = field_response(category_field_responses, field_id)
        if isinstance(response, str) and response.strip().lower().startswith("spoken"):
            langs.append(label)
    other = field_response(category_field_responses, FIELD_OTHER_LANGUAGE)
    if isinstance(other, str) and other.strip():
        langs.append(other.strip())
    return langs or None


def norm_name(value):
    """Normalize a provider name for the name+ZIP match key (Sec 7):
    lower-case, ``&`` -> ``and``, punctuation stripped to spaces, the
    llc/inc/incorporated/ltd/the/co/company/dba/owned/by stopwords dropped,
    whitespace collapsed. Returns None for empty input.
    """
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.lower().replace("&", " and ")
    text = _NORM_NAME_PUNCT_RE.sub(" ", text)
    tokens = [t for t in text.split() if t and t not in _NORM_NAME_STOPWORDS]
    return " ".join(tokens) or None


def extract_zip5(address_text):
    """The trailing 5-digit ZIP from a composed address string, or None."""
    if not address_text:
        return None
    match = _ZIP5_RE.search(address_text)
    return match.group(1) if match else None


def is_real_coordinate(latitude, longitude):
    """True unless both are missing or the (0, 0) null-island sentinel."""
    if not latitude or not longitude:
        return False
    try:
        return (float(latitude), float(longitude)) != (0.0, 0.0)
    except (TypeError, ValueError):
        return True


_LABEL_TEXT_XPATH = './/span[@class="label"][normalize-space(text())="{label}"]/following-sibling::text()[1]'
_LABEL_CIRCLE_XPATH = (
    './/span[@class="label"][normalize-space(text())="{label}"]/following-sibling::span[@class="circle"][1]/text()'
)


def _label_text(response, label):
    value = response.xpath(_LABEL_TEXT_XPATH.format(label=label)).get()
    if not isinstance(value, str):
        return None
    return value.strip() or None


def _label_circle_int(response, label):
    value = response.xpath(_LABEL_CIRCLE_XPATH.format(label=label)).get()
    if isinstance(value, str):
        value = value.strip()
    if value and value.isdigit():
        return int(value)
    return None


def build_stepup_record(response, summary, logger=None):
    """Parse one Step Up detail page into a plain record dict (not yet an
    item -- plan Sec 7). ``summary`` carries the slug/detail_url/lat/lng/icon
    collected from the search page's ``searchResults`` entry.
    """
    record = {
        "slug": summary["slug"],
        "detail_url": summary["detail_url"],
        "latitude": summary.get("lat"),
        "longitude": summary.get("lng"),
        "icon": summary.get("icon"),
    }

    h1 = response.css("#internal-hero h1::text").get()
    provider_name = (h1 or "").strip() or summary.get("name")
    if provider_name:
        record["provider_name"] = provider_name

    facility_type = response.css("#internal-hero h2::text").get()
    if isinstance(facility_type, str) and facility_type.strip():
        record["facility_type"] = facility_type.strip()

    # `.step-badge` matches on class membership regardless of any extra
    # class the site's caching layer appends (observed live:
    # `class="step-badge nitro-lazy"`) -- do not match on an exact class
    # string. ~13% of pages render this element with no digit inside.
    rating_text = response.css("div.step-badge::text").get()
    rating_text = rating_text.strip() if isinstance(rating_text, str) else ""
    if rating_text.isdigit():
        record["step_rating"] = int(rating_text)

    license_number = _label_text(response, "License Number")
    if license_number:
        record["license_number"] = license_number

    director = _label_text(response, "Director")
    if director:
        record["administrator"] = director

    full_time = _label_circle_int(response, "Full Time Staff")
    if full_time is not None:
        record["full_time_staff"] = full_time
    part_time = _label_circle_int(response, "Part Time Staff")
    if part_time is not None:
        record["part_time_staff"] = part_time
    capacity = _label_circle_int(response, "Capacity")
    if capacity is not None:
        record["capacity"] = capacity

    age_tags = [t.strip() for t in response.css("ul.tag-list li::text").getall() if t.strip()]
    record["age_tags"] = age_tags

    checklist = [t.strip() for t in response.css("ul.checklist li::text").getall() if t.strip()]
    record["checklist"] = checklist

    phone = response.css("li.icon-phone a::text").get()
    if isinstance(phone, str) and phone.strip():
        record["phone"] = phone.strip()
    email = response.css("li.icon-email a::text").get()
    if isinstance(email, str) and email.strip():
        record["email"] = email.strip()
    address_parts = [t.strip() for t in response.css("li.icon-address a::text").getall() if t.strip()]
    if address_parts:
        record["address"] = ", ".join(address_parts)

    if logger and "provider_name" not in record:
        logger.warning("Nebraska: Step Up detail %s has no h1 name", response.url)

    return record


def stepup_provider_type(record, logger=None):
    """provider_type for a standalone Step Up record: the detail page's h2
    facility type, falling back to the search marker icon (Sec 7).
    """
    facility_type = record.get("facility_type")
    if facility_type:
        return facility_type
    icon = record.get("icon") or ""
    icon_file = icon.rsplit("/", 1)[-1]
    mapped = ICON_TO_PROVIDER_TYPE.get(icon_file)
    if mapped:
        return mapped
    if logger:
        logger.warning(
            "Nebraska: Step Up record %r has neither an h2 facility type nor a recognized marker icon (%r)",
            record.get("provider_name"),
            icon,
        )
    return None


def build_stepup_index(records, logger=None):
    """Build the license-number and name+ZIP lookup indexes (Sec 4 A3).
    Values are slugs (keys into ``records``); a key that would collide across
    two different providers is dropped from that index and logged -- never
    guessed.
    """
    by_license = {}
    ambiguous_license = set()
    by_namezip = {}
    ambiguous_namezip = set()

    for slug, record in records.items():
        license_number = record.get("license_number")
        if license_number:
            key = license_number.strip().upper()
            if key in by_license and by_license[key] != slug:
                ambiguous_license.add(key)
            else:
                by_license.setdefault(key, slug)

        name = norm_name(record.get("provider_name"))
        zip5 = extract_zip5(record.get("address"))
        if name and zip5:
            key = (name, zip5)
            if key in by_namezip and by_namezip[key] != slug:
                ambiguous_namezip.add(key)
            else:
                by_namezip.setdefault(key, slug)

    for key in ambiguous_license:
        del by_license[key]
        if logger:
            logger.info("Nebraska: dropped ambiguous Step Up license key %r (collision across providers)", key)
    for key in ambiguous_namezip:
        del by_namezip[key]
        if logger:
            logger.info("Nebraska: dropped ambiguous Step Up name+zip key %r (collision across providers)", key)

    return by_license, by_namezip


def apply_stepup_common_fields(item, record):
    """Fields applied identically whether ``record`` is merged onto a
    matched NRRS item or emitted standalone (plan Sec 7).
    """
    item["ne_step_participating"] = True
    if record.get("step_rating") is not None:
        item["ne_step_rating"] = record["step_rating"]
    if record.get("full_time_staff") is not None:
        item["ne_full_time_staff"] = record["full_time_staff"]
    if record.get("part_time_staff") is not None:
        item["ne_part_time_staff"] = record["part_time_staff"]
    if record.get("capacity") is not None:
        item["capacity"] = record["capacity"]  # preferred over NRRS (Sec 7/12)

    for tag in record.get("age_tags") or []:
        flag = AGE_TAG_TO_FLAG.get(tag)
        if flag:
            item[flag] = True

    # The checklist only lists items that are actually true (verified against
    # both the provided and live-fetched detail samples), so an absent entry
    # is read as an explicit False, not "unknown" -- this is the sole source
    # for both booleans, so there is no risk of clobbering a value NRRS set.
    checklist = record.get("checklist") or []
    item["scholarships_accepted"] = "Accepts child care subsidy" in checklist
    item["ne_serves_special_needs"] = "Serves children with special needs" in checklist

    latitude, longitude = record.get("latitude"), record.get("longitude")
    if is_real_coordinate(latitude, longitude):
        item["latitude"] = str(latitude)
        item["longitude"] = str(longitude)
        item["geocode_source"] = "state"

    if not item.get("phone") and record.get("phone"):
        item["phone"] = record["phone"]
    if not item.get("email") and record.get("email"):
        item["email"] = record["email"]
    if not item.get("administrator") and record.get("administrator"):
        item["administrator"] = record["administrator"]


def apply_stepup_match_fields(item, record, method):
    """Fields set only when ``record`` is merged onto a matched NRRS item --
    NRRS's richer provider_name/provider_type/address/license_number are
    kept as-is (Sec 7)."""
    item["ne_stepup_url"] = record.get("detail_url")
    item["ne_match_method"] = method
    apply_stepup_common_fields(item, record)


def apply_stepup_standalone_fields(item, record, logger=None):
    """Fields set only for a Step Up leftover emitted with no NRRS match
    (D-2, Sec 7)."""
    item["source_state"] = "Nebraska"
    if record.get("provider_name"):
        item["provider_name"] = record["provider_name"]
    if record.get("detail_url"):
        item["provider_url"] = record["detail_url"]
    if record.get("license_number"):
        item["license_number"] = record["license_number"]
    if record.get("address"):
        item["address"] = record["address"]
    provider_type = stepup_provider_type(record, logger)
    if provider_type:
        item["provider_type"] = provider_type
        if provider_type == "Head Start":
            item["head_start"] = True
    item["ne_stepup_only"] = True
    item["ne_match_method"] = "stepup_only"


class NebraskaSpider(scrapy.Spider):
    """Two-phase, two-source Nebraska spider: buffer + index Step Up to
    Quality (Phase A), then page NRRS and emit joined ProviderItems (Phase
    B). See the module docstring and ``tasks/nebraska/nebraska_plan.md``.
    """

    name = "nebraska"
    allowed_domains = ["nrrs.ne.gov", "stepuptoquality.ne.gov"]

    custom_settings = {
        "DOWNLOAD_DELAY": 0.25,
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "RETRY_TIMES": 5,
        "DOWNLOAD_TIMEOUT": 90,  # NRRS detail payloads reach ~120 KB
        "ROBOTSTXT_OBEY": False,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # D-1: fuzzy/token name matching is intentionally not implemented in
        # this build -- reserved for a future pass. `-a fuzzy=1` is accepted
        # so the flag exists, but today it only logs a note; the tiered
        # license + name+ZIP matching below runs unchanged either way.
        self.fuzzy_enabled = str(getattr(self, "fuzzy", "0")).strip().lower() in {"1", "true", "yes"}

        # --- Phase A: Step Up buffer + index ---
        self.stepup_records = {}  # slug -> record dict
        self.stepup_pending_details = 0
        self.stepup_detail_failures = 0
        self.stepup_total_parsed = 0
        self.stepup_blank_rating_count = 0
        self.stepup_by_license = {}
        self.stepup_by_namezip = {}
        self.stepup_consumed = set()  # slugs consumed by an NRRS match
        self.stepup_leftover_count = 0
        self._nrrs_phase_started = False

        # --- Phase B: NRRS spine ---
        self.nrrs_seen_ids = set()
        self.nrrs_search_pages_remaining = None
        self.nrrs_search_done = False
        self.nrrs_pending_details = 0
        self.nrrs_detail_failures = 0
        self.nrrs_search_failures = 0
        self.nrrs_skipped_non_provider = 0
        self.nrrs_emitted = 0
        self.nrrs_from_licensure_true = 0
        self.nrrs_from_licensure_false = 0
        self._leftovers_emitted = False

        # --- merge report ---
        self.match_license_count = 0
        self.match_namezip_count = 0
        self.match_none_count = 0

    # --- Phase A: Step Up to Quality (buffer + index) -------------------- #

    def start_requests(self):
        self.logger.info(
            "Nebraska: starting -- fetching the Step Up to Quality provider list first "
            "(buffer + index) before enumerating NRRS",
        )
        if self.fuzzy_enabled:
            self.logger.warning(
                "Nebraska: -a fuzzy=1 requested, but fuzzy name matching is not implemented "
                "in this build (plan Sec 3 D-1) -- using the default license + name+ZIP tiers only",
            )
        yield scrapy.Request(
            STEPUP_SEARCH_URL,
            headers=STEPUP_HEADERS,
            callback=self.parse_stepup_search,
            errback=self.stepup_search_errback,
            dont_filter=True,
        )

    def parse_stepup_search(self, response):
        data = extract_search_results(response.text)
        providers = data.get("providers") if isinstance(data, dict) else None
        if not isinstance(providers, list):
            self.logger.error(
                "Nebraska: could not locate/parse the Step Up `searchResults` JS variable "
                "(site redesign?) -- proceeding with NO Step Up quality data; every NRRS "
                "record will be ne_match_method=nrrs_only",
            )
            yield from self._start_nrrs_phase()
            return

        self.stepup_total_parsed = len(providers)
        self.logger.info(
            "Nebraska: Step Up search parsed -- %d providers found (baseline ~%d)",
            len(providers),
            STEPUP_BASELINE,
        )
        if len(providers) < STEPUP_EXPECTED_MIN:
            self.logger.warning(
                "Nebraska: Step Up provider count %d is far below the %d baseline -- site may have changed",
                len(providers),
                STEPUP_BASELINE,
            )

        scheduled = 0
        for entry in providers:
            name, detail_url = parse_stepup_search_entry(entry.get("html"))
            if not detail_url:
                self.logger.warning(
                    "Nebraska: Step Up search entry with no detail URL skipped: %r",
                    (entry.get("html") or "")[:200],
                )
                continue
            slug = detail_url.rstrip("/").rsplit("/", 1)[-1]
            summary = {
                "slug": slug,
                "detail_url": detail_url,
                "name": name,
                "lat": entry.get("lat"),
                "lng": entry.get("lng"),
                "icon": entry.get("icon"),
            }
            self.stepup_pending_details += 1
            scheduled += 1
            yield scrapy.Request(
                detail_url,
                headers=STEPUP_HEADERS,
                callback=self.parse_stepup_detail,
                errback=self.stepup_detail_errback,
                meta={"summary": summary},
                dont_filter=True,
            )

        self.logger.info("Nebraska: scheduled %d Step Up detail requests", scheduled)
        if scheduled == 0:
            yield from self._start_nrrs_phase()

    def stepup_search_errback(self, failure):
        self.logger.error(
            "Nebraska: Step Up search request failed (%s) -- proceeding with NO Step Up quality data",
            failure.value,
        )
        yield from self._start_nrrs_phase()

    def parse_stepup_detail(self, response):
        summary = response.meta["summary"]
        record = build_stepup_record(response, summary, self.logger)
        self.stepup_records[summary["slug"]] = record
        if record.get("step_rating") is None:
            self.stepup_blank_rating_count += 1
        yield from self._stepup_detail_finished()

    def stepup_detail_errback(self, failure):
        summary = failure.request.meta.get("summary") or {}
        self.stepup_detail_failures += 1
        self.logger.warning(
            "Nebraska: Step Up detail request failed for %s (%s)",
            summary.get("slug"),
            failure.value,
        )
        yield from self._stepup_detail_finished()

    def _stepup_detail_finished(self):
        self.stepup_pending_details -= 1
        if self.stepup_pending_details <= 0:
            yield from self._start_nrrs_phase()

    def _start_nrrs_phase(self):
        if self._nrrs_phase_started:
            return
        self._nrrs_phase_started = True
        self.stepup_by_license, self.stepup_by_namezip = build_stepup_index(self.stepup_records, self.logger)
        self.logger.info(
            "Nebraska: Step Up index built -- %d providers buffered, %d license keys, "
            "%d name+zip keys, %d blank ratings, %d detail failures -- starting NRRS enumeration",
            len(self.stepup_records),
            len(self.stepup_by_license),
            len(self.stepup_by_namezip),
            self.stepup_blank_rating_count,
            self.stepup_detail_failures,
        )
        yield self._nrrs_search_request(1)

    # --- Phase B: NRRS (spine, page + detail, emits joined items) -------- #

    def _nrrs_search_request(self, page):
        body = {
            "keyword": "",
            "zip": "",
            "city": "",
            "distance": "",
            "state": "NE",
            "age": "0-120",
            "parent_category": "12",
            "child_category": "0",
            "page": page,
            "search_categories": [],
            "fields": {},
        }
        return scrapy.Request(
            NRRS_SEARCH_URL,
            method="POST",
            body=json.dumps(body),
            headers=NRRS_SEARCH_HEADERS,
            callback=self.parse_nrrs_search,
            errback=self.nrrs_search_errback,
            meta={"page": page},
            dont_filter=True,
        )

    def parse_nrrs_search(self, response):
        page = response.meta["page"]
        try:
            data = response.json()
        except Exception:
            data = {}
            self.logger.warning("Nebraska: NRRS search page %d returned invalid JSON", page)
        resources = data.get("resources") or {}
        rows = resources.get("data") or []
        last_page = resources.get("last_page")

        if page == 1:
            last_page = last_page or 1
            self.nrrs_search_pages_remaining = last_page
            self.logger.info(
                "Nebraska: NRRS search total=%s last_page=%d -- fanning out pages 2..%d",
                resources.get("total"),
                last_page,
                last_page,
            )
            for p in range(2, last_page + 1):
                yield self._nrrs_search_request(p)

        for row in rows:
            resource_id = row.get("id")
            if resource_id is None or resource_id in self.nrrs_seen_ids:
                continue
            self.nrrs_seen_ids.add(resource_id)
            self.nrrs_pending_details += 1
            yield scrapy.Request(
                NRRS_DETAIL_URL.format(id=resource_id),
                headers=NRRS_DETAIL_HEADERS,
                callback=self.parse_nrrs_detail,
                errback=self.nrrs_detail_errback,
                meta={"summary": row},
                dont_filter=True,
            )

        self.logger.info(
            "Nebraska: NRRS search page %d/%s parsed -- %d rows, %d distinct ids so far",
            page,
            last_page,
            len(rows),
            len(self.nrrs_seen_ids),
        )

        self.nrrs_search_pages_remaining -= 1
        if self.nrrs_search_pages_remaining <= 0:
            self.nrrs_search_done = True
            self.logger.info(
                "Nebraska: NRRS search enumeration complete -- %d distinct ids, %d pending details",
                len(self.nrrs_seen_ids),
                self.nrrs_pending_details,
            )
        yield from self._maybe_finish_nrrs()

    def nrrs_search_errback(self, failure):
        page = failure.request.meta.get("page")
        self.nrrs_search_failures += 1
        self.logger.warning(
            "Nebraska: NRRS search page %s failed after retries (%s) -- some providers may be missing",
            page,
            failure.value,
        )
        if page == 1:
            # last_page was never learned -- nothing more can be scheduled.
            self.nrrs_search_pages_remaining = 0
            self.nrrs_search_done = True
        else:
            self.nrrs_search_pages_remaining -= 1
            if self.nrrs_search_pages_remaining <= 0:
                self.nrrs_search_done = True
        yield from self._maybe_finish_nrrs()

    def parse_nrrs_detail(self, response):
        summary = response.meta["summary"]
        try:
            detail = response.json()
        except Exception:
            detail = {}
            self.logger.warning(
                "Nebraska: NRRS detail for id=%s returned invalid JSON",
                summary.get("id"),
            )
        item = self._build_nrrs_item(summary, detail)
        if item is not None:
            yield item
        yield from self._nrrs_detail_finished()

    def nrrs_detail_errback(self, failure):
        summary = failure.request.meta.get("summary") or {}
        self.nrrs_detail_failures += 1
        self.logger.warning(
            "Nebraska: NRRS detail request failed for id=%s (%s)",
            summary.get("id"),
            failure.value,
        )
        yield from self._nrrs_detail_finished()

    def _nrrs_detail_finished(self):
        self.nrrs_pending_details -= 1
        yield from self._maybe_finish_nrrs()

    def _maybe_finish_nrrs(self):
        if self._leftovers_emitted:
            return
        if self.nrrs_search_done and self.nrrs_pending_details <= 0:
            self._leftovers_emitted = True
            yield from self._emit_stepup_leftovers()

    def _match_stepup(self, license_number, provider_name, zip5):
        """Tiered match (D-1): exact license, else exact name+ZIP. A Step Up
        record already consumed by an earlier NRRS record is never matched
        again (D-2)."""
        if license_number:
            slug = self.stepup_by_license.get(license_number.strip().upper())
            if slug and slug not in self.stepup_consumed:
                return slug, "license"
        name = norm_name(provider_name)
        if name and zip5:
            slug = self.stepup_by_namezip.get((name, zip5))
            if slug and slug not in self.stepup_consumed:
                return slug, "name_zip"
        return None, "nrrs_only"

    def _build_nrrs_item(self, summary, detail):
        resource_id = summary.get("id")
        if resource_id is None:
            self.logger.warning("Nebraska: NRRS search row with no id skipped: %r", summary)
            return None

        resource_category_county = detail.get("resourceCategoryCounty") or []
        license_number = summary.get("license_no")
        provider_type, is_non_provider = derive_provider_type(
            license_number, resource_category_county, self.logger, resource_id
        )
        if is_non_provider:
            self.nrrs_skipped_non_provider += 1
            self.logger.info(
                "Nebraska: resource %s (%r) is a non-provider locator/registry record -- skipped",
                resource_id,
                summary.get("name1"),
            )
            return None

        item = ProviderItem()
        item["source_state"] = "Nebraska"
        item["provider_url"] = NRRS_PROVIDER_URL.format(id=resource_id)
        item["ne_resource_id"] = resource_id

        def put(key, value):
            if isinstance(value, str):
                value = value.strip() or None
            elif isinstance(value, list):
                value = value or None
            if value is not None:
                item[key] = value

        put("provider_name", summary.get("name1") or detail.get("name1"))
        put("license_holder", summary.get("name2") or detail.get("name2"))
        put("license_number", license_number)
        if provider_type:
            put("provider_type", provider_type)

        is_from_licensure = summary.get("is_from_licensure")
        if is_from_licensure is None:
            is_from_licensure = detail.get("is_from_licensure")
        if is_from_licensure is not None:
            item["ne_from_licensure"] = bool(is_from_licensure)
            if is_from_licensure:
                self.nrrs_from_licensure_true += 1
            else:
                self.nrrs_from_licensure_false += 1

        physical = detail.get("addressPhysical") or summary.get("addressPhysical") or {}
        zip5 = normalize_zip(physical.get("zip"))
        physical_address = compose_address(
            physical.get("address1"), physical.get("address2"), physical.get("city"), physical.get("state"), zip5
        )
        put("address", physical_address)
        put("city", physical.get("city"))
        put("state", physical.get("state"))
        put("zip", zip5)

        mailing = detail.get("addressMailing") or {}
        mailing_zip5 = normalize_zip(mailing.get("zip"))
        mailing_address = compose_address(
            mailing.get("address1"), mailing.get("address2"), mailing.get("city"), mailing.get("state"), mailing_zip5
        )
        if mailing_address and mailing_address != item.get("address"):
            put("mailing_address", mailing_address)

        put("county", derive_county(resource_category_county, provider_type, NE_COUNTY_NAMES, self.logger, resource_id))

        phone = (detail.get("phoneNumberPrimary") or summary.get("phoneNumberPrimary") or {}).get("phone_number")
        put("phone", phone)
        put("email", detail.get("email") or summary.get("email"))
        put("provider_website", detail.get("url") or summary.get("url"))

        contact_person = detail.get("contact_person") or summary.get("contact_person")
        contact_title = (detail.get("contact_title") or summary.get("contact_title") or "").strip()
        if contact_person and contact_person.strip():
            put("administrator", f"{contact_person.strip()} ({contact_title})" if contact_title else contact_person)

        cfr = detail.get("categoryFieldResponses") or {}

        # --- match lookup (D-1) -- done before ages_served so the age-flag
        # fallback below knows whether a matched Step Up record already
        # supplies its own (preferred) tags. ---
        slug, method = self._match_stepup(license_number, item.get("provider_name"), zip5)
        matched_record = self.stepup_records.get(slug) if slug else None

        ages = field_response(cfr, FIELD_AGES_SERVED)
        if isinstance(ages, dict):
            lower, upper = ages.get("lower"), ages.get("upper")
            if lower is not None and upper is not None:
                put("ages_served", f"{lower}-{upper}")
            elif lower is not None:
                put("ages_served", str(lower))
            elif upper is not None:
                put("ages_served", str(upper))
            if (lower is not None or upper is not None) and not (matched_record and matched_record.get("age_tags")):
                for flag, value in age_flags_from_nrrs_range(lower, upper).items():
                    item[flag] = value

        put("hours", format_nrrs_hours(field_response(cfr, FIELD_HOURS)))

        capacity = field_response(cfr, FIELD_CAPACITY)
        if isinstance(capacity, bool):
            pass
        elif isinstance(capacity, (int, float)):
            put("capacity", int(capacity))
        elif isinstance(capacity, str) and capacity.strip().isdigit():
            put("capacity", int(capacity.strip()))

        meal_bits = []
        if field_response(cfr, FIELD_USDA_FOOD_PROGRAM) == "Yes":
            meal_bits.append("USDA Food Program")
        if field_response(cfr, FIELD_MEALS_ONSITE) == "Yes":
            meal_bits.append("Meals prepared on-site")
        if meal_bits:
            put("meals", "; ".join(meal_bits))

        put("transportation", field_response(cfr, FIELD_TRANSPORTATION))

        languages = languages_from_responses(cfr)
        put("languages", ", ".join(languages) if languages else None)

        if slug:
            self.stepup_consumed.add(slug)
            apply_stepup_match_fields(item, matched_record, method)
            if method == "license":
                self.match_license_count += 1
            else:
                self.match_namezip_count += 1
        else:
            item["ne_match_method"] = "nrrs_only"
            self.match_none_count += 1

        self.nrrs_emitted += 1
        return item

    def _emit_stepup_leftovers(self):
        """D-2: every un-consumed Step Up record, emitted standalone, after
        the last NRRS detail is done (Sec 4 B4)."""
        for slug, record in self.stepup_records.items():
            if slug in self.stepup_consumed:
                continue
            item = ProviderItem()
            apply_stepup_standalone_fields(item, record, self.logger)
            apply_stepup_common_fields(item, record)
            self.stepup_leftover_count += 1
            yield item
        self.logger.info("Nebraska: emitted %d Step Up standalone leftovers", self.stepup_leftover_count)

    # --- shutdown ---------------------------------------------------------- #

    def closed(self, reason):
        self.logger.info(
            "Nebraska: finished (%s) -- NRRS: %d distinct ids, %d emitted, %d non-provider "
            "skipped, %d from_licensure=True, %d from_licensure=False, %d search failures, "
            "%d detail failures",
            reason,
            len(self.nrrs_seen_ids),
            self.nrrs_emitted,
            self.nrrs_skipped_non_provider,
            self.nrrs_from_licensure_true,
            self.nrrs_from_licensure_false,
            self.nrrs_search_failures,
            self.nrrs_detail_failures,
        )
        self.logger.info(
            "Nebraska: Step Up: %d parsed, %d buffered, %d blank ratings, %d detail failures",
            self.stepup_total_parsed,
            len(self.stepup_records),
            self.stepup_blank_rating_count,
            self.stepup_detail_failures,
        )
        self.logger.info(
            "Nebraska: merge report -- matched by license: %d, matched by name+zip: %d, "
            "nrrs_only: %d, stepup_only leftovers: %d",
            self.match_license_count,
            self.match_namezip_count,
            self.match_none_count,
            self.stepup_leftover_count,
        )
        if len(self.nrrs_seen_ids) < NRRS_EXPECTED_MIN:
            self.logger.warning(
                "Nebraska: only %d distinct NRRS ids found (< %d baseline) -- possible incomplete crawl",
                len(self.nrrs_seen_ids),
                NRRS_EXPECTED_MIN,
            )
        if self.stepup_total_parsed < STEPUP_EXPECTED_MIN:
            self.logger.warning(
                "Nebraska: only %d Step Up providers parsed (< %d baseline) -- possible incomplete crawl",
                self.stepup_total_parsed,
                STEPUP_EXPECTED_MIN,
            )
        if not self._leftovers_emitted:
            self.logger.warning(
                "Nebraska: crawl ended before Step Up leftovers were emitted -- run may be incomplete",
            )

"""Pure normalization helpers for provider items.

Every public function here is **pure**: it takes a value (or a plain ``dict``)
in and returns a value out, with no Scrapy objects, network, or file I/O. That
keeps them trivially unit-testable (see ``spiders/test_normalization.py``) and
keeps normalization logic out of the pipeline glue in ``pipelines.py``.

The single runtime consumer is ``NormalizationPipeline`` (see ``pipelines.py``),
which converts each scraped item to a ``dict`` and calls :func:`normalize_item`
(and :func:`normalize_inspection` for each inspection).

Decisions honored (see ``data_cleanup_implementation_plan.md`` §3):

- **D1** format normalization replaces values in place (no ``*_normalized``).
- **D2** field collapse is additive (populate the common field, keep the
  source state field).
- **D4** controlled-vocabulary fields are replaced in place (no ``*_raw``).

Later tasks (03–08) fill in the individual steps; for now the orchestrators are
no-ops so the pipeline can be wired in with zero behavior change.
"""
import logging
import re
from datetime import datetime

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Task 03 — whitespace & string hygiene
# --------------------------------------------------------------------------- #

# Name-like common fields that get ALL-CAPS -> Title Case treatment. Kept
# explicit and conservative: do NOT add IDs, codes, emails, or URLs here.
NAME_FIELDS = ("provider_name", "license_holder", "administrator")

# Tokens kept uppercase when title-casing a name: corporate suffixes and the
# common Roman numerals seen in personal names (e.g. "John Smith III").
NAME_PRESERVE_UPPER = {
    "LLC", "LLP", "LLLP", "LP", "INC", "PLLC", "PC", "LTD", "USA",
    "II", "III", "IV", "VI", "VII", "VIII", "IX",
}

# Whitespace characters (incl. non-breaking space) collapsed to a normal space.
_WHITESPACE_RE = re.compile(r"\s+")


def clean_whitespace(value):
    """Normalize whitespace on a string, or recurse into a list of values.

    - str: replace non-breaking/odd whitespace with normal spaces, collapse
      internal runs to one space, strip the ends. An empty result becomes
      ``None`` (so it reads as "missing").
    - list: clean each element and drop any that become ``None``/empty.
    - anything else (int, bool, dict, ...): returned unchanged.
    """
    if isinstance(value, str):
        # ``\s`` covers \xa0 (NBSP), tabs, newlines, and other unicode spaces.
        cleaned = _WHITESPACE_RE.sub(" ", value).strip()
        return cleaned or None
    if isinstance(value, list):
        out = []
        for element in value:
            cleaned = clean_whitespace(element)
            if cleaned is None or cleaned == "":
                continue
            out.append(cleaned)
        return out
    return value


def _title_token(token: str) -> str:
    """Title-case a single whitespace-delimited token.

    Keeps corporate/Roman tokens uppercase, capitalizes the first letter and
    letters following a hyphen/slash, and leaves the letter after an apostrophe
    lowercase (so ``CHILD'S`` -> ``Child's``, not ``Child'S``).
    """
    core = re.sub(r"[^A-Za-z]", "", token)
    if core and core.upper() in NAME_PRESERVE_UPPER:
        return token.upper()
    lowered = token.lower()
    return re.sub(
        r"(^|[-/])([a-z])",
        lambda m: m.group(1) + m.group(2).upper(),
        lowered,
    )


def title_case_name(value):
    """Convert an ALL-CAPS-looking name to Title Case.

    Only acts when the string is entirely upper-case (``str.isupper()`` — i.e.
    every cased letter is upper and there is at least one). Already mixed-case
    names (``"Bright Beginnings LLC"``) are left untouched.

    Known, accepted limitation: a purely acronymic name (e.g. ``"YMCA"``) is
    title-cased to ``"Ymca"``. The rule is intentionally simple (plan §5.1).
    """
    if not isinstance(value, str) or not value.isupper():
        return value
    return " ".join(_title_token(t) for t in value.split())


# --------------------------------------------------------------------------- #
# Task 04 — date normalization (ISO 8601 YYYY-MM-DD)
# --------------------------------------------------------------------------- #

# Common (provider-level) date fields converted to ISO 8601.
DATE_FIELDS = ("status_date", "license_begin_date", "license_expiration")

# Inspection-level date fields. ``status_updated`` and ``az_date_resolved`` are
# the other confirmed dates inside an inspection entry.
INSPECTION_DATE_FIELDS = ("date", "status_updated", "az_date_resolved")

# strptime patterns tried in order for purely numeric dates.
_NUMERIC_DATE_PATTERNS = ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%Y/%m/%d")

# Month-name lookup covering full names and abbreviations, including the
# AP-style "Sept." (4 letters, with period) seen in inspection dates. Keyed by
# the lower-cased token with any trailing period removed.
_MONTH_NUMBERS = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

# "Sept. 23, 2025" / "January 5, 2024" / "Dec 1, 2025".
_MONTH_NAME_DATE_RE = re.compile(r"^([A-Za-z]+)\.?\s+(\d{1,2}),\s+(\d{4})$")


def _parse_month_name_date(value: str):
    """Parse a 'Month D, YYYY' / 'Mon. D, YYYY' date; return datetime or None."""
    match = _MONTH_NAME_DATE_RE.match(value)
    if not match:
        return None
    month = _MONTH_NUMBERS.get(match.group(1).lower())
    if not month:
        return None
    try:
        return datetime(int(match.group(3)), month, int(match.group(2)))
    except ValueError:
        return None


def normalize_date(value):
    """Convert a date string to ISO 8601 ``YYYY-MM-DD`` (date only).

    Handles ``M/D/YYYY``, ``MM/DD/YYYY``, ``YYYY-MM-DD``, ISO-with-time
    (the time component is dropped), and full/abbreviated month names
    (``"Sept. 23, 2025"``). Non-strings and empties are returned unchanged.

    On an unparseable value the **original value is returned unchanged and a
    warning is logged** — we never silently drop or corrupt date data.
    """
    if not isinstance(value, str):
        return value
    candidate = value.strip()
    if not candidate:
        return value
    # ISO with a time component (e.g. "2025-10-01T06:00:00.000Z") -> date part.
    if "T" in candidate:
        candidate = candidate.split("T", 1)[0]
    for pattern in _NUMERIC_DATE_PATTERNS:
        try:
            return datetime.strptime(candidate, pattern).strftime("%Y-%m-%d")
        except ValueError:
            continue
    parsed = _parse_month_name_date(candidate)
    if parsed is not None:
        return parsed.strftime("%Y-%m-%d")
    logger.warning("normalize_date: could not parse date %r (left unchanged)",
                   value)
    return value


# --------------------------------------------------------------------------- #
# Task 05 — numeric & type consistency
# --------------------------------------------------------------------------- #

# Common fields normalized to a consistent type.
COORDINATE_FIELDS = ("latitude", "longitude")
CAPACITY_FIELD = "capacity"
AGES_SERVED_FIELD = "ages_served"

_INTEGER_RE = re.compile(r"\d+")


def normalize_capacity(value):
    """Coerce a clean numeric ``capacity`` to ``int``.

    - already an ``int`` (and not a ``bool``) -> returned as-is.
    - a clean integer string (``"8"``, ``"85"``) -> ``int``.
    - anything with extra text or a range (``"6-12"``, ``"up to 50"``) is
      **left unchanged and logged** — we do not guess.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if _INTEGER_RE.fullmatch(stripped):
            return int(stripped)
        logger.warning(
            "normalize_capacity: non-integer capacity %r (left unchanged)",
            value)
        return value
    return value


def normalize_ages_served(value):
    """Make ``ages_served`` a single string (plan §5.3: string is canonical).

    A list is whitespace-cleaned and joined with ``", "``; a string is left
    as-is. (Semantic age-range parsing is intentionally out of scope.)
    """
    if isinstance(value, list):
        parts = []
        for element in value:
            cleaned = clean_whitespace(element)
            if cleaned:
                parts.append(str(cleaned))
        return ", ".join(parts) or None
    return value


def normalize_coordinate(value):
    """Make a latitude/longitude value a trimmed **string**.

    Keeping coordinates as strings avoids float precision drift (e.g.
    ``41.69836129999999``). String inputs keep their exact digits; numeric
    inputs are stringified. We never parse a string into a float.
    """
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return str(value)
    return value


# --------------------------------------------------------------------------- #
# Task 06 — controlled vocabularies: status + facility_category
# --------------------------------------------------------------------------- #

# Authored source of truth: canonical bucket -> raw status values (original
# casing kept for documentation). The runtime lookup ``STATUS_MAP`` is derived
# from this, keyed by the trimmed, lower-cased raw value (plan §5.4).
STATUS_BUCKETS = {
    "active": [
        "LICENSED", "License", "Licensed", "Open", "Active", "Operational",
        "Regular", "Registration", "Registered", "Listed", "Full Permit",
        "CONTINUOUS LICENSE", "License issued (IL)", "Certified", "CERTIFIED",
        "Open - Certified", "Open - Payment Only", "Compliance Certificate",
        "Original", "Amended permit (AP)", "Continuing - Full",
    ],
    "provisional": [
        "PROVISIONAL LICENSE", "Initial Permit", "Provisional 1",
        "Provisional 2", "Provisional 3", "Permit issued (IP)",
        "Renewed Initial", "Initial - Full", "Provisional",
    ],
    "pending": [
        "Pending renewal application (RN)", "PENDING", "Pending",
        "Pending address change application (AD)", "Pending/Re-license",
        "Pending - Certified",
    ],
    "enforcement": [
        "Pending Revocation", "ON PROBATION", "ENFORCEMENT",
        "Pending Revocation and Denial", "Suspended",
        "Pending revocation (PR)", "Suspended - Emergency",
        # NB: en-dash (–), exactly as emitted by the source.
        "Open – Pending Legal Action Outcome", "RevocationPending",
        "Refuse to Renew (RR)", "Revoke License (RL)",
    ],
    "closed": [
        "CLOSED", "Closed", "INACTIVE", "NOT LICENSED", "Revoked",
        "Surrendered under Investigation (SI)", "Surrendered with Cause (SC)",
        "Temporary Closure",
    ],
}

# Runtime lookup: trimmed+lower-cased raw value -> canonical bucket.
STATUS_MAP = {
    raw.strip().lower(): canonical
    for canonical, raws in STATUS_BUCKETS.items()
    for raw in raws
}


def canonical_status(value):
    """Map a raw ``status`` to its canonical bucket (replaces in place, D4).

    Lookup is case-insensitive and whitespace-tolerant. Unmapped values become
    ``"unknown"`` and are **logged** so the table can be extended. The raw
    value remains recoverable via a non-normalized run (NORMALIZE_ENABLED=False).
    """
    if not isinstance(value, str):
        return value
    canonical = STATUS_MAP.get(value.strip().lower())
    if canonical is None:
        logger.warning("canonical_status: unmapped status %r -> 'unknown'",
                       value)
        return "unknown"
    return canonical


# Authored source of truth: canonical facility category -> raw provider_type
# values (the 73 distinct values from the Task 01 inventory). ``facility_category``
# is an additive facet — ``provider_type`` keeps its exact state value (D2).
#
# Judgment calls (documented): center-based institutional care (public schools,
# local school systems, Head Start, university, DoD) -> ``center``; Utah's
# residential certificate (care in a residence) -> ``family_home``; camps,
# placement agencies, FFN/nanny informal care, and youth orgs -> ``other``.
FACILITY_CATEGORY_BUCKETS = {
    "center": [
        "DAY CARE CENTER", "Child Care Center", "Licensed Center", "Center",
        "Child Care Facility", "Child Care Learning Center", "INFANT CENTER",
        "Child Day Center", "SINGLE LICENSED CHILD CARE CENTER", "DCC",
        "Preschool Center", "Preschool Program", "Public School",
        "Local School System", "Child Care Registered Center Based Program",
        "Infant and Toddler Center", "Certified Pre-School",
        "Child Care Commercial Preschool", "Short Term Child Day Center",
        "Outdoor Nature Based Program", "GA Head Start", "GA Early Head Start",
        "Small Employer Based Child Care", "Department of Defense",
        "DAY CARE CENTER - ILL CENTER", "University", "SDCC",
        "Child Care Hourly Center",
        # Maryland (checkccmd.org) codes: CTR = center, LOC = a center-based
        # program operating under a Letter of Compliance.
        "CTR", "LOC",
        # Ohio (childcaresearch.ohio.gov) program types.
        "Licensed Child Care Center", "Licensed School-Based Preschool",
        # North Dakota (search.ec.hhs.nd.gov) facilityType labels. Facility- and
        # school-based institutional care -> center.
        "HHS-Licensed Child Care Center", "HHS-Licensed Group Child Care Facility",
        "HHS-Licensed Preschool", "HHS Four-Year Old Program", "Head Start Site",
    ],
    "family_home": [
        "FAMILY DAY CARE HOME", "Family Child Care Home", "Family Home", "FDC",
        "Registered Child-Care Home", "Licensed Child-Care Home",
        "Listed Family Home", "Family Day Home", "Family Day Care Home",
        "Family Child Care Learning Home", "Family Child Care",
        "Registered Home", "Child Care Licensed Family", "Family",
        "Licensed Family Home", "Family Home Child Care",
        "Large Family Child Care Home", "Unlicensed/Unregistered FDH",
        "System Approved FDH", "Child Care Residential Certificate",
        # Maryland codes: FCCH = family child care home, LFCCH = large FCCH.
        "FCCH", "LFCCH",
        # Ohio program types: Type A/B are family child care homes.
        "Licensed Type A Family Child Care Home",
        "Licensed Type B Family Child Care Home",
        # North Dakota: family child care in a residence.
        "HHS-Licensed Family Child Care",
    ],
    "group_home": [
        "GFDC", "Group Home", "Group Home Child Care", "Group",
        "Group Child Care Home",
        # North Dakota: group child care operated in a home.
        "HHS-Licensed Group Child Care Home",
    ],
    "school_age": [
        "SACC", "SCHOOL AGE DAY CARE CENTER", "School-age Program",
        "School Age Program", "School-age Center",
        "Child Care Out of School Time Program",
        # Ohio program type.
        "Licensed School-Age Child Care",
        # North Dakota.
        "HHS-Licensed School Age Child Care",
    ],
    "exempt": [
        "Exempt Only", "Religious Exempt Child Day Center",
        "Exempt Child Care Center", "DWS Approved, Exempt Center",
        "DWS Approved, Exempt School Age Program", "Child Care Exempt Program",
        "Voluntary Registration",
        # North Dakota: license-exempt self-declared programs.
        "Self-Declared Provider",
    ],
    "other": [
        "Other", "Resident Camp", "Summer Day Camp",
        "Substitute Placement Agency",
        "Family, Friends & Neighbor (FFN) Providers",
        "Neighborhood Youth Organization", "(FCC)Nanny Individual",
        # Ohio: camps -> other; in-home aide is informal in-home care -> other.
        "Registered Day Camp or Approved Day Camp", "Certified In Home Aide",
        # North Dakota: a provider holding multiple license types (ambiguous
        # category) and tribal subsidy recipients (informal/subsidy).
        "HHS-Licensed Multiple License", "Tribal Subsidy Recipient",
    ],
}

# Runtime lookup: trimmed+lower-cased provider_type -> canonical category.
FACILITY_CATEGORY_MAP = {
    raw.strip().lower(): canonical
    for canonical, raws in FACILITY_CATEGORY_BUCKETS.items()
    for raw in raws
}


def facility_category_from_type(provider_type):
    """Derive the canonical ``facility_category`` from a raw ``provider_type``.

    Case-insensitive, whitespace-tolerant lookup. Unmapped values become
    ``"other"`` and are **logged** so the table can be extended. Does not
    modify ``provider_type``.
    """
    if not isinstance(provider_type, str):
        return None
    category = FACILITY_CATEGORY_MAP.get(provider_type.strip().lower())
    if category is None:
        logger.warning(
            "facility_category_from_type: unmapped provider_type %r -> 'other'",
            provider_type)
        return "other"
    return category


# --------------------------------------------------------------------------- #
# Task 07 — field collapse (state-specific -> common, additive D2)
# --------------------------------------------------------------------------- #

# common field -> source state-specific fields. Populating the common field
# never removes the source field (D2). Approved set from plan §6; the
# license_begin_date date-merge is intentionally excluded, and quality ratings
# are never collapsed (they stay state-specific). Keep owner/licensee mailing
# addresses (nc_owner_mailing_address, mi_licensee_address) and VPK-specific
# curriculum (fl_vpk_curriculum) OUT of these lists.
FIELD_COLLAPSE_MAP = {
    "license_type": [
        "va_license_type", "mt_license_type", "ut_license_type",
        "co_license_type", "az_license_type", "nc_license_type",
        "nj_license_type", "wv_license_type", "wa_license_type",
        "hi_license_type",
    ],
    "school_district": [
        "co_school_district", "ny_school_district_name", "pa_school_district",
        "ut_school_district", "wa_school_district",
    ],
    "mailing_address": [
        "al_mailing_address", "ga_mailing_address", "hi_mailing_address",
    ],
    "accreditation": [
        "al_accreditations", "ga_accreditation", "hi_accreditations",
        "nj_accreditation", "md_accreditation",
    ],
    "meals": [
        "ut_meals", "ga_meals", "hi_meals", "nm_meals", "nj_meal_options",
        "pa_meal_options",
    ],
    "accepting_new_children": [
        "co_accepting_new_children", "ga_accepting_new_children",
    ],
    "transportation": ["ga_transportation", "nj_transportation"],
    "head_start": [
        "co_head_start", "az_headstart", "wa_head_start", "ri_head_start",
        "fl_is_head_start",
    ],
    "curriculum": ["ga_curriculum", "nj_curriculum"],
}

# Common fields whose collapsed value is coerced to a boolean.
_BOOLEAN_COLLAPSE_FIELDS = {"head_start"}

_AFFIRMATIVE = {"yes", "y", "true", "t", "1"}
_NEGATIVE = {"no", "n", "false", "f", "0", "none", "n/a", "na",
             "not applicable"}


def _is_present(value):
    """True if a value carries signal. ``False``/``0`` count as present (a state
    explicitly saying "no"); ``None``/``""``/``[]`` do not."""
    return value is not None and value != "" and value != []


def _coerce_bool(value):
    """Best-effort map of a mixed boolean/text value to ``True``/``False``."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        token = value.strip().lower()
        if token in _NEGATIVE:
            return False
        if token in _AFFIRMATIVE:
            return True
        # Any other descriptive, non-empty text (e.g. a program name) is
        # treated as affirmative presence of the program.
        return True
    return bool(value)


def collapse_state_fields(item: dict) -> dict:
    """Populate common fields from their single populated source field.

    Rules (plan §6, D2):
      - Never overwrite an already-populated common field.
      - Copy only when **exactly one** source field is populated (never guess).
      - Keep the source field in place (additive).
      - ``head_start`` is coerced to a boolean.
    """
    for common, sources in FIELD_COLLAPSE_MAP.items():
        if _is_present(item.get(common)):
            continue
        present = [item[src] for src in sources if _is_present(item.get(src))]
        if len(present) != 1:
            continue
        value = present[0]
        if common in _BOOLEAN_COLLAPSE_FIELDS:
            value = _coerce_bool(value)
        item[common] = value
    return item


# --------------------------------------------------------------------------- #
# Task 08 — address cleanup (in place) + component parse (additive)
# --------------------------------------------------------------------------- #

# Valid USPS 2-letter codes (50 states + DC), used to validate a parsed state.
US_STATE_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC",
}

# Spelled-out state name -> USPS code. Matched longest-first so "West Virginia"
# wins over "Virginia".
SPELLED_STATE_TO_USPS = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT",
    "delaware": "DE", "florida": "FL", "georgia": "GA", "hawaii": "HI",
    "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME",
    "maryland": "MD", "massachusetts": "MA", "michigan": "MI",
    "minnesota": "MN", "mississippi": "MS", "missouri": "MO", "montana": "MT",
    "nebraska": "NE", "nevada": "NV", "new hampshire": "NH",
    "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH",
    "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA",
    "rhode island": "RI", "south carolina": "SC", "south dakota": "SD",
    "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
}

# Spelled state names sorted longest-first for greedy, correct matching.
_SPELLED_STATES_BY_LEN = sorted(
    SPELLED_STATE_TO_USPS.items(), key=lambda kv: len(kv[0]), reverse=True)

_COUNTRY_SUFFIX_RE = re.compile(
    r",?\s*(United States(?: of America)?|U\.?S\.?A\.?)\s*$", re.IGNORECASE)
_ZIP_RE = re.compile(r"(\d{5})(?:-?\d{4})?\s*$")
_TWO_LETTER_STATE_RE = re.compile(r"[,\s]([A-Za-z]{2})\s*$")


def clean_address(value):
    """Clean an ``address`` string in place (D1).

    Whitespace cleanup, strip a trailing ``, United States`` / ``, USA``, and
    normalize spacing around commas. Does **not** invent missing pieces.
    """
    if not isinstance(value, str):
        return value
    cleaned = clean_whitespace(value)
    if cleaned is None:
        return None
    cleaned = _COUNTRY_SUFFIX_RE.sub("", cleaned)
    cleaned = re.sub(r"\s*,\s*", ", ", cleaned)
    cleaned = cleaned.strip().strip(",").strip()
    return cleaned or None


def _extract_state(before_zip: str):
    """Return ``(usps_code, prefix_before_state)`` or ``(None, before_zip)``."""
    low = before_zip.lower()
    for name, code in _SPELLED_STATES_BY_LEN:
        if low.endswith(name):
            idx = len(before_zip) - len(name)
            if idx == 0 or before_zip[idx - 1] in " ,":
                prefix = before_zip[:idx].strip().rstrip(",").strip()
                return code, prefix
    match = _TWO_LETTER_STATE_RE.search(before_zip)
    if match:
        code = match.group(1).upper()
        if code in US_STATE_CODES:
            prefix = before_zip[:match.start()].strip().rstrip(",").strip()
            return code, prefix
    return None, before_zip


def _extract_city(prefix: str):
    """City only when it is a clean trailing comma-delimited field; else None."""
    if not prefix or "," not in prefix:
        return None
    city = prefix.rsplit(",", 1)[1].strip()
    return city or None


def parse_address_components(value):
    """Best-effort ``(city, state, zip)`` from a cleaned address (additive).

    Conservative by design — **never guesses**. A component is left ``None``
    unless the format clearly supports it:
      - requires a trailing 5-digit ZIP **and** a recognizable state (2-letter
        USPS or spelled-out); otherwise all three are ``None`` and it is logged.
      - ``city`` is filled only when it is a clean comma-delimited field
        immediately before the state (so street/city mashes stay ``None``).
    ``zip`` is the 5-digit code (any +4 suffix is dropped).
    """
    if not isinstance(value, str) or not value.strip():
        return (None, None, None)
    text = value.strip()
    zip_match = _ZIP_RE.search(text)
    if not zip_match:
        logger.warning("parse_address_components: no ZIP in %r (unparsed)",
                       value)
        return (None, None, None)
    zip_code = zip_match.group(1)
    before_zip = text[:zip_match.start()].strip().rstrip(",").strip()
    state, prefix = _extract_state(before_zip)
    if state is None:
        logger.warning(
            "parse_address_components: no recognizable state in %r (unparsed)",
            value)
        return (None, None, None)
    city = _extract_city(prefix)
    return (city, state, zip_code)


def normalize_item(item: dict, state: str) -> dict:
    """Apply all normalization steps to one provider item, in order.

    ``state`` is ``spider.name`` (e.g. ``"montana"``) and is the key into any
    per-state mapping tables. Mutates and returns the same ``dict``.

    The processing order (filled in by later tasks) is:
      1. whitespace / string hygiene
      2. field collapse (state-specific -> common)
      3. date normalization
      4. numeric / type normalization
      5. controlled vocabulary (status, facility_category)
      6. address cleanup + component parse
    """
    # 1. whitespace / string hygiene on every string/list value. ``inspections``
    #    is handled separately (it is a list of dicts, not strings).
    for key, value in list(item.items()):
        if key == "inspections":
            continue
        if isinstance(value, (str, list)):
            item[key] = clean_whitespace(value)

    # Name casing on the explicit name-field allowlist only.
    for field in NAME_FIELDS:
        if item.get(field) is not None:
            item[field] = title_case_name(item[field])

    # 2. field collapse (state-specific -> common) before format/vocab steps,
    #    so a collapsed value is then format-normalized by the steps below.
    collapse_state_fields(item)

    # 3. date normalization -> ISO 8601.
    for field in DATE_FIELDS:
        if item.get(field) is not None:
            item[field] = normalize_date(item[field])

    # 4. numeric / type normalization.
    if item.get(CAPACITY_FIELD) is not None:
        item[CAPACITY_FIELD] = normalize_capacity(item[CAPACITY_FIELD])
    if item.get(AGES_SERVED_FIELD) is not None:
        item[AGES_SERVED_FIELD] = normalize_ages_served(
            item[AGES_SERVED_FIELD])
    for coord in COORDINATE_FIELDS:
        if item.get(coord) is not None:
            item[coord] = normalize_coordinate(item[coord])

    # 5. controlled vocabulary. status is replaced in place (D4); the
    #    facility_category facet is derived additively from provider_type (D2)
    #    and provider_type itself is left untouched.
    if item.get("status") is not None:
        item["status"] = canonical_status(item["status"])
    if item.get("provider_type") is not None:
        item["facility_category"] = facility_category_from_type(
            item["provider_type"])

    # 6. address cleanup (in place, D1) + best-effort component parse (additive,
    #    D2). Components are only set when clearly parsed and not already set.
    #    When the scraper already supplied all of city/state/zip (e.g. from
    #    structured source fields), skip the parse entirely: it can add nothing
    #    and would otherwise log a spurious "no ZIP" warning for a street-only
    #    `address`.
    if item.get("address") is not None:
        item["address"] = clean_address(item["address"])
        already_have_components = all(
            _is_present(item.get(key)) for key in ("city", "state", "zip"))
        if item.get("address") and not already_have_components:
            city, parsed_state, zip_code = parse_address_components(
                item["address"])
            for key, parsed in (("city", city), ("state", parsed_state),
                                ("zip", zip_code)):
                if parsed is not None and not _is_present(item.get(key)):
                    item[key] = parsed

    return item


def normalize_inspection(inspection: dict, state: str) -> dict:
    """Normalize one inspection entry (whitespace + dates only).

    The ``status`` controlled vocabulary deliberately does **not** apply to
    inspections. Mutates and returns the same ``dict``.
    """
    for key, value in list(inspection.items()):
        if isinstance(value, (str, list)):
            inspection[key] = clean_whitespace(value)
    for field in INSPECTION_DATE_FIELDS:
        if inspection.get(field) is not None:
            inspection[field] = normalize_date(inspection[field])
    return inspection

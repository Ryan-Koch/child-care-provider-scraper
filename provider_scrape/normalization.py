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

from provider_scrape.common_fields import (
    INSPECTION_LEVEL,
    announced_flag,
    day_names,
    has_list_token,
    invert,
    is_present,
    last_first_name,
    license_holder_address,
    lower_email,
    none_if_sentinel,
    person_name,
    phone_extension_digits,
    run_adapters,
    to_bool,
    to_id_string,
    to_int,
    to_iso_date,
    wi_total_vacancies,
    wi_vacancies_updated_date,
)

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
    "LLC",
    "LLP",
    "LLLP",
    "LP",
    "INC",
    "PLLC",
    "PC",
    "LTD",
    "USA",
    "II",
    "III",
    "IV",
    "VI",
    "VII",
    "VIII",
    "IX",
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


def _title_token(token: str, preserve_upper=NAME_PRESERVE_UPPER) -> str:
    """Title-case a single whitespace-delimited token.

    Keeps any token in ``preserve_upper`` uppercase (default:
    ``NAME_PRESERVE_UPPER``, corporate suffixes and Roman numerals),
    capitalizes the first letter and letters following a hyphen/slash, and
    leaves the letter after an apostrophe lowercase (so ``CHILD'S`` ->
    ``Child's``, not ``Child'S``).
    """
    core = re.sub(r"[^A-Za-z]", "", token)
    if core and core.upper() in preserve_upper:
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
# the other confirmed dates inside an inspection entry; ``in_correction_date`` is
# Indiana's non-compliance correction date.
INSPECTION_DATE_FIELDS = (
    "date",
    "status_updated",
    "az_date_resolved",
    "in_correction_date",
    "ms_end_date",
    "mo_disposition_date",
)

# strptime patterns tried in order for purely numeric dates.
_NUMERIC_DATE_PATTERNS = ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%Y/%m/%d")

# Month-name lookup covering full names and abbreviations, including the
# AP-style "Sept." (4 letters, with period) seen in inspection dates. Keyed by
# the lower-cased token with any trailing period removed.
_MONTH_NUMBERS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

# "Sept. 23, 2025" / "January 5, 2024" / "Dec 1, 2025" / "May 29th, 2026"
# (Idaho's idahochildcarecheck.org renders inspection dates with an ordinal
# suffix on the day -- st/nd/rd/th -- which this optionally consumes).
_MONTH_NAME_DATE_RE = re.compile(r"^([A-Za-z]+)\.?\s+(\d{1,2})(?:st|nd|rd|th)?,\s+(\d{4})$")


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
    (the time component is dropped), a trailing clock time (e.g.
    ``"05/07/2026 08:42 AM"`` -> the date part), and full/abbreviated month
    names (``"Sept. 23, 2025"``). Non-strings and empties are returned unchanged.

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
    # Drop a trailing clock time (e.g. "05/07/2026 08:42 AM" or "... 14:30:00")
    # so a date+time timestamp normalizes to its date. Month-name dates
    # ("Sept. 23, 2025") have no trailing clock time and are left untouched.
    candidate = re.sub(r"\s+\d{1,2}:\d{2}(?::\d{2})?(?:\s*[AaPp][Mm])?$", "", candidate).strip()
    for pattern in _NUMERIC_DATE_PATTERNS:
        try:
            return datetime.strptime(candidate, pattern).strftime("%Y-%m-%d")
        except ValueError:
            continue
    parsed = _parse_month_name_date(candidate)
    if parsed is not None:
        return parsed.strftime("%Y-%m-%d")
    logger.warning("normalize_date: could not parse date %r (left unchanged)", value)
    return value


# --------------------------------------------------------------------------- #
# Task 05 — numeric & type consistency
# --------------------------------------------------------------------------- #

# Common fields normalized to a consistent type.
COORDINATE_FIELDS = ("latitude", "longitude")
CAPACITY_FIELD = "capacity"
AGES_SERVED_FIELD = "ages_served"
DEFICIENCIES_FIELD = "deficiencies"

_INTEGER_RE = re.compile(r"\d+")

# Thousands-grouped count, e.g. NY's "1,100" school-age capacity or TX's rare
# high deficiency counts. Shared by normalize_deficiencies (below) and
# coerce_age_served_flag/_coerce_strict_bool_flag (Task 10).
_GROUPED_INTEGER_RE = re.compile(r"\d{1,3}(?:,\d{3})+")


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
        logger.warning("normalize_capacity: non-integer capacity %r (left unchanged)", value)
        return value
    return value


def normalize_deficiencies(value):
    """Coerce the provider-level ``deficiencies`` count to ``int``-or-``None``
    (bug_10).

    CT/KS/WI already compute this as a plain Python ``int`` in their spiders
    (a ``sum``/``len`` over parsed inspection rows); TX instead passes its CSV
    ``Deficiencies`` column straight through as a numeric **string**
    (``'0'``, ``'1'``, ``'2'``, ...). This normalizes every shape the pipeline
    might see to the same ``int``-or-``None``, unlike :func:`normalize_capacity`
    (which leaves an unparseable value unchanged) -- a bad deficiency count is
    data we don't trust, so it is dropped to ``None`` rather than kept:

    - already an ``int`` (and not a ``bool``) -> returned as-is, unless
      negative (see below).
    - a clean digit string (``"0"``, ``"47"``) -> ``int``.
    - a thousands-grouped string (``"1,234"``) -> commas stripped, then ``int``.
    - an integral float or integral-float **string** (``2.0``, ``"2.0"``) ->
      ``int`` (``2``). A non-integral float (``2.5``) is *not* rounded --
      treated as unparseable (below).
    - ``None``, ``""``, or whitespace-only -> ``None`` (no signal).
    - negative (any numeric shape), non-numeric text, or a non-integral float
      -> ``None``, **logged at DEBUG**. We never guess a count.

    ``bool`` is checked first and becomes ``None`` (logged at DEBUG) --
    ``isinstance(True, int)`` is ``True`` in Python, so without this guard a
    stray boolean would fall into the ``int`` branch and be read as a count
    of 0/1. Unlike :func:`normalize_capacity`, a bool is not passed through:
    ``deficiencies`` is strictly int-or-None.
    """
    if isinstance(value, bool):
        logger.debug("normalize_deficiencies: boolean value %r -> None", value)
        return None
    if isinstance(value, int):
        if value < 0:
            logger.debug("normalize_deficiencies: negative value %r -> None", value)
            return None
        return value
    if isinstance(value, float):
        if value < 0:
            logger.debug("normalize_deficiencies: negative value %r -> None", value)
            return None
        if value.is_integer():
            return int(value)
        logger.debug("normalize_deficiencies: non-integer value %r -> None", value)
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        if stripped.startswith("-"):
            logger.debug("normalize_deficiencies: negative value %r -> None", value)
            return None
        # Strip thousands-grouping commas (e.g. "1,234") before the digit
        # check, same gated approach as coerce_age_served_flag -- only when
        # the *whole* trimmed string matches the grouped pattern, so a
        # malformed value like "1,2,3" isn't silently reinterpreted.
        candidate = stripped.replace(",", "") if _GROUPED_INTEGER_RE.fullmatch(stripped) else stripped
        if _INTEGER_RE.fullmatch(candidate):
            return int(candidate)
        try:
            as_float = float(candidate)
        except ValueError:
            logger.debug("normalize_deficiencies: non-numeric value %r -> None", value)
            return None
        if as_float.is_integer():
            return int(as_float)
        logger.debug("normalize_deficiencies: non-integer value %r -> None", value)
        return None
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
        "LICENSED",
        "License",
        "Licensed",
        "Open",
        "Active",
        "Operational",
        "Regular",
        "Registration",
        "Registered",
        "Listed",
        "Full Permit",
        "CONTINUOUS LICENSE",
        "License issued (IL)",
        "Certified",
        "CERTIFIED",
        "Open - Certified",
        "Open - Payment Only",
        "Compliance Certificate",
        "Original",
        "Amended permit (AP)",
        "Continuing - Full",
        # Alaska (AKCCIS): providerStatus values from Facility/Search. "Exempt"
        # here is a *status* meaning "operating under an exemption" (i.e.
        # actively open); the exempt facility_category is derived separately
        # from provider_type. The project's status vocab has no `exempt`
        # bucket, so `active` is the operationally-correct home.
        "Active/Open",
        "Exempt",
        # Iowa (Titan StatusCode): "A" is the only value observed (170/170,
        # then 3,370/3,370 on a full harvest -- the public search appears to
        # expose only active providers).
        "A",
        # Kentucky (kynect.ky.gov): the only status on an operating provider.
        "APPROVED",
        # Idaho (idahostars.org ICCPStatus): a provider mid-renewal, or with a
        # pending facility-type change, is still operating -- `Certified`
        # above already covers the steady-state value.
        "Pending Renewal",
        "Pending Facility Type Change",
        # Massachusetts (childcare.mass.gov): providerStatus values observed
        # across recon + a live smoke run (2026-09-04, ZIP 02301). "Renewal
        # in progress"/"Funding Renewal In Progress" are providers still
        # appearing in the public search while their licence/funding renews
        # -- still operating, matching the Idaho "Pending Renewal" precedent
        # above. Other values (Closed/enforcement stages) are likely but
        # unseen -- an unmapped value falls through to "unknown" with a
        # logged warning (the spider's closed() also flags any new raw value).
        "Current",
        "Renewal in progress",
        "Funding Renewal In Progress",
    ],
    "provisional": [
        "PROVISIONAL LICENSE",
        "Initial Permit",
        "Provisional 1",
        "Provisional 2",
        "Provisional 3",
        "Permit issued (IP)",
        "Renewed Initial",
        "Initial - Full",
        "Provisional",
        # Maine: a conditional license is a short-term license issued while
        # a facility corrects deficiencies, analogous to a provisional license.
        "Conditional",
        # Mississippi (mdhs.provider.webapps.ms.gov): a short-term licence
        # issued outside the normal annual cycle (e.g. a new/relocated
        # facility awaiting its first full-year licence). Ryan, 2026-09-04.
        "TEMPORARY",
        # New Hampshire (NHCIS License_Status__c): a permit issued ahead of
        # full licensure -- matches the "Permit issued (IP)" / "Initial
        # Permit" precedents above.
        "Permit Issued",
    ],
    "pending": [
        "Pending renewal application (RN)",
        "PENDING",
        "Pending",
        "Pending address change application (AD)",
        "Pending/Re-license",
        "Pending - Certified",
        # Mississippi: a whole PENDING-* family, all -> pending (the vocab
        # was flagged open in §4.3; each entry below surfaced on a
        # different full live run, not in the original recon sample -- if
        # another PENDING-* variant shows up, it almost certainly belongs
        # here too rather than being a new bucket).
        # operating while awaiting its licensing inspection -- matches the
        # bare "PENDING" precedent above. Approved Ryan 2026-09-04.
        "PENDING-INSPECTION",
        # operating while awaiting document review ahead of inspection.
        "PENDING-DOCS-INSPECT",
        # operating while awaiting document submission/review generally --
        # surfaced 2026-09-05 on the county-fan-out full run.
        "PENDING-DOCUMENTS",
    ],
    "enforcement": [
        "Pending Revocation",
        "ON PROBATION",
        "ENFORCEMENT",
        "Pending Revocation and Denial",
        "Suspended",
        "Pending revocation (PR)",
        "Suspended - Emergency",
        # NB: en-dash (–), exactly as emitted by the source.
        "Open – Pending Legal Action Outcome",
        "RevocationPending",
        "Refuse to Renew (RR)",
        "Revoke License (RL)",
        # Indiana (secure.in.gov): open but with a pending enforcement action
        # (hyphen, exactly as emitted).
        "Open - Enforcement Pending",
        # Alaska (AKCCIS): Denied and App Withdrawn are regulatory blocks
        # (not neutral closures), so they live with enforcement rather than
        # closed.
        "Denied",
        "App Withdrawn",
        # Kansas (khap.kdhe.ks.gov OIDS): a suspended licence is a regulatory
        # action, not a neutral closure -- matches the bare "Suspended" above.
        # Approved Ryan 2026-08-20.
        "License Suspended",
        # Delaware (data.delaware.gov iuzd-3dbt): probation and
        # intent-to-revoke stages, derived onto `status` by
        # delaware.derive_status. `enforcement_action` values (an action
        # already taken) and `intent_to_revoke` values (a notice of one
        # pending) share this bucket -- see delaware_plan.md Sec 7.3/8.
        "Probation",
        "Probation Extension",
        "Warning of Probation",
        "Warning of Probation Extension",
        "Intent to Revoke",
        "Intent to Place on Probation",
        "Intent to Place on Probation Extension",
        "Intent to Place on Warning of Probation",
        "Intent to Place on Warning of Probation Extension",
        # Mississippi (mdhs.provider.webapps.ms.gov): surfaced once on the
        # 2026-09-05 full live run, not in the original recon sample -- a
        # restricted licence is a regulatory limitation, matching the
        # Probation/Suspended precedents above. Single-occurrence value;
        # flagged to confirm with Ryan later if more show up.
        "RESTRICTED",
    ],
    "closed": [
        "CLOSED",
        "Closed",
        "INACTIVE",
        "NOT LICENSED",
        "Revoked",
        "Surrendered under Investigation (SI)",
        "Surrendered with Cause (SC)",
        "Temporary Closure",
        # Alaska (AKCCIS): Closed / Temporary Closure / Suspended are already
        # covered by the generic entries above; Revoked/Not Renewed is the
        # state-specific label.
        "Revoked/Not Renewed",
        # Kansas: the licence lapsed -- no longer licensed, cf. "NOT
        # LICENSED". Approved Ryan 2026-08-20.
        "License Expired",
        # Maine: license has expired.
        "Expired",
        # Connecticut (211childcare.org): the directory publishes no licence
        # status. `searchable: false` means CT withholds the provider from its
        # own public search; 62 of 63 sampled such records have zero CT OEC
        # inspections and 83% are not accepting referrals, so a lapsed/closed
        # listing is the best reading. This is an inference, not a published
        # status. Approved Ryan 2026-08-20.
        "Not Listed",
        # Idaho (idahostars.org ICCPStatus): opted out of the ICCP program --
        # "Closed" above already covers the provider's shutdown status.
        "Not Participating",
    ],
}

# Runtime lookup: trimmed+lower-cased raw value -> canonical bucket.
STATUS_MAP = {raw.strip().lower(): canonical for canonical, raws in STATUS_BUCKETS.items() for raw in raws}


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
        logger.warning("canonical_status: unmapped status %r -> 'unknown'", value)
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
        "DAY CARE CENTER",
        "Child Care Center",
        "Licensed Center",
        "Center",
        "Child Care Facility",
        "Child Care Learning Center",
        "INFANT CENTER",
        "Child Day Center",
        "SINGLE LICENSED CHILD CARE CENTER",
        "DCC",
        "Preschool Center",
        "Preschool Program",
        "Public School",
        "Local School System",
        "Child Care Registered Center Based Program",
        "Infant and Toddler Center",
        "Certified Pre-School",
        "Child Care Commercial Preschool",
        "Short Term Child Day Center",
        "Outdoor Nature Based Program",
        "GA Head Start",
        "GA Early Head Start",
        "Small Employer Based Child Care",
        "Department of Defense",
        "DAY CARE CENTER - ILL CENTER",
        "University",
        "SDCC",
        "Child Care Hourly Center",
        # Maryland (checkccmd.org) codes: CTR = center, LOC = a center-based
        # program operating under a Letter of Compliance.
        "CTR",
        "LOC",
        # Ohio (childcaresearch.ohio.gov) program types. New Jersey
        # (childcarenj.gov ProgramFacilityType code 1, resolved via bug_07's
        # programTypes lookup table) reuses this exact string -- no separate
        # entry needed.
        "Licensed Child Care Center",
        "Licensed School-Based Preschool",
        # North Dakota (search.ec.hhs.nd.gov) facilityType labels. Facility- and
        # school-based institutional care -> center.
        "HHS-Licensed Child Care Center",
        "HHS-Licensed Group Child Care Facility",
        "HHS-Licensed Preschool",
        "HHS Four-Year Old Program",
        "Head Start Site",
        # Washington DC (mychildcare.dc.gov): CDC = center.
        "CDC (Child Development Center)",
        # Wisconsin (childcarefinder.wisconsin.gov) Regulation Type: a licensed
        # group child care center (9+ children, non-residential) -> center. The
        # finder's own toggle labels these "Group Centers".
        "Licensed Group",
        # Vermont (brightfutures.dcf.state.vt.us) detail "License Type": a
        # licensed, facility-based center-and-preschool program -> center. The
        # "- Non-Recurring" variant is a short-term/seasonal center (cf. the
        # "Short Term Child Day Center" precedent above).
        "Center Based Child Care and Preschool Program",
        "Center Based Child Care and Preschool Program - Non-Recurring",
        # Tennessee (onedhs.tn.gov): coarse provider_type.
        "Child Care",
        "DOE",
        # Iowa (Titan TypeOfCareDesc) "Licensed Center" reuses the exact
        # "Licensed Center" string already listed above for Indiana -- no
        # new entry needed.
        # Kansas (khap.kdhe.ks.gov OIDS): data-label program types (NOT the
        # search-form labels, which differ -- see kansas_plan.md Sec 5.7).
        # A bare "Preschool" and a facility-based Head Start center are both
        # center-based institutional care, matching the "Preschool Program" /
        # "Head Start Site" precedents already above.
        "Preschool",
        "Head Start Child Care Center",
        # Kentucky (kynect.ky.gov): "Licensed" == a Licensed Child Care Center
        # (licence numbers are L-prefixed).
        "Licensed",
        # Maine: Nursery School (from search form) -> center, matching the
        # "Nursery School" precedent above.
        "Nursery",
        # Connecticut (211childcare.org): a preschool program -- matches the
        # "Preschool Program" / "Certified Pre-School" precedents above. Note
        # DCEX (license-exempt) records also carry this same "Child Care
        # Center" -> center mapping (see ct_license_type doc, items.py).
        "Nursery School",
        # Idaho (idahostars.org detail page "Facility Type"): the largest of
        # Idaho's three facility-size tiers.
        "Large Child Care Center (26 or more children)",
        # Massachusetts (childcare.mass.gov) recordTypeName values: Large/Small
        # Group are facility-based center licenses (capacity-tiered, not a
        # residence); Private School matches the "Public School" precedent
        # already mapped above. ("Family Child Care" -> family_home is already
        # present in the family_home bucket below.)
        "Large Group",
        "Small Group",
        "Private School",
        # Mississippi (mdhs.provider.webapps.ms.gov): the facility-based
        # licence type -- matches the "Child Care Facility" / "Child Care
        # Center" precedents above. Approved Ryan 2026-09-04.
        "Center based Child Care Facility",
        # Louisiana (louisianaschools.com): Type I (church/nonprofit),
        # Type II (for-profit), Type III (publicly-funded) are all
        # facility-based centers. Approved Ryan 2026-09-08.
        "Type I",
        "Type II",
        "Type III",
        # Nebraska (stepuptoquality.ne.gov) detail h2 facility type for a
        # Head Start marker -- facility-based institutional care, matching
        # the "Head Start Site" / "GA Head Start" precedents above.
        "Head Start",
        # Nebraska (nrrs.ne.gov) resourceCategoryCounty sub-categories seen in
        # a full run beyond the license-prefix vocabulary: "Provisional Child
        # Care Center" is the provisional-status flavor of "Child Care Center"
        # (cf. the "Provisional Family Child Care Home" -> family_home entries
        # below), "Provisional Preschool" the same for "Preschool" (already
        # above), and "Child Care for Children with Disabilities" is
        # specialized facility-based care -> center (Ryan 2026-09-15).
        "Provisional Child Care Center",
        "Provisional Preschool",
        "Child Care for Children with Disabilities",
        # New Hampshire (NHCIS RecordType): facility-based center care --
        # matches the Wisconsin "Licensed Group" precedent above.
        "Licensed Group Child Care Program",
    ],
    "family_home": [
        "FAMILY DAY CARE HOME",
        "Family Child Care Home",
        "Family Home",
        "FDC",
        "Registered Child-Care Home",
        "Licensed Child-Care Home",
        "Listed Family Home",
        "Family Day Home",
        "Family Day Care Home",
        "Family Child Care Learning Home",
        "Family Child Care",
        "Registered Home",
        "Child Care Licensed Family",
        "Family",
        "Licensed Family Home",
        "Family Home Child Care",
        "Large Family Child Care Home",
        "Unlicensed/Unregistered FDH",
        "System Approved FDH",
        "Child Care Residential Certificate",
        # Maryland codes: FCCH = family child care home, LFCCH = large FCCH.
        "FCCH",
        "LFCCH",
        # Ohio program types: Type A/B are family child care homes.
        "Licensed Type A Family Child Care Home",
        "Licensed Type B Family Child Care Home",
        # North Dakota: family child care in a residence.
        "HHS-Licensed Family Child Care",
        # Alaska (AKCCIS): single-license home care.
        "Licensed Home",
        # Washington DC (mychildcare.dc.gov): CDH = a home-based program.
        "CDH (Child Development Home)",
        # Wisconsin (childcarefinder.wisconsin.gov) Regulation Types: licensed
        # family child care (4-8 children in the provider's home) and county
        # "regularly"/"provisionally" certified small home care (the results
        # list labels certified providers "Certified Family"). The
        # "(Probational)" status variants are handled by the suffix strip in
        # facility_category_from_type.
        "Licensed Family",
        "Regular Certified",
        "Provisional Certified",
        # Vermont (brightfutures.dcf.state.vt.us) detail "License Type": the
        # registered and licensed home-based options.
        "Registered Family Child Care Home",
        "Licensed Family Child Care Home",
        # South Dakota (olapublic.sd.gov) Program Category: registered care
        # provided in the provider's own residence.
        "Family Day Care",
        # Maine: Family Child Care Provider (from detail page) -> family_home.
        "Family Child Care Provider",
        # Iowa (Titan TypeOfCareDesc): all four Registered Child Development
        # Home tiers (A/B/C/C1) are one registration scheme scaling
        # capacity/staffing within the provider's own residence -- the
        # category letter is a size/staffing tier, not a distinct facility
        # type, so all four -> family_home (a deliberate departure from the
        # ND "HHS-Licensed Group Child Care Home" / AK "Licensed Group Home"
        # precedent below -- Iowa does not name a separate group license).
        # provider_type preserves the exact tier. Non-Registered Child Care
        # Home matches the existing "Unlicensed/Unregistered FDH" entry.
        "Registered Child Development Home A",
        "Registered Child Development Home B",
        "Registered Child Development Home C",
        "Registered Child Development Home C1",
        "Non-Registered Child Care Home",
        # Kentucky: "Certified" == a Certified Family Child Care Home, care in
        # the provider's residence (licence numbers are C-prefixed).
        "Certified",
        # Delaware (data.delaware.gov iuzd-3dbt): care in a private
        # residence -- matches the "Family Child Care Home" / "Family Day
        # Home" precedents above.
        "Licensed Family Child Care",
        # Idaho (idahostars.org detail page "Facility Type"): the smallest of
        # Idaho's three facility-size tiers, care in the provider's residence.
        "Family Child Care Facility (up to 6 children)",
        # Mississippi (mdhs.provider.webapps.ms.gov): licensed care in the
        # provider's own residence -- matches the "Family Home" precedents
        # above. Approved Ryan 2026-09-04.
        "Home based Child Care Facility",
        # Nebraska (nrrs.ne.gov) license-prefix / sub-category vocabulary:
        # FI/FII license holders and their unlicensed "Provisional" status
        # variant (the trailing space nrrs.ne.gov emits on "Provisional
        # Family Child Care Home II " is stripped at parse time, in the
        # spider, before this lookup runs).
        "Family Child Care Home I",
        "Family Child Care Home II",
        "Provisional Family Child Care Home I",
        "Provisional Family Child Care Home II",
        # Nebraska (nrrs.ne.gov) sub-category for care delivered in the
        # provider's own home -> family_home (Ryan 2026-09-15).
        "In Home Provider",
        # New Hampshire (NHCIS RecordType): licensed care in the provider's
        # own residence -- matches the Delaware "Licensed Family Child Care"
        # precedent above (NH's exact string carries a trailing "Program").
        "Licensed Family Child Care Program",
        # New Jersey (childcarenj.gov ProgramFacilityType code 2, resolved
        # via bug_07's programTypes lookup table): registered care in the
        # provider's own residence -- a *different* exact string from the
        # existing "Registered Family Child Care Home" (Vermont) entry above
        # ("Provider" vs. "Home"), so it needs its own entry or it would be
        # unmapped -> "other" with a logged warning.
        "Registered Family Child Care Provider",
    ],
    "group_home": [
        "GFDC",
        "Group Home",
        "Group Home Child Care",
        "Group",
        "Group Child Care Home",
        # North Dakota: group child care operated in a home.
        "HHS-Licensed Group Child Care Home",
        # Washington DC: CDX = an "expanded" home (larger home-based care).
        "CDX (Child Development Home Expanded)",
        # Alaska (AKCCIS): matches the north_dakota precedent -- a licensed
        # group operation in a residence -> group_home (not family_home).
        "Licensed Group Home",
        # Delaware: DE's large-family tier (capacity ~12) is the group-home
        # rung -- matches the "Group Child Care Home" / "CDX" precedents
        # above.
        "Licensed Large Family Child Care",
        # Idaho (idahostars.org detail page "Facility Type"): the mid-size
        # tier -- matches the "Group Child Care Home" / "Licensed Group Home"
        # precedents above (capacity band, not a residence, but still
        # smaller/less institutional than the "Large Child Care Center" tier).
        "Group Child Care Facility (up to 12 children)",
        # Wyoming: the middle tier -- a home-based provider licensed for more
        # children than a Family Child Care Home but below a center. Matches
        # the Delaware "Licensed Large Family Child Care" / North Dakota
        # "Group Child Care Home" precedents above.
        "Family Child Care Center",
    ],
    "school_age": [
        "SACC",
        "SCHOOL AGE DAY CARE CENTER",
        "School-age Program",
        "School Age Program",
        "School-age Center",
        "Child Care Out of School Time Program",
        # Ohio program type.
        "Licensed School-Age Child Care",
        # North Dakota.
        "HHS-Licensed School Age Child Care",
        # Vermont (brightfutures.dcf.state.vt.us) detail "License Type".
        "Afterschool Child Care Program",
        # Kansas (khap.kdhe.ks.gov OIDS): licensed for school-age youth
        # programming -- cf. "School Age Program" above (Ryan, 2026-08-19).
        # "School Age Program" itself is already covered by the entry above.
        "Youth Development Program",
    ],
    "exempt": [
        "Exempt Only",
        "Religious Exempt Child Day Center",
        "Exempt Child Care Center",
        "DWS Approved, Exempt Center",
        "DWS Approved, Exempt School Age Program",
        "Child Care Exempt Program",
        "Voluntary Registration",
        # North Dakota: license-exempt self-declared programs.
        "Self-Declared Provider",
        # Indiana (secure.in.gov providersearch): the three "Unlicensed" types.
        # A registered ministry operates under Indiana's religious exemption
        # (cf. "Religious Exempt Child Day Center" / "Voluntary Registration"
        # above); the CCDF-certified center/home types are subsidy-certified but
        # NOT state-licensed (mirrors Alaska's "CCAP Certified/Accredited" ->
        # exempt). The raw provider_type is preserved, so the center/home
        # structure stays recoverable.
        "Unlicensed CCDF Certified Center/School",
        "Unlicensed Registered Ministry",
        "Unlicensed CCDF Certified Home",
        # Alaska (AKCCIS): every subtype of exempt care rolls up to the
        # coarse facilityType "License Exempt"; CCAP-accredited providers
        # participate in the subsidy program without being state-licensed.
        "License Exempt",
        "CCAP Certified/Accredited",
        "Exempt",
        # Iowa (Titan TypeOfCareDesc).
        "Exempt from Licensing",
        # New Hampshire (NHCIS RecordType): license-exempt family care and a
        # license-exempt facility -- exempt wins over the underlying setting,
        # per the existing exempt entries above.
        "License Exempt Family Child Care Program",
        "License Exempt Facility",
        # Missouri (DESE Child Care Search) Facility Type. "6 or Fewer" is a
        # judgment call: a Missouri home caring for no more than six unrelated
        # children is license-exempt, so it follows New Hampshire's "License
        # Exempt Family Child Care Program" above. The raw provider_type is
        # preserved on the item.
        "License Exempt Program",
        "6 or Fewer",
        "Exempt Program",
    ],
    "other": [
        "Other",
        "Resident Camp",
        "Summer Day Camp",
        "Substitute Placement Agency",
        "Family, Friends & Neighbor (FFN) Providers",
        "Neighborhood Youth Organization",
        "(FCC)Nanny Individual",
        # Ohio: camps -> other; in-home aide is informal in-home care -> other.
        "Registered Day Camp or Approved Day Camp",
        "Certified In Home Aide",
        # North Dakota: a provider holding multiple license types (ambiguous
        # category) and tribal subsidy recipients (informal/subsidy).
        "HHS-Licensed Multiple License",
        "Tribal Subsidy Recipient",
        # Alaska (AKCCIS): state-flagged operations without a license --
        # not legitimate exempt care, kept separate from `exempt`.
        "Illegally Unlicensed",
        # Wisconsin: a licensed day camp -> other (matches the day-camp
        # precedent above). "(Probational)" variant handled by the suffix strip.
        "Licensed Camp",
        # South Dakota (olapublic.sd.gov) Program Category: informal FFN-style
        # care and single-family in-home care -- both are informal care per
        # the field-mapping playbook, not a licensed home/center category.
        # Iowa's Titan "In-Home" type is the same concept (care in the
        # child's own home) and maps here too.
        "Informal",
        "In-Home",
        # Kansas (khap.kdhe.ks.gov OIDS): outdoor/day camp program -- matches
        # the "Resident Camp" / "Summer Day Camp" / "Licensed Camp" precedents
        # above.
        "Outdoor Summer Camp",
        # Connecticut (211childcare.org): a licensed youth/day camp -- matches
        # the "Resident Camp" / "Summer Day Camp" / "Licensed Camp" precedents
        # above (license numbers are YCYC-prefixed).
        "Summer Camp/Program",
        # Massachusetts (childcare.mass.gov): license-exempt FFN-style home
        # care -- matches the "Informal" / "In-Home" precedents above.
        "Informal Child Care",
        # Mississippi (mdhs.provider.webapps.ms.gov): a licensed youth camp --
        # matches the "Resident Camp" / "Summer Day Camp" / "Licensed Camp"
        # precedents above. Approved Ryan 2026-09-04.
        "Youth Camp",
        # New Hampshire (NHCIS RecordType): group residential child care, not
        # day care in a private residence -- doesn't fit family_home/group_home.
        "Residential Child Care Program",
    ],
}

# Runtime lookup: trimmed+lower-cased provider_type -> canonical category.
FACILITY_CATEGORY_MAP = {
    raw.strip().lower(): canonical for canonical, raws in FACILITY_CATEGORY_BUCKETS.items() for raw in raws
}


def facility_category_from_type(provider_type):
    """Derive the canonical ``facility_category`` from a raw ``provider_type``.

    Case-insensitive, whitespace-tolerant lookup. Unmapped values become
    ``"other"`` and are **logged** so the table can be extended. Does not
    modify ``provider_type``.

    Wisconsin appends a licence-status modifier -- e.g. "Licensed Group
    (Probational)" -- to its Regulation Type. That status doesn't change the
    facility category, so it is stripped before lookup rather than enumerating
    every base x status combination.
    """
    if not isinstance(provider_type, str):
        return None
    key = re.sub(r"\s*\(probation(?:al|ary)?\)$", "", provider_type.strip().lower())
    category = FACILITY_CATEGORY_MAP.get(key)
    if category is None:
        logger.warning("facility_category_from_type: unmapped provider_type %r -> 'other'", provider_type)
        return "other"
    return category


# --------------------------------------------------------------------------- #
# Task 07 — field collapse (state-specific -> common, additive D2)
# --------------------------------------------------------------------------- #

_AFFIRMATIVE = {"yes", "y", "true", "t", "1"}
_NEGATIVE = {"no", "n", "false", "f", "0", "none", "n/a", "na", "not applicable"}


# Shared with provider_scrape.common_fields (adapters use the same rule).
_is_present = is_present


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


def _scholarships_flag(value):
    """Forward reference to :func:`coerce_scholarships_accepted_flag`, which is
    defined below this map (late binding keeps the map near the top)."""
    return coerce_scholarships_accepted_flag(value)


# common field -> source state-specific fields. Populating the common field
# never removes the source field (D2). Each entry is either a plain source
# field name (copied as-is) or a ``(source, transform)`` tuple, where
# ``transform`` is a ``value -> value`` callable (see common_fields.py; ``None``
# means copy as-is) -- e.g. ``("fl_display_address_on_web", invert)``. The
# transform runs on the one populated source; a ``None`` result leaves the
# common field unset. Multi-source / one-source-to-several-fields cases do not
# belong here: register a per-state adapter in common_fields.py instead.
# Approved set from plan §6; the round-1 cross-state license_begin_date
# merge stays excluded (only NC's current-license effective date feeds it,
# below), and quality ratings are never collapsed (they stay state-specific).
# Keep VPK-specific curriculum (fl_vpk_curriculum) OUT of these lists. Owner/licensee mailing addresses
# (nc_owner_mailing_address, mi_licensee_address) stay out of
# `mailing_address`; they feed `license_holder_address` instead.
FIELD_COLLAPSE_MAP = {
    # Trade name ("doing business as"). FL note: the FL API already returns the
    # DBA as `providerName`, so fl_dba always equals provider_name; mapping it
    # marks provider_name as a trade name rather than adding a new one. WA's
    # license name and MA's umbrella org are different concepts: not mapped.
    "doing_business_as": [
        ("fl_dba", none_if_sentinel),
        ("ok_doing_business_as", none_if_sentinel),
    ],
    # License holder's own address (distinct from the facility's mailing
    # address). The transform cleans it, so it does not depend on the
    # `address` cleanup step. MO's mo_mailing_address is deferred, not mapped.
    "license_holder_address": [
        ("mi_licensee_address", license_holder_address),
        ("nc_owner_mailing_address", license_holder_address),
    ],
    # Weekdays open, as full day names in Monday -> Sunday order. GA uses
    # 2-letter codes, IA 3-letter abbreviations, VT full names.
    "days_of_operation": [
        ("ga_operating_days", day_names),
        ("ia_days_of_operation", day_names),
        ("vt_days_of_operation", day_names),
    ],
    # Subsidy acceptance. WA's "Yes"/None only ever says yes (empty -> unset,
    # never False). NH's checklist is a fallback only: the NH spider sets the
    # field from its detail-page label, which wins; token absent -> unset.
    "scholarships_accepted": [
        ("fl_is_offering_school_readiness", _scholarships_flag),
        ("va_current_subsidy_provider", _scholarships_flag),
        ("wa_subsidy", _scholarships_flag),
        ("nh_financial_assistance", has_list_token("STATE CC ASSISTANCE (SCHOLARSHIP)")),
    ],
    # Start of the CURRENT license term. NC's spider takes licenses[0] as the
    # current license (older ones go to nc_license_history), so its effective
    # date is the term start. Other states' license_begin_date are untouched.
    "license_begin_date": [
        ("nc_license_effective_date", to_iso_date),
    ],
    # The state system's own record key, one source per state. Opaque and
    # state-specific. AK uses facility_gen_id (the key in its profile and
    # inspection URLs), not ak_facility_number; NV has no fallback to
    # nv_license_base and WI does not use wi_location_number. VT is excluded
    # on purpose: vt_provider_id equals license_number in every row and has
    # duplicates, so it adds nothing.
    "source_provider_id": [
        ("ak_facility_gen_id", to_id_string),
        ("az_facility_id", to_id_string),
        ("ct_provider_id", to_id_string),
        ("hi_service_id", to_id_string),
        ("il_provider_id", to_id_string),
        ("in_provider_id", to_id_string),
        ("ky_provider_id", to_id_string),
        ("la_unique_id", to_id_string),
        ("ma_program_number", to_id_string),
        ("ms_facility_id", to_id_string),
        ("ne_resource_id", to_id_string),
        ("nh_account_id", to_id_string),
        ("nj_unique_program_id", to_id_string),
        ("nv_operation_id", to_id_string),
        ("ny_facility_id", to_id_string),
        ("nd_program_id", to_id_string),
        ("sc_provider_id", to_id_string),
        ("tn_provider_id", to_id_string),
        ("tx_operation_id", to_id_string),
        ("va_ID", to_id_string),
        ("wa_provider_id", to_id_string),
        ("wi_facility_number", to_id_string),
        ("wy_facility_id", to_id_string),
    ],
    "license_type": [
        "va_license_type",
        "mt_license_type",
        "ut_license_type",
        "co_license_type",
        "az_license_type",
        "nc_license_type",
        # bug_07: source renamed from the raw nj_license_type to the resolved
        # nj_license_type_label (int code -> human label; see new_jersey.py
        # and tasks/common_fields_epic/bug_07_nj_lookups.json). nj_license_type
        # itself is untouched (D2) and still carries the raw code.
        "nj_license_type_label",
        "wv_license_type",
        "wa_license_type",
        "hi_license_type",
        "ak_license_type",
        "ct_license_type",
    ],
    "school_district": [
        "co_school_district",
        "ny_school_district_name",
        "pa_school_district",
        "ut_school_district",
        "wa_school_district",
        "ct_school_districts",
    ],
    "mailing_address": [
        "al_mailing_address",
        "ga_mailing_address",
        "hi_mailing_address",
    ],
    "accreditation": [
        "al_accreditations",
        "ga_accreditation",
        "hi_accreditations",
        # bug_07: renamed from raw nj_accreditation to resolved labels.
        "nj_accreditation_labels",
        "md_accreditation",
        "ct_accreditations",
    ],
    "meals": [
        "ut_meals",
        "ga_meals",
        "hi_meals",
        "nm_meals",
        # bug_07: renamed from raw nj_meal_options to resolved labels.
        "nj_meal_options_labels",
        "pa_meal_options",
    ],
    "accepting_new_children": [
        "co_accepting_new_children",
        "ga_accepting_new_children",
        "ct_accepting_referrals",
    ],
    # bug_07: nj_transportation renamed to resolved nj_transportation_labels.
    "transportation": ["ga_transportation", "nj_transportation_labels", "ct_transportation"],
    # Every head_start source is coerced to a boolean.
    "head_start": [
        (source, _coerce_bool)
        for source in (
            "co_head_start",
            "az_headstart",
            "wa_head_start",
            "ri_head_start",
            "fl_is_head_start",
            "ct_head_start",
            "nh_head_start",
        )
    ],
    # Early Head Start is a distinct program from head_start: its sources are
    # never added to the head_start list above.
    "early_head_start": [
        (source, _coerce_bool)
        for source in (
            "ms_early_head_start",
            "nh_early_head_start",
            "wa_early_head_start",
        )
    ],
    # CT stores real bools, TN Yes/No text. NH lists it as one token in a
    # comma-separated environment checklist: token present -> True, absent ->
    # None (a checklist omission is not a confirmed "no").
    "wheelchair_accessible": [
        ("ct_wheelchair_accessible", _coerce_bool),
        ("tn_wheelchair_accessible", _coerce_bool),
        ("nh_environment", has_list_token("WHEELCHAIR ACCESSIBLE")),
    ],
    # True = the state deliberately withheld it. FL publishes the opposite
    # ("display on web"), so it is inverted. MA's single "contact redacted" flag
    # feeds both fields. NY (blank -> False) is an adapter in common_fields.py.
    "address_suppressed": [
        ("ct_address_suppressed", to_bool),
        ("ks_address_suppressed", to_bool),
        ("fl_display_address_on_web", invert),
        ("ma_contact_redacted", to_bool),
    ],
    "phone_suppressed": [
        ("fl_display_phone_on_web", invert),
        ("ma_contact_redacted", to_bool),
    ],
    # Extension that goes with `phone`. NY's input mask leaves underscore padding
    # (`11202_____`); both states carry stray words, which become None.
    "phone_extension": [
        ("nj_phone_extension", phone_extension_digits),
        ("ny_phone_extension", phone_extension_digits),
    ],
    "curriculum": ["ga_curriculum", "nj_curriculum"],
    # Assigned licensing specialist. VA (va_inspector packs name +
    # phone) is an adapter in common_fields.py. ma_licensor is the provider-level
    # field only; the inspection-level one is the inspector field.
    "licensing_specialist": [
        ("ak_licensing_specialist", person_name),
        ("ma_licensor", person_name),
        ("me_licensing_specialist", person_name),
        ("ok_licensing_specialist", person_name),
        ("sc_licensing_specialist_name", last_first_name),  # "Last, First" -> "First Last"
        ("tn_regulatory_individual", person_name),
        ("wv_licensing_specialist", person_name),  # ALL-CAPS -> Title Case
    ],
    "licensing_specialist_phone": [
        ("ok_licensing_specialist_phone", none_if_sentinel),
        ("sc_licensing_specialist_phone", none_if_sentinel),
    ],
    "licensing_specialist_email": [("me_licensing_specialist_email", lower_email)],
    # Open slots per age band (the state's own band definitions).
    # IA, ND and DC are adapters in common_fields.py (one list -> four fields).
    "vacancies_infant": [
        ("co_infant_openings", to_int),
        ("me_infant_slots", to_int),
        ("nh_infant_openings", to_int),
        ("vt_infant_vacancies", to_int),
    ],
    "vacancies_toddler": [
        ("co_toddler_openings", to_int),
        ("me_toddler_slots", to_int),
        ("nh_toddler_openings", to_int),
        ("vt_toddler_vacancies", to_int),
    ],
    "vacancies_preschool": [
        ("co_preschool_openings", to_int),
        ("me_preschool_slots", to_int),
        ("nh_preschool_openings", to_int),
        ("vt_preschool_vacancies", to_int),
    ],
    "vacancies_school_age": [
        ("co_school_age_openings", to_int),
        ("me_school_age_slots", to_int),
        ("nh_school_age_openings", to_int),
        ("vt_school_age_vacancies", to_int),
    ],
    # Total open slots as reported (never summed from vacancies_*).
    # ID's id_openings_available is a yes/no ("Are there openings available"),
    # deliberately NOT mapped. WI's prose is parsed by wi_total_vacancies.
    "total_vacancies": [
        ("ia_total_openings", to_int),
        ("id_number_of_openings", to_int),
        ("nd_total_vacancies", to_int),
        ("tn_vacancy", to_int),
        ("ut_vacancies", to_int),
        ("vt_current_vacancy", to_int),
        ("wa_available_slots", to_int),
        ("wi_vacancies", wi_total_vacancies),
    ],
    # "As of" date of the provider's self-reported openings (ISO). ME/ND are
    # already ISO (to_iso_date is idempotent); WI's date is inside the prose.
    "vacancies_updated_date": [
        ("ia_openings_as_of", to_iso_date),
        ("me_openings_updated", to_iso_date),
        ("nd_vacancies_updated", to_iso_date),
        ("vt_vacancy_as_of", to_iso_date),
        ("wi_vacancies", wi_vacancies_updated_date),
    ],
}
# NOTE Most states set `scholarships_accepted` directly in the spider.
# The few xx_* sources mapped above are only fallbacks -- the collapse never
# overwrites a spider-set value (including False). The final bool-or-None
# coercion still lives at the end of normalize_item (step 9), after this
# collapse step runs, via coerce_scholarships_accepted_flag. Per-source
# transforms for this field must use coerce_scholarships_accepted_flag (via
# _scholarships_flag), NOT _coerce_bool, since plain _coerce_bool would misread
# "Not Accepted"-shaped text as True. OK's ok_subsidy_contract_number is deliberately not mapped: the
# OK spider sets the field from the state's explicit isSubsidyAccepted flag.

# Same shape as FIELD_COLLAPSE_MAP, for inspection rows (normalize_inspection).
# Each common inspection field lists its state-specific sources; as with the
# provider map, exactly one source may be populated on a row.
INSPECTION_FIELD_COLLAPSE_MAP = {
    # Staff member who ran the visit, as `First Last`. MA has two sources:
    # ma_licensor on routine visits and ma_investigator on investigation rows;
    # they are never both filled, but if a row did carry both the collapse
    # leaves `inspector` unset rather than guessing. MO's specialist is either
    # ALL CAPS (`CHARMEL WILLIAMS`) or `LAST, FIRST M`, so it is flipped.
    "inspector": [
        ("ak_licensing_specialist", person_name),
        ("ma_licensor", person_name),
        ("ma_investigator", person_name),
        ("mo_specialist", last_first_name),
        ("mt_inspector_name", person_name),
        ("nh_licensor", person_name),
        ("ri_licensor", person_name),
    ],
    # The state's own key for the visit/report, as an opaque string via
    # to_id_string (ints stringified with no `.0`, strings only trimmed). MA has
    # two sources, never both filled: ma_visit_id on routine visits and
    # ma_investigation_id on investigation rows (different Salesforce objects).
    # KS maps ks_nosf_id only: ks_survey_number repeats across rows, so it is
    # not a row key.
    "inspection_id": [
        ("ct_inspection_id", to_id_string),
        ("fl_inspection_id", to_id_string),
        ("hi_visit_id", to_id_string),
        ("ky_inspection_id", to_id_string),
        ("ma_visit_id", to_id_string),
        ("ma_investigation_id", to_id_string),
        ("mo_inspection_id", to_id_string),
        ("nh_visit_id", to_id_string),
        ("nv_inspection_number", to_id_string),
        ("ks_nosf_id", to_id_string),
        ("az_enforcement_name", to_id_string),
    ],
    # Announced (True) / unannounced (False) visit. Dedicated source fields only;
    # NC, CT and RI encode it in the common `type` and are inspection-level
    # adapters in common_fields.py. announced_flag, not a bool coercion: the
    # text "Unannounced" must read False, and "N/A" / no signal stays unset.
    "announced": [
        ("ak_visit_type", announced_flag),
        ("ma_announcement_type", announced_flag),
        ("nh_announcement_type", announced_flag),
        ("mo_notice", announced_flag),
    ],
}


def split_collapse_entry(entry):
    """Normalize a collapse-map entry to ``(source_field, transform_or_None)``."""
    if isinstance(entry, str):
        return entry, None
    source, transform = entry
    return source, transform


def collapse_state_fields(item: dict, collapse_map: dict | None = None) -> dict:
    """Populate common fields from their single populated source field.

    ``collapse_map`` defaults to ``FIELD_COLLAPSE_MAP``; inspections pass
    ``INSPECTION_FIELD_COLLAPSE_MAP``.

    Rules (plan §6, D2):
      - Never overwrite an already-populated common field.
      - Copy only when **exactly one** source field is populated (never guess).
      - Keep the source field in place (additive).
      - If the map entry names a transform, it is applied to the source value;
        a transform result with no signal (``None``) leaves the field unset.
    """
    if collapse_map is None:
        collapse_map = FIELD_COLLAPSE_MAP
    for common, entries in collapse_map.items():
        if _is_present(item.get(common)):
            continue
        present = [(src, tf) for src, tf in map(split_collapse_entry, entries) if _is_present(item.get(src))]
        if len(present) != 1:
            continue
        source, transform = present[0]
        value = item[source]
        if transform is not None:
            value = transform(value)
            if not _is_present(value):
                continue
        item[common] = value
    return item


# --------------------------------------------------------------------------- #
# Task 08 — address cleanup (in place) + component parse (additive)
# --------------------------------------------------------------------------- #

# Valid USPS 2-letter codes (50 states + DC), used to validate a parsed state.
US_STATE_CODES = {
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "DC",
}

# Spelled-out state name -> USPS code. Matched longest-first so "West Virginia"
# wins over "Virginia".
SPELLED_STATE_TO_USPS = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
    "district of columbia": "DC",
}

# Spelled state names sorted longest-first for greedy, correct matching.
_SPELLED_STATES_BY_LEN = sorted(SPELLED_STATE_TO_USPS.items(), key=lambda kv: len(kv[0]), reverse=True)

_COUNTRY_SUFFIX_RE = re.compile(r",?\s*(United States(?: of America)?|U\.?S\.?A\.?)\s*$", re.IGNORECASE)
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
            prefix = before_zip[: match.start()].strip().rstrip(",").strip()
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
        logger.warning("parse_address_components: no ZIP in %r (unparsed)", value)
        return (None, None, None)
    zip_code = zip_match.group(1)
    before_zip = text[: zip_match.start()].strip().rstrip(",").strip()
    state, prefix = _extract_state(before_zip)
    if state is None:
        logger.warning("parse_address_components: no recognizable state in %r (unparsed)", value)
        return (None, None, None)
    city = _extract_city(prefix)
    return (city, state, zip_code)


# --------------------------------------------------------------------------- #
# Task 09 — languages: order-preserving de-dupe + ALL CAPS -> Title Case
# (bug_04: Illinois builds a 3-column ``languages`` list with no de-dupe,
# giving ``["ENGLISH", "ENGLISH"]`` on 66% of records)
# --------------------------------------------------------------------------- #

LANGUAGES_FIELD = "languages"

# Acronyms kept uppercase when title-casing an ALL-CAPS ``languages`` entry
# (c.f. NAME_PRESERVE_UPPER for provider names). A full hf_sample survey
# (2026-10-15) found no list-type state emitting one of these today -- CT/MA/NJ
# already emit mixed-case entries like "American Sign Language (ASL)", which
# is left alone by the isupper() guard below regardless -- but they're common,
# recurring language-field acronyms and cheap to guard against before a state
# emits one in ALL CAPS.
LANGUAGE_PRESERVE_UPPER = {"ASL", "ESL"}


def title_case_language(value):
    """Convert an ALL-CAPS-looking ``languages`` entry to Title Case.

    Mirrors :func:`title_case_name`: only acts when the whole string is
    upper-case (``str.isupper()``), using the languages-specific
    ``LANGUAGE_PRESERVE_UPPER`` acronym allowlist in place of corporate
    suffixes. Already mixed-case entries (``"American Sign Language (ASL)"``,
    ``"Punjabi (Gurmukhi)"``) are intentional as emitted and left untouched.
    """
    if not isinstance(value, str) or not value.isupper():
        return value
    return " ".join(_title_token(t, LANGUAGE_PRESERVE_UPPER) for t in value.split())


def normalize_languages(value):
    """De-dupe and case-normalize a ``languages`` value.

    - **list** (CT, HI, IL, MA, NJ): order-preserving de-dupe, ignoring case
      and surrounding whitespace -- the *first* occurrence of a value wins, so
      a later differently-cased repeat (``["ENGLISH", "English"]``) is
      dropped rather than replacing the earlier one. Empty/whitespace-only
      entries are dropped. Each kept entry is then title-cased if (and only
      if) it is entirely upper-case (bug_04: ``"ENGLISH"`` -> ``"English"``),
      via :func:`title_case_language`. An empty result becomes ``None``.
    - **str** (AZ, CO, GA, IA, NE, NM, ND, RI, VT, WA, DC): left unchanged.
      Roughly a dozen states emit a single delimited string instead of a list
      (``"English, Spanish"``, ``"English (Spoken by Staff)"``), each with its
      own separator and quirks (GA's per-language annotations, NM's boilerplate
      sentence, WA's hyphen-joined "Chinese-Mandarin"). Splitting those
      generally risks mangling a value like "Chinese (Mandarin)" or
      "French/ Arabic", so it is intentionally out of scope here.
    - anything else: returned unchanged.
    """
    if not isinstance(value, list):
        return value
    seen = set()
    deduped = []
    for entry in value:
        if not isinstance(entry, str):
            # Not expected in practice (languages entries are always strings),
            # but de-dupe defensively rather than raising.
            if entry not in seen:
                seen.add(entry)
                deduped.append(entry)
            continue
        cleaned = entry.strip()
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(title_case_language(cleaned))
    return deduped or None


# --------------------------------------------------------------------------- #
# Task 10 — age-served flags: infant/toddler/preschool/school -> bool-or-None
# (bug_05: NY emits per-age capacity count strings, VT emits per-age capacity
# ints, TX emits "Y"/"N" (+ a stray "1"/"0"), GA emits "Yes"/absent into these
# four common fields instead of a plain boolean)
# --------------------------------------------------------------------------- #

# The four age-served common fields, coerced to bool-or-None by this step.
# NY/VT's raw per-age *capacity counts* (confirmed from source: NY's CSV
# columns are literally "Infant Capacity"/"Toddler Capacity"/.../"School Age
# Capacity"; VT's detail page groups field_infant4/etc. under the same
# "capacity" section as field_totcap4 -> `capacity`, distinct from the
# separate field_ivac4/etc. "vacancy" section already emitted as
# vt_infant_vacancies/etc.) are preserved by the *spiders* in
# ny_capacity_{age}/vt_{age}_capacity fields before this step runs — see
# new_york.py.parse and vermont.py.parse_detail. That preservation lives in
# the spiders (state-specific raw data), not here, per the project's
# prefer-pipeline-fix rule for *shared* logic: this coercion is shared across
# every state, but which raw field to preserve it in is a per-state naming
# decision the spider already owns (ny_capacity_description, vt_*_vacancies).
AGE_SERVED_FLAG_FIELDS = ("infant", "toddler", "preschool", "school")

# _GROUPED_INTEGER_RE (thousands-grouped count, e.g. NY's "1,100" school-age
# capacity) is defined in Task 05 above, shared with normalize_deficiencies.


def _coerce_strict_bool_flag(value, *, func_name, extra_affirmative=frozenset(), extra_negative=frozenset()):
    """Shared body for the strict bool-or-None flag coercers below.

    Unlike ``_coerce_bool`` (used for ``head_start``'s field collapse), this
    coercer has **no** "any other non-empty text is affirmative" fallback:
    an unrecognized token always becomes ``None`` (logged at DEBUG), never a
    guessed ``True``. ``extra_affirmative``/``extra_negative`` let a caller
    add field-specific vocabulary (e.g. NM's "Accepted"/"Not Accepted" for
    ``scholarships_accepted``) on top of the shared ``_AFFIRMATIVE``/
    ``_NEGATIVE`` token sets, with negative tokens always checked first so a
    negative phrase can never fall through to True.

    ``func_name`` is only used to label the DEBUG log lines (each public
    wrapper passes its own name) -- the two log messages below must keep the
    literal substrings "unparseable flag" / "unexpected type for flag" that
    existing tests assert on.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value > 0
    if isinstance(value, str):
        token = value.strip().lower()
        if token == "":
            return None
        if token in _NEGATIVE or token in extra_negative:
            return False
        if token in _AFFIRMATIVE or token in extra_affirmative:
            return True
        digits = token.replace(",", "") if _GROUPED_INTEGER_RE.fullmatch(token) else token
        if _INTEGER_RE.fullmatch(digits):
            return int(digits) > 0
        logger.debug("%s: unparseable flag %r -> None", func_name, value)
        return None
    logger.debug("%s: unexpected type for flag %r -> None", func_name, value)
    return None


def coerce_age_served_flag(value):
    """Coerce one age-served flag (infant/toddler/preschool/school) to bool-or-None.

    States emit a mix of shapes for these four fields (appendix D1 / bug_05):

    - already a plain ``bool`` -- returned as-is (most states).
    - a non-bool ``int``/``float`` count (VT) -- ``> 0`` is ``True``, ``0`` is
      ``False``. (VT's raw count is separately preserved as
      ``vt_{age}_capacity`` by the spider; see module note above.)
    - a clean digit-string count (NY, e.g. ``"8"``, ``"0"``) -- the same
      ``int(...) > 0`` rule. (NY's raw count is separately preserved as
      ``ny_capacity_{age}``.)
    - ``"Y"``/``"N"`` (TX) and ``"Yes"`` (GA) -- the obvious bool reading, via
      the existing ``_AFFIRMATIVE``/``_NEGATIVE`` token sets (Task 07). TX's
      two stray ``"1"`` values and one ``"0"`` are *also* covered by those same
      sets (``"1"`` is affirmative, ``"0"`` is negative) -- which happens to
      agree with the "count > 0" rule above, so there's no conflict between
      the token vocabulary and the numeric reading.
    - a recognized negative-ish token (``"No"``, ``"false"``, ``"0"``,
      ...) -- ``False``, *never* ``True``: a negative token is
      checked, and matched, **before** any "any other text is affirmative"
      fallback exists at all (unlike ``_coerce_bool``, this function has no
      such fallback -- see the final ``else`` below).
    - a thousands-grouped count (NY's ``"1,100"``/``"1,305"``) -- commas
      stripped, then the same ``> 0`` rule.
    - ``None`` or an empty string -- ``None`` (no signal).
    - anything else unparseable -- becomes ``None`` and is **logged at
      DEBUG**. We never guess
      ``True`` for a token we don't recognize.

    The coercion itself is shared with ``coerce_scholarships_accepted_flag``
    (bug_06) via ``_coerce_strict_bool_flag``; this wrapper adds no extra
    tokens.
    """
    return _coerce_strict_bool_flag(value, func_name="coerce_age_served_flag")


# Extra vocabulary for scholarships_accepted (bug_06): NM emits "Accepted"/
# "Not Accepted" rather than the "Yes"/"No" the other nine affected states
# use. "Not Accepted" is an explicit negative token -- without it, the
# generic "any other non-empty text is affirmative" reading (the one
# `_coerce_bool` uses) would misread it as True, which is the bug.
_SCHOLARSHIPS_ACCEPTED_EXTRA_AFFIRMATIVE = {"accepted"}
_SCHOLARSHIPS_ACCEPTED_EXTRA_NEGATIVE = {"not accepted"}


def coerce_scholarships_accepted_flag(value):
    """Coerce ``scholarships_accepted`` to bool-or-None (bug_06).

    States emit three shapes for this field (appendix D2):

    - already a plain ``bool`` (most states, e.g. CT, DE, IN, IA, KY, LA, ME,
      MA, MS, NE, NC, ND) -- returned as-is.
    - ``"Yes"``/``"No"`` (AK, CO, GA, RI, TN, TX, UT, VT, DC) -- via the
      shared ``_AFFIRMATIVE``/``_NEGATIVE`` token sets, same as
      ``coerce_age_served_flag``.
    - ``"Accepted"``/``"Not Accepted"`` (NM) -- NM's own vocabulary, added via
      ``extra_affirmative``/``extra_negative``. ``"Not Accepted"`` is checked
      as a negative token *before* any affirmative fallback, so it can never
      become ``True``.
    - ``None`` or an empty string -- ``None`` (no signal; e.g. VT leaves this
      unset for ~23% of records).
    - anything else unrecognized -- ``None``, logged at DEBUG. Never guessed
      as ``True``.

    This is the identical strict bool-or-None rule as
    ``coerce_age_served_flag`` (Task 10 / bug_05) -- see
    ``_coerce_strict_bool_flag`` -- with NM's two extra tokens layered on.
    """
    return _coerce_strict_bool_flag(
        value,
        func_name="coerce_scholarships_accepted_flag",
        extra_affirmative=_SCHOLARSHIPS_ACCEPTED_EXTRA_AFFIRMATIVE,
        extra_negative=_SCHOLARSHIPS_ACCEPTED_EXTRA_NEGATIVE,
    )


def normalize_item(item: dict, state: str) -> dict:
    """Apply all normalization steps to one provider item, in order.

    ``state`` is ``spider.name`` (e.g. ``"montana"``) and is the key into any
    per-state mapping tables. Mutates and returns the same ``dict``.

    The processing order (filled in by later tasks) is:
      1. whitespace / string hygiene
      2. field collapse (state-specific -> common)
      3. date normalization
      4. numeric / type normalization
      5. languages de-dupe + casing (bug_04)
      6. controlled vocabulary (status, facility_category)
      7. address cleanup + component parse (also cleans ``mailing_address``,
         bug_09)
      8. age-served flags -> bool-or-None (bug_05)
      9. scholarships_accepted -> bool-or-None (bug_06)
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
    #    Per-state adapters (common_fields.py) run right after the map.
    collapse_state_fields(item)
    run_adapters(item, state)

    # 3. date normalization -> ISO 8601.
    for field in DATE_FIELDS:
        if item.get(field) is not None:
            item[field] = normalize_date(item[field])

    # 4. numeric / type normalization.
    if item.get(CAPACITY_FIELD) is not None:
        item[CAPACITY_FIELD] = normalize_capacity(item[CAPACITY_FIELD])
    if item.get(AGES_SERVED_FIELD) is not None:
        item[AGES_SERVED_FIELD] = normalize_ages_served(item[AGES_SERVED_FIELD])
    if item.get(DEFICIENCIES_FIELD) is not None:
        item[DEFICIENCIES_FIELD] = normalize_deficiencies(item[DEFICIENCIES_FIELD])
    for coord in COORDINATE_FIELDS:
        if item.get(coord) is not None:
            item[coord] = normalize_coordinate(item[coord])

    # 5. languages (bug_04): order-preserving de-dupe + ALL CAPS -> Title Case
    #    on list-type values only; a delimited string is left untouched (see
    #    normalize_languages docstring).
    if item.get(LANGUAGES_FIELD) is not None:
        item[LANGUAGES_FIELD] = normalize_languages(item[LANGUAGES_FIELD])

    # 6. controlled vocabulary. status is replaced in place (D4); the
    #    facility_category facet is derived additively from provider_type (D2)
    #    and provider_type itself is left untouched.
    if item.get("status") is not None:
        item["status"] = canonical_status(item["status"])
    if item.get("provider_type") is not None:
        item["facility_category"] = facility_category_from_type(item["provider_type"])

    # 7. address cleanup (in place, D1) + best-effort component parse (additive,
    #    D2). Components are only set when clearly parsed and not already set.
    #    When the scraper already supplied all of city/state/zip (e.g. from
    #    structured source fields), skip the parse entirely: it can add nothing
    #    and would otherwise log a spurious "no ZIP" warning for a street-only
    #    `address`.
    if item.get("address") is not None:
        item["address"] = clean_address(item["address"])
        already_have_components = all(_is_present(item.get(key)) for key in ("city", "state", "zip"))
        if item.get("address") and not already_have_components:
            city, parsed_state, zip_code = parse_address_components(item["address"])
            for key, parsed in (("city", city), ("state", parsed_state), ("zip", zip_code)):
                if parsed is not None and not _is_present(item.get(key)):
                    item[key] = parsed

    # bug_09: `mailing_address` gets the same whitespace/punctuation cleanup
    # as `address` (trailing "United States"/"USA", comma spacing), but never
    # feeds `parse_address_components` -- city/state/zip are the provider's
    # physical location, not its mailing address, and the two can legitimately
    # differ (a PO box, a different city, etc.).
    if item.get("mailing_address") is not None:
        item["mailing_address"] = clean_address(item["mailing_address"])

    # 8. age-served flags (bug_05): coerce infant/toddler/preschool/school to
    #    bool-or-None. Independent of the steps above (it only ever touches
    #    these four fields), so it can safely run last.
    for field in AGE_SERVED_FLAG_FIELDS:
        if item.get(field) is not None:
            item[field] = coerce_age_served_flag(item[field])

    # 9. scholarships_accepted (bug_06): coerce bool / "Yes"-"No" /
    #    "Accepted"-"Not Accepted" to bool-or-None. Not a FIELD_COLLAPSE_MAP
    #    target today (every state that fills it does so directly in the
    #    spider, not via collapse of an xx_* source field), so this just
    #    needs to run after collapse (step 2) -- which it does, being last.
    if item.get("scholarships_accepted") is not None:
        item["scholarships_accepted"] = coerce_scholarships_accepted_flag(item["scholarships_accepted"])

    return item


def normalize_inspection(inspection: dict, state: str) -> dict:
    """Normalize one inspection entry (whitespace, field collapse, dates).

    Field collapse (``INSPECTION_FIELD_COLLAPSE_MAP`` + inspection-level
    adapters) is additive and runs before date normalization, mirroring
    ``normalize_item``.

    The ``status`` controlled vocabulary deliberately does **not** apply to
    inspections. Mutates and returns the same ``dict``.
    """
    for key, value in list(inspection.items()):
        if isinstance(value, (str, list)):
            inspection[key] = clean_whitespace(value)
    collapse_state_fields(inspection, INSPECTION_FIELD_COLLAPSE_MAP)
    run_adapters(inspection, state, INSPECTION_LEVEL)
    for field in INSPECTION_DATE_FIELDS:
        if inspection.get(field) is not None:
            inspection[field] = normalize_date(inspection[field])
    return inspection

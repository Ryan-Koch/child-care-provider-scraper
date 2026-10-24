"""Reusable building blocks for the common-field collapse (epic: common fields, round 2).

Two things live here:

1. A small **transform library** -- pure ``value -> value`` functions (and
   factories that return them) used per source in ``FIELD_COLLAPSE_MAP`` /
   ``INSPECTION_FIELD_COLLAPSE_MAP`` (``normalization.py``), e.g.
   ``("fl_display_address_on_web", invert)``. A transform returns ``None`` for
   "no usable value"; the collapse then leaves the common field unset.

2. A per-state **adapter registry** for what a one-to-one map entry cannot
   express: one source feeding several common fields (VA ``va_inspector`` ->
   name + phone) or several sources feeding one field. Adapters run *after*
   the map collapse and obey the same rules: additive (sources are kept) and
   they never overwrite an already-populated common field.

Conventions (epic README): ``0`` and ``False`` are real values, ``None`` means
"not reported"; sentinel text such as ``N/A`` becomes ``None``.

This module deliberately has no import-time dependency on ``normalization``
(which imports it); ``to_iso_date`` imports ``normalize_date`` lazily.
"""

import logging
import re
from collections.abc import Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)


def is_present(value):
    """True if a value carries signal. ``False``/``0`` count as present (a state
    explicitly saying "no"); ``None``/``""``/``[]`` do not."""
    return value is not None and value != "" and value != []


# --------------------------------------------------------------------------- #
# Transform library
# --------------------------------------------------------------------------- #

# Lower-cased text that means "the state reported nothing". NOT included:
# "$0.00" / "0" -- those are real zeros (README: 0 is a real value).
SENTINELS = {
    "",
    "-",
    "--",
    "n/a",
    "na",
    "none",
    "null",
    "none reported",
    "none reported.",
    "not reported",
    "not applicable",
    "unknown",
    "tbd",
    "no data available",  # DC enrollment table
}

_TRUE_TOKENS = {"yes", "y", "true", "t", "1"}
_FALSE_TOKENS = {"no", "n", "false", "f", "0"}


def _is_sentinel(value):
    return isinstance(value, str) and value.strip().lower() in SENTINELS


def describe_transform(transform):
    """Short human label for docs (``invert``, ``vocab(3 entries)``...)."""
    if transform is None:
        return ""
    return getattr(transform, "label", None) or transform.__name__.lstrip("_")


def _to_flag(value):
    """Strict bool-or-None: real bools and Yes/No-style tokens only."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        token = value.strip().lower()
        if token in _TRUE_TOKENS:
            return True
        if token in _FALSE_TOKENS:
            return False
    return None


def invert(value):
    """Flip a boolean source into the positive sense of the common field.

    Accepts bools and Yes/No-style text (``"No"`` -> ``True``). Sentinels and
    unrecognised text -> ``None`` (never guessed; unrecognised text is logged).
    """
    if _is_sentinel(value) or value is None:
        return None
    flag = _to_flag(value)
    if flag is None:
        logger.warning("invert: unrecognised boolean %r -> None", value)
        return None
    return not flag


def to_bool(value):
    """Strict bool copy: real bools and Yes/No-style text; blanks/sentinels -> ``None``.

    Unlike the collapse module's lenient coercer, unrecognised text is logged and
    left unset rather than read as ``True``. ``False`` is a real value.
    """
    if _is_sentinel(value) or value is None:
        return None
    flag = _to_flag(value)
    if flag is None:
        logger.warning("to_bool: unrecognised boolean %r -> None", value)
    return flag


def phone_extension_digits(value):
    """Phone extension as a digit string; anything else -> ``None``.

    Trims whitespace and strips the underscore padding left by an input mask
    (``11202_____`` -> ``11202``). The remainder must be all digits and is kept
    as text so leading zeros survive. Digits are never fished out of mixed text
    (``Salas``, ``NJ``, ``class`` -> ``None``, logged); blanks, sentinels and a
    mask with nothing typed in it (``__________``) give ``None`` silently.
    """
    if value is None or _is_sentinel(value):
        return None
    cleaned = str(value).strip().strip("_").strip()
    if not cleaned:
        return None
    if not (cleaned.isascii() and cleaned.isdigit()):
        logger.warning("phone_extension_digits: non-numeric extension %r -> None", value)
        return None
    return cleaned


def _parse_number(value):
    """Shared parse for to_int/to_float: returns a ``float`` or ``None``.

    Strips ``$`` and thousands commas; sentinels -> ``None``; bools and
    unparseable text -> ``None`` (unparseable text is logged).
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str) or _is_sentinel(value):
        return None
    cleaned = value.strip().replace("$", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        logger.warning("to_int/to_float: could not parse number %r -> None", value)
        return None


def to_int(value):
    """Coerce to ``int``. ``"12"``, ``"1,200"``, ``"$5.00"`` and ``12.0`` work;
    sentinels (``None Reported.``, ``N/A``) -> ``None``; a non-integral number
    (``"3.5"``) -> ``None`` and is logged rather than truncated."""
    number = _parse_number(value)
    if number is None:
        return None
    if number != int(number):
        logger.warning("to_int: non-integral value %r -> None", value)
        return None
    return int(number)


def to_float(value):
    """Coerce to ``float``. Same input handling as ``to_int``; ``"$0.00"`` is a
    real ``0.0``, not a sentinel."""
    return _parse_number(value)


def vocab(table, name=None):
    """Build a transform mapping raw values through ``table`` (like STATUS_MAP).

    Lookup is case-insensitive and whitespace-tolerant. A miss is **logged** so
    the table can be extended, and yields ``None`` (the common field stays
    unset rather than carrying an unmapped raw value). Sentinels -> ``None``
    silently.
    """
    lookup = {str(k).strip().lower(): v for k, v in table.items()}

    def transform(value):
        if value is None or _is_sentinel(value):
            return None
        key = value.strip().lower() if isinstance(value, str) else str(value).strip().lower()
        if key not in lookup:
            logger.warning("vocab%s: unmapped value %r -> None", f" {name}" if name else "", value)
            return None
        return lookup[key]

    transform.label = f"vocab({name or f'{len(lookup)} entries'})"
    return transform


# "Unannounced" must be tested first: it contains "announced". The optional
# letters tolerate the typos seen in Rhode Island data ("Uannounced",
# "Unannounce", ...), where a dropped "n" would otherwise read as announced.
_UNANNOUNCED_RE = re.compile(r"un?n?announc", re.IGNORECASE)
_ANNOUNCED_RE = re.compile(r"announc", re.IGNORECASE)


def announced_flag(value):
    """Classify free text as announced (``True``) / unannounced (``False``) / unknown (``None``).

    Case-insensitive substring match, unannounced first (typo tolerant). Text
    naming neither (e.g. "Follow-Up Inspection") and sentinels such as ``N/A``
    -> ``None``. Deliberately not a Yes/No bool coercion.
    """
    if not isinstance(value, str) or _is_sentinel(value):
        return None
    if _UNANNOUNCED_RE.search(value):
        return False
    if _ANNOUNCED_RE.search(value):
        return True
    return None


# Maryland writes one inspection row per finding. The only "clean" text is this
# sentinel; every other narrative cites a regulation and so is a violation
# (including "Specialist observed no evidence of ..." -- deliberately NOT
# matched on the words "no evidence").
MD_NO_NONCOMPLIANCE_TEXT = "no noncompliances found"


def md_finding_flag(value):
    """Classify a Maryland finding as violation (``True``) / clean (``False``) / unknown (``None``).

    ``No Noncompliances Found`` (case-insensitive) -> ``False``; any other
    narrative -> ``True``. Blank, sentinel and digit-only text (a leaked grid
    pager cell) -> ``None``.
    """
    if not isinstance(value, str) or _is_sentinel(value):
        return None
    text = value.strip()
    if not text or text.isdigit():
        return None
    return text.lower().rstrip(".") != MD_NO_NONCOMPLIANCE_TEXT


def has_token(pattern):
    """Build a transform: ``True`` if ``pattern`` (regex, case-insensitive)
    matches the value (a string, or any element of a list); otherwise ``None``.

    Absence of a token in a checklist is "not reported", not ``False`` (epic
    README) -- stories that need ``False`` should combine this in an adapter.
    """
    compiled = re.compile(pattern, re.IGNORECASE)

    def transform(value):
        if isinstance(value, (list, tuple)):
            texts = [str(v) for v in value if v is not None]
        elif value is None:
            return None
        else:
            texts = [str(value)]
        return True if any(compiled.search(text) for text in texts) else None

    transform.label = f"has_token({pattern})"
    return transform


def has_list_token(token):
    """Build a transform: ``True`` if ``token`` is one whole entry of a
    comma-separated checklist string (case-insensitive), else ``None``.

    Built on :func:`has_token`. Each entry must equal ``token`` exactly (after
    trimming), so "NOT WHEELCHAIR ACCESSIBLE" does not match
    "WHEELCHAIR ACCESSIBLE". Absence is ``None``, never ``False``.
    """
    entry_matcher = has_token(rf"^\s*{re.escape(token)}\s*$")

    def transform(value):
        if isinstance(value, str):
            return entry_matcher(value.split(","))
        return entry_matcher(value)

    transform.label = f"has_list_token({token})"
    return transform


def to_iso_date(value):
    """Normalize a date string to ISO 8601 via ``normalization.normalize_date``.

    Sentinels -> ``None``. An unparseable value is returned unchanged (and
    logged by ``normalize_date``) -- we never silently drop date data.
    """
    from provider_scrape.normalization import normalize_date

    if value is None or _is_sentinel(value):
        return None
    return normalize_date(value)


def none_if_sentinel(value):
    """Whitespace-trim a string; sentinels (``N/A``, ``None``...) -> ``None``.

    Non-strings are returned unchanged. Generic "copy, but drop placeholders"
    transform (used for the specialist phone numbers).
    """
    if value is None:
        return None
    if not isinstance(value, str):
        return value
    return None if _is_sentinel(value) else value.strip()


def to_id_string(value):
    """Stringify a state record key without reformatting it.

    ``int`` -> ``str``; a string is only stripped (leading zeros kept:
    ``"003251"``); an integral float (``11216.0``) -> ``"11216"``. Bools,
    sentinels, blanks and anything else -> ``None``.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else None
    if isinstance(value, str):
        return None if _is_sentinel(value) else value.strip()
    return None


def lower_email(value):
    """Lower-case and trim an email address; sentinels -> ``None``."""
    cleaned = none_if_sentinel(value)
    return cleaned.lower() if isinstance(cleaned, str) else cleaned


def person_name(value):
    """Canonical ``First Last`` display name: sentinels -> ``None`` and an
    ALL-CAPS name is Title-cased (``title_case_name``); mixed case is untouched."""
    from provider_scrape.normalization import title_case_name

    cleaned = none_if_sentinel(value)
    return title_case_name(cleaned) if isinstance(cleaned, str) else cleaned


# One alias table for every state's spelling of a weekday: 2-letter codes
# (GA), 3-letter abbreviations (IA) and full names (VT). Keys are lower-case.
_FULL_DAYS = (
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
)
_DAY_ALIASES = {alias: day for day in _FULL_DAYS for alias in (day.lower(), day[:3].lower(), day[:2].lower())}
_DAY_ORDER = {day: index for index, day in enumerate(_FULL_DAYS)}


def day_names(value):
    """Comma-separated weekdays -> full day names in Monday-first order.

    Accepts a string (``MO,TU,WE`` / ``Mon, Tue`` / ``Monday, Tuesday``) or a
    list of such tokens, case-insensitive and whitespace-tolerant. Duplicates
    collapse, the result is sorted Monday -> Sunday, unrecognised tokens are
    logged and dropped, and an empty result is ``None``.
    """
    value = none_if_sentinel(value) if isinstance(value, str) else value
    if value is None:
        return None
    raw_tokens = value if isinstance(value, (list, tuple)) else [value]
    found = set()
    for raw in raw_tokens:
        for token in str(raw).split(","):
            token = token.strip()
            if not token:
                continue
            day = _DAY_ALIASES.get(token.lower())
            if day is None:
                logger.warning("day_names: unrecognised day %r dropped", token)
                continue
            found.add(day)
    if not found:
        return None
    return sorted(found, key=_DAY_ORDER.__getitem__)


def license_holder_address(value):
    """Single-line license-holder address, cleaned but with its case kept.

    MI builds the string from components, so absent parts appear as a literal
    ``null`` piece (``21885 Dunham Road, null, MI, 48036``). Comma-separated
    pieces that are exactly ``null`` (any case) are dropped and the rest kept;
    ``null`` inside a word (``100 Nullarbor Way``) is never touched. The result
    goes through ``clean_address`` (whitespace, comma spacing, trailing
    ``, USA``); an address with nothing left becomes ``None``. Case is left
    as-is (NC's ALL-CAPS addresses stay ALL-CAPS).
    """
    from provider_scrape.normalization import clean_address

    cleaned = none_if_sentinel(value)
    if not isinstance(cleaned, str):
        return None
    pieces = [p for p in cleaned.split(",") if p.strip().lower() != "null"]
    return clean_address(",".join(pieces))


def last_first_name(value):
    """Flip a ``Last, First`` name to ``First Last`` (then ``person_name``).

    Only a string with exactly one comma and text on both sides is flipped; a
    name with no comma (or an unexpected shape such as ``Smith, Jr., John``)
    is passed through unflipped rather than guessed at.
    """
    cleaned = none_if_sentinel(value)
    if isinstance(cleaned, str):
        parts = [part.strip() for part in cleaned.split(",")]
        if len(parts) == 2 and all(parts):
            cleaned = f"{parts[1]} {parts[0]}"
    return person_name(cleaned)


# Wisconsin publishes vacancies as prose: "None Reported." or
# "0 vacancies have been reported. Last updated on 9/1/2026Vacancies Age Range ...".
# One pattern captures both the count and the "Last updated" date so
# vacancies_updated_date can reuse it instead of re-parsing the string.
WI_VACANCIES_RE = re.compile(
    r"^\s*(?P<count>\d+)\s+vacanc(?:y|ies)\s+(?:has|have)\s+been\s+reported"
    r"(?:\.\s*Last updated on\s*(?P<updated>\d{1,2}/\d{1,2}/\d{4}))?",
    re.IGNORECASE,
)


def wi_total_vacancies(value):
    """Take the leading count from WI's vacancy prose; `None Reported.` -> None.

    ``"0 vacancies have been reported..."`` is a real ``0``. Text that is neither a
    sentinel nor matches the sentence shape is logged and left unset, never 0.
    """
    if value is None or _is_sentinel(value):
        return None
    match = WI_VACANCIES_RE.match(value) if isinstance(value, str) else None
    if match is None:
        logger.warning("wi_total_vacancies: unrecognised vacancies text %r -> None", value)
        return None
    return int(match.group("count"))


def wi_vacancies_updated_date(value):
    """Take the ``Last updated on`` date from WI's vacancy prose as ISO.

    ``None Reported.`` -> None. Text that is neither a sentinel nor matches the
    sentence shape is logged and left unset. Prose without a date -> None.
    """
    if value is None or _is_sentinel(value):
        return None
    match = WI_VACANCIES_RE.match(value) if isinstance(value, str) else None
    if match is None:
        logger.warning("wi_vacancies_updated_date: unrecognised vacancies text %r -> None", value)
        return None
    return to_iso_date(match.group("updated"))


# --------------------------------------------------------------------------- #
# Per-state adapters
# --------------------------------------------------------------------------- #

PROVIDER_LEVEL = "provider"
INSPECTION_LEVEL = "inspection"


@dataclass(frozen=True)
class Adapter:
    """A registered adapter plus the metadata the doc generator renders."""

    func: Callable
    state: str  # spider.name, e.g. "virginia"
    level: str  # PROVIDER_LEVEL or INSPECTION_LEVEL
    sources: tuple  # fields the adapter reads
    fills: tuple  # common fields the adapter may set

    @property
    def description(self):
        doc = (self.func.__doc__ or "").strip()
        return doc.splitlines()[0].replace("``", "`") if doc else self.func.__name__


# (level, state) -> adapters in registration order (= run order).
ADAPTERS = {}


def register_adapter(state, *, sources, fills, level=PROVIDER_LEVEL):
    """Decorator registering ``func(record) -> {common_field: value}`` for a state.

    ``state`` is the spider name; ``level`` picks provider vs. inspection rows.
    ``sources``/``fills`` declare what the adapter reads and may set (used by
    ``state_pipeline_details.md``). The first docstring line is rendered there
    too. Adapters for the same state/level run in registration order.
    """
    if level not in (PROVIDER_LEVEL, INSPECTION_LEVEL):
        raise ValueError(f"unknown adapter level {level!r}")

    def decorator(func):
        adapter = Adapter(func, state, level, tuple(sources), tuple(fills))
        ADAPTERS.setdefault((level, state), []).append(adapter)
        return func

    return decorator


def all_adapters():
    """Every registered adapter, sorted by (level, state) then run order."""
    return [a for key in sorted(ADAPTERS) for a in ADAPTERS[key]]


def run_adapters(record, state, level=PROVIDER_LEVEL):
    """Run the state's adapters over ``record`` (after the map collapse).

    Same rules as the map: additive (sources untouched), never overwrite a
    populated common field, and only fields declared in ``fills`` are applied.
    A returned ``None``/empty value is skipped. An adapter that raises is
    logged and skipped so one bad record cannot break the pipeline.
    """
    for adapter in ADAPTERS.get((level, state), ()):
        try:
            produced = adapter.func(record) or {}
        except Exception:  # adapters must never crash the pipeline
            logger.warning("adapter %s failed for state %s", adapter.func.__name__, state, exc_info=True)
            continue
        for field, value in produced.items():
            if field not in adapter.fills:
                logger.warning("adapter %s returned undeclared field %r (ignored)", adapter.func.__name__, field)
                continue
            if is_present(record.get(field)) or not is_present(value):
                continue
            record[field] = value
    return record


# Virginia packs "Angela Farmer: (276) 608-2283" into one string; the name and
# phone are split on the first colon. "N/A" (no inspector assigned) and a
# colon-less string (name only) are both observed.
_VA_INSPECTOR_RE = re.compile(r"^\s*(?P<name>[^:]*?)\s*(?::\s*(?P<phone>.*?))?\s*$")


@register_adapter(
    "virginia",
    sources=["va_inspector"],
    fills=["licensing_specialist", "licensing_specialist_phone"],
)
def virginia_inspector(record):
    """Split `va_inspector` ("Name: (555) 555-5555") into specialist name + phone; `N/A` -> None."""
    raw = record.get("va_inspector")
    if not isinstance(raw, str) or _is_sentinel(raw):
        return {}
    match = _VA_INSPECTOR_RE.match(raw)
    return {
        "licensing_specialist": person_name(match.group("name")),
        "licensing_specialist_phone": none_if_sentinel(match.group("phone")),
    }


# --------------------------------------------------------------------------- #
# Announced vs. unannounced visits encoded in the common inspection `type`
# --------------------------------------------------------------------------- #
# NC, CT and RI have no dedicated field; the word is part of `type` (e.g.
# "UNANNOUNCED INSPECTION - FULL"). `type` is every state's field, so these
# are state-scoped adapters: another state's `type` never feeds `announced`.
def _register_announced_from_type(state):
    @register_adapter(state, level=INSPECTION_LEVEL, sources=["type"], fills=["announced"])
    def announced_from_type(record):
        """Read announced (True) / unannounced (False) from the `type` text; neither word -> unset."""
        return {"announced": announced_flag(record.get("type"))}

    announced_from_type.__name__ = f"{state}_announced_from_type"
    return announced_from_type


for _state in ("north_carolina", "connecticut", "rhode_island"):
    _register_announced_from_type(_state)


# --------------------------------------------------------------------------- #
# Violation found, read from the common inspection `original_status`
# --------------------------------------------------------------------------- #
# `original_status` is every state's field, so AK and WI get state-scoped
# adapters with their own explicit tables. Unmatched text is logged by `vocab`
# and leaves `violation_found` unset.
AK_VIOLATION_STATUS = {
    "In Compliance": False,
    "Non-Compliance": True,
}
WI_VIOLATION_STATUS = {
    "See Violations below": True,
    "No Violations found ***": False,
}


def _register_violation_from_status(state, table):
    classify = vocab(table, name=f"{state} violation status")

    @register_adapter(state, level=INSPECTION_LEVEL, sources=["original_status"], fills=["violation_found"])
    def violation_from_status(record):
        """Read violation_found from the `original_status` text via an explicit table; unmatched -> unset."""
        return {"violation_found": classify(record.get("original_status"))}

    violation_from_status.__name__ = f"{state}_violation_from_status"
    return violation_from_status


_register_violation_from_status("alaska", AK_VIOLATION_STATUS)
_register_violation_from_status("wisconsin", WI_VIOLATION_STATUS)

# --------------------------------------------------------------------------- #
# Violation found, derived from the common `violation_count`
# --------------------------------------------------------------------------- #
# These states have no explicit flag, only a count. Adapters run after the map
# collapse, so `violation_count` is already populated. A missing count leaves
# `violation_found` unset; an explicit flag is never overwritten (run_adapters).
# DE's count is >= 1 by construction (clean visits have no row), so DE is
# always True and cannot show a clean visit.
VIOLATION_FROM_COUNT_STATES = (
    "connecticut",
    "delaware",
    "nevada",
    "kansas",
    "hawaii",
    "south_carolina",
)


def _register_violation_from_count(state):
    @register_adapter(state, level=INSPECTION_LEVEL, sources=["violation_count"], fills=["violation_found"])
    def violation_from_count(record):
        """Derive violation_found = violation_count > 0; no count -> unset."""
        count = record.get("violation_count")
        if isinstance(count, bool) or not isinstance(count, int):
            return {}
        return {"violation_found": count > 0}

    violation_from_count.__name__ = f"{state}_violation_from_count"
    return violation_from_count


for _state in VIOLATION_FROM_COUNT_STATES:
    _register_violation_from_count(_state)


# --------------------------------------------------------------------------- #
# Per-domain compliance roll-up (MA, NH)
# --------------------------------------------------------------------------- #
_RATIO_RE = re.compile(r"^\s*(\d+)\s*/\s*(\d+)\s*$")


def parse_ratio(value):
    """Parse a "x / y" checklist ratio ("19 / 20", "17/17") into ``(x, y)`` ints.

    Returns ``None`` for anything that is not two non-negative integers
    separated by a slash, or when ``x > y`` (more compliant than reviewed).
    """
    if not isinstance(value, str):
        return None
    match = _RATIO_RE.match(value)
    if not match:
        return None
    numerator, denominator = int(match.group(1)), int(match.group(2))
    if numerator > denominator:
        return None
    return numerator, denominator


@register_adapter("new_hampshire", level=INSPECTION_LEVEL, sources=["nh_domains"], fills=["compliance_domains"])
def nh_compliance_domains(record):
    """Split each `nh_domains` `level_of_compliance` ("19 / 20") into compliant / reviewed ints per domain."""
    rows = []
    for row in record.get("nh_domains") or []:
        ratio = parse_ratio(row.get("level_of_compliance")) if isinstance(row, dict) else None
        if ratio is None or not row.get("domain"):
            logger.warning("new_hampshire: dropping unparseable nh_domains row %r", row)
            continue
        rows.append({"domain": row["domain"], "compliant": ratio[0], "reviewed": ratio[1]})
    return {"compliance_domains": rows}


@register_adapter("massachusetts", level=INSPECTION_LEVEL, sources=["ma_domain_counts"], fills=["compliance_domains"])
def ma_compliance_domains(record):
    """Roll `ma_domain_counts` ({domain: {status: n}}) up to compliant (Compliant + Complete) / reviewed per domain."""
    # Imported here: the spider module imports normalization, which imports this module.
    from provider_scrape.spiders.massachusetts import PASSING_DOMAIN_COMPLIANCE

    counts = record.get("ma_domain_counts")
    if not isinstance(counts, dict):
        return {}
    rows = []
    for domain, statuses in counts.items():
        if not isinstance(statuses, dict):
            continue
        if not domain:
            logger.warning("massachusetts: dropping domain counts with no domain name: %r", statuses)
            continue
        if "Unknown" in statuses:
            logger.debug("massachusetts: excluding %s 'Unknown' status row(s) from %r", statuses["Unknown"], domain)
        known = {status: n for status, n in statuses.items() if status != "Unknown"}
        reviewed = sum(known.values())
        if not reviewed:  # every status Unknown: nothing was actually reported
            continue
        compliant = sum(n for status, n in known.items() if status in PASSING_DOMAIN_COMPLIANCE)
        rows.append({"domain": domain, "compliant": compliant, "reviewed": reviewed})
    return {"compliance_domains": rows}


# --------------------------------------------------------------------------- #
# Vacancies by age band (IA, ND, DC)
# --------------------------------------------------------------------------- #
# Each state labels its age bands differently (and defines them differently:
# ND infant = 0-17 months, IA = 0-12 + 13-23 months, DC = 0-12 months). The
# tables below map the state's own label to the common band; a label that is
# not listed is logged and skipped, never guessed.
VACANCY_FIELDS = {
    "infant": "vacancies_infant",
    "toddler": "vacancies_toddler",
    "preschool": "vacancies_preschool",
    "school_age": "vacancies_school_age",
}

IOWA_AGE_BANDS = {
    "Infant (0-12 mo.)": "infant",
    "Infant (13-23 mo.)": "infant",
    "Toddler (2 yo.)": "toddler",
    "Preschool (3 yo.)": "preschool",
    "Preschool (4-5 yo.)": "preschool",
    "Before & After School": "school_age",
    "School Age Full Time": "school_age",
}

NORTH_DAKOTA_AGE_BANDS = {
    "Infant (0-17 months)": "infant",
    "Toddler (18-35 months)": "toddler",
    "Preschool (3-5 years)": "preschool",
    "School Age (6-11 years)": "school_age",
}

DC_AGE_BANDS = {
    "Infant (0 - 12 Months)": "infant",
    "Toddler (12 - 36 Months)": "toddler",
    "Preschool (3 - 5 Years)": "preschool",
    "School Age (5 - 19 Years)": "school_age",
}


def _vacancies_by_band(rows, label_key, band_table, value_keys, state):
    """Fold a list of per-age-label rows into ``{vacancies_<band>: int}``.

    Values from every row mapping to the same band (and from every key in
    ``value_keys``) are summed. A band whose contributing values are all
    ``None`` is left out (not reported); a genuine ``0`` is kept. Unknown
    labels are logged and skipped.
    """
    if not isinstance(rows, list):
        return {}
    totals = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        label = row.get(label_key)
        band = band_table.get(label.strip()) if isinstance(label, str) else None
        if band is None:
            logger.warning("%s: unknown vacancy age-band label %r (skipped)", state, label)
            continue
        for key in value_keys:
            number = to_int(row.get(key))
            if number is not None:
                totals[band] = totals.get(band, 0) + number
    return {VACANCY_FIELDS[band]: total for band, total in totals.items()}


@register_adapter("iowa", sources=["ia_openings_by_age"], fills=list(VACANCY_FIELDS.values()))
def iowa_vacancies(record):
    """Sum IA `fullTime` + `partTime` openings per band across the 7 age labels."""
    return _vacancies_by_band(
        record.get("ia_openings_by_age"), "ageGroup", IOWA_AGE_BANDS, ("fullTime", "partTime"), "iowa"
    )


@register_adapter("north_dakota", sources=["nd_vacancies_by_age"], fills=list(VACANCY_FIELDS.values()))
def north_dakota_vacancies(record):
    """Map ND `nd_vacancies_by_age` rows (by age label) onto the four vacancy bands."""
    return _vacancies_by_band(
        record.get("nd_vacancies_by_age"), "age_group", NORTH_DAKOTA_AGE_BANDS, ("vacancies",), "north_dakota"
    )


@register_adapter("washington_dc", sources=["dc_enrollment"], fills=list(VACANCY_FIELDS.values()))
def washington_dc_vacancies(record):
    """Map DC `dc_enrollment` `openings` (text; `Not Applicable` -> None) onto the four vacancy bands."""
    return _vacancies_by_band(record.get("dc_enrollment"), "age_group", DC_AGE_BANDS, ("openings",), "washington_dc")


# --------------------------------------------------------------------------- #
# Address / phone suppressed (NY)
# --------------------------------------------------------------------------- #
# NY open data carries "Address Omitted" / "Phone Number Omitted" columns on
# every row: "Y" when the state withheld the item, blank otherwise. Because the
# map collapse drops blank sources before a transform runs, blank -> False has to
# be an adapter (it only runs for NY records).
def _ny_omitted_flag(record, source):
    """``"Y"`` -> True, blank/absent -> False, anything else logged and None."""
    raw = record.get(source)
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return False
    if isinstance(raw, str) and raw.strip().upper() == "Y":
        return True
    logger.warning("new_york: unexpected %s value %r (left unset)", source, raw)
    return None


@register_adapter(
    "new_york",
    sources=["ny_address_omitted", "ny_phone_number_omitted"],
    fills=["address_suppressed", "phone_suppressed"],
)
def new_york_suppressed(record):
    """NY `Address Omitted` / `Phone Number Omitted`: `Y` -> True, blank or absent -> False."""
    return {
        "address_suppressed": _ny_omitted_flag(record, "ny_address_omitted"),
        "phone_suppressed": _ny_omitted_flag(record, "ny_phone_number_omitted"),
    }


# --------------------------------------------------------------------------- #
# Weekly schedule (CT, IN, NH, OK, WY, MA, FL)
# --------------------------------------------------------------------------- #
# Every source is a per-day open/close structure; they differ in key names and
# in how the time is written. One time parser and one range splitter feed all
# of them, and each state adapter only maps its own container shape onto rows.
# Output rows: {"day": "Monday", "open": "07:00", "close": "18:00"}.
_TIME_RE = re.compile(
    r"""^(?P<hour>\d{1,2})
        (?::(?P<minute>\d{2}))?        # minutes optional only with AM/PM
        (?::[0-5]\d)?                  # seconds (MA: 07:00:00 AM), ignored
        \s*(?P<meridiem>[ap])?\.?(?:m\.?)?$""",
    re.IGNORECASE | re.VERBOSE,
)
# "A - B", "A to B" (also en/em dashes); the first separator wins.
_RANGE_SPLIT_RE = re.compile(r"\s+to\s+|\s*[-–—]\s*", re.IGNORECASE)


def parse_time(value):
    """One clock time -> 24h ``HH:MM``, or ``None`` when it cannot be read.

    Accepts ``6:00 AM``, ``7:00AM``, ``07:00 AM``, ``07:00`` and ``6 PM``;
    trailing seconds (``07:00:00 AM``) are dropped. ``12:00 AM`` -> ``00:00``
    and ``12:00 PM`` -> ``12:00``; MA's ``00:15 AM`` is read as ``00:15``. A
    time without AM/PM must already be 24h and carry minutes. CT's ``24:00``
    (open until midnight) is kept as ``24:00``, the ISO 8601 end of day.
    """
    if not isinstance(value, str):
        return None
    match = _TIME_RE.match(value.strip())
    if match is None:
        return None
    hour = int(match.group("hour"))
    minute = int(match.group("minute") or 0)
    meridiem = (match.group("meridiem") or "").lower()
    if minute > 59:
        return None
    if meridiem:
        if not 1 <= hour <= 12 and not (hour == 0 and meridiem == "a"):
            return None
        hour = hour % 12 + (12 if meridiem == "p" else 0)
    elif hour == 24 and minute == 0:
        pass
    elif match.group("minute") is None or hour > 23:
        return None
    return f"{hour:02d}:{minute:02d}"


def split_time_range(value):
    """``"6:30AM - 6:00PM"`` / ``"6:30AM to 6:00PM"`` -> ``("6:30AM", "6:00PM")``.

    Returns ``None`` unless the text splits into exactly two non-empty parts.
    """
    if not isinstance(value, str):
        return None
    parts = [part.strip() for part in _RANGE_SPLIT_RE.split(value.strip())]
    if len(parts) != 2 or not all(parts):
        return None
    return parts[0], parts[1]


def _schedule_row(day, open_raw, close_raw, context, **extra):
    """Build one validated row, or log and return ``None`` (the row is dropped)."""
    full_day = _DAY_ALIASES.get(str(day).strip().lower()) if day is not None else None
    opens, closes = parse_time(open_raw), parse_time(close_raw)
    if full_day is None or opens is None or closes is None:
        logger.warning(
            "weekly_schedule(%s): unparseable row day=%r open=%r close=%r dropped",
            context,
            day,
            open_raw,
            close_raw,
        )
        return None
    return {"day": full_day, "open": opens, "close": closes, **extra}


def _sorted_rows(rows):
    """Monday -> Sunday; the sort is stable so a day keeps its source order."""
    return sorted(rows, key=lambda row: _DAY_ORDER[row["day"]])


def _schedule_fields(rows):
    """Adapter result: ``weekly_schedule`` plus the ``days_of_operation`` it implies.

    The adapter runs after the direct-source collapse and the pipeline never
    overwrites a populated field, so a state with a direct days source keeps it.
    """
    rows = _sorted_rows(rows)
    if not rows:
        return {}
    return {
        "weekly_schedule": rows,
        "days_of_operation": day_names([row["day"] for row in rows]),
    }


def _rows_from_entries(entries, context, day_key, open_key, close_key):
    """Rows from a list of ``{day, <open>, <close>}`` dicts (CT, IN, NH, WY)."""
    if not isinstance(entries, list):
        return []
    rows = []
    for entry in entries:
        if not isinstance(entry, dict):
            logger.warning("weekly_schedule(%s): non-dict entry %r dropped", context, entry)
            continue
        row = _schedule_row(entry.get(day_key), entry.get(open_key), entry.get(close_key), context)
        if row is not None:
            rows.append(row)
    return rows


def _is_closed(text):
    """True for an explicit "Closed" (or empty) day, which yields no row."""
    return text is None or not str(text).strip() or str(text).strip().lower().startswith("closed")


def _rows_from_range_text(day, text, context, **extra):
    """Rows for one day from ``"A - B"`` / ``"A to B"``; ``;`` separates ranges."""
    if _is_closed(text):
        return []
    rows = []
    for chunk in str(text).split(";"):
        if not chunk.strip():
            continue
        bounds = split_time_range(chunk)
        row = _schedule_row(day, *(bounds or (None, None)), context, **extra)
        if row is None and bounds is None:
            logger.warning("weekly_schedule(%s): no time range in %r", context, chunk)
        if row is not None:
            rows.append(row)
    return rows


@register_adapter(
    "connecticut",
    sources=["ct_schedule"],
    fills=["weekly_schedule", "days_of_operation"],
)
def connecticut_schedule(record):
    """CT `ct_schedule` rows (already 24h `open`/`close`) -> `weekly_schedule`; days derived."""
    return _schedule_fields(_rows_from_entries(record.get("ct_schedule"), "connecticut", "day", "open", "close"))


@register_adapter(
    "indiana",
    sources=["in_schedule"],
    fills=["weekly_schedule", "days_of_operation"],
)
def indiana_schedule(record):
    """IN `in_schedule` rows (12h `open`/`close`) -> 24h `weekly_schedule`; days derived."""
    return _schedule_fields(_rows_from_entries(record.get("in_schedule"), "indiana", "day", "open", "close"))


@register_adapter(
    "new_hampshire",
    sources=["nh_schedule"],
    fills=["weekly_schedule", "days_of_operation"],
)
def new_hampshire_schedule(record):
    """NH `nh_schedule` sessions (`start`/`end`, 12h) -> one `weekly_schedule` row each; days derived."""
    return _schedule_fields(_rows_from_entries(record.get("nh_schedule"), "new_hampshire", "day", "start", "end"))


@register_adapter(
    "wyoming",
    sources=["wy_schedule"],
    fills=["weekly_schedule", "days_of_operation"],
)
def wyoming_schedule(record):
    """WY `wy_schedule` rows (`start`/`end`, 24h) -> `weekly_schedule`; days derived."""
    return _schedule_fields(_rows_from_entries(record.get("wy_schedule"), "wyoming", "day", "start", "end"))


@register_adapter(
    "oklahoma",
    sources=["ok_schedule"],
    fills=["weekly_schedule", "days_of_operation"],
)
def oklahoma_schedule(record):
    """OK `ok_schedule` `hours` ("6:30AM - 6:00PM", None when closed) -> `weekly_schedule`; days derived."""
    entries = record.get("ok_schedule")
    rows = []
    for entry in entries if isinstance(entries, list) else []:
        if not isinstance(entry, dict):
            logger.warning("weekly_schedule(oklahoma): non-dict entry %r dropped", entry)
            continue
        rows.extend(_rows_from_range_text(entry.get("day"), entry.get("hours"), "oklahoma"))
    return _schedule_fields(rows)


# Preference order when a Massachusetts provider lists several schedules.
_MA_SCHEDULE_PREFERENCE = ("full year schedule", "school year schedule", "summer schedule")


def _pick_massachusetts_schedule(schedules):
    """Full Year, else first School Year, else first Summer, else first listed.

    Only schedules that actually list days are candidates, so an empty
    preferred schedule does not hide a usable one.
    """
    candidates = [s for s in schedules if isinstance(s, dict) and s.get("days")]
    for wanted in _MA_SCHEDULE_PREFERENCE:
        for schedule in candidates:
            if str(schedule.get("schedule_type") or "").strip().lower() == wanted:
                return schedule
    return candidates[0] if candidates else None


@register_adapter(
    "massachusetts",
    sources=["ma_schedules"],
    fills=["weekly_schedule", "days_of_operation"],
)
def massachusetts_schedule(record):
    """MA `ma_schedules`: ONE schedule (Full Year > School Year > Summer > first) -> rows tagged `schedule_type`."""
    schedules = record.get("ma_schedules")
    chosen = _pick_massachusetts_schedule(schedules) if isinstance(schedules, list) else None
    if chosen is None:
        return {}
    schedule_type = str(chosen.get("schedule_type") or "").strip()
    rows = []
    for entry in chosen["days"] if isinstance(chosen["days"], list) else []:
        if not isinstance(entry, dict):
            logger.warning("weekly_schedule(massachusetts): non-dict entry %r dropped", entry)
            continue
        row = _schedule_row(
            entry.get("day"), entry.get("start"), entry.get("end"), "massachusetts", schedule_type=schedule_type
        )
        if row is not None:
            rows.append(row)
    return _schedule_fields(rows)


@register_adapter(
    "florida",
    sources=["hours"],
    fills=["weekly_schedule", "days_of_operation"],
)
def florida_schedule(record):
    """FL dict-shaped `hours` ({"monday": "6:45AM to 6:00PM", ...}) -> `weekly_schedule`; `hours` kept."""
    hours = record.get("hours")
    if not isinstance(hours, dict):
        return {}
    rows = []
    for day, text in hours.items():
        rows.extend(_rows_from_range_text(day, text, "florida"))
    return _schedule_fields(rows)


# --------------------------------------------------------------------------- #
# Year-round operation and operating months (GA, MS, WI, SD, ND, VT, OK)
# --------------------------------------------------------------------------- #
# GA/MS/WI publish months; SD/ND/VT/OK only a season category. Months are kept
# only where the state lists them; every state can fill ``year_round``.
_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
_MONTH_ALIASES = {
    alias: month
    for month, full in zip(
        _MONTHS,
        (
            "january",
            "february",
            "march",
            "april",
            "may",
            "june",
            "july",
            "august",
            "september",
            "october",
            "november",
            "december",
        ),
        strict=True,
    )
    for alias in (month.lower(), full)
}
_MONTH_ALIASES["sept"] = "Sep"
# GA appends this non-month marker to `Operation_Months`; it carries no month.
_IGNORED_MONTH_TOKENS = {"otherschoolbreak"}
_MONTH_RANGE_RE = re.compile(r"^\s*([A-Za-z]+)\s*[-–—]\s*([A-Za-z]+)\s*$")


def month_names(value):
    """Month tokens -> 3-letter Title-case months in calendar order, de-duplicated.

    Accepts a comma-separated string (``JAN,FEB``) or a list of month names.
    Full names and ``Sept`` are tolerated. GA's ``OtherSchoolBreak`` marker is
    dropped silently; any other unknown token is logged and dropped. Nothing
    usable -> ``None``.
    """
    value = none_if_sentinel(value) if isinstance(value, str) else value
    if value is None:
        return None
    raw_tokens = value if isinstance(value, (list, tuple)) else [value]
    found = set()
    for raw in raw_tokens:
        for token in str(raw).split(","):
            token = token.strip()
            if not token or token.lower() in _IGNORED_MONTH_TOKENS:
                continue
            month = _MONTH_ALIASES.get(token.lower())
            if month is None:
                logger.warning("month_names: unrecognised month %r dropped", token)
                continue
            found.add(month)
    if not found:
        return None
    return sorted(found, key=_MONTHS.index)


def month_range(value):
    """``"Sep - Jun"`` -> inclusive month list, wrapping across the year end.

    ``Jan - Dec`` is all 12; ``Sep - Jun`` is Sep..Dec + Jan..Jun (10 months).
    Result is in calendar order. An unparseable value is logged -> ``None``.
    """
    if not isinstance(value, str) or none_if_sentinel(value) is None:
        return None
    match = _MONTH_RANGE_RE.match(value)
    start = _MONTH_ALIASES.get(match.group(1).lower()) if match else None
    end = _MONTH_ALIASES.get(match.group(2).lower()) if match else None
    if start is None or end is None:
        logger.warning("month_range: unparseable month range %r", value)
        return None
    first, last = _MONTHS.index(start), _MONTHS.index(end)
    span = range(first, last + 1) if first <= last else [*range(first, 12), *range(0, last + 1)]
    return sorted((_MONTHS[i] for i in span), key=_MONTHS.index)


def _months_fields(months):
    """Adapter result: ``operating_months`` plus the ``year_round`` it implies."""
    if not months:
        return {}
    return {"operating_months": months, "year_round": len(months) == len(_MONTHS)}


@register_adapter("georgia", sources=["ga_operating_months"], fills=["operating_months", "year_round"])
def georgia_months(record):
    """GA `ga_operating_months` (`JAN,FEB,...`) -> `operating_months`; year_round = all 12."""
    return _months_fields(month_names(record.get("ga_operating_months")))


@register_adapter("mississippi", sources=["ms_months_of_operation"], fills=["operating_months", "year_round"])
def mississippi_months(record):
    """MS `ms_months_of_operation` (list of month names) -> `operating_months`; year_round = all 12."""
    return _months_fields(month_names(record.get("ms_months_of_operation")))


@register_adapter("wisconsin", sources=["wi_months_open"], fills=["operating_months", "year_round"])
def wisconsin_months(record):
    """WI `wi_months_open` (`Sep - Jun`, wraps the year) -> `operating_months`; year_round = all 12."""
    return _months_fields(month_range(record.get("wi_months_open")))


# Category labels -> year_round. ``None`` marks a known label that says nothing
# about the season. Lookups are case-insensitive (see :func:`vocab`).
SD_YEAR_ROUND_LABELS = {
    "12 months": True,
    "school year only": False,
    "summer only": False,
    "other": None,
}
ND_YEAR_ROUND_LABELS = {
    "full calendar year": True,
    "school year only (typically september-may)": False,
}
# VT: seasonal-only values mean not year-round; `In-Service Days` is not a
# season and a blank value is never assumed to mean year-round.
VT_YEAR_ROUND_LABELS = {
    "school year only": False,
    "summer only": False,
    "in-service days": None,
}
nd_year_round = vocab(ND_YEAR_ROUND_LABELS, name="nd_year_round")
vt_year_round = vocab(VT_YEAR_ROUND_LABELS, name="vt_year_round")
_sd_year_round_label = vocab(SD_YEAR_ROUND_LABELS, name="sd_year_round")


def ok_care_type_set(value):
    """OK-style care-type labels (list, or comma-separated string) -> lower-case set.

    Shared by every field parsed from the OK ``ok_care_types`` tokens
    (``Year Round``, ``Evening Hours``, ``Weekend Hours``, ...).
    """
    if value is None:
        return set()
    raw = value if isinstance(value, (list, tuple)) else str(value).split(",")
    return {str(label).strip().lower() for label in raw if label is not None and str(label).strip()}


_OK_SEASONAL_LABELS = ("school year only", "summer only", "school vacation only")


@register_adapter("south_dakota", sources=["sd_months_of_operation"], fills=["year_round"])
def south_dakota_year_round(record):
    """SD `sd_months_of_operation` category list (`12 Months` / seasonal) -> `year_round`."""
    labels = record.get("sd_months_of_operation")
    if isinstance(labels, str):
        labels = [labels]
    results = [_sd_year_round_label(label) for label in labels or []]
    if True in results:
        return {"year_round": True}
    if False in results:
        return {"year_round": False}
    return {}


@register_adapter("oklahoma", sources=["ok_care_types"], fills=["year_round"])
def oklahoma_year_round(record):
    """OK `ok_care_types`: `Year Round` -> True; else a seasonal-only label -> False; else unset."""
    labels = ok_care_type_set(record.get("ok_care_types"))
    if "year round" in labels:
        return {"year_round": True}
    if labels.intersection(_OK_SEASONAL_LABELS):
        return {"year_round": False}
    return {}


# --------------------------------------------------------------------------- #
# Special needs (IA, NE, NC, CO, CT, MA, NH, ND)
# --------------------------------------------------------------------------- #
# Fixed category list, in the order ``special_needs_types`` is emitted.
SPECIAL_NEEDS_CATEGORIES = (
    "Autism",
    "Behavioral/Social-Emotional",
    "ADHD",
    "Developmental/Cognitive",
    "Speech/Language",
    "Physical",
    "Hearing/Vision",
    "Medical/Health",
    "Dietary",
    "Sensory",
    "At Risk",
)
# State label -> canonical category. Lookups are case-insensitive and
# whitespace-trimmed (see :func:`vocab`), which also covers NH's UPPERCASE
# MA-style labels. An unmapped label is logged and dropped.
SPECIAL_NEEDS_LABELS = {
    # Autism
    "Autism Spectrum Disorders": "Autism",  # CT
    "Autism": "Autism",  # ND
    # Behavioral/Social-Emotional
    "Social Emotional Behaviors": "Behavioral/Social-Emotional",  # CO
    "Social/Emotional/Behavior Concerns": "Behavioral/Social-Emotional",  # CT
    "Behavioral": "Behavioral/Social-Emotional",  # MA
    "Emotional/Social": "Behavioral/Social-Emotional",  # MA
    "Emotional/Behavioral": "Behavioral/Social-Emotional",  # ND
    # ADHD
    "ADD/ADHD": "ADHD",  # MA
    "ADHD/ADD": "ADHD",  # ND
    # Developmental/Cognitive
    "Cognitive Delays": "Developmental/Cognitive",  # CO
    "Developmental Delays/Learning Disabilities": "Developmental/Cognitive",  # CT
    "Intellectual Disabilities": "Developmental/Cognitive",  # CT
    "Developmental": "Developmental/Cognitive",  # MA
    "Developmental Delay": "Developmental/Cognitive",  # ND
    # Speech/Language
    "Speech/Communication": "Speech/Language",  # CO
    "Speech/Language Impairment": "Speech/Language",  # CT
    "Speech/Language": "Speech/Language",  # MA
    # Physical
    "Physical Delays/Limitations": "Physical",  # CO
    "Physical Disabilities": "Physical",  # CT
    "Physical": "Physical",  # MA, ND
    # Hearing/Vision
    "Hearing/Visual Impairment": "Hearing/Vision",  # CT
    "Visual": "Hearing/Vision",  # ND
    "Hearing/Sign Language": "Hearing/Vision",  # ND
    # Medical/Health
    "Medical/Special Procedures": "Medical/Health",  # CO
    "Respiratory Illness": "Medical/Health",  # CO
    "Diabetes": "Medical/Health",  # CO
    "Seizure Disorders": "Medical/Health",  # CO
    "Health Impairment": "Medical/Health",  # CT
    "Special Health Needs": "Medical/Health",  # ND
    "Seizures": "Medical/Health",  # ND
    "Monitors": "Medical/Health",  # ND
    "HIV/Hepatitis B": "Medical/Health",  # ND
    "Asthma/Allergies": "Medical/Health",  # MA
    "Asthma/Severe Allergies": "Medical/Health",  # ND
    # Dietary
    "Food/Dietary": "Dietary",  # CO
    "Special Diet": "Dietary",  # MA
    # Sensory
    "Sensory Integration": "Sensory",  # MA
    # At Risk
    "At risk": "At Risk",  # MA
}
_special_needs_category = vocab(SPECIAL_NEEDS_LABELS, name="special_needs")

# Labels that say the provider accepts children with special needs but name no
# condition: they set ``serves_special_needs`` and are never a type.
_SPECIAL_NEEDS_MARKERS_CO = {"willing to support"}
_SPECIAL_NEEDS_MARKERS_MA_NH = {"experience"}


def _special_needs_labels(value, separator):
    """A list, or a ``separator``-joined string, -> trimmed non-empty labels."""
    if value is None:
        return []
    raw = value if isinstance(value, (list, tuple)) else str(value).split(separator)
    return [str(label).strip() for label in raw if label is not None and str(label).strip()]


def _special_needs_fields(labels, *, markers=(), any_label_serves=False):
    """Adapter result for a list of state labels.

    Mapped labels become de-duplicated ``special_needs_types`` in canonical
    order. A mapped label or a marker means ``serves_special_needs`` True. With
    ``any_label_serves`` any non-sentinel label does too (list sources whose
    mere presence says the provider serves special needs). Nothing usable ->
    ``{}`` (never False).
    """
    categories = set()
    serves = False
    for label in labels:
        if label.lower() in markers:
            serves = True
            continue
        if _is_sentinel(label):
            continue
        if any_label_serves:
            serves = True
        category = _special_needs_category(label)
        if category is not None:
            categories.add(category)
            serves = True
    fields = {}
    if serves:
        fields["serves_special_needs"] = True
    if categories:
        fields["special_needs_types"] = sorted(categories, key=SPECIAL_NEEDS_CATEGORIES.index)
    return fields


@register_adapter("colorado", sources=["co_special_needs"], fills=["serves_special_needs", "special_needs_types"])
def colorado_special_needs(record):
    """CO `co_special_needs` (`;`-separated) -> `serves_special_needs` + `special_needs_types`; `None` -> False."""
    value = record.get("co_special_needs")
    if isinstance(value, str) and value.strip().lower() == "none":
        return {"serves_special_needs": False}
    labels = _special_needs_labels(value, ";")
    return _special_needs_fields(labels, markers=_SPECIAL_NEEDS_MARKERS_CO)


@register_adapter("connecticut", sources=["ct_special_needs"], fills=["serves_special_needs", "special_needs_types"])
def connecticut_special_needs(record):
    """CT `ct_special_needs` (list) -> `special_needs_types`; serves True when non-empty."""
    labels = _special_needs_labels(record.get("ct_special_needs"), ";")
    return _special_needs_fields(labels, any_label_serves=True)


@register_adapter("massachusetts", sources=["ma_special_needs"], fills=["serves_special_needs", "special_needs_types"])
def massachusetts_special_needs(record):
    """MA `ma_special_needs` (`;`-separated) -> types; `Experience` -> serves True, never a type."""
    labels = _special_needs_labels(record.get("ma_special_needs"), ";")
    return _special_needs_fields(labels, markers=_SPECIAL_NEEDS_MARKERS_MA_NH)


@register_adapter("new_hampshire", sources=["nh_special_needs"], fills=["serves_special_needs", "special_needs_types"])
def new_hampshire_special_needs(record):
    """NH `nh_special_needs` (`, `-separated UPPERCASE) -> types; `EXPERIENCE` -> serves True, never a type."""
    labels = _special_needs_labels(record.get("nh_special_needs"), ",")
    return _special_needs_fields(labels, markers=_SPECIAL_NEEDS_MARKERS_MA_NH)


@register_adapter(
    "north_dakota", sources=["nd_special_populations"], fills=["serves_special_needs", "special_needs_types"]
)
def north_dakota_special_needs(record):
    """ND `nd_special_populations` (list) -> `special_needs_types`; serves True when non-empty."""
    labels = _special_needs_labels(record.get("nd_special_populations"), ";")
    return _special_needs_fields(labels, any_label_serves=True)

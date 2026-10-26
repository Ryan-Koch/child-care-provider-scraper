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

import itertools
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


# Arizona stamps this placeholder on monitoring visits that found nothing, so
# it is "no finding", not a narrative. Matched whole (after a trim, ignoring
# case) and never fuzzily: any other wording is kept as a real finding.
AZ_NO_ISSUES_TEXT = "monitor visit completed; no issues"


def _narrative_text(value):
    """Trimmed narrative string, or ``None`` for blanks, sentinels and non-strings.

    Text is otherwise kept verbatim (no re-casing, no truncation).
    """
    cleaned = none_if_sentinel(value)
    return cleaned if isinstance(cleaned, str) and cleaned else None


def finding_text_md(value):
    """Maryland finding narrative; the ``No Noncompliances Found`` sentinel -> ``None``.

    Uses the same match as :func:`md_finding_flag` (case-insensitive, trailing
    period tolerated), so ``finding_text`` is unset exactly when
    ``violation_found`` is ``False``.
    """
    text = _narrative_text(value)
    if text is None or text.lower().rstrip(".") == MD_NO_NONCOMPLIANCE_TEXT:
        return None
    return text


def finding_text_az(value):
    """Arizona decision/correction narrative; the no-issues placeholder -> ``None``."""
    text = _narrative_text(value)
    if text is None or text.lower() == AZ_NO_ISSUES_TEXT:
        return None
    return text


def finding_text_list(value):
    """Oklahoma list of noncompliance strings joined with a blank line.

    Blank entries are dropped; an empty or all-blank list -> ``None``.
    """
    if isinstance(value, str):
        return _narrative_text(value)
    if not isinstance(value, (list, tuple)):
        return None
    parts = [t for t in (_narrative_text(v) for v in value) if t]
    return "\n\n".join(parts) if parts else None


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


# Lower-cased "not accredited" answers, on top of SENTINELS. Scoped to the
# accreditation field only: "no" is a real answer elsewhere (a yes/no source).
ACCREDITATION_NEGATIVES = {
    "no",
    "not accredited",
    "not nationally accredited",
}


def _is_negative_accreditation(value):
    """True for a placeholder or "not accredited" string (trimmed, any case)."""
    if not isinstance(value, str):
        return False
    return _is_sentinel(value) or value.strip().lower() in ACCREDITATION_NEGATIVES


def accreditation_value(value):
    """Drop "not accredited" answers from an accreditation source, keeping its shape.

    A string stays a string (trimmed) and becomes ``None`` when negative. A list
    stays a list with negative entries removed (``None`` when none are left);
    dict entries (HI) are judged by their ``name``. Real values pass through
    unchanged.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return None if _is_negative_accreditation(value) else value.strip()
    if isinstance(value, (list, tuple)):
        kept = [
            entry
            for entry in value
            if not _is_negative_accreditation(entry.get("name") if isinstance(entry, dict) else entry)
        ]
        return kept or None
    return value


def accreditation_list(value):
    """Split a ", "-joined accreditation string into a de-duplicated list.

    Order is kept, entries are trimmed and negatives dropped; all-negative or
    empty input gives ``None``. Non-strings go through :func:`accreditation_value`.
    """
    if not isinstance(value, str):
        return accreditation_value(value)
    names = []
    for part in value.split(","):
        name = part.strip()
        if not _is_negative_accreditation(name) and name not in names:
            names.append(name)
    return names or None


# Provider-written descriptions that say nothing. Matched whole-string,
# case-insensitively, after trimming; short real answers ("Montessori") are kept.
DESCRIPTION_FILLER = {
    "please ask provider",
    "please ask provider.",
    "please ask program",
    "please ask program.",
    "ask provider",
    "coming soon",
}
_X_RUN_RE = re.compile(r"^x+$", re.IGNORECASE)
_PUNCTUATION_ONLY_RE = re.compile(r"^[\W_]+$")


def description_text(value):
    """Trimmed free-text description, or ``None`` when it is filler.

    Filler: blank or a sentinel, punctuation/symbol-only (``.``, ``-``, ``*``),
    a run of ``x``, and the phrases in ``DESCRIPTION_FILLER``. Text is kept
    verbatim apart from trimming. Non-strings are returned unchanged.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text or _is_sentinel(text):
        return None
    if _PUNCTUATION_ONLY_RE.match(text) or _X_RUN_RE.match(text):
        return None
    if text.lower() in DESCRIPTION_FILLER:
        return None
    return text


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
# Visit-level items compliant / items reviewed (MA, RI, NH, OK, KS)
# --------------------------------------------------------------------------- #
# Units differ by state: checklist items for MA / RI / NH, areas for OK, and
# regulations for KS. A visit whose figures are unusable sets neither field.
def _is_count(value):
    """True for a non-negative int (bool excluded)."""
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _items_pair(compliant, reviewed):
    """Return the ``items_compliant`` / ``items_reviewed`` dict, or ``{}`` unless the pair is valid."""
    if not (_is_count(compliant) and _is_count(reviewed)) or compliant > reviewed:
        return {}
    return {"items_compliant": compliant, "items_reviewed": reviewed}


def _register_items_ratio(state, source):
    @register_adapter(state, level=INSPECTION_LEVEL, sources=[source], fills=["items_compliant", "items_reviewed"])
    def items_from_ratio(record):
        """Split the visit-level `x / y` compliance ratio into items_compliant / items_reviewed; bad ratio -> unset."""
        ratio = parse_ratio(record.get(source))
        if ratio is None:
            if is_present(record.get(source)):
                logger.warning(
                    "%s: unparseable %s %r; items_compliant/items_reviewed unset", state, source, record.get(source)
                )
            return {}
        return _items_pair(*ratio)

    items_from_ratio.__name__ = f"{state}_items_from_ratio"
    return items_from_ratio


_register_items_ratio("massachusetts", "ma_level_of_compliance")
_register_items_ratio("rhode_island", "ri_compliance")
_register_items_ratio("new_hampshire", "nh_level_of_compliance")


@register_adapter(
    "oklahoma",
    level=INSPECTION_LEVEL,
    sources=["ok_compliance_count", "ok_compliance_total"],
    fills=["items_compliant", "items_reviewed"],
)
def oklahoma_items(record):
    """Copy `ok_compliance_count` / `ok_compliance_total` (areas in compliance / reviewed); invalid pair -> unset."""
    return _items_pair(record.get("ok_compliance_count"), record.get("ok_compliance_total"))


@register_adapter("kansas", level=INSPECTION_LEVEL, sources=["ks_regulations_reviewed"], fills=["items_reviewed"])
def kansas_items_reviewed(record):
    """Copy `ks_regulations_reviewed` to items_reviewed only; items_compliant is never derived from findings."""
    reviewed = record.get("ks_regulations_reviewed")
    return {"items_reviewed": reviewed} if _is_count(reviewed) else {}


# --------------------------------------------------------------------------- #
# Correction date (WV; AZ and IN are plain collapse entries)
# --------------------------------------------------------------------------- #
WV_NULL_DATE = "1/1/1753"  # SQL Server minimum date, WV's "no date" placeholder


@register_adapter(
    "west_virginia",
    level=INSPECTION_LEVEL,
    sources=["wv_issue_completed_date", "wv_outcome_code"],
    fills=["correction_date"],
)
def west_virginia_correction_date(record):
    """WV `wv_issue_completed_date` as ISO, only when `wv_outcome_code` is exactly `Achieved`.

    Pending, Not Achieved and blank outcomes are not corrections even with a real date,
    and `1/1/1753` is the null placeholder (also on Achieved rows) -> unset.
    """
    if record.get("wv_outcome_code") != "Achieved":
        return {}
    raw = record.get("wv_issue_completed_date")
    if isinstance(raw, str) and raw.strip() == WV_NULL_DATE:
        return {}
    return {"correction_date": to_iso_date(raw)}


# --------------------------------------------------------------------------- #
# Licensing period (HI, ME)
# --------------------------------------------------------------------------- #
# Both states stamp each inspection with the license term it is associated with.
# The period is copied as published: it is NOT filtered on whether the visit date
# falls inside it (renewal visits are filed under the term they led to). One
# adapter per state fills both fields because the end < start check needs both.
def _licensing_period(start_raw, end_raw):
    """Return the ``licensing_period_start`` / ``licensing_period_end`` dict as ISO dates.

    A date that cannot be parsed is kept as emitted (``to_iso_date`` logs it), and a
    missing side is simply left out. When both sides parse and end < start, neither
    field is set.
    """
    start, end = to_iso_date(start_raw), to_iso_date(end_raw)
    if (
        isinstance(start, str)
        and isinstance(end, str)
        and _ISO_DATE_RE.fullmatch(start)
        and _ISO_DATE_RE.fullmatch(end)
        and end < start
    ):
        logger.warning("licensing period end %s is before start %s; both unset", end, start)
        return {}
    return {"licensing_period_start": start, "licensing_period_end": end}


_ISO_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def _register_licensing_period(state, start_source, end_source):
    @register_adapter(
        state,
        level=INSPECTION_LEVEL,
        sources=[start_source, end_source],
        fills=["licensing_period_start", "licensing_period_end"],
    )
    def licensing_period(record):
        """Copy the stamped license term as ISO dates; end < start -> both unset."""
        return _licensing_period(record.get(start_source), record.get(end_source))

    licensing_period.__name__ = f"{state}_licensing_period"
    return licensing_period


_register_licensing_period("hawaii", "hi_licensing_period_start", "hi_licensing_period_end")
_register_licensing_period("maine", "me_licensed_from", "me_licensed_to")


# --------------------------------------------------------------------------- #
# Visit reason (MA, OK; KS is a plain collapse entry on `ks_survey_reason`)
# --------------------------------------------------------------------------- #
# The state's own purpose-of-visit label, copied as emitted after a whitespace
# trim (no canonical vocabulary). MA and OK fall back to `type` when the reason is
# unset: their substantiated-complaint / investigation rows carry the purpose only
# in `type` (`Investigation`, `Complaint`). The fallback is state-scoped on purpose;
# in most other states `type` is something else, so a plain collapse entry on
# `type` would be wrong. KS has no fallback (`Administrative Order` stays unset).
def _visit_reason_text(value):
    """Trimmed string, or ``None`` for blanks, sentinels and non-strings."""
    cleaned = none_if_sentinel(value)
    return cleaned if isinstance(cleaned, str) and cleaned else None


def _register_visit_reason(state, reason_source):
    @register_adapter(state, level=INSPECTION_LEVEL, sources=[reason_source, "type"], fills=["visit_reason"])
    def visit_reason(record):
        """Copy the state's visit-reason field; when unset fall back to `type`."""
        reason = _visit_reason_text(record.get(reason_source)) or _visit_reason_text(record.get("type"))
        return {"visit_reason": reason} if reason else {}

    visit_reason.__name__ = f"{state}_visit_reason"
    return visit_reason


_register_visit_reason("massachusetts", "ma_visit_reason")
_register_visit_reason("oklahoma", "ok_visit_purpose")


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


# One DE segment: "2026: 1 facility injury. No facility deaths reported."
_DE_YEAR_SEGMENT = re.compile(r"^\s*(\d{4})\s*:\s*(.*)$", re.DOTALL)
_DE_INJURIES = re.compile(r"(?:(\d+)\s+facility\s+injur(?:y|ies)|No\s+facility\s+injuries\s+reported)", re.I)
_DE_DEATHS = re.compile(r"(?:(\d+)\s+facility\s+deaths?|No\s+facility\s+deaths\s+reported)", re.I)


def _de_count(pattern, text):
    """First match of ``pattern`` in ``text``: its number, 0 for "No ... reported", else None."""
    match = pattern.search(text)
    if match is None:
        return None
    return int(match.group(1)) if match.group(1) else 0


@register_adapter(
    "delaware",
    sources=["de_injuries_report"],
    fills=["serious_injury_count", "fatality_count"],
)
def delaware_injuries(record):
    """Parse `de_injuries_report` ("; "-separated per-year segments).

    Only the segment with the highest year is used. With no year segments, the
    "No facility ... reported" text means 0. Anything unrecognised -> None.
    """
    report = record.get("de_injuries_report")
    if not isinstance(report, str) or not report.strip():
        return {}
    segments = []  # (year, text)
    for part in report.split(";"):
        year_match = _DE_YEAR_SEGMENT.match(part)
        if year_match:
            segments.append((int(year_match.group(1)), year_match.group(2)))
    text = max(segments)[1] if segments else report
    injuries = _de_count(_DE_INJURIES, text)
    deaths = _de_count(_DE_DEATHS, text)
    if injuries is None or deaths is None:
        logger.warning("delaware: unrecognised de_injuries_report %r -> None", report)
        return {"serious_injury_count": None, "fatality_count": None}
    return {"serious_injury_count": injuries, "fatality_count": deaths}


@register_adapter("indiana", sources=["in_complaints"], fills=["complaint_count"])
def indiana_complaint_count(record):
    """Count `in_complaints` rows; the spider omits the key when there are none -> 0."""
    complaints = record.get("in_complaints")
    return {"complaint_count": len(complaints) if isinstance(complaints, list) else 0}


@register_adapter(
    "idaho",
    sources=["id_program_description", "id_philosophy_description"],
    fills=["program_description"],
)
def idaho_program_description(record):
    """Program description first, falling back to the philosophy description.

    Either source that is filler is skipped. `id_philosophy_comment` (curriculum
    notes) is deliberately not used.
    """
    for source in ("id_program_description", "id_philosophy_description"):
        text = description_text(record.get(source))
        if text:
            return {"program_description": text}
    return {}


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


@register_adapter("washington_dc", sources=["dc_enrollment"], fills=["current_enrollment"])
def washington_dc_current_enrollment(record):
    """Sum the numeric `current_enrollment` rows of DC `dc_enrollment`.

    `Not Applicable` (age group not served), `No Data Available` and None are
    skipped; no numeric row -> None, while numeric `0` rows give a real 0. Rows
    marked `No Data Available` may hide children, so the total can undercount.
    """
    counts = []
    for entry in _dict_entries(record.get("dc_enrollment"), "current_enrollment(washington_dc)"):
        raw = entry.get("current_enrollment")
        if raw is None or _is_sentinel(raw):
            continue
        count = to_int(raw)
        if count is not None:
            counts.append(count)
    return {"current_enrollment": sum(counts)} if counts else {}


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


def _pick_preferred_schedule_type(entries, usable=lambda entry: True):
    """Pick one entry by ``schedule_type``: Full Year, else first School Year, else first Summer, else first.

    ``entries`` are dicts carrying a ``schedule_type``; only ``usable`` ones are candidates.
    """
    candidates = [e for e in entries if isinstance(e, dict) and usable(e)]
    for wanted in _MA_SCHEDULE_PREFERENCE:
        for entry in candidates:
            if str(entry.get("schedule_type") or "").strip().lower() == wanted:
                return entry
    return candidates[0] if candidates else None


def _pick_massachusetts_schedule(schedules):
    """Full Year, else first School Year, else first Summer, else first listed.

    Only schedules that actually list days are candidates, so an empty
    preferred schedule does not hide a usable one.
    """
    return _pick_preferred_schedule_type(schedules, lambda schedule: bool(schedule.get("days")))


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


# --------------------------------------------------------------------------- #
# USDA food program (CACFP)
# --------------------------------------------------------------------------- #
# One shared, case-insensitive phrase for every list/text source: DE
# `Child Care Food Program`, VT `Food Program`, WY `Child and Adult Food Program`,
# UT `On USDA Food Program`, PA/DC `Child and Adult Care Food Program` / `CACFP`.
FOOD_PROGRAM_PATTERN = r"food program|CACFP"
food_program_token = has_token(FOOD_PROGRAM_PATTERN)


def _food_program_from_meals(record):
    """The common `meals` text mentioning the food program -> `food_program` True; else unset."""
    return {"food_program": True} if food_program_token(record.get("meals")) else {}


# `meals` is common to every state, so this is scoped to the states whose `meals`
# is known to carry the phrase rather than put in the state-agnostic map. `meals`
# itself is left untouched.
for _state in ("utah", "pennsylvania", "washington_dc"):
    register_adapter(_state, sources=["meals"], fills=["food_program"])(_food_program_from_meals)


# --------------------------------------------------------------------------- #
# State pre-K participation
# --------------------------------------------------------------------------- #
# Program brand per state, set on ``state_prek_program`` only when ``state_prek`` is True.
STATE_PREK_PROGRAMS = {
    "florida": "VPK",
    "rhode_island": "RI Pre-K",
    "washington": "ECEAP",
    "washington_dc": "Pre-K Enhancement and Expansion",
    "kentucky": "Preschool Partnership",
    "indiana": "On My Way Pre-K",
    "georgia": "Georgia's Pre-K",
    "south_carolina": "First Steps 4K",
    "vermont": "Vermont Universal Pre-K",
    "nevada": "Nevada Ready! Pre-K",  # official spelling; the source data has no "!"
}

# State -> bool source field (``to_bool``; False is real, blank/None stays unset).
_STATE_PREK_BOOL_SOURCES = {
    "florida": "fl_is_vpk",
    "rhode_island": "ri_state_prek",
    "washington": "wa_eceap",
    "washington_dc": "dc_prek_enhancement",
    "kentucky": "ky_prek_partnership",
}

# State -> (token source field, exact whole-entry token, entry separators). Absence -> unset.
# GA joins entries with ``; `` (and ``Georgia's Pre-K Transition`` is a different entry that
# does not count), VT with ``, ``; IN/SC are real lists; NV is one value per record, split on
# both separators defensively.
_STATE_PREK_TOKEN_SOURCES = {
    "indiana": ("in_programs", "On My Way Pre-K", ",;"),
    "georgia": ("ga_other_care_type", "Georgia's Pre-K", ";"),
    "south_carolina": ("sc_program_participation", "First Steps 4K", ",;"),
    "vermont": ("vt_program_participation", "Pre-K Program", ","),
    "nevada": ("nv_site_characteristic", "Nevada Ready Pre-K", ",;"),
}


def _has_entry(value, token, separators):
    """True if ``token`` equals one whole entry (case-insensitive) of a list or delimited string."""
    if value is None:
        return False
    entries = value if isinstance(value, (list, tuple)) else re.split(f"[{re.escape(separators)}]", str(value))
    wanted = token.lower()
    return any(entry is not None and str(entry).strip().lower() == wanted for entry in entries)


def _state_prek_fields(state, flag, record):
    """``state_prek`` plus the brand, which is set only when the flag is True."""
    if flag is None or record.get("state_prek") is not None:
        return {}  # nothing to say, or `state_prek` already set (the brand must agree with it)
    fields = {"state_prek": flag}
    if flag:
        fields["state_prek_program"] = STATE_PREK_PROGRAMS[state]
    return fields


def _register_state_prek_bool(state, source):
    @register_adapter(state, sources=[source], fills=["state_prek", "state_prek_program"])
    def adapter(record):
        """Bool `{source}` -> `state_prek`; program brand set only when True."""
        return _state_prek_fields(state, to_bool(record.get(source)), record)

    adapter.__doc__ = f"`{source}` (bool/Yes-No) -> `state_prek`; program brand set only when True."
    return adapter


def _register_state_prek_token(state, source, token, separators):
    @register_adapter(state, sources=[source], fills=["state_prek", "state_prek_program"])
    def adapter(record):
        return _state_prek_fields(state, True if _has_entry(record.get(source), token, separators) else None, record)

    adapter.__doc__ = f"`{source}` entry `{token}` -> `state_prek` True; absent -> unset (never False)."
    return adapter


for _state, _source in _STATE_PREK_BOOL_SOURCES.items():
    _register_state_prek_bool(_state, _source)
for _state, (_source, _token, _separators) in _STATE_PREK_TOKEN_SOURCES.items():
    _register_state_prek_token(_state, _source, _token, _separators)


# --------------------------------------------------------------------------- #
# Night, evening and weekend care
# --------------------------------------------------------------------------- #
_NONTRADITIONAL_FLAGS = ("night_care", "evening_care", "weekend_care")
_AL_TIME_RE = re.compile(r"\d{1,2}:\d{2}\s*[ap]m", re.IGNORECASE)


def capacity_to_bool(value):
    """Capacity count (int, ``"8"``, junk ``"0 0"``) -> ``> 0`` bool; blank -> ``None``.

    ``0`` and whitespace-separated all-zero junk (``"0 0"``) are ``False``. Any other
    unparseable value is logged and left unset.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str):
        if not value.strip() or _is_sentinel(value):
            return None
        parts = value.split()
        if len(parts) > 1:
            numbers = [to_int(part) for part in parts]
            if all(number == 0 for number in numbers):
                return False
            logger.warning("capacity_to_bool: unparseable capacity %r -> None", value)
            return None
    number = to_int(value)
    if number is None or number < 0:
        return None
    return number > 0


def _al_night_care(value):
    """AL `07:00 PM - 11:00 PM` -> True (any real range); `N/A - N/A` -> False; blank -> ``None``."""
    if value is None or not str(value).strip():
        return None
    text = str(value)
    if _AL_TIME_RE.search(text):
        return True
    if all(_is_sentinel(part.strip()) for part in text.split("-")):
        return False
    logger.warning("al_nighttime_hours: unrecognised value %r -> None", value)
    return None


def _nontraditional_result(record, flags):
    """Specific ``flags`` plus the roll-up: True if any is True; never False from specifics."""
    result = {name: value for name, value in flags.items() if value is not None}
    combined = {name: record.get(name) for name in _NONTRADITIONAL_FLAGS} | result
    if any(value is True for value in combined.values()):
        result["nontraditional_hours"] = True
    return result


def _register_nontraditional(state, sources, extract, doc):
    """Register an adapter filling the specific flags ``extract(record)`` returns plus the roll-up."""

    @register_adapter(state, sources=sources, fills=[*_NONTRADITIONAL_FLAGS, "nontraditional_hours"])
    def adapter(record):
        return _nontraditional_result(record, extract(record))

    adapter.__doc__ = doc
    return adapter


_register_nontraditional(
    "louisiana",
    ["la_night_care"],
    lambda r: {"night_care": to_bool(r.get("la_night_care"))},
    "`la_night_care` (bool) -> `night_care`; roll-up True when night care is offered.",
)
_register_nontraditional(
    "wyoming",
    ["wy_evening_care", "wy_weekend_care"],
    lambda r: {
        "evening_care": to_bool(r.get("wy_evening_care")),
        "weekend_care": to_bool(r.get("wy_weekend_care")),
    },
    "`wy_evening_care`/`wy_weekend_care` (bool) -> `evening_care`/`weekend_care`; roll-up True if either.",
)
_register_nontraditional(
    "alabama",
    ["al_nighttime_hours"],
    lambda r: {"night_care": _al_night_care(r.get("al_nighttime_hours"))},
    "`al_nighttime_hours` any time range -> `night_care` True; `N/A - N/A` -> False; blank -> unset.",
)
_register_nontraditional(
    "illinois",
    ["il_night_capacity"],
    lambda r: {"night_care": capacity_to_bool(r.get("il_night_capacity"))},
    "`il_night_capacity` (string int) -> `night_care` = capacity > 0.",
)
_register_nontraditional(
    "wisconsin",
    ["wi_night_capacity"],
    lambda r: {"night_care": capacity_to_bool(r.get("wi_night_capacity"))},
    "`wi_night_capacity` -> `night_care` = capacity > 0 (junk `0 0` -> False).",
)
_register_nontraditional(
    "north_carolina",
    ["nc_capacity_second_shift", "nc_capacity_third_shift"],
    lambda r: {
        "evening_care": capacity_to_bool(r.get("nc_capacity_second_shift")),
        "night_care": capacity_to_bool(r.get("nc_capacity_third_shift")),
    },
    "`nc_capacity_second_shift`/`nc_capacity_third_shift` (int) -> `evening_care`/`night_care` = capacity > 0.",
)


def _ok_hours_flags(record):
    labels = ok_care_type_set(record.get("ok_care_types"))
    return {
        "evening_care": True if "evening hours" in labels else None,
        "weekend_care": True if "weekend hours" in labels else None,
        "night_care": True if "overnight hours" in labels else None,
    }


_register_nontraditional(
    "oklahoma",
    ["ok_care_types"],
    _ok_hours_flags,
    "OK `ok_care_types` Evening/Weekend/Overnight Hours -> evening/weekend/night care True; absent -> unset.",
)


@register_adapter("washington_dc", sources=["dc_nontraditional_hours"], fills=["nontraditional_hours"])
def washington_dc_nontraditional_hours(record):
    """DC `dc_nontraditional_hours` (bool) -> the `nontraditional_hours` roll-up only; False is a real no."""
    return {"nontraditional_hours": to_bool(record.get("dc_nontraditional_hours"))}


# --------------------------------------------------------------------------- #
# Care schedules (FL, MS, SD, RI, WI, ND, IA, VT, OK, MI, GA, LA, MA, NH, NJ, WY)
# --------------------------------------------------------------------------- #
# Fixed token list, in the order ``care_schedules`` is emitted.
CARE_SCHEDULE_TOKENS = (
    "before_school",
    "after_school",
    "drop_in",
    "full_day",
    "half_day",
    "full_time",
    "part_time",
    "summer",
    "school_holidays",
    "emergency",
    "flexible",
    "sick_care",
)

# Raw state label -> canonical token(s). One raw value may mean several tokens.
# Lookups are case-insensitive, whitespace-trimmed and apostrophe-tolerant (a
# curly ’ equals '), which also covers NH's UPPERCASE labels. Anything in
# neither this table nor ``CARE_SCHEDULE_IGNORED`` is logged and dropped.
CARE_SCHEDULE_LABELS = {
    # before_school / after_school
    "Before School": ("before_school",),
    "Before school care": ("before_school",),
    "Before School Care provided.": ("before_school",),
    "Before and After School": ("before_school", "after_school"),  # VT
    "Before/After School": ("before_school", "after_school"),  # MI
    "After School": ("after_school",),
    "School Age After School": ("after_school",),  # MS
    "After school care": ("after_school",),  # RI
    "After School Care provided.": ("after_school",),  # WI
    # drop_in
    "Drop In": ("drop_in",),  # FL, GA, OK
    "Drop-in": ("drop_in",),
    "Drop In Care": ("drop_in",),
    "Drop-in Care": ("drop_in",),
    "Drop-In Care": ("drop_in",),  # WY
    "Drop In Care provided.": ("drop_in",),  # WI
    "Drop-in/Hourly": ("drop_in",),  # ND
    "Hourly Care (Only)": ("drop_in",),
    "Drop in": ("drop_in",),  # GA
    "Parent’s night out": ("drop_in",),  # GA
    "Parent’s morning out": ("drop_in",),  # GA
    "Drop-in care": ("drop_in",),  # NJ
    # full_day / half_day
    "Full Day": ("full_day",),
    "Full day care": ("full_day",),  # RI
    "Full day": ("full_day",),  # MA
    "Full Day Care": ("full_day",),  # WY
    "Half Day": ("half_day",),
    "Half Day Morning": ("half_day",),
    "Half Day Afternoon": ("half_day",),
    "Half Day Morning & Afternoon": ("half_day",),
    "Half-day care (defined as 4 hours or less of care per day)": ("half_day",),  # RI
    "Part Day": ("half_day",),
    "Part day": ("half_day",),
    "Half Day Care": ("half_day",),  # WY
    # full_time / part_time
    "Full-time": ("full_time",),
    "Full-Time": ("full_time",),
    "Full Time": ("full_time",),  # GA
    "Full week": ("full_time",),  # MA
    "Part-time": ("part_time",),
    "Part-Time": ("part_time",),
    "Part Time": ("part_time",),  # GA
    "Partial week (less than 5 days a week care)": ("part_time",),
    "Part week": ("part_time",),  # MA
    # summer
    "Summer Day Camp": ("summer",),
    "Summer Camp": ("summer",),  # GA
    "Before Camp Care": ("summer",),
    "After Camp Care": ("summer",),
    "School age care - summer vacation": ("summer",),
    "Summer Vacation": ("summer",),
    "Summer Only": ("summer",),  # OK
    # school_holidays
    "Days when school is not in session": ("school_holidays",),  # IA
    "School age care - school vacations": ("school_holidays",),
    "School age care - holidays/professional development days/snow day care": ("school_holidays",),
    "Open school breaks": ("school_holidays",),  # GA
    "Open school holidays": ("school_holidays",),
    "Open school vacation week": ("school_holidays",),  # MA
    "Open Holidays": ("school_holidays",),  # IA
    "Holidays": ("school_holidays",),  # NJ
    "School Vacation Only": ("school_holidays",),  # OK
    # emergency
    "Temporary/Emergency": ("emergency",),
    "Temp/Emergency": ("emergency",),  # IA
    "Temporary Care": ("emergency",),  # NJ
    "Emergency Care": ("emergency",),  # NJ
    # flexible
    "Flexible Schedule": ("flexible",),
    "Flexible Opening Hours": ("flexible",),
    "Flexible Closing Hours": ("flexible",),
    "Rotating": ("flexible",),  # IA
    "Rotating Care provided.": ("flexible",),  # WI
    "Rotating Shifts": ("flexible",),  # NJ
    "Intermittent Care": ("flexible",),  # NJ
    # sick_care
    "Sick Care": ("sick_care",),  # OK
    "Sick Care provided.": ("sick_care",),  # WI
    "Sick care": ("sick_care",),  # NJ
}

# Known labels that are deliberately not schedule tokens, so they are dropped
# silently (not logged): hours/year-round labels owned by the nontraditional
# hours and ``year_round`` fields, other services, and "none" sentinels.
CARE_SCHEDULE_IGNORED = {
    # routed to nontraditional hours / year_round
    "Night Care",
    "Night care (care after 7pm on a consistent basis)",  # WI
    "Weekend Care",
    "Overnight",
    "Overnight Care",
    "Evening",
    "24-Hour",
    "24 Hour",
    "Second Shift",
    "Third Shift/Overnight",
    "Weekends",
    "Weekend (Sat and/or Sun)",
    "Some weekends",
    "Some overnights",
    "Night Time",
    "Non-Standard Care",
    "Daytime",
    "Year Round",
    "School Year Only",
    "Daytime Hours",
    "Evening Hours",
    "Weekend Hours",
    "Overnight Hours",
    # not schedules: programs, services, payment, age groups
    "Food Served",
    "Food Service",
    "Infant Care",
    "Transportation",
    "Transportation to/from public schools",
    "Special Needs",
    "Head Start",
    "Early Head Start",
    "Pre-School",
    "School Age",
    "School Age Exempt",
    "Infant/Toddler",
    "GSRP",
    "Kindergarten",
    "Swimming",
    "Snacks included",
    "Meals included",
    "Accepts DCYF/005 payment",
    "Accepts CCAP as a form of payment",
    "Ability to work with children who have an IEP/Early Intervention Services",
    "Offers Kids Connect and/or additional support to children with behavioral needs",
    "Respite for children or parents with special needs",
    "Other (please specify)",
    "Other -Ex. teen care parenting",
    # WY service names that are not schedules
    "Provides Transportation",
    "Meals and Snacks Provided",
    "Child and Adult Food Program",
    # sentinels
    "No Additional Services Offered",
    "None of the additional care schedules apply",
}


def _care_schedule_key(label):
    """Lookup key: trimmed, lower-cased, curly apostrophe folded to a straight one."""
    return str(label).strip().lower().replace("’", "'")


_CARE_SCHEDULE_LOOKUP = {_care_schedule_key(k): v for k, v in CARE_SCHEDULE_LABELS.items()}
_CARE_SCHEDULE_IGNORED_KEYS = {_care_schedule_key(k) for k in CARE_SCHEDULE_IGNORED}


def care_schedule_tokens(labels):
    """Raw labels -> de-duplicated canonical tokens in ``CARE_SCHEDULE_TOKENS`` order.

    Ignored labels and sentinels are dropped silently; an unknown label is
    logged (so the table can be extended) and dropped.
    """
    found = set()
    for label in labels:
        if label is None or not str(label).strip() or _is_sentinel(label):
            continue
        key = _care_schedule_key(label)
        if key in _CARE_SCHEDULE_IGNORED_KEYS:
            continue
        tokens = _CARE_SCHEDULE_LOOKUP.get(key)
        if tokens is None:
            logger.warning("care_schedules: unmapped value %r -> dropped", label)
            continue
        found.update(tokens)
    return sorted(found, key=CARE_SCHEDULE_TOKENS.index)


def _register_care_schedules(state, sources, extract, doc):
    """Register an adapter filling ``care_schedules`` from the labels ``extract(record)`` returns."""

    @register_adapter(state, sources=sources, fills=["care_schedules"])
    def adapter(record):
        return {"care_schedules": care_schedule_tokens(extract(record))}

    adapter.__doc__ = doc
    adapter.__name__ = f"{state}_care_schedules"
    return adapter


def _schedule_source(field, separator=";"):
    """Extractor for one list-or-``separator``-joined source field."""
    return lambda record: _special_needs_labels(record.get(field), separator)


for _state, _field, _separator, _shape in (
    ("florida", "fl_services", ";", "list"),
    ("mississippi", "ms_services", ";", "list"),
    ("south_dakota", "sd_services_offered", ";", "list"),
    ("rhode_island", "ri_services_offered", ";", "list"),
    ("wisconsin", "wi_special_care_types", ";", "list"),
    ("north_dakota", "nd_supplemental_care", ";", "list"),
    ("massachusetts", "ma_schedule_options", ";", "list"),
    ("wyoming", "wy_services", ";", "list"),
    ("new_jersey", "nj_special_schedules_labels", ";", "list"),
    ("iowa", "ia_care_types", ",", "`, `-separated string"),
    ("vermont", "vt_type_of_care", ",", "`, `-separated string"),
    ("michigan", "mi_services_provided", ";", "`;`-separated string"),
    ("new_hampshire", "nh_schedule_options", ";", "`;`-separated UPPERCASE string"),
):
    _register_care_schedules(
        _state,
        [_field],
        _schedule_source(_field, _separator),
        f"{_state.replace('_', ' ').title()} `{_field}` ({_shape}) -> `care_schedules` via the explicit label table.",
    )

_register_care_schedules(
    "oklahoma",
    ["ok_care_types"],
    lambda record: sorted(ok_care_type_set(record.get("ok_care_types"))),
    "OK `ok_care_types` (list) -> `care_schedules` (Drop In, Sick Care, Summer Only, School Vacation Only).",
)
_register_care_schedules(
    "georgia",
    ["ga_special_hours", "ga_accepts_children_type", "ga_summer_camp"],
    lambda record: [
        label
        for field in ("ga_special_hours", "ga_accepts_children_type", "ga_summer_camp")
        for label in _special_needs_labels(record.get(field), ";")
    ],
    "GA `ga_special_hours`, `ga_accepts_children_type`, `ga_summer_camp` (`; `-separated) merged -> `care_schedules`.",
)
_register_care_schedules(
    "louisiana",
    ["la_before_care", "la_after_care"],
    lambda record: [
        label
        for field, label in (("la_before_care", "Before School"), ("la_after_care", "After School"))
        if to_bool(record.get(field)) is True
    ],
    "LA `la_before_care`/`la_after_care` (bool) -> `before_school`/`after_school`; False adds nothing.",
)


# --------------------------------------------------------------------------- #
# Discounts (GA, MA, NH, VT, NJ)
# --------------------------------------------------------------------------- #
# Fixed token list, in the order ``discounts`` is emitted.
DISCOUNT_TOKENS = ("sibling", "sliding_scale", "employee", "military", "other")

# Raw state label -> canonical token. Lookups are case-insensitive and
# whitespace-trimmed, which also covers NH's UPPERCASE labels. Anything in
# neither this table nor ``DISCOUNT_IGNORED`` is logged and dropped.
DISCOUNT_LABELS = {
    # sibling
    "Multi-Child Discount": "sibling",  # GA, NH
    "Sibling Discounts": "sibling",  # MA
    "Discount provided for siblings": "sibling",  # NJ
    # sliding_scale
    "Sliding Scale Fee": "sliding_scale",  # GA
    "Sliding Fee Scale": "sliding_scale",  # MA, NH
    "Discounts provided on a sliding scale": "sliding_scale",  # NJ
    # employee
    "Employee Discounts": "employee",  # MA
    "Discounts provided for employees": "employee",  # NJ
    # military
    "Discounts provided for parents in military service": "military",  # NJ
    # other
    "Other": "other",  # MA
}

# Known subsidy/scholarship labels: they feed ``scholarships_accepted`` and are
# not discounts, so they are dropped silently (not logged).
DISCOUNT_IGNORED = {
    "Scholarship",  # GA
    "EEC Subsidies",  # MA
    "Private Scholarships/Grants",  # MA
    "Head Start",  # MA
    "STATE CC ASSISTANCE (SCHOLARSHIP)",  # NH
    "PREVENTIVE AND PROTECTIVE CARE",  # NH
}


def _discount_key(label):
    """Lookup key: trimmed and lower-cased."""
    return str(label).strip().lower()


_DISCOUNT_LOOKUP = {_discount_key(k): v for k, v in DISCOUNT_LABELS.items()}
_DISCOUNT_IGNORED_KEYS = {_discount_key(k) for k in DISCOUNT_IGNORED}


def discount_tokens(labels):
    """Raw labels -> de-duplicated canonical tokens in ``DISCOUNT_TOKENS`` order, or ``None``.

    Subsidy labels and sentinels are dropped silently; an unknown label is
    logged (so the table can be extended) and dropped. No discount at all is
    ``None``, never ``[]``.
    """
    found = set()
    for label in labels:
        if label is None or not str(label).strip() or _is_sentinel(label):
            continue
        key = _discount_key(label)
        if key in _DISCOUNT_IGNORED_KEYS:
            continue
        token = _DISCOUNT_LOOKUP.get(key)
        if token is None:
            logger.warning("discounts: unmapped value %r -> dropped", label)
            continue
        found.add(token)
    return sorted(found, key=DISCOUNT_TOKENS.index) or None


def _register_discounts(state, sources, extract, doc):
    """Register an adapter filling ``discounts`` from the labels ``extract(record)`` returns."""

    @register_adapter(state, sources=sources, fills=["discounts"])
    def adapter(record):
        return {"discounts": discount_tokens(extract(record))}

    adapter.__doc__ = doc
    adapter.__name__ = f"{state}_discounts"
    return adapter


for _state, _field, _separator, _shape in (
    ("georgia", "ga_financial_info", ";", "`; `-separated string"),
    ("massachusetts", "ma_financial_assistance", ";", "list"),
    ("new_hampshire", "nh_financial_assistance", ",", "`, `-separated UPPERCASE string"),
    ("new_jersey", "nj_discounts_labels", ";", "list of resolved labels"),
):
    _register_discounts(
        _state,
        [_field],
        _schedule_source(_field, _separator),
        f"{_state.replace('_', ' ').title()} `{_field}` ({_shape}) -> `discounts` via the explicit label table; "
        "subsidy labels are skipped (they feed `scholarships_accepted`).",
    )

_register_discounts(
    "vermont",
    ["vt_sibling_discount"],
    lambda record: ["Sibling Discounts"] if str(record.get("vt_sibling_discount")).strip().lower() == "yes" else [],
    "VT `vt_sibling_discount` (Yes/No) -> `discounts`: Yes -> `sibling`, No -> None.",
)


# --------------------------------------------------------------------------- #
# Environment features (GA, MA, NH, UT, VT, NJ)
# --------------------------------------------------------------------------- #
# Fixed token list, in the order ``environment_features`` is emitted.
ENVIRONMENT_TOKENS = (
    "smoke_free",
    "fenced_yard",
    "outdoor_play",
    "air_conditioned",
    "peanut_free",
    "secure_entry",
    "video_surveillance",
    "parent_webcam",
    "pool",
    "sports_facilities",
    "separate_play_areas",
    "field_trips",
    "near_public_transit",
    "no_tv",
)

# Raw state label -> canonical token. Lookups are case-insensitive and
# whitespace-trimmed, which also covers NH's UPPERCASE labels. Anything that is
# in none of this table, ``ENVIRONMENT_IGNORED`` or the skip patterns below is
# logged and dropped.
ENVIRONMENT_LABELS = {
    # smoke_free
    "Smoke Free Environment": "smoke_free",  # GA
    "Smoke Free": "smoke_free",  # MA, NH, VT
    "Smoke-free Environment": "smoke_free",  # UT, NJ
    # fenced_yard
    "Fenced Yard": "fenced_yard",  # MA, VT
    "Fenced In Yard": "fenced_yard",  # NH
    "Fenced-in play area": "fenced_yard",  # NJ
    # outdoor_play
    "Outdoor Play area": "outdoor_play",  # GA
    "Outdoor Play Equipment": "outdoor_play",  # NH
    # air_conditioned
    "Air Conditioned": "air_conditioned",  # MA, NH
    # peanut_free
    "Peanut Free": "peanut_free",  # MA, NH
    # secure_entry
    "Secure Grounds/Keypad Access": "secure_entry",  # GA
    # video_surveillance
    "Video surveillance": "video_surveillance",  # GA
    # parent_webcam
    "Webcam for parents": "parent_webcam",  # GA
    # pool
    "Pool": "pool",  # GA
    "Adult Pool": "pool",  # MA
    "Wading Pool": "pool",  # NH
    # sports_facilities
    "Sports fields": "sports_facilities",  # GA
    "Tennis Courts": "sports_facilities",  # GA
    # separate_play_areas
    "Separated play areas": "separate_play_areas",  # NJ
    # field_trips
    "Field Trips Taken": "field_trips",  # MA, NH
    # near_public_transit
    "Near Public Transportation": "near_public_transit",  # UT
    "Public Transportation": "near_public_transit",  # NH
    # no_tv
    "No TV Use": "no_tv",  # NH
}

# Known labels that are not environment amenities. They are dropped silently
# (not logged): wheelchair/handicap labels feed ``wheelchair_accessible`` and
# the rest describe the program, not the facility.
ENVIRONMENT_IGNORED = {
    "Wheelchair Accessible",  # MA, NH
    "Not Wheelchair Accessible",  # NH
    "Handicap accessible",  # NJ
    "Approved Assistant",  # MA, NH
    "School Bus Route",  # NH
    "Provide your own equipment",  # GA
}

# Whole-word pet labels ("No pets", "NO CAT", "INDOOR PETS", "OUTDOOR PETS",
# "DOG"...) belong to a separate pets field; "MIXED AGE (0-3)" style labels are
# age-grouping notes; "Unknown (code N)" is the NJ spider's placeholder for
# environmental-feature codes 6-13, which have no known label (it already logs
# an aggregate count). All are dropped silently.
_ENVIRONMENT_SKIP_PATTERNS = (
    re.compile(r"\b(pets?|cat|dog)\b"),
    re.compile(r"^mixed age \("),
    re.compile(r"^unknown \(code \d+\)$"),
)


def _environment_key(label):
    """Lookup key: trimmed and lower-cased."""
    return str(label).strip().lower()


_ENVIRONMENT_LOOKUP = {_environment_key(k): v for k, v in ENVIRONMENT_LABELS.items()}
_ENVIRONMENT_IGNORED_KEYS = {_environment_key(k) for k in ENVIRONMENT_IGNORED}


def environment_tokens(labels):
    """Raw labels -> de-duplicated canonical tokens in ``ENVIRONMENT_TOKENS`` order, or ``None``.

    Pet, wheelchair, non-amenity and NJ placeholder labels (and sentinels) are
    dropped silently; an unknown label is logged (so the table can be extended)
    and dropped. No feature at all is ``None``, never ``[]``.
    """
    found = set()
    for label in labels:
        if label is None or not str(label).strip() or _is_sentinel(label):
            continue
        key = _environment_key(label)
        if key in _ENVIRONMENT_IGNORED_KEYS or any(p.search(key) for p in _ENVIRONMENT_SKIP_PATTERNS):
            continue
        token = _ENVIRONMENT_LOOKUP.get(key)
        if token is None:
            logger.warning("environment_features: unmapped value %r -> dropped", label)
            continue
        found.add(token)
    return sorted(found, key=ENVIRONMENT_TOKENS.index) or None


def _register_environment(state, field, separator, shape):
    """Register an adapter filling ``environment_features`` from one source field."""

    @register_adapter(state, sources=[field], fills=["environment_features"])
    def adapter(record):
        return {"environment_features": environment_tokens(_schedule_source(field, separator)(record))}

    adapter.__doc__ = (
        f"{state.replace('_', ' ').title()} `{field}` ({shape}) -> `environment_features` via the explicit label "
        "table; pet and wheelchair labels are skipped (they belong to other fields)."
    )
    adapter.__name__ = f"{state}_environment_features"
    return adapter


for _state, _field, _separator, _shape in (
    ("georgia", "ga_environment", ";", "`; `-separated string"),
    ("massachusetts", "ma_environment", ";", "list"),
    ("new_hampshire", "nh_environment", ",", "`, `-separated UPPERCASE string"),
    ("utah", "ut_environment", ",", "`,`-separated string"),
    ("vermont", "vt_area_description", ",", "`, `-separated string"),
    ("new_jersey", "nj_environmental_features_labels", ";", "list of resolved labels"),
):
    _register_environment(_state, _field, _separator, _shape)


# Pets (ID, VT, GA, UT, NJ, NH)
# --------------------------------------------------------------------------- #
PET_TYPES = ("dogs", "cats", "other")
PETS_EXCLUDABLE = ("dogs", "cats")


def _pets_fields(state, *, no_pets=False, pets=False, types=(), excluded=()):
    """Three-field pets dict from parsed flags; ``None`` wherever the source is silent.

    ``no_pets`` is an explicit "no pets", ``pets`` a bare "has pets" (type
    unknown), ``types`` the animal types present and ``excluded`` the dogs/cats
    the source says are absent. Contradictory flags are logged and the
    conflicting fields are left ``None``. Excluded types alone never imply pets.
    """
    types, excluded = set(types), set(excluded)
    has_pets_conflict = no_pets and (pets or types)
    if has_pets_conflict:
        logger.warning("pets(%s): 'no pets' together with pets/types -> has_pets, pet_types left None", state)
        types = set()
    for kind in sorted(types & excluded):
        logger.warning("pets(%s): %s both present and excluded -> dropped from both", state, kind)
        types.discard(kind)
        excluded.discard(kind)
        has_pets_conflict = True
    if has_pets_conflict:
        has_pets = True if (pets or types) and not no_pets else None
    elif no_pets:
        has_pets = False
    else:
        has_pets = True if (pets or types) else None
    return {
        "has_pets": has_pets,
        "pet_types": [t for t in PET_TYPES if t in types] or None,
        "pets_excluded": [t for t in PETS_EXCLUDABLE if t in excluded] or None,
    }


def _register_pets(state, sources, parse, doc):
    """Register an adapter filling the three pets fields from ``parse(record)`` -> ``_pets_fields`` kwargs."""

    @register_adapter(state, sources=sources, fills=["has_pets", "pet_types", "pets_excluded"])
    def adapter(record):
        return _pets_fields(state, **parse(record))

    adapter.__doc__ = doc
    adapter.__name__ = f"{state}_pets"
    return adapter


_ID_PET_POLICY = {
    "pet free": {"no_pets": True},
    "dogs only": {"types": ["dogs"]},
    "cats only": {"types": ["cats"]},
    "other": {"types": ["other"]},
}


def _idaho_pets(record):
    raw = record.get("id_pet_policy")
    if raw is None or not str(raw).strip() or _is_sentinel(raw):
        return {}
    parsed = _ID_PET_POLICY.get(str(raw).strip().lower())
    if parsed is None:
        logger.warning("pets(idaho): unmapped id_pet_policy %r -> None", raw)
    return parsed or {}


_register_pets(
    "idaho",
    ["id_pet_policy"],
    _idaho_pets,
    "ID `id_pet_policy` (Pet Free / Dogs only / Cats only / Other) -> `has_pets`, `pet_types`; unknown values logged.",
)

_VT_PET_TOKENS = {
    "dog": "dogs",
    "cat": "cats",
    "other furry animal": "other",
    "other non-furry animal": "other",
}


def _vermont_pets(record):
    types = []
    for label in _schedule_source("vt_pets", ",")(record):
        if _is_sentinel(label):
            continue
        kind = _VT_PET_TOKENS.get(label.lower())
        if kind is None:
            logger.warning("pets(vermont): unmapped vt_pets entry %r -> dropped", label)
            continue
        types.append(kind)
    return {"types": types}


_register_pets(
    "vermont",
    ["vt_pets"],
    _vermont_pets,
    "VT `vt_pets` (`, `-separated Dog / Cat / Other ... Animal`) -> `has_pets`, `pet_types`; absent = None, not False.",
)


def _label_pets_parser(field, separator):
    """Parser for the "No pets" / "Pets" entries inside an environment list."""

    def parse(record):
        keys = {label.lower() for label in _schedule_source(field, separator)(record)}
        return {"no_pets": "no pets" in keys, "pets": "pets" in keys}

    return parse


for _state, _field, _separator, _doc in (
    ("georgia", "ga_environment", ";", "GA `ga_environment` entry `No pets` -> `has_pets` False; absent = None."),
    (
        "utah",
        "ut_environment",
        ",",
        "UT `ut_environment` `No Pets` -> `has_pets` False, `Pets` -> True; absent = None.",
    ),
    (
        "new_jersey",
        "nj_environmental_features_labels",
        ";",
        "NJ `nj_environmental_features_labels` label `No pets` -> `has_pets` False; absent = None.",
    ),
):
    _register_pets(_state, [_field], _label_pets_parser(_field, _separator), _doc)

_NH_PETS_ANY = {"pets", "indoor pets", "outdoor pets"}
_NH_PETS_TYPES = {"dog": "dogs", "dogs": "dogs", "cat": "cats", "cats": "cats"}
_NH_PETS_NEGATIVES = {f"no {k}": v for k, v in _NH_PETS_TYPES.items()}


def _new_hampshire_pets(record):
    keys = {label.lower() for label in _schedule_source("nh_environment", ",")(record)}
    return {
        "no_pets": "no pets" in keys,
        "pets": bool(keys & _NH_PETS_ANY),
        "types": [v for k, v in _NH_PETS_TYPES.items() if k in keys],
        "excluded": [v for k, v in _NH_PETS_NEGATIVES.items() if k in keys],
    }


_register_pets(
    "new_hampshire",
    ["nh_environment"],
    _new_hampshire_pets,
    "NH `nh_environment` pet entries (NO PETS / PETS / DOG / CAT) -> `has_pets`, `pet_types`; NO DOG / NO CAT -> "
    "`pets_excluded` only.",
)


# --------------------------------------------------------------------------- #
# Rates (CT, GA, KY, PA, TN, MA, NJ, WY; NH deliberately skipped)
# --------------------------------------------------------------------------- #
RATE_PERIODS = {
    "hour": "hour",
    "hourly": "hour",
    "day": "day",
    "daily": "day",
    "week": "week",
    "weekly": "week",
    "month": "month",
    "monthly": "month",
}
RATE_UNITS = {
    "full_time": "full_time",
    "full-time": "full_time",
    "full time": "full_time",
    "full": "full_time",
    "part_time": "part_time",
    "part-time": "part_time",
    "part time": "part_time",
    "part": "part_time",
}


def rate_row(age_group, amount, *, unit=None, period=None, context="", **extra):
    """One canonical ``rates`` row, or ``None`` when the amount is blank, unparseable or <= 0.

    ``extra`` carries the optional ``schedule_type`` / ``care_type`` keys (omitted when blank).
    Unparseable amounts are logged by ``to_float``.
    """
    value = to_float(amount)
    if value is None or value <= 0:
        return None
    row = {
        "age_group": str(age_group).strip() if age_group is not None else "",
        "unit": unit,
        "period": period,
        "amount": value,
    }
    row.update({key: val for key, val in extra.items() if val})
    return row


def _rate_rows(rows):
    return [row for row in rows if row is not None]


def _register_rates(state, sources, extract, doc):
    """Register an adapter filling ``rates`` from the rows ``extract(record)`` returns."""

    @register_adapter(state, sources=sources, fills=["rates"])
    def adapter(record):
        return {"rates": _rate_rows(extract(record))}

    adapter.__doc__ = doc
    adapter.__name__ = f"{state}_rates"
    return adapter


def _dict_entries(value, context):
    """The dict entries of a list-valued source; anything else is logged and skipped."""
    for entry in value if isinstance(value, list) else []:
        if isinstance(entry, dict):
            yield entry
        else:
            logger.warning("%s: non-dict entry %r dropped", context, entry)


def _unit_period_column(column):
    """``full_time_weekly`` -> ``("full_time", "week")``; ``None`` when not a ``{unit}_{period}`` name."""
    unit_text, _, period_text = column.rpartition("_")
    unit = RATE_UNITS.get(unit_text)
    period = RATE_PERIODS.get(period_text)
    return (unit, period) if unit and period else None


def _connecticut_rates(record):
    rows = []
    for entry in _dict_entries(record.get("ct_rates"), "rates(connecticut)"):
        age_group = entry.get("age_group")
        label = entry.get("label")
        name = f"{age_group} ({label})" if age_group and label else (age_group or label)
        for column, amount in entry.items():
            if column in ("age_group", "label"):
                continue
            parsed = _unit_period_column(column)
            if parsed is None:
                logger.warning("rates(connecticut): unknown rate column %r ignored", column)
                continue
            rows.append(rate_row(name, amount, unit=parsed[0], period=parsed[1]))
    return rows


_register_rates(
    "connecticut",
    ["ct_rates"],
    _connecticut_rates,
    "CT `ct_rates`: one row per `{unit}_{period}` amount column; age_group is `Infant (0-17 Months)` style.",
)
_register_rates(
    "georgia",
    ["ga_weekly_rates"],
    lambda record: [
        rate_row(e.get("age"), e.get("weekly_full_day"), unit="full_time", period="week")
        for e in _dict_entries(record.get("ga_weekly_rates"), "rates(georgia)")
    ],
    "GA `ga_weekly_rates`: full-day weekly rate per age -> full_time / week row.",
)


def _full_part_rates(state, source, full_key, part_key, period):
    """Register a state whose rows carry full/part-time amounts with one inferred ``period``."""
    _register_rates(
        state,
        [source],
        lambda record: [
            rate_row(e.get("age_group"), e.get(key), unit=unit, period=period)
            for e in _dict_entries(record.get(source), f"rates({state})")
            for key, unit in ((full_key, "full_time"), (part_key, "part_time"))
        ],
        f"{state.replace('_', ' ').title()} `{source}` -> full_time/part_time rows; "
        f"period `{period}` is inferred from the amounts.",
    )


_full_part_rates("kentucky", "ky_service_costs", "full_time_cost", "part_time_cost", "day")
_full_part_rates("pennsylvania", "pa_cost_table", "full_time_rate", "part_time_rate", "day")

_TN_UNIT_OF_CARE = {"full-time": ("full_time", "week"), "part-time": ("part_time", "week")}
# Descriptors whose amount is not a weekly figure (sample medians: Hourly care $12,
# Drop In $15). Drop In may be hourly or daily, so its period is left unknown.
_TN_DESCRIPTOR_PERIODS = {"hourly care": "hour", "drop in": None}


def _tennessee_rates(record):
    rows = []
    for entry in _dict_entries(record.get("tn_age_group_rates"), "rates(tennessee)"):
        descriptor = str(entry.get("unit_of_care") or "").strip()
        key = descriptor.lower()
        care_type = None
        if key in _TN_UNIT_OF_CARE:
            unit, period = _TN_UNIT_OF_CARE[key]
        elif key in ("weekly", "daily", "monthly"):
            unit, period = None, RATE_PERIODS[key]
        elif key in _TN_DESCRIPTOR_PERIODS:
            unit, period, care_type = None, _TN_DESCRIPTOR_PERIODS[key], descriptor
        else:
            # Before/After, Summer, Sch out, Yearly ...: keep the label, the amount is still a weekly figure.
            unit, period, care_type = None, "week", descriptor
        rows.append(
            rate_row(entry.get("age_group"), entry.get("weekly_rate"), unit=unit, period=period, care_type=care_type)
        )
    return rows


_register_rates(
    "tennessee",
    ["tn_age_group_rates"],
    _tennessee_rates,
    "TN `tn_age_group_rates`: unit_of_care sets unit (Full/Part-time) or period (Weekly/Daily/Monthly); "
    "other descriptors -> `care_type`; `$0.00` skipped.",
)

_MA_RATE_TYPE_RE = re.compile(r"^\s*(hourly|daily|weekly|monthly)\s+(full|part)\s+time\s*$", re.IGNORECASE)
_MA_OTHER_RATE_TYPE_RE = re.compile(r"^\s*other\s+(full|part)\s+time\s*$", re.IGNORECASE)


def _massachusetts_rates(record):
    entries = list(_dict_entries(record.get("ma_cost_table"), "rates(massachusetts)"))
    chosen = _pick_preferred_schedule_type(entries)
    if chosen is None:
        return []
    wanted = str(chosen.get("schedule_type") or "").strip().lower()
    rows = []
    for entry in entries:
        if str(entry.get("schedule_type") or "").strip().lower() != wanted:
            continue
        rate_type = str(entry.get("rate_type") or "")
        match = _MA_RATE_TYPE_RE.match(rate_type)
        other = _MA_OTHER_RATE_TYPE_RE.match(rate_type)
        if match:
            period, unit = RATE_PERIODS[match.group(1).lower()], RATE_UNITS[match.group(2).lower()]
        elif other:
            # "Other Full/Part Time": the period is genuinely unknown (amounts run $12 to $24k).
            period, unit = None, RATE_UNITS[other.group(1).lower()]
        else:
            logger.warning("rates(massachusetts): unrecognised rate_type %r", entry.get("rate_type"))
            period = unit = None
        schedule_type = str(entry.get("schedule_type") or "").strip()
        rows.append(
            rate_row(entry.get("age_group"), entry.get("amount"), unit=unit, period=period, schedule_type=schedule_type)
        )
    return rows


_register_rates(
    "massachusetts",
    ["ma_cost_table"],
    _massachusetts_rates,
    "MA `ma_cost_table`: ONE schedule (Full Year > School Year > Summer > first) -> rows tagged `schedule_type`.",
)

_NJ_TUITION_AGES = ("infant", "toddler", "preschool", "school_age")
_NJ_TUITION_PERIODS = ("hourly", "daily", "weekly", "monthly")
_NJ_TUITION_FIELDS = [f"nj_tuition_{age}_{period}" for age in _NJ_TUITION_AGES for period in _NJ_TUITION_PERIODS]


def _new_jersey_rates(record):
    return [
        rate_row(age.replace("_", " ").title(), record.get(f"nj_tuition_{age}_{period}"), period=RATE_PERIODS[period])
        for age in _NJ_TUITION_AGES
        for period in _NJ_TUITION_PERIODS
    ]


_register_rates(
    "new_jersey",
    _NJ_TUITION_FIELDS,
    _new_jersey_rates,
    "NJ `nj_tuition_{age}_{period}` (16 columns) -> one row per amount > 0; `0.0` means blank.",
)


def _wyoming_rates(record):
    rows = []
    for program in _dict_entries(record.get("wy_programs"), "rates(wyoming)"):
        age_group = program.get("name") or program.get("type")
        for rate in _dict_entries(program.get("rates"), "rates(wyoming)"):
            raw_period = rate.get("period")
            period = RATE_PERIODS.get(str(raw_period or "").strip().lower())
            if period is None and raw_period:
                logger.warning("rates(wyoming): unknown period %r kept as None", raw_period)
            raw_type = str(rate.get("type") or "").strip()
            unit = RATE_UNITS.get(raw_type.lower())
            care_type = None if unit else raw_type
            rows.append(rate_row(age_group, rate.get("rate"), unit=unit, period=period, care_type=care_type))
    return rows


_register_rates(
    "wyoming",
    ["wy_programs"],
    _wyoming_rates,
    "WY `wy_programs[].rates`: program name as age_group; FULL_TIME/PART_TIME -> unit, other types -> `care_type`.",
)


# Age range in months (CT, ND, WV, IL, WY)
# --------------------------------------------------------------------------- #
_AGE_TOKEN_RE = re.compile(r"(\d+)\s*(years?|yrs?|y|months?|mos?|m|weeks?|wks?|w)\b", re.IGNORECASE)
_AGE_EXPRESSION_RE = re.compile(rf"^\s*(?:{_AGE_TOKEN_RE.pattern}[\s,]*)+$", re.IGNORECASE)
_AGE_UNIT_MONTHS = {"y": 12, "m": 1}


def weeks_to_months(weeks):
    """Whole months for ``weeks``: floor(weeks * 12 / 52), so 6 wk -> 1, 624 wk -> 144 (exact years)."""
    return weeks * 12 // 52


def age_to_months(value):
    """One age expression -> int months, or ``None`` (unparseable input is logged).

    Handles ``"6 Weeks"``, ``"2 Years 0 Months"``, ``"0 Years 1 Month"`` (singular/plural,
    summed) and the compact IL tokens ``"6W"``, ``"15M"``, ``"12Y"``. A bare ``"0"`` is 0;
    any other bare number has no unit and is rejected. Weeks use ``weeks_to_months``.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    text = str(value).strip()
    if not text or _is_sentinel(text):
        return None
    if text == "0":
        return 0
    if _AGE_EXPRESSION_RE.match(text):
        months = 0
        for amount, unit in _AGE_TOKEN_RE.findall(text):
            amount, unit = int(amount), unit[0].lower()
            months += weeks_to_months(amount) if unit == "w" else amount * _AGE_UNIT_MONTHS[unit]
        return months
    logger.warning("age_to_months: could not parse age %r -> None", value)
    return None


def _age_range_fields(state, low, high):
    """``age_min_months``/``age_max_months`` dict from parsed bounds, applying the max sanity rule.

    A max of 0 or below the min is a source artifact: it is logged and the max dropped
    (min kept). A min of 0 is valid. Both ``None`` -> ``{}``.
    """
    if high is not None and (high == 0 or (low is not None and high < low)):
        logger.warning("age range(%s): implausible max %r (min %r) -> max None", state, high, low)
        high = None
    fields = {"age_min_months": low, "age_max_months": high}
    return {k: v for k, v in fields.items() if v is not None}


def _bounds(lows, highs):
    """Widest span of the parsed bounds: min of mins, max of maxes, ignoring ``None``."""
    lows, highs = [v for v in lows if v is not None], [v for v in highs if v is not None]
    return (min(lows) if lows else None, max(highs) if highs else None)


def _register_age_range(state, sources, extract, doc):
    """Register an adapter filling the age-range fields from ``extract(record)`` -> ``(low, high)`` months."""

    @register_adapter(state, sources=sources, fills=["age_min_months", "age_max_months"])
    def adapter(record):
        return _age_range_fields(state, *extract(record))

    adapter.__doc__ = doc
    adapter.__name__ = f"{state}_age_range"
    return adapter


def _weeks_to_months_or_none(value):
    weeks = to_int(value)
    return None if weeks is None or weeks < 0 else weeks_to_months(weeks)


_register_age_range(
    "connecticut",
    ["ct_age_range_min_weeks", "ct_age_range_max_weeks"],
    lambda r: (
        _weeks_to_months_or_none(r.get("ct_age_range_min_weeks")),
        _weeks_to_months_or_none(r.get("ct_age_range_max_weeks")),
    ),
    "CT `ct_age_range_{min,max}_weeks` -> months, floor(weeks * 12 / 52).",
)
_register_age_range(
    "north_dakota",
    ["nd_min_age", "nd_max_age"],
    lambda r: (age_to_months(r.get("nd_min_age")), age_to_months(r.get("nd_max_age"))),
    "ND `nd_min_age`/`nd_max_age` (`6 Weeks`, `12 Years`) -> months; a max of 0 is dropped.",
)
_register_age_range(
    "west_virginia",
    ["wv_age_from", "wv_age_to"],
    lambda r: (age_to_months(r.get("wv_age_from")), age_to_months(r.get("wv_age_to"))),
    "WV `wv_age_from`/`wv_age_to` (`2 Years 0 Months`) -> months.",
)


def _il_range_bounds(value):
    """``(low, high)`` months from one IL range such as ``6W TO 12Y``; bare ``0``/blank -> ``(None, None)``."""
    if not is_present(value) or str(value).strip() == "0":
        return None, None  # a bare 0 means that care (day/night) is not offered
    parts = re.split(r"\s+TO\s+", str(value).strip(), flags=re.IGNORECASE)
    if len(parts) != 2:
        logger.warning("age range(illinois): could not split range %r -> None", value)
        return None, None
    return age_to_months(parts[0]), age_to_months(parts[1])


def _illinois_age_range(record):
    day = _il_range_bounds(record.get("il_day_age_range"))
    night = _il_range_bounds(record.get("il_night_age_range"))
    return _bounds((day[0], night[0]), (day[1], night[1]))


_register_age_range(
    "illinois",
    ["il_day_age_range", "il_night_age_range"],
    _illinois_age_range,
    "IL day/night `6W TO 12Y` ranges -> widest span (min of mins, max of maxes); bare `0` ignored.",
)


def _wyoming_age_range(record):
    programs = [p for p in record.get("wy_programs") or [] if isinstance(p, dict)]
    return _bounds(
        (to_int(p.get("min_age_months")) for p in programs),
        (to_int(p.get("max_age_months")) for p in programs),
    )


_register_age_range(
    "wyoming",
    ["wy_programs"],
    _wyoming_age_range,
    "WY `wy_programs[]` min/max age months -> min of mins, max of maxes.",
)


# --------------------------------------------------------------------------- #
# capacity_by_age
# --------------------------------------------------------------------------- #
# MA `ma_capacity_by_age` keys -> labels, in the order rows are emitted.
MA_CAPACITY_AGE_LABELS = {
    "infantBirth": "Infant",
    "infantToddler": "Infant/Toddler",
    "toddler": "Toddler",
    "toddlerPreschool": "Toddler/Preschool",
    "preschool": "Preschool",
    "preschoolSA": "Preschool/School Age",
    "kindergarten": "Kindergarten",
    "schoolAge": "School Age",
    "multiAge": "Multi-Age",
}


def capacity_row(age_group, capacity, **extra):
    """One ``capacity_by_age`` row, or ``None`` when the capacity is blank or unparseable.

    Zero is a real capacity and is kept. ``extra`` (RI ``classrooms``) is omitted when ``None``.
    Unparseable capacities are logged by ``to_int``.
    """
    value = to_int(capacity)
    if value is None:
        return None
    row = {"age_group": str(age_group).strip() if age_group is not None else "", "capacity": value}
    row.update({key: val for key, val in extra.items() if val is not None})
    return row


def _register_capacity_by_age(state, sources, extract, doc):
    """Register an adapter filling ``capacity_by_age`` from the rows ``extract(record)`` returns."""

    @register_adapter(state, sources=sources, fills=["capacity_by_age"])
    def adapter(record):
        return {"capacity_by_age": [row for row in extract(record) if row is not None]}

    adapter.__doc__ = doc
    adapter.__name__ = f"{state}_capacity_by_age"
    return adapter


def _humanize_camel_case(key):
    """``multiAgeGroup`` -> ``Multi Age Group`` (fallback label for an unknown key)."""
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", str(key)).title()


def _massachusetts_capacity_by_age(record):
    counts = record.get("ma_capacity_by_age")
    if not isinstance(counts, dict):
        return []
    rows = [capacity_row(label, counts.get(key)) for key, label in MA_CAPACITY_AGE_LABELS.items() if key in counts]
    for key in counts:
        if key not in MA_CAPACITY_AGE_LABELS:
            logger.warning("capacity_by_age(massachusetts): unknown age key %r", key)
            rows.append(capacity_row(_humanize_camel_case(key), counts[key]))
    return rows


_register_capacity_by_age(
    "massachusetts",
    ["ma_capacity_by_age"],
    _massachusetts_capacity_by_age,
    "MA `ma_capacity_by_age` camelCase keys -> rows with a fixed label table, in table order.",
)
_register_capacity_by_age(
    "rhode_island",
    ["ri_age_group_capacity"],
    lambda record: [
        capacity_row(e.get("age_group"), e.get("capacity"), classrooms=to_int(e.get("classrooms")))
        for e in _dict_entries(record.get("ri_age_group_capacity"), "capacity_by_age(rhode_island)")
    ],
    "RI `ri_age_group_capacity` rows copied with their own age_group label and classroom count.",
)
_register_capacity_by_age(
    "connecticut",
    ["ct_capacity_three_and_under", "ct_capacity_school_aged", "ct_capacity_full_time"],
    lambda record: [
        capacity_row("Three and Under", record.get("ct_capacity_three_and_under")),
        capacity_row("School Aged", record.get("ct_capacity_school_aged")),
        # Not an age group: the family-home full-time bucket, kept so rows sum to the license.
        capacity_row("Full Time (any age)", record.get("ct_capacity_full_time")),
    ],
    "CT three fixed capacity buckets (three and under, school aged, full time) -> rows; blanks skipped.",
)


def _indiana_capacity_by_age(record):
    rows = []
    for entry in _dict_entries(record.get("in_licensed_ages"), "capacity_by_age(indiana)"):
        start = str(entry.get("start_age") or "").strip()
        end = str(entry.get("end_age") or "").strip()
        label = f"{start} – {end}" if start and end else start or end
        rows.append(capacity_row(label, entry.get("quantity")))
    return rows


_register_capacity_by_age(
    "indiana",
    ["in_licensed_ages"],
    _indiana_capacity_by_age,
    "IN `in_licensed_ages` start/end age -> `Start – End` label (just start when no end); quantity -> capacity.",
)


# --------------------------------------------------------------------------- #
# license_conditions
# --------------------------------------------------------------------------- #
def clean_conditions(entries):
    """Trimmed, de-duplicated condition strings in source order, or ``None`` when empty.

    Blanks and sentinels (MN's literal ``None``) are dropped; duplicates are
    matched case-insensitively and the first spelling wins.
    """
    seen = set()
    cleaned = []
    for entry in entries:
        if entry is None:
            continue
        text = str(entry).strip()
        if not text or _is_sentinel(text) or text.lower() in seen:
            continue
        seen.add(text.lower())
        cleaned.append(text)
    return cleaned or None


def _split_semicolons(value):
    """``;``-separated string -> parts (anything else -> no parts)."""
    return value.split(";") if isinstance(value, str) else []


# MN runs these standard phrases together with no delimiter; each starts a new condition.
# Matched case-insensitively at a word boundary, never at the start of the text.
MN_CONDITION_PHRASES = (
    r"No infants in care",
    r"No transporting of children under age nine",
    r"Group E Occupancy Restrictions:",
    r"EXTENSION(?=\s*[—–-])",  # only the "EXTENSION—..." form, not the word in free text
    r"TEMPORARY PROVISIONAL LICENSE",
)
_MN_PHRASE_START = re.compile(r"\b(?:" + "|".join(MN_CONDITION_PHRASES) + ")", re.IGNORECASE)


def _split_mn_phrases(text):
    """Cut ``text`` before each standard MN phrase that is not at its start."""
    cuts = [m.start() for m in _MN_PHRASE_START.finditer(text) if m.start() > 0]
    bounds = [0, *cuts, len(text)]
    return [text[start:end] for start, end in itertools.pairwise(bounds)]


@register_adapter("minnesota", sources=["mn_restrictions"], fills=["license_conditions"])
def minnesota_license_conditions(record):
    """MN `mn_restrictions` string -> `;` split, then cut at the standard run-together phrases; `None` dropped."""
    parts = [piece for part in _split_semicolons(record.get("mn_restrictions")) for piece in _split_mn_phrases(part)]
    return {"license_conditions": clean_conditions(parts)}


_NC_OTHER_PREFIX = re.compile(r"^\s*other\s*-\s*", re.IGNORECASE)


@register_adapter("north_carolina", sources=["nc_license_restrictions"], fills=["license_conditions"])
def north_carolina_license_conditions(record):
    """NC `nc_license_restrictions` list -> entries without the leading `Other -`; a bare `Other -` is dropped."""
    entries = record.get("nc_license_restrictions")
    if isinstance(entries, str):
        entries = [entries]
    if not isinstance(entries, (list, tuple)):
        return {"license_conditions": None}
    return {
        "license_conditions": clean_conditions(
            _NC_OTHER_PREFIX.sub("", entry) for entry in entries if isinstance(entry, str)
        )
    }


@register_adapter("missouri", sources=["mo_limitations"], fills=["license_conditions"])
def missouri_license_conditions(record):
    """MO `mo_limitations` ALL-CAPS string -> `;` split, case kept as published."""
    return {"license_conditions": clean_conditions(_split_semicolons(record.get("mo_limitations")))}


@register_adapter("delaware", sources=["de_special_conditions"], fills=["license_conditions"])
def delaware_license_conditions(record):
    """DE `de_special_conditions` string -> `;` split."""
    return {"license_conditions": clean_conditions(_split_semicolons(record.get("de_special_conditions")))}


_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _renewed_date(record, current_field, original_field, state):
    """Current license-term start, but only if it is later than the original license date.

    The current-term start equals ``license_begin_date``. It is a renewal only when the
    original (first-ever) date is known and strictly earlier; equal means the provider is
    still in its first term, and a missing original means a renewal cannot be confirmed.
    A current date before the original is inconsistent source data: warn and skip.
    """
    current = to_iso_date(record.get(current_field))
    original = to_iso_date(record.get(original_field))
    if not is_present(current) or not is_present(original):
        return None
    # ISO strings compare chronologically; an unparseable value (returned unchanged) won't be a renewal.
    if not (_ISO_DATE_RE.match(current) and _ISO_DATE_RE.match(original)):
        return None
    if current < original:
        logger.warning(
            "%s: %s %s is before %s %s; last_renewed_date not set",
            state,
            current_field,
            current,
            original_field,
            original,
        )
        return None
    return current if current > original else None


@register_adapter(
    "minnesota",
    sources=["mn_last_renewed_date", "mn_original_license_date"],
    fills=["last_renewed_date"],
)
def minnesota_last_renewed_date(record):
    """MN `mn_last_renewed_date`, only when later than `mn_original_license_date`."""
    return {"last_renewed_date": _renewed_date(record, "mn_last_renewed_date", "mn_original_license_date", "minnesota")}


@register_adapter(
    "rhode_island",
    sources=["ri_most_recently_renewed", "ri_original_license_date"],
    fills=["last_renewed_date"],
)
def rhode_island_last_renewed_date(record):
    """RI `ri_most_recently_renewed`, only when later than `ri_original_license_date`."""
    return {
        "last_renewed_date": _renewed_date(
            record, "ri_most_recently_renewed", "ri_original_license_date", "rhode_island"
        )
    }


@register_adapter(
    "massachusetts",
    sources=["ma_last_issue_date", "ma_original_license_date"],
    fills=["last_renewed_date"],
)
def massachusetts_last_renewed_date(record):
    """MA `ma_last_issue_date`, only when later than `ma_original_license_date`."""
    return {
        "last_renewed_date": _renewed_date(record, "ma_last_issue_date", "ma_original_license_date", "massachusetts")
    }


# HI `licenseType` codes -> label; unknown codes are logged and kept raw.
HI_LICENSE_TYPE_LABELS = {"P": "Provisional", "R": "Regular"}


def license_history_row(
    license_type=None, effective_date=None, expiration_date=None, license_id=None, status=None, capacity=None
):
    """One canonical ``license_history`` row, or ``None`` when nothing usable is left.

    Keys with a missing, blank or sentinel value (including ``Unknown``) are omitted.
    Dates go through ``to_iso_date`` and ``capacity`` through ``to_int``.
    """
    row = {}
    for key, value in (("license_type", license_type), ("license_id", license_id), ("status", status)):
        if value is None or _is_sentinel(value) or not str(value).strip():
            continue
        row[key] = str(value).strip()
    for key, value in (("effective_date", effective_date), ("expiration_date", expiration_date)):
        if value is None or not str(value).strip():
            continue
        iso = to_iso_date(str(value).strip())
        if iso:
            row[key] = iso
    count = to_int(capacity)
    if count is not None:
        row["capacity"] = count
    return row or None


def _license_history(rows):
    """Drop empty rows and sort by ``effective_date`` ascending (undated last, source order kept)."""
    rows = [row for row in rows if row]
    rows.sort(key=lambda row: (row.get("effective_date") is None, row.get("effective_date") or ""))
    return rows


def _hi_license_type(code):
    """HI `licenseType` code -> label (``P``/``R``); an unknown code is logged and kept raw."""
    if code is None or not str(code).strip():
        return None
    label = HI_LICENSE_TYPE_LABELS.get(str(code).strip().upper())
    if label is None:
        logger.warning("license_history(hawaii): unknown licenseType %r kept raw", code)
        return str(code).strip()
    return label


@register_adapter("hawaii", sources=["hi_license_history"], fills=["license_history"])
def hawaii_license_history(record):
    """HI `hi_license_history` rows: dates, `P`/`R` expanded to a license type, capacity."""
    rows = [
        license_history_row(
            license_type=_hi_license_type(entry.get("licenseType")),
            effective_date=entry.get("effectiveDate"),
            expiration_date=entry.get("expirationDate"),
            capacity=entry.get("capacity"),
        )
        for entry in _dict_entries(record.get("hi_license_history"), "license_history(hawaii)")
    ]
    return {"license_history": _license_history(rows)}


@register_adapter("washington", sources=["wa_license_history"], fills=["license_history"])
def washington_license_history(record):
    """WA `wa_license_history` rows; `closure_date` becomes `expiration_date` (a closure, not an expiry)."""
    rows = [
        license_history_row(
            license_type=entry.get("license_type"),
            effective_date=entry.get("issue_date"),
            expiration_date=entry.get("closure_date"),
            license_id=entry.get("license_id"),
            status=entry.get("license_status"),
        )
        for entry in _dict_entries(record.get("wa_license_history"), "license_history(washington)")
    ]
    return {"license_history": _license_history(rows)}

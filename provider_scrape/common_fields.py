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

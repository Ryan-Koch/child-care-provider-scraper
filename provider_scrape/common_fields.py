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

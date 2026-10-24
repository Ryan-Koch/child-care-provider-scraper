"""Generate ``state_pipeline_details.md`` (the as-built pipeline reference).

This is a **build artifact generator**: it introspects ``items.py`` +
``normalization.py`` and emits markdown documenting, for every field, the exact
transformation the normalization pipeline applies, plus the per-state field
mappings. Humans maintain only the curated explanations in
``provider_scrape/field_docs.py``.

Run:  ``.venv/bin/python scripts/generate_pipeline_details.py``

Determinism: the output must be byte-for-byte identical on every run for the
same code (a pytest drift-guard depends on it). No timestamps/hostnames/paths;
everything is emitted in a fixed, sorted order. Stdlib only.
"""

import os
import re
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from provider_scrape import common_fields as cf
from provider_scrape import normalization as norm
from provider_scrape.field_docs import FIELD_DOCS
from provider_scrape.items import InspectionItem, ProviderItem

OUTPUT_PATH = os.path.join(REPO_ROOT, "state_pipeline_details.md")

STATE_PREFIX_RE = re.compile(r"^[a-z]{2}_")
TRACKING_FIELDS = ("inspections", "provider_url", "source_state")

# Derived (additive) common fields and their source.
DERIVED_FIELDS = (
    ("facility_category", "provider_type"),
    ("city", "address"),
    ("state", "address"),
    ("zip", "address"),
    # Populated by the post-run geocoding enrichment step (geocoding.py), not
    # the normalization pipeline; keyed off address.
    ("geocode_source", "address"),
    ("geocode_confidence", "address"),
)
DERIVED_FIELD_NAMES = {name for name, _ in DERIVED_FIELDS}

# USPS code -> spelled-out state name, inverted from normalization's table so
# the per-state headers stay derived from existing code.
_CODE_TO_NAME = {code: name for name, code in norm.SPELLED_STATE_TO_USPS.items()}


def is_state_specific(field):
    return bool(STATE_PREFIX_RE.match(field))


def common_provider_fields():
    """ProviderItem fields that are neither state-specific nor tracking."""
    return sorted(
        f
        for f in ProviderItem.fields
        if not is_state_specific(f) and f not in TRACKING_FIELDS and f not in DERIVED_FIELD_NAMES
    )


def collapse_source_to_common():
    """source field -> (common field, transform label or "")."""
    mapping = {}
    for common, entries in norm.FIELD_COLLAPSE_MAP.items():
        for entry in entries:
            source, transform = norm.split_collapse_entry(entry)
            mapping[source] = (common, cf.describe_transform(transform))
    return mapping


def adapter_source_to_fills():
    """provider-level adapter source field -> sorted common fields it feeds."""
    mapping = {}
    for adapter in cf.all_adapters():
        if adapter.level != cf.PROVIDER_LEVEL:
            continue
        for source in adapter.sources:
            mapping.setdefault(source, set()).update(adapter.fills)
    return {source: sorted(fills) for source, fills in mapping.items()}


def render_map_rows(collapse_map):
    """Table rows for a collapse map; transformed sources show ``(transform)``."""
    rows = []
    for common in sorted(collapse_map):
        parsed = [norm.split_collapse_entry(e) for e in collapse_map[common]]
        cells = []
        for source, transform in sorted(parsed, key=lambda pair: pair[0]):
            label = cf.describe_transform(transform)
            cells.append(f"`{source}` ({md_escape(label)})" if label else f"`{source}`")
        rows.append(f"| `{common}` | {', '.join(cells)} |")
    return rows


def transformation_for(field):
    """Infer the transformation label for a common field from the code."""
    collapse_targets = set(norm.FIELD_COLLAPSE_MAP)
    if field in norm.NAME_FIELDS:
        tags = ["Whitespace cleanup", "Title Case (ALL-CAPS names)"]
    elif field in norm.DATE_FIELDS:
        tags = ["Whitespace cleanup", "ISO 8601 date"]
    elif field == norm.CAPACITY_FIELD:
        tags = ["Whitespace cleanup", "Integer coercion"]
    elif field == norm.DEFICIENCIES_FIELD:
        tags = ["Whitespace cleanup", "Integer coercion (bug_10)"]
    elif field == norm.AGES_SERVED_FIELD:
        tags = ["Whitespace cleanup", "List -> string"]
    elif field in norm.COORDINATE_FIELDS:
        tags = ["Whitespace cleanup", "Coordinate -> string"]
    elif field == "status":
        tags = ["Whitespace cleanup", "Controlled vocabulary (status)"]
    elif field == "provider_type":
        tags = ["Whitespace cleanup", "Preserved (drives facility_category)"]
    elif field == "address":
        tags = ["Whitespace cleanup", "Address cleanup", "Component parse -> city/state/zip"]
    elif field == "mailing_address":
        tags = ["Whitespace cleanup", "Field collapse from state-specific source", "Address cleanup"]
    elif field in collapse_targets:
        label = "Field collapse from state-specific source"
        transforms = sorted(
            {cf.describe_transform(norm.split_collapse_entry(e)[1]) for e in norm.FIELD_COLLAPSE_MAP[field]} - {""}
        )
        if transforms == ["coerce_bool"]:
            label += " (-> boolean)"
        elif transforms:
            label += f" (transforms: {', '.join(transforms)})"
        tags = ["Whitespace cleanup", label]
    else:
        tags = ["Whitespace cleanup"]
    return "; ".join(tags)


def explanation_for(field, transformation):
    doc = FIELD_DOCS.get(field)
    if doc:
        return doc
    return f"{transformation}. **TODO:** add a curated explanation to `provider_scrape/field_docs.py`."


def md_escape(value):
    return str(value).replace("|", "\\|")


def build_markdown():
    lines = []
    add = lines.append

    add("# State Pipeline Details (as-built)")
    add("")
    add("<!-- DO NOT EDIT BY HAND. Generated by `scripts/generate_pipeline_details.py`. -->")
    add("")
    add(
        "This file records what the normalization pipeline actually does to "
        "each field, plus per-state field mappings. It is **generated** from "
        "`provider_scrape/items.py` and `provider_scrape/normalization.py`; "
        "the only hand-authored content is the explanations in "
        "`provider_scrape/field_docs.py`."
    )
    add("")
    add(
        "For guidance on *where new data should go*, see "
        "[`docs/field_mapping_playbook.md`](docs/field_mapping_playbook.md)."
    )
    add("")
    add("To regenerate: `.venv/bin/python scripts/generate_pipeline_details.py`.")
    add("")

    # --- Common-field transformations ---
    add("## Common-field transformations")
    add("")
    add("| Field | Transformation | Explanation |")
    add("|---|---|---|")
    for field in common_provider_fields():
        transformation = transformation_for(field)
        explanation = explanation_for(field, transformation)
        add(f"| `{field}` | {md_escape(transformation)} | {md_escape(explanation)} |")
    add("")
    date_fields = ", ".join(f"`{f}`" for f in norm.INSPECTION_DATE_FIELDS)
    add(
        "> Inspections are normalized too: every string value gets whitespace "
        f"cleanup, and the date fields ({date_fields}) are converted to ISO 8601. The "
        "`status` vocabulary does not apply to inspections."
    )
    add("")
    tracking = ", ".join(f"`{f}`" for f in TRACKING_FIELDS)
    add(f"> Tracking fields ({tracking}) are passed through unchanged.")
    add("")

    # --- Derived fields ---
    add("## Derived fields (additive)")
    add("")
    add("These canonical fields are derived from a source field; the source is preserved unchanged.")
    add("")
    add("| Field | Derived from | Explanation |")
    add("|---|---|---|")
    for field, source in DERIVED_FIELDS:
        explanation = explanation_for(field, f"Derived from {source}")
        add(f"| `{field}` | `{source}` | {md_escape(explanation)} |")
    add("")

    # --- Field-collapse map ---
    add("## Field-collapse map")
    add("")
    add(
        "Each common field is populated from a single populated source field "
        "(additive — the source field is kept; an already-populated common "
        "field is never overwritten). A source shown with a parenthesised "
        "transform is passed through that transform first (see "
        "[Transform library](#transform-library)). Rendered from "
        "`FIELD_COLLAPSE_MAP`."
    )
    add("")
    add("| Common field | Source state-specific fields |")
    add("|---|---|")
    for row in render_map_rows(norm.FIELD_COLLAPSE_MAP):
        add(row)
    add("")

    add("## Inspection field-collapse map")
    add("")
    add("Same rules, applied to every inspection row. Rendered from `INSPECTION_FIELD_COLLAPSE_MAP`.")
    add("")
    if norm.INSPECTION_FIELD_COLLAPSE_MAP:
        add("| Common inspection field | Source state-specific fields | Explanation |")
        add("|---|---|---|")
        for field, row in zip(
            sorted(norm.INSPECTION_FIELD_COLLAPSE_MAP),
            render_map_rows(norm.INSPECTION_FIELD_COLLAPSE_MAP),
            strict=True,
        ):
            doc = FIELD_DOCS.get(field, "**TODO:** add a curated explanation to `provider_scrape/field_docs.py`.")
            add(f"{row} {md_escape(doc)} |")
    else:
        add("_No entries yet._")
    add("")

    # --- Transform library ---
    add("## Transform library")
    add("")
    add(
        "Reusable `value -> value` transforms from `provider_scrape/common_fields.py` "
        "for use per source in the collapse maps. A transform returns `None` for "
        '"no usable value", which leaves the common field unset.'
    )
    add("")
    add("| Transform | Behaviour |")
    add("|---|---|")
    for name, func in (
        ("invert", cf.invert),
        ("to_int", cf.to_int),
        ("to_float", cf.to_float),
        ("vocab(table)", cf.vocab),
        ("has_token(pattern)", cf.has_token),
        ("to_iso_date", cf.to_iso_date),
    ):
        doc = " ".join((func.__doc__ or "").split()).replace("``", "`")
        add(f"| `{name}` | {md_escape(doc)} |")
    add("")

    # --- Per-state adapters ---
    add("## Per-state adapters")
    add("")
    add(
        "Adapters (registered in `provider_scrape/common_fields.py`) cover cases "
        "a one-to-one map entry cannot: one source feeding several common "
        "fields, or several sources feeding one. They run after the "
        "field-collapse map, in registration order, and follow the same rules "
        "(additive, never overwrite a populated common field)."
    )
    add("")
    adapters = cf.all_adapters()
    if adapters:
        add("| State | Level | Reads | Fills | Description |")
        add("|---|---|---|---|---|")
        for adapter in adapters:
            reads = ", ".join(f"`{f}`" for f in adapter.sources)
            fills = ", ".join(f"`{f}`" for f in adapter.fills)
            add(f"| `{adapter.state}` | {adapter.level} | {reads} | {fills} | {md_escape(adapter.description)} |")
    else:
        add("_No adapters registered yet._")
    add("")

    # --- Controlled-vocabulary appendices ---
    add("## Controlled vocabulary: `status`")
    add("")
    add("Raw status values mapped to each canonical bucket (replaced in place; unmapped -> `unknown`).")
    add("")
    add("| Canonical | Raw values |")
    add("|---|---|")
    for canonical in sorted(norm.STATUS_BUCKETS):
        raws = ", ".join(f"`{md_escape(r)}`" for r in sorted(norm.STATUS_BUCKETS[canonical]))
        add(f"| `{canonical}` | {raws} |")
    add("")

    add("## Controlled vocabulary: `facility_category`")
    add("")
    add("Raw `provider_type` values mapped to each canonical category (additive; unmapped -> `other`).")
    add("")
    add("| Canonical | Raw provider_type values |")
    add("|---|---|")
    for canonical in sorted(norm.FACILITY_CATEGORY_BUCKETS):
        raws = ", ".join(f"`{md_escape(r)}`" for r in sorted(norm.FACILITY_CATEGORY_BUCKETS[canonical]))
        add(f"| `{canonical}` | {raws} |")
    add("")

    # --- Per-state section ---
    add("## Per-state fields")
    add("")
    add(
        "State-specific fields (matched by the `xx_` prefix) across "
        "`ProviderItem` and `InspectionItem`, each marked as collapsing into a "
        "common field or passing through unchanged."
    )
    add("")
    source_to_common = collapse_source_to_common()
    source_to_fills = adapter_source_to_fills()
    all_state_fields = sorted(
        {f for f in ProviderItem.fields if is_state_specific(f)}
        | {f for f in InspectionItem.fields if is_state_specific(f)}
    )
    by_state = {}
    for field in all_state_fields:
        by_state.setdefault(field[:2], []).append(field)

    for code in sorted(by_state):
        name = _CODE_TO_NAME.get(code.upper())
        header = f"{name.title()} ({code.upper()})" if name else code.upper()
        add(f"### {header}")
        add("")
        for field in sorted(by_state[code]):
            collapsed = source_to_common.get(field)
            fills = source_to_fills.get(field)
            if collapsed:
                common, label = collapsed
                via = f" (via {label})" if label else ""
                add(f"- `{field}` -> collapses into `{common}`{via}")
            elif fills:
                targets = ", ".join(f"`{f}`" for f in fills)
                add(f"- `{field}` -> feeds adapter, fills {targets}")
            else:
                add(f"- `{field}` -> pass-through")
        add("")

    return "\n".join(lines).rstrip("\n") + "\n"


def main():
    markdown = build_markdown()
    with open(OUTPUT_PATH, "w", encoding="utf-8") as handle:
        handle.write(markdown)
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()

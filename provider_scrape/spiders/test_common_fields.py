"""Tests for the round-2 collapse infrastructure (story 00).

Covers the transform library, the transform-aware ``FIELD_COLLAPSE_MAP`` /
``INSPECTION_FIELD_COLLAPSE_MAP`` collapse, the per-state adapter registry, and a
regression proving round-1 collapse behaviour is unchanged.
"""

import itertools
import logging

import pytest

from provider_scrape import common_fields as cf
from provider_scrape import normalization as norm


# --------------------------------------------------------------------------- #
# Transforms
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (True, False),
        (False, True),
        ("Yes", False),
        ("no", True),
        ("Y", False),
        ("0", True),
        (1, False),
        (0, True),
        (None, None),
        ("", None),
        ("N/A", None),
        ("maybe", None),
    ],
)
def test_invert(raw, expected):
    assert cf.invert(raw) is expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (5, 5),
        (0, 0),
        (12.0, 12),
        ("12", 12),
        (" 1,200 ", 1200),
        ("$5.00", 5),
        ("$0.00", 0),  # a real zero, not a sentinel
        ("None Reported.", None),
        ("N/A", None),
        ("", None),
        (None, None),
        (True, None),
        ("3.5", None),  # non-integral: not truncated
        ("abc", None),
    ],
)
def test_to_int(raw, expected):
    assert cf.to_int(raw) == expected
    if expected is None:
        assert cf.to_int(raw) is None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("$25.50", 25.5),
        ("1,000.25", 1000.25),
        ("$0.00", 0.0),
        (3, 3.0),
        ("None Reported.", None),
        ("n/a", None),
        (None, None),
        ("free", None),
    ],
)
def test_to_float(raw, expected):
    assert cf.to_float(raw) == expected
    assert (cf.to_float(raw) is None) == (expected is None)


def test_vocab_maps_case_insensitively_and_logs_misses(caplog):
    to_status = cf.vocab({"Y": "yes", "N": "no"}, name="flag")
    assert to_status(" y ") == "yes"
    assert to_status("n") == "no"
    assert to_status("N/A") is None  # sentinel: silent
    assert to_status(None) is None
    with caplog.at_level(logging.WARNING):
        assert to_status("zzz") is None
    assert "unmapped value 'zzz'" in caplog.text
    assert cf.describe_transform(to_status) == "vocab(flag)"


def test_has_token_matches_strings_and_lists_else_none():
    wheelchair = cf.has_token(r"wheelchair")
    assert wheelchair("Fenced yard; Wheelchair Accessible") is True
    assert wheelchair(["Pool", "wheelchair ramp"]) is True
    # Absence is "not reported", not False (epic README).
    assert wheelchair("Fenced yard") is None
    assert wheelchair([]) is None
    assert wheelchair(None) is None


def test_to_iso_date_normalizes_and_handles_sentinels():
    assert cf.to_iso_date("3/7/2024") == "2024-03-07"
    assert cf.to_iso_date("2024-03-07T10:00:00Z") == "2024-03-07"
    assert cf.to_iso_date("N/A") is None
    assert cf.to_iso_date(None) is None
    assert cf.to_iso_date("not a date") == "not a date"  # left unchanged (logged)


# --------------------------------------------------------------------------- #
# Transform-aware collapse
# --------------------------------------------------------------------------- #
def test_collapse_applies_per_source_transform_and_keeps_source():
    cmap = {"address_suppressed": ["ct_address_suppressed", ("fl_display_address_on_web", cf.invert)]}
    item = {"fl_display_address_on_web": True}
    out = norm.collapse_state_fields(item, cmap)
    assert out["address_suppressed"] is False  # inverted False is a real value
    assert out["fl_display_address_on_web"] is True  # additive


def test_collapse_plain_string_entry_in_mixed_map_is_copied_as_is():
    cmap = {"address_suppressed": ["ct_address_suppressed", ("fl_display_address_on_web", cf.invert)]}
    assert norm.collapse_state_fields({"ct_address_suppressed": True}, cmap)["address_suppressed"] is True


def test_collapse_transform_returning_none_leaves_field_unset():
    cmap = {"capacity_x": [("xx_cap", cf.to_int)]}
    assert "capacity_x" not in norm.collapse_state_fields({"xx_cap": "None Reported."}, cmap)


def test_collapse_transform_keeps_exactly_one_source_rule():
    cmap = {"n": [("a", cf.to_int), ("b", cf.to_int)]}
    assert "n" not in norm.collapse_state_fields({"a": "1", "b": "2"}, cmap)
    assert norm.collapse_state_fields({"a": "1", "b": None}, cmap)["n"] == 1


def test_collapse_transform_never_overwrites_populated_common_field():
    cmap = {"flag": [("src", cf.invert)]}
    assert norm.collapse_state_fields({"flag": False, "src": True}, cmap)["flag"] is False


def test_inspection_collapse_runs_for_every_inspection_additively(monkeypatch):
    monkeypatch.setattr(
        norm,
        "INSPECTION_FIELD_COLLAPSE_MAP",
        {"violation_count": [("xx_violations", cf.to_int)], "visit_date_iso": [("xx_date", cf.to_iso_date)]},
    )
    first = norm.normalize_inspection({"xx_violations": "2", "xx_date": "3/7/2024"}, "test_state")
    second = norm.normalize_inspection({"xx_violations": "None Reported."}, "test_state")
    assert first["violation_count"] == 2
    assert first["visit_date_iso"] == "2024-03-07"
    assert first["xx_violations"] == "2"  # source kept
    assert "violation_count" not in second


def test_inspection_collapse_map_defaults_to_noop():
    inspection = {"date": "3/7/2024", "type": " Routine "}
    out = norm.normalize_inspection(inspection, "virginia")
    assert out == {"date": "2024-03-07", "type": "Routine"}


# --------------------------------------------------------------------------- #
# Round-1 regression: byte-identical collapse behaviour
# --------------------------------------------------------------------------- #
# Frozen copy of the pre-story-00 implementation and map (sources only).
_LEGACY_MAP = {
    "license_type": [
        "va_license_type", "mt_license_type", "ut_license_type", "co_license_type", "az_license_type",
        "nc_license_type", "nj_license_type_label", "wv_license_type", "wa_license_type", "hi_license_type",
        "ak_license_type", "ct_license_type",
    ],
    "school_district": [
        "co_school_district", "ny_school_district_name", "pa_school_district", "ut_school_district",
        "wa_school_district", "ct_school_districts",
    ],
    "mailing_address": ["al_mailing_address", "ga_mailing_address", "hi_mailing_address"],
    "accreditation": [
        "al_accreditations", "ga_accreditation", "hi_accreditations", "nj_accreditation_labels",
        "md_accreditation", "ct_accreditations",
    ],
    "meals": ["ut_meals", "ga_meals", "hi_meals", "nm_meals", "nj_meal_options_labels", "pa_meal_options"],
    "accepting_new_children": ["co_accepting_new_children", "ga_accepting_new_children", "ct_accepting_referrals"],
    "transportation": ["ga_transportation", "nj_transportation_labels", "ct_transportation"],
    "head_start": [
        "co_head_start", "az_headstart", "wa_head_start", "ri_head_start", "fl_is_head_start",
        "ct_head_start", "nh_head_start",
    ],
    "curriculum": ["ga_curriculum", "nj_curriculum"],
}  # fmt: skip


def _legacy_collapse(item):
    for common, sources in _LEGACY_MAP.items():
        if norm._is_present(item.get(common)):
            continue
        present = [item[src] for src in sources if norm._is_present(item.get(src))]
        if len(present) != 1:
            continue
        value = present[0]
        if common == "head_start":
            value = norm._coerce_bool(value)
        item[common] = value
    return item


def test_round1_map_has_same_commons_and_sources():
    # Round-2 stories add new commons; this pins only the round-1 ones.
    current = {
        common: [norm.split_collapse_entry(e)[0] for e in entries]
        for common, entries in norm.FIELD_COLLAPSE_MAP.items()
        if common in _LEGACY_MAP
    }
    assert current == _LEGACY_MAP
    # Only head_start carries a transform, and it is the bool coercion.
    for common, entries in norm.FIELD_COLLAPSE_MAP.items():
        if common not in _LEGACY_MAP:
            continue
        transforms = {norm.split_collapse_entry(e)[1] for e in entries}
        assert transforms == ({norm._coerce_bool} if common == "head_start" else {None})


_SAMPLE_VALUES = ["Center", "Yes", "No", "Program X", True, False, 0, 1, ["a", "b"], "", None, []]


def test_round1_collapse_is_identical_to_legacy_for_all_source_combinations():
    for common, sources in _LEGACY_MAP.items():
        # none populated, each single source, every pair, and all sources.
        combos = [()] + [(s,) for s in sources] + list(itertools.combinations(sources, 2)) + [tuple(sources)]
        for combo in combos:
            for value in _SAMPLE_VALUES:
                for preset in (None, "Preset"):
                    item = {src: value for src in combo}
                    if preset:
                        item[common] = preset
                    expected = _legacy_collapse(dict(item))
                    actual = norm.collapse_state_fields(dict(item))
                    assert actual == expected, (common, combo, value, preset)
                    # type-exact too (True == 1 would otherwise slip through)
                    assert {k: type(v) for k, v in actual.items()} == {k: type(v) for k, v in expected.items()}


def test_round1_collapse_unchanged_with_all_sources_populated_at_once():
    item = {src: "x" for sources in _LEGACY_MAP.values() for src in sources}
    assert norm.collapse_state_fields(dict(item)) == _legacy_collapse(dict(item))


# --------------------------------------------------------------------------- #
# Adapters
# --------------------------------------------------------------------------- #
@pytest.fixture
def adapter_registry(monkeypatch):
    """Isolated registry so test adapters never leak into the real one."""
    registry = {}
    monkeypatch.setattr(cf, "ADAPTERS", registry)
    return registry


def _register_split_adapter():
    @cf.register_adapter("testland", sources=["tl_inspector"], fills=["specialist", "specialist_phone"])
    def split_inspector(record):
        """Split ``Name: phone`` into specialist name and phone."""
        raw = record.get("tl_inspector")
        if not raw or raw == "N/A":
            return {}
        name, _, phone = raw.partition(":")
        return {"specialist": name.strip(), "specialist_phone": phone.strip() or None}

    return split_inspector


def test_adapter_fills_several_fields_from_one_source(adapter_registry):
    _register_split_adapter()
    item = {"tl_inspector": "Angela Farmer: (276) 608-2283"}
    out = norm.normalize_item(item, "testland")
    assert out["specialist"] == "Angela Farmer"
    assert out["specialist_phone"] == "(276) 608-2283"
    assert out["tl_inspector"] == "Angela Farmer: (276) 608-2283"  # source kept


def test_adapter_is_scoped_to_its_state(adapter_registry):
    _register_split_adapter()
    out = norm.normalize_item({"tl_inspector": "A B: 1"}, "otherland")
    assert "specialist" not in out


def test_adapter_never_overwrites_populated_common_field_and_skips_empty(adapter_registry):
    _register_split_adapter()
    out = norm.normalize_item({"tl_inspector": "A B: 555", "specialist": "Spider Set"}, "testland")
    assert out["specialist"] == "Spider Set"
    assert out["specialist_phone"] == "555"
    assert "specialist" not in norm.normalize_item({"tl_inspector": "N/A"}, "testland")


def test_adapter_runs_after_map_collapse_and_sees_collapsed_values(adapter_registry, monkeypatch):
    monkeypatch.setattr(norm, "FIELD_COLLAPSE_MAP", {"mid": ["tl_src"]})

    @cf.register_adapter("testland", sources=["mid"], fills=["derived"])
    def derive(record):
        """Derive from the collapsed field."""
        return {"derived": record.get("mid", "").upper()}

    assert norm.normalize_item({"tl_src": "abc"}, "testland")["derived"] == "ABC"


def test_adapters_run_in_registration_order(adapter_registry):
    @cf.register_adapter("testland", sources=[], fills=["x"])
    def first(record):
        """First."""
        return {"x": "first"}

    @cf.register_adapter("testland", sources=[], fills=["x"])
    def second(record):
        """Second."""
        return {"x": "second"}

    assert norm.normalize_item({}, "testland")["x"] == "first"  # later one cannot overwrite


def test_adapter_undeclared_field_is_ignored_and_exception_is_contained(adapter_registry, caplog):
    @cf.register_adapter("testland", sources=[], fills=["ok"])
    def sloppy(record):
        """Returns an undeclared field."""
        return {"ok": 1, "surprise": 2}

    @cf.register_adapter("testland", sources=[], fills=["boom"])
    def broken(record):
        """Raises."""
        raise RuntimeError("bad record")

    with caplog.at_level(logging.WARNING):
        out = norm.normalize_item({}, "testland")
    assert out["ok"] == 1
    assert "surprise" not in out
    assert "undeclared field 'surprise'" in caplog.text
    assert "adapter broken failed" in caplog.text


def test_inspection_level_adapter_runs_per_inspection(adapter_registry):
    @cf.register_adapter("testland", sources=["tl_raw"], fills=["inspector"], level=cf.INSPECTION_LEVEL)
    def inspector(record):
        """Copy the raw inspector."""
        return {"inspector": record.get("tl_raw")}

    out = norm.normalize_inspection({"tl_raw": "Pat"}, "testland")
    assert out["inspector"] == "Pat"
    # provider-level pipeline does not run inspection adapters
    assert "inspector" not in norm.normalize_item({"tl_raw": "Pat"}, "testland")


def test_register_adapter_rejects_unknown_level(adapter_registry):
    with pytest.raises(ValueError, match="unknown adapter level"):
        cf.register_adapter("testland", sources=[], fills=[], level="bogus")


def test_adapter_description_uses_first_docstring_line(adapter_registry):
    _register_split_adapter()
    (adapter,) = cf.all_adapters()
    assert adapter.description == "Split `Name: phone` into specialist name and phone."


# --------------------------------------------------------------------------- #
# Story 01 -- assigned licensing specialist
# --------------------------------------------------------------------------- #
_SPECIALIST_FIELDS = ("licensing_specialist", "licensing_specialist_phone", "licensing_specialist_email")


def _specialist(state, **source):
    """Run one raw record through the full provider normalization."""
    item = norm.normalize_item(dict(source), state)
    return tuple(item.get(f) for f in _SPECIALIST_FIELDS), item


@pytest.mark.parametrize(
    ("state", "source", "name"),
    [
        ("alaska", {"ak_licensing_specialist": "Alyssa Pulice"}, "Alyssa Pulice"),
        ("massachusetts", {"ma_licensor": "Noelia Ramirez"}, "Noelia Ramirez"),
        ("tennessee", {"tn_regulatory_individual": "Sharon Neely"}, "Sharon Neely"),
        ("west_virginia", {"wv_licensing_specialist": "TODD MCDANIEL"}, "Todd Mcdaniel"),
        ("west_virginia", {"wv_licensing_specialist": "Jessica Hudson"}, "Jessica Hudson"),
    ],
)
def test_specialist_name_only_states(state, source, name):
    values, item = _specialist(state, **source)
    assert values == (name, None, None)
    # collapse is additive: the source field stays
    assert all(item[k] == source[k] or k == "wv_licensing_specialist" for k in source)


def test_specialist_maine_name_and_lowercased_email():
    values, item = _specialist(
        "maine", me_licensing_specialist="Karen Quinn", me_licensing_specialist_email="Karen.E.Quinn@Maine.gov"
    )
    assert values == ("Karen Quinn", None, "karen.e.quinn@maine.gov")
    assert item["me_licensing_specialist_email"] == "Karen.E.Quinn@Maine.gov"


def test_specialist_oklahoma_name_and_phone():
    values, _ = _specialist(
        "oklahoma", ok_licensing_specialist="Peyton Crawford", ok_licensing_specialist_phone="(405) 343-2186"
    )
    assert values == ("Peyton Crawford", "(405) 343-2186", None)


def test_specialist_oklahoma_missing_phone():
    values, _ = _specialist("oklahoma", ok_licensing_specialist="Peyton Crawford")
    assert values == ("Peyton Crawford", None, None)


@pytest.mark.parametrize(
    ("raw", "name"),
    [
        ("Woodward, Rebecca", "Rebecca Woodward"),
        ("Mcarthur, Wilena", "Wilena Mcarthur"),
        ("Smith-Jones, Ann", "Ann Smith-Jones"),
        ("WOODWARD, REBECCA", "Rebecca Woodward"),
        ("Rebecca Woodward", "Rebecca Woodward"),  # no comma: passed through
        ("Smith, Jr., John", "Smith, Jr., John"),  # unexpected shape: not guessed
        ("Woodward,", "Woodward,"),
    ],
)
def test_specialist_south_carolina_name_flip(raw, name):
    values, _ = _specialist(
        "south_carolina", sc_licensing_specialist_name=raw, sc_licensing_specialist_phone="(843) 661-6623"
    )
    assert values == (name, "(843) 661-6623", None)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Angela Farmer: (276) 608-2283", ("Angela Farmer", "(276) 608-2283")),
        ("Mary O'Neil-Smith: (804) 629-8167", ("Mary O'Neil-Smith", "(804) 629-8167")),
        ("Beth Velke", ("Beth Velke", None)),  # no phone
        ("Beth Velke:", ("Beth Velke", None)),
        ("N/A", (None, None)),
        ("n/a", (None, None)),
        ("", (None, None)),
    ],
)
def test_specialist_virginia_split(raw, expected):
    values, item = _specialist("virginia", va_inspector=raw)
    assert values == (expected[0], expected[1], None)
    assert item["va_inspector"] == (raw or None)  # source kept in place


def test_specialist_virginia_adapter_ignores_non_strings_and_missing():
    assert cf.virginia_inspector({}) == {}
    assert cf.virginia_inspector({"va_inspector": None}) == {}


def test_specialist_is_not_applied_to_other_states_and_never_overwrites():
    values, _ = _specialist("virginia", va_inspector="Angela Farmer: (276) 608-2283", licensing_specialist="Preset")
    assert values == ("Preset", "(276) 608-2283", None)
    # the VA adapter is scoped to Virginia
    values, _ = _specialist("ohio", va_inspector="Angela Farmer: (276) 608-2283")
    assert values == (None, None, None)


def test_specialist_transforms():
    assert cf.last_first_name("Woodward, Rebecca") == "Rebecca Woodward"
    assert cf.last_first_name(None) is None
    assert cf.last_first_name("N/A") is None
    assert cf.person_name("  N/A ") is None
    assert cf.person_name("TODD MCDANIEL") == "Todd Mcdaniel"
    assert cf.lower_email(" A.B@Maine.GOV ") == "a.b@maine.gov"
    assert cf.lower_email("") is None
    assert cf.none_if_sentinel("N/A") is None
    assert cf.none_if_sentinel(" (405) 343-2186 ") == "(405) 343-2186"

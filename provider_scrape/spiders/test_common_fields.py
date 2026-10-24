"""Tests for the round-2 collapse infrastructure.

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
# Assigned licensing specialist
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


# --------------------------------------------------------------------------- #
# Vacancies by age band
# --------------------------------------------------------------------------- #
_VACANCY_FIELDS = ("vacancies_infant", "vacancies_toddler", "vacancies_preschool", "vacancies_school_age")


def _vacancies(state, **source):
    item = norm.normalize_item(dict(source), state)
    return tuple(item.get(f) for f in _VACANCY_FIELDS), item


@pytest.mark.parametrize(
    ("state", "pattern", "raw", "expected"),
    [
        ("colorado", "co_{}_openings", ("0", "2", "", "5"), (0, 2, None, 5)),
        ("maine", "me_{}_slots", (0, 1, 3, 0), (0, 1, 3, 0)),
        ("new_hampshire", "nh_{}_openings", ("0", 2, 1, "4"), (0, 2, 1, 4)),
        ("vermont", "vt_{}_vacancies", (0, 1, 2, 0), (0, 1, 2, 0)),
    ],
)
def test_vacancies_map_states(state, pattern, raw, expected):
    bands = ("infant", "toddler", "preschool", "school_age")
    source = {pattern.format(b): v for b, v in zip(bands, raw, strict=True)}
    values, item = _vacancies(state, **source)
    assert values == expected
    assert all(item[k] == v or v == "" for k, v in source.items())  # additive (blank is nulled)


def test_vacancies_map_is_not_applied_to_other_states_or_overwriting():
    values, _ = _vacancies("ohio", co_infant_openings="3")
    assert values == (3, None, None, None)  # the map is state-agnostic by design
    values, _ = _vacancies("colorado", co_infant_openings="3", vacancies_infant=9)
    assert values[0] == 9


IA_ROWS = [
    {"ageGroup": "Infant (0-12 mo.)", "fullTime": 1, "partTime": 2},
    {"ageGroup": "Infant (13-23 mo.)", "fullTime": 3, "partTime": 0},
    {"ageGroup": "Toddler (2 yo.)", "fullTime": 0, "partTime": 0},
    {"ageGroup": "Preschool (3 yo.)", "fullTime": 1, "partTime": 1},
    {"ageGroup": "Preschool (4-5 yo.)", "fullTime": 2, "partTime": 0},
    {"ageGroup": "Before & After School", "fullTime": 0, "partTime": 4},
    {"ageGroup": "School Age Full Time", "fullTime": 5, "partTime": 0},
]


def test_vacancies_iowa_sums_full_and_part_time_across_rows_in_a_band():
    values, item = _vacancies("iowa", ia_openings_by_age=IA_ROWS)
    assert values == (6, 0, 4, 9)  # toddler is a genuine 0, not None
    assert item["ia_openings_by_age"] == IA_ROWS


def test_vacancies_iowa_none_handling():
    rows = [
        {"ageGroup": "Infant (0-12 mo.)", "fullTime": None, "partTime": None},
        {"ageGroup": "Infant (13-23 mo.)", "fullTime": None, "partTime": None},
        {"ageGroup": "Toddler (2 yo.)", "fullTime": None, "partTime": 2},
    ]
    values, _ = _vacancies("iowa", ia_openings_by_age=rows)
    assert values == (None, 2, None, None)


def test_vacancies_north_dakota():
    rows = [
        {"age_group": "Infant (0-17 months)", "vacancies": 1},
        {"age_group": "Toddler (18-35 months)", "vacancies": 0},
        {"age_group": "Preschool (3-5 years)", "vacancies": None},
    ]
    values, _ = _vacancies("north_dakota", nd_vacancies_by_age=rows)
    assert values == (1, 0, None, None)


def test_vacancies_dc_text_values():
    rows = [
        {"age_group": "Infant (0 - 12 Months)", "openings": "0"},
        {"age_group": "Toddler (12 - 36 Months)", "openings": "Not Applicable"},
        {"age_group": "Preschool (3 - 5 Years)", "openings": "12"},
        {"age_group": "School Age (5 - 19 Years)", "openings": "No Data Available"},
    ]
    values, _ = _vacancies("washington_dc", dc_enrollment=rows)
    assert values == (0, None, 12, None)


def test_vacancies_unknown_label_is_logged_and_skipped(caplog):
    rows = [{"age_group": "Infant (0-17 months)", "vacancies": 2}, {"age_group": "Gifted (9 years)", "vacancies": 7}]
    with caplog.at_level(logging.WARNING, logger=cf.logger.name):
        values, _ = _vacancies("north_dakota", nd_vacancies_by_age=rows)
    assert values == (2, None, None, None)
    assert "Gifted (9 years)" in caplog.text


def test_vacancies_adapters_tolerate_missing_and_malformed_sources():
    for state, field in (
        ("iowa", "ia_openings_by_age"),
        ("north_dakota", "nd_vacancies_by_age"),
        ("washington_dc", "dc_enrollment"),
    ):
        for bad in (None, [], "oops", [None]):
            values, _ = _vacancies(state, **{field: bad})
            assert values == (None, None, None, None)


def test_vacancies_ri_is_deliberately_excluded():
    values, _ = _vacancies(
        "rhode_island", ri_availability=[{"age_group": "Infant (0-12 mo.)", "slot_info": "Slots available"}]
    )
    assert values == (None, None, None, None)


# --------------------------------------------------------------------------- #
# Total vacancies
# --------------------------------------------------------------------------- #
def _total(state, **source):
    item = norm.normalize_item(dict(source), state)
    return item.get("total_vacancies"), item


@pytest.mark.parametrize(
    ("state", "field", "raw", "expected"),
    [
        ("iowa", "ia_total_openings", 2, 2),
        ("iowa", "ia_total_openings", 0, 0),
        ("north_dakota", "nd_total_vacancies", 7, 7),
        ("tennessee", "tn_vacancy", "5", 5),
        ("tennessee", "tn_vacancy", "0", 0),
        ("utah", "ut_vacancies", 0, 0),
        ("utah", "ut_vacancies", 12, 12),
        ("vermont", "vt_current_vacancy", 3, 3),
        ("washington", "wa_available_slots", "2", 2),
        ("washington", "wa_available_slots", "0", 0),
        ("idaho", "id_number_of_openings", "4", 4),
        ("idaho", "id_number_of_openings", 4, 4),
    ],
)
def test_total_vacancies_map_states(state, field, raw, expected):
    total, item = _total(state, **{field: raw})
    assert total == expected
    assert item[field] == raw  # additive


@pytest.mark.parametrize(
    ("state", "field"),
    [
        ("iowa", "ia_total_openings"),
        ("north_dakota", "nd_total_vacancies"),
        ("tennessee", "tn_vacancy"),
        ("utah", "ut_vacancies"),
        ("vermont", "vt_current_vacancy"),
        ("washington", "wa_available_slots"),
        ("idaho", "id_number_of_openings"),
        ("wisconsin", "wi_vacancies"),
    ],
)
def test_total_vacancies_missing_or_blank_is_none(state, field):
    for raw in (None, "", "N/A"):
        total, _ = _total(state, **{field: raw})
        assert total is None
    assert _total(state)[0] is None


def test_total_vacancies_idaho_yes_no_flag_is_not_mapped():
    total, _ = _total("idaho", id_openings_available="Yes")
    assert total is None


def test_total_vacancies_non_numeric_text_is_logged_not_zero(caplog):
    with caplog.at_level(logging.WARNING):
        total, _ = _total("tennessee", tn_vacancy="Call for availability")
    assert total is None
    assert "Call for availability" in caplog.text


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("None Reported.", None),
        ("0 vacancies have been reported. Last updated on 9/1/2026Vacancies Age Range", 0),
        ("12 vacancies have been reported. Last updated on 10/14/2026Vacancies Age Range Under 1 Year", 12),
        ("1 vacancy have been reported. Last updated on 8/5/2026Vacancies Age Range 2-years-old", 1),
        (None, None),
    ],
)
def test_total_vacancies_wisconsin_prose(raw, expected):
    total, item = _total("wisconsin", wi_vacancies=raw)
    assert total == expected
    assert item["wi_vacancies"] == raw


def test_total_vacancies_wisconsin_unrecognised_prose_is_logged(caplog):
    with caplog.at_level(logging.WARNING):
        total, _ = _total("wisconsin", wi_vacancies="Ask the provider")
    assert total is None
    assert "Ask the provider" in caplog.text


def test_total_vacancies_wisconsin_regex_exposes_updated_date():
    match = cf.WI_VACANCIES_RE.match("3 vacancies have been reported. Last updated on 9/14/2026Vacancies Age Range")
    assert match.group("count") == "3"
    assert match.group("updated") == "9/14/2026"


def test_total_vacancies_not_derived_from_age_bands():
    total, _ = _total("colorado", co_infant_openings="3", co_toddler_openings="2")
    assert total is None


def test_total_vacancies_does_not_overwrite_existing():
    total, _ = _total("iowa", ia_total_openings=4, total_vacancies=9)
    assert total == 9


# --------------------------------------------------------------------------- #
# Vacancies updated date
# --------------------------------------------------------------------------- #
def _updated(state, **source):
    item = norm.normalize_item(dict(source), state)
    return item.get("vacancies_updated_date"), item


@pytest.mark.parametrize(
    ("state", "field", "raw", "expected"),
    [
        ("iowa", "ia_openings_as_of", "09/15/2026", "2026-09-15"),
        ("maine", "me_openings_updated", "2025-05-29", "2025-05-29"),
        ("north_dakota", "nd_vacancies_updated", "2025-05-29", "2025-05-29"),
        ("vermont", "vt_vacancy_as_of", "09/08/2026", "2026-09-08"),
    ],
)
def test_vacancies_updated_date_map_states(state, field, raw, expected):
    updated, item = _updated(state, **{field: raw})
    assert updated == expected
    assert item[field] == raw  # additive


@pytest.mark.parametrize(
    ("state", "field"),
    [
        ("iowa", "ia_openings_as_of"),
        ("maine", "me_openings_updated"),
        ("north_dakota", "nd_vacancies_updated"),
        ("vermont", "vt_vacancy_as_of"),
        ("wisconsin", "wi_vacancies"),
    ],
)
def test_vacancies_updated_date_missing_or_blank_is_none(state, field):
    for raw in (None, "", "N/A"):
        updated, _ = _updated(state, **{field: raw})
        assert updated is None
    assert _updated(state)[0] is None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("None Reported.", None),
        ("0 vacancies have been reported. Last updated on 9/1/2026Vacancies Age Range", "2026-09-01"),
        ("12 vacancies have been reported. Last updated on 10/14/2026Vacancies Age Range", "2026-10-14"),
        ("1 vacancy have been reported.", None),  # no date in the prose
        (None, None),
    ],
)
def test_vacancies_updated_date_wisconsin_prose(raw, expected):
    updated, item = _updated("wisconsin", wi_vacancies=raw)
    assert updated == expected
    assert item["wi_vacancies"] == raw


def test_vacancies_updated_date_wisconsin_unrecognised_prose_is_logged(caplog):
    with caplog.at_level(logging.WARNING):
        updated, _ = _updated("wisconsin", wi_vacancies="Ask the provider")
    assert updated is None
    assert "Ask the provider" in caplog.text


def test_vacancies_updated_date_does_not_overwrite_existing():
    updated, _ = _updated("iowa", ia_openings_as_of="09/15/2026", vacancies_updated_date="2026-01-01")
    assert updated == "2026-01-01"


# --------------------------------------------------------------------------- #
# Early Head Start
# --------------------------------------------------------------------------- #
def _ehs(state, **source):
    return norm.normalize_item(dict(source), state)


@pytest.mark.parametrize(
    ("state", "field", "raw", "expected"),
    [
        ("mississippi", "ms_early_head_start", True, True),
        ("mississippi", "ms_early_head_start", False, False),
        ("new_hampshire", "nh_early_head_start", "Yes", True),
        ("new_hampshire", "nh_early_head_start", "No", False),
        ("washington", "wa_early_head_start", "Yes", True),
        ("washington", "wa_early_head_start", "No", False),
    ],
)
def test_early_head_start_map_states(state, field, raw, expected):
    item = _ehs(state, **{field: raw})
    assert item["early_head_start"] is expected
    assert item[field] == raw  # additive: source kept


@pytest.mark.parametrize(
    "field", ["ms_early_head_start", "nh_early_head_start", "wa_early_head_start"]
)
def test_early_head_start_blank_or_missing_is_unset(field):
    assert "early_head_start" not in _ehs("mississippi", **{field: ""})
    assert "early_head_start" not in _ehs("mississippi", **{field: None})
    assert "early_head_start" not in _ehs("mississippi")


@pytest.mark.parametrize(
    ("state", "field", "raw"),
    [
        ("mississippi", "ms_early_head_start", True),
        ("new_hampshire", "nh_early_head_start", "Yes"),
        ("washington", "wa_early_head_start", "Yes"),
    ],
)
def test_early_head_start_does_not_feed_head_start(state, field, raw):
    item = _ehs(state, **{field: raw})
    assert item["early_head_start"] is True
    assert "head_start" not in item


def test_early_head_start_sources_not_in_head_start_list():
    head_start_sources = {norm.split_collapse_entry(e)[0] for e in norm.FIELD_COLLAPSE_MAP["head_start"]}
    ehs_sources = {norm.split_collapse_entry(e)[0] for e in norm.FIELD_COLLAPSE_MAP["early_head_start"]}
    assert ehs_sources == {"ms_early_head_start", "nh_early_head_start", "wa_early_head_start"}
    assert not head_start_sources & ehs_sources


def test_early_head_start_and_head_start_are_independent():
    item = _ehs("washington", wa_head_start="No", wa_early_head_start="Yes")
    assert item["head_start"] is False
    assert item["early_head_start"] is True


# --------------------------------------------------------------------------- #
# Wheelchair accessible
# --------------------------------------------------------------------------- #
def _wheelchair(state, **source):
    return norm.normalize_item(dict(source), state)


@pytest.mark.parametrize(
    ("state", "field", "raw", "expected"),
    [
        ("connecticut", "ct_wheelchair_accessible", True, True),
        ("connecticut", "ct_wheelchair_accessible", False, False),
        ("tennessee", "tn_wheelchair_accessible", "Yes", True),
        ("tennessee", "tn_wheelchair_accessible", "No", False),
    ],
)
def test_wheelchair_accessible_ct_tn(state, field, raw, expected):
    item = _wheelchair(state, **{field: raw})
    assert item["wheelchair_accessible"] is expected
    assert item[field] == raw  # additive: source kept


@pytest.mark.parametrize(
    ("state", "field"),
    [("connecticut", "ct_wheelchair_accessible"), ("tennessee", "tn_wheelchair_accessible")],
)
def test_wheelchair_accessible_absent_is_unset(state, field):
    assert "wheelchair_accessible" not in _wheelchair(state, **{field: ""})
    assert "wheelchair_accessible" not in _wheelchair(state, **{field: None})
    assert "wheelchair_accessible" not in _wheelchair(state)


def test_wheelchair_accessible_nh_token_present():
    env = "WHEELCHAIR ACCESSIBLE, NO CAT, NO TV USE, OUTDOOR PLAY EQUIPMENT, MIXED AGE (3-5)"
    item = _wheelchair("new_hampshire", nh_environment=env)
    assert item["wheelchair_accessible"] is True
    assert item["nh_environment"] == env  # additive: source kept


def test_wheelchair_accessible_nh_token_anywhere_and_case_insensitive():
    item = _wheelchair("new_hampshire", nh_environment="NO CAT, wheelchair accessible ,NO TV USE")
    assert item["wheelchair_accessible"] is True


def test_wheelchair_accessible_nh_list_without_token_is_none_not_false():
    item = _wheelchair("new_hampshire", nh_environment="NO CAT, NO TV USE, MIXED AGE (3-5)")
    assert item.get("wheelchair_accessible") is None
    assert item.get("wheelchair_accessible") is not False
    assert item["nh_environment"] == "NO CAT, NO TV USE, MIXED AGE (3-5)"


def test_wheelchair_accessible_nh_negated_token_does_not_match():
    item = _wheelchair("new_hampshire", nh_environment="NOT WHEELCHAIR ACCESSIBLE, NO CAT")
    assert item.get("wheelchair_accessible") is None
    item = _wheelchair("new_hampshire", nh_environment="NO CAT, WHEELCHAIR ACCESSIBLE ONLY BY RAMP")
    assert item.get("wheelchair_accessible") is None


def test_wheelchair_accessible_nh_blank_environment_is_unset():
    assert "wheelchair_accessible" not in _wheelchair("new_hampshire", nh_environment="")
    assert "wheelchair_accessible" not in _wheelchair("new_hampshire")


def test_has_list_token_is_reusable_for_other_tokens():
    no_cat = cf.has_list_token("NO CAT")
    assert no_cat("WHEELCHAIR ACCESSIBLE, no cat") is True
    assert no_cat("NO CATS, WHEELCHAIR ACCESSIBLE") is None
    assert no_cat(None) is None
    assert no_cat(["X", " NO CAT "]) is True


# --------------------------------------------------------------------------- #
# Address / phone suppressed
# --------------------------------------------------------------------------- #
def _suppressed(state, **source):
    return norm.normalize_item(dict(source), state)


@pytest.mark.parametrize(
    ("state", "field"),
    [
        ("connecticut", "ct_address_suppressed"),
        ("kansas", "ks_address_suppressed"),
    ],
)
@pytest.mark.parametrize("raw", [True, False])
def test_address_suppressed_ct_ks_copy_real_bools(state, field, raw):
    item = _suppressed(state, **{field: raw})
    assert item["address_suppressed"] is raw  # False survives, not dropped as empty
    assert item[field] is raw  # additive: source kept
    assert "phone_suppressed" not in item


@pytest.mark.parametrize(
    ("display", "suppressed"),
    [(False, True), (True, False)],
)
def test_florida_display_flags_are_inverted(display, suppressed):
    item = _suppressed("florida", fl_display_address_on_web=display, fl_display_phone_on_web=display)
    assert item["address_suppressed"] is suppressed
    assert item["phone_suppressed"] is suppressed
    assert item["fl_display_address_on_web"] is display  # additive: sources kept
    assert item["fl_display_phone_on_web"] is display


def test_florida_address_and_phone_flags_are_independent():
    item = _suppressed("florida", fl_display_address_on_web=True, fl_display_phone_on_web=False)
    assert item["address_suppressed"] is False
    assert item["phone_suppressed"] is True


@pytest.mark.parametrize("raw", [True, False])
def test_massachusetts_contact_redacted_feeds_both_fields(raw):
    item = _suppressed("massachusetts", ma_contact_redacted=raw)
    assert item["address_suppressed"] is raw
    assert item["phone_suppressed"] is raw
    assert item["ma_contact_redacted"] is raw  # additive: source kept


def test_new_york_y_blank_and_missing_map_to_true_false_false():
    item = _suppressed("new_york", ny_address_omitted="Y", ny_phone_number_omitted="")
    assert item["address_suppressed"] is True
    assert item["phone_suppressed"] is False
    assert item["ny_address_omitted"] == "Y"  # additive: sources kept
    assert not item["ny_phone_number_omitted"]  # blank stays blank

    item = _suppressed("new_york", ny_address_omitted=None, ny_phone_number_omitted="Y")
    assert item["address_suppressed"] is False
    assert item["phone_suppressed"] is True

    item = _suppressed("new_york")  # columns missing entirely
    assert item["address_suppressed"] is False
    assert item["phone_suppressed"] is False


def test_new_york_unexpected_value_is_unset_and_logged(caplog):
    with caplog.at_level(logging.WARNING, logger="provider_scrape.common_fields"):
        item = _suppressed("new_york", ny_address_omitted="maybe", ny_phone_number_omitted="Y")
    assert "address_suppressed" not in item
    assert item["phone_suppressed"] is True
    assert "ny_address_omitted" in caplog.text
    assert "maybe" in caplog.text


def test_non_new_york_record_without_ny_fields_gets_no_false():
    for state in ("connecticut", "kansas", "florida", "massachusetts", "ohio"):
        item = _suppressed(state)
        assert "address_suppressed" not in item
        assert "phone_suppressed" not in item


def test_to_bool_is_strict_and_keeps_false():
    assert cf.to_bool(True) is True
    assert cf.to_bool(False) is False
    assert cf.to_bool("Yes") is True
    assert cf.to_bool("") is None
    assert cf.to_bool(None) is None
    assert cf.to_bool("maybe") is None  # unlike _coerce_bool, never read as True


# --------------------------------------------------------------------------- #
# Phone extension
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("321", "321"),
        ("2399", "2399"),
        ("1", "1"),
        ("120", "120"),
        (" 120 ", "120"),
        ("0042", "0042"),  # leading zeros kept
        ("11202_____", "11202"),
        ("3193______", "3193"),
        ("__________", None),
        ("_____", None),
        ("", None),
        ("   ", None),
        (None, None),
        ("N/A", None),
    ],
)
def test_phone_extension_digits(raw, expected):
    assert cf.phone_extension_digits(raw) == expected


@pytest.mark.parametrize("raw", ["NJ", "PA", "Salas", "class", "12ab", "1_2", "x123"])
def test_phone_extension_digits_rejects_mixed_text_and_logs(raw, caplog):
    with caplog.at_level(logging.WARNING, logger="provider_scrape.common_fields"):
        assert cf.phone_extension_digits(raw) is None  # never "", never digits pulled out
    assert repr(raw) in caplog.text


@pytest.mark.parametrize("raw", ["", "   ", "__________", None, "N/A"])
def test_phone_extension_digits_blanks_and_masks_are_silent(raw, caplog):
    with caplog.at_level(logging.WARNING, logger="provider_scrape.common_fields"):
        assert cf.phone_extension_digits(raw) is None
    assert caplog.text == ""


@pytest.mark.parametrize(
    ("state", "field", "raw", "expected"),
    [
        ("new_jersey", "nj_phone_extension", "321", "321"),
        ("new_jersey", "nj_phone_extension", "2399", "2399"),
        ("new_jersey", "nj_phone_extension", "007", "007"),
        ("new_jersey", "nj_phone_extension", "NJ", None),
        ("new_jersey", "nj_phone_extension", "PA", None),
        ("new_jersey", "nj_phone_extension", "Salas", None),
        ("new_york", "ny_phone_extension", "120", "120"),
        ("new_york", "ny_phone_extension", "1", "1"),
        ("new_york", "ny_phone_extension", " 120 ", "120"),
        ("new_york", "ny_phone_extension", "11202_____", "11202"),
        ("new_york", "ny_phone_extension", "3193______", "3193"),
        ("new_york", "ny_phone_extension", "__________", None),
        ("new_york", "ny_phone_extension", "class", None),
    ],
)
def test_phone_extension_collapse(state, field, raw, expected):
    item = norm.normalize_item({field: raw}, state)
    if expected is None:
        assert "phone_extension" not in item
    else:
        assert item["phone_extension"] == expected
        assert isinstance(item["phone_extension"], str)
    assert item[field] == raw.strip()  # additive: source kept (whitespace-cleaned only)


def test_phone_extension_absent_when_no_source():
    assert "phone_extension" not in norm.normalize_item({}, "new_york")


# --------------------------------------------------------------------------- #
# Doing business as
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("state", "field", "raw", "expected"),
    [
        ("florida", "fl_dba", "The Learning Experience", "The Learning Experience"),
        ("florida", "fl_dba", "  Sunny Days  ", "Sunny Days"),
        ("florida", "fl_dba", "N/A", None),
        ("florida", "fl_dba", "   ", None),
        ("oklahoma", "ok_doing_business_as", "Lil Bison Learning", "Lil Bison Learning"),
        ("oklahoma", "ok_doing_business_as", "N/A", None),
        ("oklahoma", "ok_doing_business_as", "", None),
    ],
)
def test_doing_business_as_collapse(state, field, raw, expected):
    item = norm.normalize_item({field: raw}, state)
    if expected is None:
        assert "doing_business_as" not in item
    else:
        assert item["doing_business_as"] == expected
    assert field in item  # additive: source kept


@pytest.mark.parametrize(
    ("state", "field"),
    [("washington", "wa_license_name"), ("massachusetts", "ma_umbrella_name")],
)
def test_doing_business_as_not_fed_by_license_name_or_umbrella(state, field):
    item = norm.normalize_item({field: "Some Legal Entity LLC"}, state)
    assert "doing_business_as" not in item
    assert item[field] == "Some Legal Entity LLC"


def test_doing_business_as_absent_when_no_source():
    assert "doing_business_as" not in norm.normalize_item({}, "florida")


# --------------------------------------------------------------------------- #
# License holder address
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("state", "field", "raw", "expected"),
    [
        ("michigan", "mi_licensee_address", "5005 Meadows Road, Lake Oswego, OR, 97035",
         "5005 Meadows Road, Lake Oswego, OR, 97035"),
        ("michigan", "mi_licensee_address", "null, null, null,", None),
        ("michigan", "mi_licensee_address", "NULL,null", None),
        ("michigan", "mi_licensee_address", "", None),
        # null piece in the middle / at the start: dropped, the rest kept
        ("michigan", "mi_licensee_address", "21885 Dunham Road, null, MI, 48036",
         "21885 Dunham Road, MI, 48036"),
        ("michigan", "mi_licensee_address", "null, Camden, MI, 49232", "Camden, MI, 49232"),
        ("michigan", "mi_licensee_address", "1 Main St, Camden, MI, NULL", "1 Main St, Camden, MI"),
        # "null" inside a word is never stripped
        ("michigan", "mi_licensee_address", "100 Nullarbor Way, null, MI, 48036",
         "100 Nullarbor Way, MI, 48036"),
        ("michigan", "mi_licensee_address", "100 Nullarbor Way", "100 Nullarbor Way"),
        # NC: case kept (ALL CAPS stays ALL CAPS), whitespace/comma cleanup
        ("north_carolina", "nc_owner_mailing_address",
         "1901 HERBERT SPAUGH LANE CHARLOTTE, NC 28208",
         "1901 HERBERT SPAUGH LANE CHARLOTTE, NC 28208"),
        ("north_carolina", "nc_owner_mailing_address", "PO BOX 905 BURLINGTON, NC",
         "PO BOX 905 BURLINGTON, NC"),
        ("north_carolina", "nc_owner_mailing_address", "  PO BOX  905   BURLINGTON ,NC  27215 , USA ",
         "PO BOX 905 BURLINGTON, NC 27215"),
        ("north_carolina", "nc_owner_mailing_address", "   ", None),
    ],
)
def test_license_holder_address_collapse(state, field, raw, expected):
    item = norm.normalize_item({field: raw}, state)
    if expected is None:
        assert "license_holder_address" not in item
    else:
        assert item["license_holder_address"] == expected
    assert field in item  # additive: source kept


@pytest.mark.parametrize(
    ("state", "field"),
    [("michigan", "mi_licensee_address"), ("north_carolina", "nc_owner_mailing_address")],
)
def test_license_holder_address_does_not_feed_mailing_address(state, field):
    item = norm.normalize_item({field: "5005 Meadows Road, Lake Oswego, OR, 97035"}, state)
    assert "mailing_address" not in item


def test_mo_mailing_address_not_mapped():
    item = norm.normalize_item({"mo_mailing_address": "1 Corp Way, Overland Park, KS"}, "missouri")
    assert "license_holder_address" not in item
    assert "mailing_address" not in item
    assert item["mo_mailing_address"] == "1 Corp Way, Overland Park, KS"


def test_license_holder_address_absent_when_no_source():
    assert "license_holder_address" not in norm.normalize_item({}, "michigan")


# --------------------------------------------------------------------------- #
# Days of operation
# --------------------------------------------------------------------------- #
WEEK = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


@pytest.mark.parametrize(
    ("state", "field", "raw", "expected"),
    [
        ("georgia", "ga_operating_days", "MO,TU,WE,TH,FR", WEEK[:5]),
        ("georgia", "ga_operating_days", "MO,TU,WE,TH,FR,SA,SU", WEEK),
        ("iowa", "ia_days_of_operation", "Mon, Tue, Wed, Thu, Fri", WEEK[:5]),
        ("iowa", "ia_days_of_operation", "Sat, Sun", ["Saturday", "Sunday"]),
        ("vermont", "vt_days_of_operation",
         "Monday, Tuesday, Wednesday, Thursday, Friday", WEEK[:5]),
        ("vermont", "vt_days_of_operation", "Saturday", ["Saturday"]),
    ],
)
def test_days_of_operation_collapse(state, field, raw, expected):
    item = norm.normalize_item({field: raw}, state)
    assert item["days_of_operation"] == expected
    assert item[field] == raw  # additive: source kept


def test_day_names_out_of_order_and_duplicates():
    assert cf.day_names("SU, FR, MO, FR, mo") == ["Monday", "Friday", "Sunday"]
    assert cf.day_names("Wed, Mon, Wed") == ["Monday", "Wednesday"]


@pytest.mark.parametrize(
    "raw",
    ["mo,tu", "Mo, Tu", "  MO ,  TU  ", "MONDAY,tuesday", "mon,TUE", "Monday ,\tTue"],
)
def test_day_names_case_and_whitespace(raw):
    assert cf.day_names(raw) == ["Monday", "Tuesday"]


def test_day_names_accepts_list():
    assert cf.day_names(["Tue", "MO", "Mon, Sun"]) == ["Monday", "Tuesday", "Sunday"]


def test_day_names_unrecognised_token_logged_and_dropped(caplog):
    with caplog.at_level(logging.WARNING, logger="provider_scrape.common_fields"):
        assert cf.day_names("MO, XX, Funday, FR") == ["Monday", "Friday"]
    assert "XX" in caplog.text
    assert "Funday" in caplog.text


@pytest.mark.parametrize("raw", ["", "   ", ",", "XX, YY", "N/A", None, [], ["bogus"]])
def test_day_names_nothing_usable_is_none(raw):
    assert cf.day_names(raw) is None


def test_days_of_operation_all_unrecognised_leaves_field_unset():
    item = norm.normalize_item({"ga_operating_days": "XX,YY"}, "georgia")
    assert "days_of_operation" not in item
    assert item["ga_operating_days"] == "XX,YY"


def test_days_of_operation_absent_when_no_source():
    assert "days_of_operation" not in norm.normalize_item({}, "georgia")


def test_days_of_operation_list_survives_pipeline():
    item = norm.normalize_item({"ia_days_of_operation": "Sat,Mon"}, "iowa")
    assert item["days_of_operation"] == ["Monday", "Saturday"]
    # idempotent when the pipeline runs on its own output
    assert norm.normalize_item(dict(item), "iowa")["days_of_operation"] == [
        "Monday",
        "Saturday",
    ]


# --------------------------------------------------------------------------- #
# Weekly schedule
# --------------------------------------------------------------------------- #
def _row(day, open_, close, **extra):
    return {"day": day, "open": open_, "close": close, **extra}


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("6:00 AM", "06:00"),
        ("7:00AM", "07:00"),
        ("07:00 AM", "07:00"),
        ("07:00", "07:00"),
        ("17:30", "17:30"),
        ("12:00 AM", "00:00"),
        ("12:00 PM", "12:00"),
        ("12:30 AM", "00:30"),
        ("12:30PM", "12:30"),
        ("11:59 PM", "23:59"),
        ("6:30pm", "18:30"),
        ("6 PM", "18:00"),
        ("  5:00 PM ", "17:00"),
        ("00:00", "00:00"),
        ("23:59", "23:59"),
        ("07:00:00 AM", "07:00"),
        ("06:30:00 PM", "18:30"),
        ("17:30:00", "17:30"),
        ("00:00 AM", "00:00"),
        ("0:30 AM", "00:30"),
        ("24:00", "24:00"),
    ],
)
def test_parse_time_formats(raw, expected):
    assert cf.parse_time(raw) == expected


@pytest.mark.parametrize(
    "raw", [None, "", "Closed", "25:00", "13:00 PM", "0:30 PM", "6:75 AM", "07:00:75 AM", "24:30", "24 AM", "7", "noon", 7, "6:00 XM"]
)
def test_parse_time_unparseable_is_none(raw):
    assert cf.parse_time(raw) is None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("6:30AM - 6:00PM", ("6:30AM", "6:00PM")),
        ("6:30AM to 6:00PM", ("6:30AM", "6:00PM")),
        ("07:00-17:30", ("07:00", "17:30")),
        ("6:30 AM – 6:00 PM", ("6:30 AM", "6:00 PM")),
        ("7:00 AM TO 5:00 PM", ("7:00 AM", "5:00 PM")),
    ],
)
def test_split_time_range(raw, expected):
    assert cf.split_time_range(raw) == expected


@pytest.mark.parametrize("raw", [None, "", "Closed", "6:00AM", "1 - 2 - 3", " - 5PM"])
def test_split_time_range_bad_is_none(raw):
    assert cf.split_time_range(raw) is None


def test_weekly_schedule_connecticut_copied():
    sched = [_row("Monday", "07:30", "17:30"), _row("Tuesday", "07:30", "17:30")]
    item = norm.normalize_item({"ct_schedule": [dict(r) for r in sched]}, "connecticut")
    assert item["weekly_schedule"] == sched
    assert item["ct_schedule"] == sched  # additive: source kept
    assert item["days_of_operation"] == ["Monday", "Tuesday"]


def test_weekly_schedule_indiana_12h_to_24h_including_24h_care():
    raw = [
        {"day": "Monday", "open": "6:00 AM", "close": "6:00 PM"},
        {"day": "Tuesday", "open": "12:00 AM", "close": "11:59 PM"},
        {"day": "Wednesday", "open": "12:00 PM", "close": "12:00 AM"},
    ]
    item = norm.normalize_item({"in_schedule": raw}, "indiana")
    assert item["weekly_schedule"] == [
        _row("Monday", "06:00", "18:00"),
        _row("Tuesday", "00:00", "23:59"),
        _row("Wednesday", "12:00", "00:00"),
    ]
    assert item["in_schedule"] == raw


def test_weekly_schedule_new_hampshire_multi_session_rows_kept():
    raw = [
        {"day": "Tuesday", "start": "07:00 AM", "end": "08:30 AM"},
        {"day": "Monday", "start": "07:00 AM", "end": "08:30 AM"},
        {"day": "Monday", "start": "03:00 PM", "end": "05:00 PM"},
    ]
    item = norm.normalize_item({"nh_schedule": raw}, "new_hampshire")
    assert item["weekly_schedule"] == [
        _row("Monday", "07:00", "08:30"),
        _row("Monday", "15:00", "17:00"),  # same day: source order kept
        _row("Tuesday", "07:00", "08:30"),
    ]
    assert item["days_of_operation"] == ["Monday", "Tuesday"]
    assert item["nh_schedule"] == raw


def test_weekly_schedule_oklahoma_closed_days_have_no_row():
    raw = [
        {"day": "Monday", "hours": "6:30AM - 6:00PM"},
        {"day": "Saturday", "hours": None},
        {"day": "Sunday", "hours": "Closed"},
        {"day": "Friday", "hours": "7:00AM - 12:00PM"},
    ]
    item = norm.normalize_item({"ok_schedule": raw}, "oklahoma")
    assert item["weekly_schedule"] == [
        _row("Monday", "06:30", "18:00"),
        _row("Friday", "07:00", "12:00"),
    ]
    assert item["days_of_operation"] == ["Monday", "Friday"]
    assert item["ok_schedule"] == raw


def test_weekly_schedule_wyoming_renamed_keys():
    raw = [{"day": "Monday", "start": "07:00", "end": "17:30"}]
    item = norm.normalize_item({"wy_schedule": raw}, "wyoming")
    assert item["weekly_schedule"] == [_row("Monday", "07:00", "17:30")]
    assert item["wy_schedule"] == raw


def _ma_sched(kind, *days):
    return {
        "schedule_type": kind,
        "drop_in": False,
        "extended_day": False,
        "days": [{"day": d, "start": "08:15 AM", "end": "04:15 PM"} for d in days],
    }


def _ma(schedules):
    return norm.normalize_item({"ma_schedules": schedules}, "massachusetts")


def test_ma_prefers_full_year_and_tags_rows():
    item = _ma(
        [
            _ma_sched("Summer Schedule", "Monday"),
            _ma_sched("School Year Schedule", "Tuesday"),
            _ma_sched("Full Year Schedule", "Monday", "Wednesday"),
        ]
    )
    assert item["weekly_schedule"] == [
        _row("Monday", "08:15", "16:15", schedule_type="Full Year Schedule"),
        _row("Wednesday", "08:15", "16:15", schedule_type="Full Year Schedule"),
    ]
    assert item["days_of_operation"] == ["Monday", "Wednesday"]
    assert len(item["ma_schedules"]) == 3  # source kept


def test_ma_falls_back_to_first_school_year():
    item = _ma(
        [
            _ma_sched("Temporary Schedule", "Monday"),
            _ma_sched("Summer Schedule", "Friday"),
            _ma_sched("School Year Schedule", "Tuesday"),
            _ma_sched("School Year Schedule", "Thursday"),
        ]
    )
    assert item["weekly_schedule"] == [
        _row("Tuesday", "08:15", "16:15", schedule_type="School Year Schedule")
    ]


def test_ma_falls_back_to_first_summer():
    item = _ma(
        [
            _ma_sched("Weekend Schedule", "Saturday"),
            _ma_sched("Summer Schedule", "Monday"),
            _ma_sched("Summer Schedule", "Friday"),
        ]
    )
    assert item["weekly_schedule"] == [
        _row("Monday", "08:15", "16:15", schedule_type="Summer Schedule")
    ]


def test_ma_falls_back_to_first_in_source_order_even_when_type_blank():
    item = _ma([_ma_sched("", "Monday"), _ma_sched("Evening Schedule", "Friday")])
    assert item["weekly_schedule"] == [_row("Monday", "08:15", "16:15", schedule_type="")]
    item = _ma([_ma_sched("Evening Schedule", "Friday"), _ma_sched("AM Session", "Monday")])
    assert item["weekly_schedule"][0]["schedule_type"] == "Evening Schedule"


def test_ma_schedule_without_days_is_not_picked_over_usable_one():
    item = _ma([_ma_sched("Full Year Schedule"), _ma_sched("Summer Schedule", "Monday")])
    assert item["weekly_schedule"][0]["schedule_type"] == "Summer Schedule"


def test_ma_no_usable_schedule_leaves_field_unset():
    assert "weekly_schedule" not in _ma([_ma_sched("Full Year Schedule")])
    assert "weekly_schedule" not in _ma([])


FL_HOURS = {
    "monday": "6:45AM to 6:00PM",
    "tuesday": "6:45AM to 6:00PM",
    "wednesday": "6:00PM to 11:59PM; 12:00AM to 6:00AM",
    "thursday": "10:00PM to 6:00AM",
    "friday": None,
    "saturday": "Closed",
    "sunday": "Closed",
}


def test_weekly_schedule_florida_from_dict_hours():
    original = dict(FL_HOURS)
    item = norm.normalize_item({"hours": dict(FL_HOURS)}, "florida")
    assert item["weekly_schedule"] == [
        _row("Monday", "06:45", "18:00"),
        _row("Tuesday", "06:45", "18:00"),
        _row("Wednesday", "18:00", "23:59"),
        _row("Wednesday", "00:00", "06:00"),  # multi-range split on ';'
        _row("Thursday", "22:00", "06:00"),  # overnight kept as reported
    ]
    assert item["days_of_operation"] == ["Monday", "Tuesday", "Wednesday", "Thursday"]
    assert item["hours"] == original  # hours left exactly as it was


def test_florida_closed_all_week_leaves_field_unset():
    item = norm.normalize_item({"hours": {"monday": "Closed", "sunday": "Closed"}}, "florida")
    assert "weekly_schedule" not in item
    assert "days_of_operation" not in item


@pytest.mark.parametrize("state", ["florida", "georgia", "ohio", "texas"])
def test_string_hours_never_feed_weekly_schedule(state):
    item = norm.normalize_item({"hours": "Mon-Fri 6:00 AM - 6:00 PM"}, state)
    assert "weekly_schedule" not in item
    assert item["hours"] == "Mon-Fri 6:00 AM - 6:00 PM"


def test_dict_hours_outside_florida_does_not_feed_weekly_schedule():
    item = norm.normalize_item({"hours": {"monday": "6:45AM to 6:00PM"}}, "texas")
    assert "weekly_schedule" not in item


def test_bad_rows_dropped_and_logged_rest_kept(caplog):
    raw = [
        {"day": "Monday", "open": "7:00 AM", "close": "5:00 PM"},
        {"day": "Funday", "open": "7:00 AM", "close": "5:00 PM"},
        {"day": "Tuesday", "open": "soon", "close": "5:00 PM"},
        {"day": "Wednesday", "open": "7:00 AM", "close": None},
        "garbage",
        {"day": "Friday", "open": "8:00 AM", "close": "4:00 PM"},
    ]
    with caplog.at_level(logging.WARNING, logger="provider_scrape.common_fields"):
        item = norm.normalize_item({"in_schedule": raw}, "indiana")
    assert item["weekly_schedule"] == [
        _row("Monday", "07:00", "17:00"),
        _row("Friday", "08:00", "16:00"),
    ]
    assert "Funday" in caplog.text
    assert "soon" in caplog.text
    assert "garbage" in caplog.text


def test_bad_range_text_dropped_and_logged(caplog):
    raw = [{"day": "Monday", "hours": "whenever"}, {"day": "Tuesday", "hours": "7:00AM - 5:00PM"}]
    with caplog.at_level(logging.WARNING, logger="provider_scrape.common_fields"):
        item = norm.normalize_item({"ok_schedule": raw}, "oklahoma")
    assert item["weekly_schedule"] == [_row("Tuesday", "07:00", "17:00")]
    assert "whenever" in caplog.text


@pytest.mark.parametrize(
    ("state", "field", "raw"),
    [
        ("connecticut", "ct_schedule", [{"day": "Moonday", "open": "07:00", "close": "08:00"}]),
        ("indiana", "in_schedule", []),
        ("new_hampshire", "nh_schedule", "not a list"),
        ("oklahoma", "ok_schedule", [{"day": "Monday", "hours": None}]),
    ],
)
def test_nothing_usable_leaves_schedule_and_days_unset(state, field, raw):
    item = norm.normalize_item({field: raw}, state)
    assert "weekly_schedule" not in item
    assert "days_of_operation" not in item


def test_days_derived_from_schedule_are_canonical_and_distinct():
    raw = [
        {"day": "sunday", "open": "08:00", "close": "12:00"},
        {"day": "Mon", "open": "08:00", "close": "12:00"},
        {"day": "Monday", "open": "13:00", "close": "17:00"},
    ]
    item = norm.normalize_item({"ct_schedule": raw}, "connecticut")
    assert [r["day"] for r in item["weekly_schedule"]] == ["Monday", "Monday", "Sunday"]
    assert item["days_of_operation"] == ["Monday", "Sunday"]


def test_derived_days_never_overwrite_a_direct_source():
    # A direct source is collapsed before the schedule adapter runs.
    record = {
        "ga_operating_days": "MO,TU",
        "ct_schedule": [{"day": "Friday", "open": "07:00", "close": "08:00"}],
    }
    item = norm.normalize_item(record, "georgia")
    assert item["days_of_operation"] == ["Monday", "Tuesday"]
    # Even if an adapter state already carries days, they stay.
    record = {
        "days_of_operation": ["Saturday"],
        "ct_schedule": [{"day": "Friday", "open": "07:00", "close": "08:00"}],
    }
    item = norm.normalize_item(record, "connecticut")
    assert item["days_of_operation"] == ["Saturday"]
    assert item["weekly_schedule"] == [_row("Friday", "07:00", "08:00")]


def test_weekly_schedule_idempotent_through_pipeline():
    raw = [{"day": "Monday", "open": "6:00 AM", "close": "6:00 PM"}]
    once = norm.normalize_item({"in_schedule": raw}, "indiana")
    twice = norm.normalize_item(dict(once), "indiana")
    assert twice["weekly_schedule"] == once["weekly_schedule"]
    assert twice["days_of_operation"] == ["Monday"]


def test_weekly_schedule_absent_when_no_source():
    assert "weekly_schedule" not in norm.normalize_item({}, "connecticut")


# --------------------------------------------------------------------------- #
# Scholarships accepted
# --------------------------------------------------------------------------- #
NH_TOKEN_VALUE = "PREVENTIVE AND PROTECTIVE CARE, STATE CC ASSISTANCE (SCHOLARSHIP)"


@pytest.mark.parametrize(
    ("state", "field", "raw", "expected"),
    [
        ("florida", "fl_is_offering_school_readiness", True, True),
        ("florida", "fl_is_offering_school_readiness", False, False),
        ("virginia", "va_current_subsidy_provider", "Yes", True),
        ("virginia", "va_current_subsidy_provider", "No", False),
        ("washington", "wa_subsidy", "Yes", True),
        ("new_hampshire", "nh_financial_assistance", NH_TOKEN_VALUE, True),
        ("new_hampshire", "nh_financial_assistance",
         "state cc assistance (scholarship)", True),
    ],
)
def test_scholarships_accepted_collapse(state, field, raw, expected):
    item = norm.normalize_item({field: raw}, state)
    assert item["scholarships_accepted"] is expected
    assert item[field] == raw  # additive: source kept


@pytest.mark.parametrize("raw", [None, ""])
def test_scholarships_accepted_wa_empty_is_unset(raw):
    item = norm.normalize_item({"wa_subsidy": raw}, "washington")
    assert "scholarships_accepted" not in item


@pytest.mark.parametrize(
    "raw", ["PREVENTIVE AND PROTECTIVE CARE", "NOT STATE CC ASSISTANCE", "", None]
)
def test_scholarships_accepted_nh_without_token_is_unset(raw):
    item = norm.normalize_item({"nh_financial_assistance": raw}, "new_hampshire")
    assert "scholarships_accepted" not in item


@pytest.mark.parametrize("spider_value", [False, True, "No", "Yes"])
def test_scholarships_accepted_nh_fallback_never_overwrites_spider(spider_value):
    item = norm.normalize_item(
        {
            "scholarships_accepted": spider_value,
            "nh_financial_assistance": NH_TOKEN_VALUE,
        },
        "new_hampshire",
    )
    assert item["scholarships_accepted"] is (spider_value in (True, "Yes"))
    assert item["nh_financial_assistance"] == NH_TOKEN_VALUE


def test_scholarships_accepted_ok_contract_number_not_mapped():
    item = norm.normalize_item(
        {"scholarships_accepted": False, "ok_subsidy_contract_number": "59555"},
        "oklahoma",
    )
    assert item["scholarships_accepted"] is False
    assert item["ok_subsidy_contract_number"] == "59555"
    # contract number alone does not imply acceptance
    bare = norm.normalize_item({"ok_subsidy_contract_number": "59555"}, "oklahoma")
    assert "scholarships_accepted" not in bare


def test_scholarships_accepted_absent_when_no_source():
    assert "scholarships_accepted" not in norm.normalize_item({}, "florida")


# --------------------------------------------------------------------------- #
# License begin date (NC extension)
# --------------------------------------------------------------------------- #
def test_license_begin_date_nc_effective_date_to_iso():
    item = norm.normalize_item(
        {"nc_license_effective_date": "8/24/2026"}, "north_carolina"
    )
    assert item["license_begin_date"] == "2026-08-24"
    assert item["nc_license_effective_date"] == "8/24/2026"  # source kept


def test_license_begin_date_nc_future_date_passes_through():
    item = norm.normalize_item(
        {"nc_license_effective_date": "1/2/2029"}, "north_carolina"
    )
    assert item["license_begin_date"] == "2029-01-02"


def test_license_begin_date_spider_value_not_overwritten():
    item = norm.normalize_item(
        {
            "license_begin_date": "2020-01-01",
            "nc_license_effective_date": "8/24/2026",
        },
        "north_carolina",
    )
    assert item["license_begin_date"] == "2020-01-01"
    assert item["nc_license_effective_date"] == "8/24/2026"


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_license_begin_date_nc_blank_is_unset(raw):
    item = norm.normalize_item({"nc_license_effective_date": raw}, "north_carolina")
    assert not item.get("license_begin_date")


def test_license_begin_date_non_nc_item_unaffected():
    item = norm.normalize_item({"fl_inspection_id": "1"}, "florida")
    assert "license_begin_date" not in item
    item = norm.normalize_item({"license_begin_date": "03/01/2025"}, "minnesota")
    assert item["license_begin_date"] == "2025-03-01"


# --------------------------------------------------------------------------- #
# Source provider ID
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (61, "61"),
        ("003251", "003251"),
        ("  003251  ", "003251"),
        (11216.0, "11216"),
        (3.5, None),
        (True, None),
        (False, None),
        ("", None),
        ("   ", None),
        ("N/A", None),
        ("None", None),
        (None, None),
        ([], None),
    ],
)
def test_to_id_string(raw, expected):
    assert cf.to_id_string(raw) == expected


SOURCE_PROVIDER_ID_SOURCES = [
    ("alaska", "ak_facility_gen_id", 1234),
    ("arizona", "az_facility_id", "a0Ht0000001abcDEF"),
    ("connecticut", "ct_provider_id", 61),
    ("hawaii", "hi_service_id", 77),
    ("illinois", "il_provider_id", "003251"),
    ("indiana", "in_provider_id", "682844"),
    ("kentucky", "ky_provider_id", 9001),
    ("louisiana", "la_unique_id", "001003"),
    ("massachusetts", "ma_program_number", "P-261705"),
    ("mississippi", "ms_facility_id", "20014077"),
    ("nebraska", "ne_resource_id", 4521),
    ("new_hampshire", "nh_account_id", "001abc"),
    ("new_jersey", "nj_unique_program_id", "711671"),
    ("nevada", "nv_operation_id", "626"),
    ("new_york", "ny_facility_id", "41021"),
    ("north_dakota", "nd_program_id", "ObjId9"),
    ("south_carolina", "sc_provider_id", "2465"),
    ("tennessee", "tn_provider_id", "116896"),
    ("texas", "tx_operation_id", "1404306"),
    ("virginia", "va_ID", "35291"),
    ("washington", "wa_provider_id", "961182"),
    ("wisconsin", "wi_facility_number", "2006942"),
    ("wyoming", "wy_facility_id", "goa2ejdubka4h4o"),
]


@pytest.mark.parametrize(("state", "source", "raw"), SOURCE_PROVIDER_ID_SOURCES)
def test_source_provider_id_per_state(state, source, raw):
    item = norm.normalize_item({source: raw}, state)
    assert item["source_provider_id"] == str(raw)
    assert item[source] == raw  # source kept


def test_source_provider_id_alaska_uses_gen_id_not_facility_number():
    item = norm.normalize_item(
        {"ak_facility_gen_id": 555, "ak_facility_number": 1000652}, "alaska"
    )
    assert item["source_provider_id"] == "555"
    assert item["ak_facility_number"] == 1000652
    only_number = norm.normalize_item({"ak_facility_number": 1000652}, "alaska")
    assert "source_provider_id" not in only_number


def test_source_provider_id_nevada_no_license_base_fallback():
    item = norm.normalize_item({"nv_license_base": "NV123"}, "nevada")
    assert "source_provider_id" not in item


def test_source_provider_id_wisconsin_ignores_location_number():
    item = norm.normalize_item({"wi_location_number": "0001"}, "wisconsin")
    assert "source_provider_id" not in item


def test_source_provider_id_vermont_excluded():
    item = norm.normalize_item(
        {"vt_provider_id": "123", "license_number": "123"}, "vermont"
    )
    assert "source_provider_id" not in item
    assert item["vt_provider_id"] == "123"


def test_source_provider_id_blank_source_unset():
    item = norm.normalize_item({"il_provider_id": "  "}, "illinois")
    assert not item.get("source_provider_id")

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

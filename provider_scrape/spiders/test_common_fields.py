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
# Frozen copy of the original collapse implementation and map (sources only). Fields
# whose collapse later gained transforms or sources (accreditation) are tested
# on their own below.
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
    "meals": ["ut_meals", "ga_meals", "hi_meals", "nm_meals", "nj_meal_options_labels", "pa_meal_options"],
    "accepting_new_children": ["co_accepting_new_children", "ga_accepting_new_children", "ct_accepting_referrals"],
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


@pytest.mark.parametrize("field", ["ms_early_head_start", "nh_early_head_start", "wa_early_head_start"])
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
        (
            "michigan",
            "mi_licensee_address",
            "5005 Meadows Road, Lake Oswego, OR, 97035",
            "5005 Meadows Road, Lake Oswego, OR, 97035",
        ),
        ("michigan", "mi_licensee_address", "null, null, null,", None),
        ("michigan", "mi_licensee_address", "NULL,null", None),
        ("michigan", "mi_licensee_address", "", None),
        # null piece in the middle / at the start: dropped, the rest kept
        ("michigan", "mi_licensee_address", "21885 Dunham Road, null, MI, 48036", "21885 Dunham Road, MI, 48036"),
        ("michigan", "mi_licensee_address", "null, Camden, MI, 49232", "Camden, MI, 49232"),
        ("michigan", "mi_licensee_address", "1 Main St, Camden, MI, NULL", "1 Main St, Camden, MI"),
        # "null" inside a word is never stripped
        ("michigan", "mi_licensee_address", "100 Nullarbor Way, null, MI, 48036", "100 Nullarbor Way, MI, 48036"),
        ("michigan", "mi_licensee_address", "100 Nullarbor Way", "100 Nullarbor Way"),
        # NC: case kept (ALL CAPS stays ALL CAPS), whitespace/comma cleanup
        (
            "north_carolina",
            "nc_owner_mailing_address",
            "1901 HERBERT SPAUGH LANE CHARLOTTE, NC 28208",
            "1901 HERBERT SPAUGH LANE CHARLOTTE, NC 28208",
        ),
        ("north_carolina", "nc_owner_mailing_address", "PO BOX 905 BURLINGTON, NC", "PO BOX 905 BURLINGTON, NC"),
        (
            "north_carolina",
            "nc_owner_mailing_address",
            "  PO BOX  905   BURLINGTON ,NC  27215 , USA ",
            "PO BOX 905 BURLINGTON, NC 27215",
        ),
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


def test_mo_mailing_address_feeds_license_holder_address():
    raw = "6901 SHAWNEE MISSION PKWY STE 300, OVERLAND PARK, KS 66202-4029"
    item = norm.normalize_item({"mo_mailing_address": raw}, "missouri")
    assert item["license_holder_address"] == raw
    assert "mailing_address" not in item
    assert item["mo_mailing_address"] == raw  # additive: source kept


def test_mo_mailing_address_does_not_overwrite_license_holder_address():
    item = norm.normalize_item(
        {"mo_mailing_address": "1 Corp Way, Overland Park, KS", "license_holder_address": "9 Owner St, Joplin, MO"},
        "missouri",
    )
    assert item["license_holder_address"] == "9 Owner St, Joplin, MO"


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
        ("vermont", "vt_days_of_operation", "Monday, Tuesday, Wednesday, Thursday, Friday", WEEK[:5]),
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
    "raw",
    [
        None,
        "",
        "Closed",
        "25:00",
        "13:00 PM",
        "0:30 PM",
        "6:75 AM",
        "07:00:75 AM",
        "24:30",
        "24 AM",
        "7",
        "noon",
        7,
        "6:00 XM",
    ],
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
    assert item["weekly_schedule"] == [_row("Tuesday", "08:15", "16:15", schedule_type="School Year Schedule")]


def test_ma_falls_back_to_first_summer():
    item = _ma(
        [
            _ma_sched("Weekend Schedule", "Saturday"),
            _ma_sched("Summer Schedule", "Monday"),
            _ma_sched("Summer Schedule", "Friday"),
        ]
    )
    assert item["weekly_schedule"] == [_row("Monday", "08:15", "16:15", schedule_type="Summer Schedule")]


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
        ("new_hampshire", "nh_financial_assistance", "state cc assistance (scholarship)", True),
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


@pytest.mark.parametrize("raw", ["PREVENTIVE AND PROTECTIVE CARE", "NOT STATE CC ASSISTANCE", "", None])
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
    item = norm.normalize_item({"nc_license_effective_date": "8/24/2026"}, "north_carolina")
    assert item["license_begin_date"] == "2026-08-24"
    assert item["nc_license_effective_date"] == "8/24/2026"  # source kept


def test_license_begin_date_nc_future_date_passes_through():
    item = norm.normalize_item({"nc_license_effective_date": "1/2/2029"}, "north_carolina")
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
    item = norm.normalize_item({"ak_facility_gen_id": 555, "ak_facility_number": 1000652}, "alaska")
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
    item = norm.normalize_item({"vt_provider_id": "123", "license_number": "123"}, "vermont")
    assert "source_provider_id" not in item
    assert item["vt_provider_id"] == "123"


def test_source_provider_id_blank_source_unset():
    item = norm.normalize_item({"il_provider_id": "  "}, "illinois")
    assert not item.get("source_provider_id")


# --- inspector (inspection-level) -----------------------------------------

INSPECTOR_SOURCES = [
    ("alaska", "ak_licensing_specialist", "Juliana Burya", "Juliana Burya"),
    ("massachusetts", "ma_licensor", "Madelyn Marichal", "Madelyn Marichal"),
    ("massachusetts", "ma_investigator", "Pat Example", "Pat Example"),
    ("missouri", "mo_specialist", "PENNY SNODGRASS", "Penny Snodgrass"),
    ("montana", "mt_inspector_name", "Jodi Linne", "Jodi Linne"),
    ("new_hampshire", "nh_licensor", "Olya Mahoney", "Olya Mahoney"),
    ("rhode_island", "ri_licensor", "Jessica Ullrich", "Jessica Ullrich"),
]


@pytest.mark.parametrize(("state", "source", "raw", "expected"), INSPECTOR_SOURCES)
def test_inspector_per_state(state, source, raw, expected):
    insp = norm.normalize_inspection({source: raw}, state)
    assert insp["inspector"] == expected
    assert insp[source] == raw  # source kept (collapse is additive)


def test_inspector_mo_all_caps_and_last_first_flip():
    caps = norm.normalize_inspection({"mo_specialist": "CHARMEL WILLIAMS"}, "missouri")
    assert caps["inspector"] == "Charmel Williams"
    flipped = norm.normalize_inspection({"mo_specialist": "HUGHES, CHRISTINE D"}, "missouri")
    assert flipped["inspector"] == "Christine D Hughes"
    assert flipped["mo_specialist"] == "HUGHES, CHRISTINE D"


def test_inspector_ma_licensor_row():
    insp = norm.normalize_inspection({"ma_licensor": "Madelyn Marichal"}, "massachusetts")
    assert insp["inspector"] == "Madelyn Marichal"
    assert "ma_investigator" not in insp


def test_inspector_ma_investigator_row():
    insp = norm.normalize_inspection({"ma_investigator": "JOE SMITH"}, "massachusetts")
    assert insp["inspector"] == "Joe Smith"


def test_inspector_ma_both_filled_stays_unset():
    # Existing collapse rule: with two populated sources the common field is
    # left unset rather than picking one. The two MA sources are never both
    # filled in real data (visits vs. investigations), so this is just the guard.
    insp = norm.normalize_inspection(
        {"ma_licensor": "Madelyn Marichal", "ma_investigator": "Joe Smith"},
        "massachusetts",
    )
    assert not insp.get("inspector")
    assert insp["ma_licensor"] == "Madelyn Marichal"
    assert insp["ma_investigator"] == "Joe Smith"


@pytest.mark.parametrize(
    ("state", "source"),
    [(s, src) for s, src, _raw, _exp in INSPECTOR_SOURCES],
)
@pytest.mark.parametrize("blank", ["", "   ", "N/A", None])
def test_inspector_blank_or_sentinel_unset(state, source, blank):
    insp = norm.normalize_inspection({source: blank}, state)
    assert not insp.get("inspector")


def test_inspector_does_not_touch_provider_level_licensing_specialist():
    item = norm.normalize_item({"ma_licensor": "Madelyn Marichal"}, "massachusetts")
    assert item["licensing_specialist"] == "Madelyn Marichal"
    assert "inspector" not in item
    insp = norm.normalize_inspection({"ma_licensor": "Madelyn Marichal"}, "massachusetts")
    assert "licensing_specialist" not in insp


# --- inspection_id (inspection-level) -------------------------------------

INSPECTION_ID_SOURCES = [
    ("connecticut", "ct_inspection_id", 12345, "12345"),
    ("florida", "fl_inspection_id", "0b1f2c3d-aaaa-bbbb-cccc-1234567890ab", "0b1f2c3d-aaaa-bbbb-cccc-1234567890ab"),
    ("hawaii", "hi_visit_id", 987, "987"),
    ("kentucky", "ky_inspection_id", 55001, "55001"),
    ("massachusetts", "ma_visit_id", "a0X5e000001AbCdEAK", "a0X5e000001AbCdEAK"),
    ("massachusetts", "ma_investigation_id", "a0Y5e000002ZyXwEAK", "a0Y5e000002ZyXwEAK"),
    ("missouri", "mo_inspection_id", "2507905226", "2507905226"),
    ("new_hampshire", "nh_visit_id", "a1B5e000003QrStEAK", "a1B5e000003QrStEAK"),
    ("nevada", "nv_inspection_number", "79452", "79452"),
    ("kansas", "ks_nosf_id", "161446", "161446"),
    ("arizona", "az_enforcement_name", "D-2029537", "D-2029537"),
]


@pytest.mark.parametrize(("state", "source", "raw", "expected"), INSPECTION_ID_SOURCES)
def test_inspection_id_per_state(state, source, raw, expected):
    insp = norm.normalize_inspection({source: raw}, state)
    assert insp["inspection_id"] == expected
    assert isinstance(insp["inspection_id"], str)
    assert insp[source] == raw  # source kept (collapse is additive)


def test_inspection_id_int_has_no_decimal_suffix():
    insp = norm.normalize_inspection({"ct_inspection_id": 12345}, "connecticut")
    assert insp["inspection_id"] == "12345"
    floaty = norm.normalize_inspection({"hi_visit_id": 987.0}, "hawaii")
    assert floaty["inspection_id"] == "987"


def test_inspection_id_string_only_trimmed():
    insp = norm.normalize_inspection({"ks_nosf_id": " 000161 "}, "kansas")
    assert insp["inspection_id"] == "000161"


def test_inspection_id_ma_visit_row():
    insp = norm.normalize_inspection({"ma_visit_id": "a0X1"}, "massachusetts")
    assert insp["inspection_id"] == "a0X1"
    assert "ma_investigation_id" not in insp


def test_inspection_id_ma_investigation_row():
    insp = norm.normalize_inspection({"ma_investigation_id": "a0Y2"}, "massachusetts")
    assert insp["inspection_id"] == "a0Y2"
    assert "ma_visit_id" not in insp


def test_inspection_id_kansas_survey_number_alone_unset():
    insp = norm.normalize_inspection({"ks_survey_number": "25-001610"}, "kansas")
    assert not insp.get("inspection_id")
    assert insp["ks_survey_number"] == "25-001610"


def test_inspection_id_kansas_uses_nosf_id_not_survey_number():
    insp = norm.normalize_inspection({"ks_nosf_id": "161446", "ks_survey_number": "25-001610"}, "kansas")
    assert insp["inspection_id"] == "161446"
    assert insp["ks_survey_number"] == "25-001610"


@pytest.mark.parametrize(
    ("state", "source"),
    [(s, src) for s, src, _raw, _exp in INSPECTION_ID_SOURCES],
)
@pytest.mark.parametrize("blank", ["", "   ", None])
def test_inspection_id_blank_unset(state, source, blank):
    insp = norm.normalize_inspection({source: blank}, state)
    assert not insp.get("inspection_id")


def test_inspection_id_does_not_touch_source_provider_id():
    item = norm.normalize_item({"ct_provider_id": 4321}, "connecticut")
    assert item["source_provider_id"] == "4321"
    assert "inspection_id" not in item
    insp = norm.normalize_inspection({"ct_inspection_id": 12345}, "connecticut")
    assert "source_provider_id" not in insp


# --- announced (inspection-level) -----------------------------------------

ANNOUNCED_TEXT_CASES = [
    ("Unannounced Monitoring Visit", False),
    ("UnannouncedMonitoringVisit", False),
    ("Uannounced Monitoring Visit", False),
    ("Unannounce Monitoring Visit", False),
    ("UnannouncedMonoitoringVisit", False),
    ("Child Care Center I/T/PS Unannounced", False),
    ("DCEX - UNANNOUNCED PARTIAL", False),
    ("UNANNOUNCED INSPECTION - FULL", False),
    ("Announced", True),
    ("ANNOUNCED INSPECTION - FULL", True),
    ("DCEX - ANNOUNCED", True),
    ("YCEX - ANNOUNCED PARTIAL", True),
    ("announced", True),
    ("Follow-Up Inspection", None),
    ("Investigation - Complaints", None),
    ("Child Care Center - Probationary Visit", None),
    ("Admin Action Follow-Up A/N", None),
    ("N/A", None),
    ("", None),
    (None, None),
]


@pytest.mark.parametrize(("text", "expected"), ANNOUNCED_TEXT_CASES)
def test_announced_flag_classifier(text, expected):
    assert cf.announced_flag(text) is expected


ANNOUNCED_SOURCES = [
    ("alaska", "ak_visit_type"),
    ("massachusetts", "ma_announcement_type"),
    ("new_hampshire", "nh_announcement_type"),
    ("missouri", "mo_notice"),
]


@pytest.mark.parametrize(("state", "source"), ANNOUNCED_SOURCES)
@pytest.mark.parametrize(
    ("raw", "expected"),
    [("Announced", True), ("Unannounced", False), ("ANNOUNCED", True), ("UNANNOUNCED", False)],
)
def test_announced_dedicated_sources(state, source, raw, expected):
    insp = norm.normalize_inspection({source: raw}, state)
    assert insp["announced"] is expected
    assert insp[source] == raw  # source kept (collapse is additive)


def test_announced_mo_not_applicable_unset():
    insp = norm.normalize_inspection({"mo_notice": "N/A"}, "missouri")
    assert not cf.is_present(insp.get("announced"))
    assert insp["mo_notice"] == "N/A"


@pytest.mark.parametrize("state", ["north_carolina", "connecticut", "rhode_island"])
@pytest.mark.parametrize(("text", "expected"), [c for c in ANNOUNCED_TEXT_CASES if c[0]])
def test_announced_from_type_adapters(state, text, expected):
    insp = norm.normalize_inspection({"type": text}, state)
    if expected is None:
        assert "announced" not in insp
    else:
        assert insp["announced"] is expected
    assert insp["type"] == text  # type unchanged


@pytest.mark.parametrize("state", ["north_carolina", "connecticut", "rhode_island"])
@pytest.mark.parametrize("text", ["Follow-Up Inspection", "Investigation - Complaints"])
def test_announced_type_without_either_word_unset(state, text):
    insp = norm.normalize_inspection({"type": text}, state)
    assert "announced" not in insp


@pytest.mark.parametrize("state", ["florida", "alaska", "missouri", "virginia"])
def test_announced_not_read_from_type_in_other_states(state):
    insp = norm.normalize_inspection({"type": "Unannounced"}, state)
    assert "announced" not in insp
    assert insp["type"] == "Unannounced"


def test_announced_ri_typo_type_is_unannounced():
    insp = norm.normalize_inspection({"type": "Unannounced Monitoring Visit"}, "rhode_island")
    assert insp["announced"] is False


# --- violation_found (inspection-level) -----------------------------------

VIOLATION_BOOL_SOURCES = [
    ("florida", "fl_has_violation"),
    ("massachusetts", "ma_noncompliance_identified"),
    ("wyoming", "wy_violation_found"),
]


@pytest.mark.parametrize(("state", "source"), VIOLATION_BOOL_SOURCES)
@pytest.mark.parametrize(("raw", "expected"), [(True, True), (False, False)])
def test_violation_found_bool_sources(state, source, raw, expected):
    insp = norm.normalize_inspection({source: raw}, state)
    assert insp["violation_found"] is expected
    assert insp[source] is raw  # source kept


@pytest.mark.parametrize("state", ["massachusetts"])
def test_violation_found_ma_visit_none_stays_unset(state):
    insp = norm.normalize_inspection({"ma_noncompliance_identified": None}, state)
    assert not cf.is_present(insp.get("violation_found"))


@pytest.mark.parametrize(("raw", "expected"), [("Yes", True), ("No", False)])
def test_violation_found_virginia_yes_no(raw, expected):
    insp = norm.normalize_inspection({"va_violations": raw}, "virginia")
    assert insp["violation_found"] is expected
    assert insp["va_violations"] == raw


@pytest.mark.parametrize(("in_compliance", "expected"), [(True, False), (False, True)])
def test_violation_found_missouri_inverted(in_compliance, expected):
    insp = norm.normalize_inspection({"mo_in_compliance": in_compliance}, "missouri")
    assert insp["violation_found"] is expected
    assert insp["mo_in_compliance"] is in_compliance


@pytest.mark.parametrize(
    ("state", "raw", "expected"),
    [
        ("alaska", "In Compliance", False),
        ("alaska", "Non-Compliance", True),
        ("wisconsin", "See Violations below", True),
        ("wisconsin", "No Violations found ***", False),
    ],
)
def test_violation_found_from_original_status(state, raw, expected):
    insp = norm.normalize_inspection({"original_status": raw}, state)
    assert insp["violation_found"] is expected
    assert insp["original_status"] == raw


@pytest.mark.parametrize("state", ["alaska", "wisconsin"])
def test_violation_found_unmatched_status_unset_and_logged(state, caplog):
    with caplog.at_level("WARNING"):
        insp = norm.normalize_inspection({"original_status": "Something Else"}, state)
    assert "violation_found" not in insp
    assert any("Something Else" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("state", ["alaska", "wisconsin"])
def test_violation_found_blank_status_unset(state):
    for blank in (None, ""):
        insp = norm.normalize_inspection({"original_status": blank}, state)
        assert "violation_found" not in insp


def test_violation_found_status_vocabs_are_state_scoped():
    # WI's text means nothing in AK and vice versa.
    insp = norm.normalize_inspection({"original_status": "See Violations below"}, "alaska")
    assert "violation_found" not in insp
    insp = norm.normalize_inspection({"original_status": "Non-Compliance"}, "wisconsin")
    assert "violation_found" not in insp


@pytest.mark.parametrize("state", ["florida", "missouri", "virginia", "montana", "maryland"])
def test_violation_found_not_read_from_original_status_elsewhere(state):
    for raw in ("Non-Compliance", "See Violations below", "In Compliance"):
        insp = norm.normalize_inspection({"original_status": raw}, state)
        assert "violation_found" not in insp
        assert insp["original_status"] == raw


@pytest.mark.parametrize(
    ("finding", "expected"),
    [
        ("No Noncompliances Found", False),
        ("no noncompliances found", False),
        ("A child was picked up abruptly and placed down on the floor.", True),
        # A regulation is cited, so this is a violation despite "no evidence".
        ("Specialist observed no evidence of lead test results on file.", True),
        ("5", None),
        ("12", None),
        ("", None),
        (None, None),
    ],
)
def test_violation_found_maryland_finding(finding, expected):
    insp = norm.normalize_inspection({"md_finding": finding}, "maryland")
    if expected is None:
        assert "violation_found" not in insp
    else:
        assert insp["violation_found"] is expected
    assert insp["md_finding"] == (finding or None)  # source kept (blank cleaned to None)


def test_violation_found_not_derived_from_other_fields():
    insp = norm.normalize_inspection({"type": "Non-Compliance", "md_regulation": "13A.1"}, "maryland")
    assert "violation_found" not in insp


# --------------------------------------------------------------------------- #
# violation_count and the violation_found derivation
# --------------------------------------------------------------------------- #
COUNT_SOURCES = [
    ("connecticut", "ct_violations_count"),
    ("delaware", "de_violation_count"),
    ("nevada", "nv_deficiency_count"),
    ("kansas", "ks_findings_count"),
    ("hawaii", "hi_requirements_not_met"),
    ("south_carolina", "sc_alert_count"),
]


@pytest.mark.parametrize(("state", "source"), COUNT_SOURCES)
@pytest.mark.parametrize(("raw", "expected"), [(3, 3), (0, 0), ("7", 7), (1, 1)])
def test_violation_count_collapse(state, source, raw, expected):
    insp = norm.normalize_inspection({source: raw}, state)
    assert insp["violation_count"] == expected
    assert type(insp["violation_count"]) is int
    assert insp[source] == raw  # source kept


@pytest.mark.parametrize(("state", "source"), COUNT_SOURCES)
def test_violation_count_none_stays_unset(state, source):
    insp = norm.normalize_inspection({source: None}, state)
    assert "violation_count" not in insp
    assert "violation_found" not in insp


def test_violation_count_sc_missing_badge_not_zero():
    insp = norm.normalize_inspection({"type": "Routine"}, "south_carolina")
    assert "violation_count" not in insp
    assert "violation_found" not in insp


def test_violation_count_mo_open_violations_not_mapped():
    insp = norm.normalize_inspection({"mo_open_violations": 4, "mo_in_compliance": "No"}, "missouri")
    assert "violation_count" not in insp
    assert insp["mo_open_violations"] == 4
    assert insp["violation_found"] is True  # from mo_in_compliance, not the count
    sources = [norm.split_collapse_entry(e)[0] for e in norm.INSPECTION_FIELD_COLLAPSE_MAP["violation_count"]]
    assert "mo_open_violations" not in sources


@pytest.mark.parametrize(("state", "source"), COUNT_SOURCES)
@pytest.mark.parametrize(("raw", "expected"), [(5, True), (1, True), (0, False)])
def test_violation_found_derived_from_count(state, source, raw, expected):
    insp = norm.normalize_inspection({source: raw}, state)
    assert insp["violation_found"] is expected


def test_violation_found_delaware_always_true():
    # DE only publishes noncompliance visits, so the count is >= 1.
    insp = norm.normalize_inspection({"de_violation_count": 1}, "delaware")
    assert insp["violation_found"] is True


def test_violation_found_explicit_flag_not_overwritten_by_count():
    insp = norm.normalize_inspection({"fl_has_violation": "No", "ct_violations_count": 4}, "florida")
    assert insp["violation_found"] is False
    assert insp["violation_count"] == 4
    # Same protection when the flag was already populated on a derivation state.
    insp = norm.normalize_inspection({"violation_found": False, "ct_violations_count": 4}, "connecticut")
    assert insp["violation_found"] is False


def test_violation_found_not_derived_for_unlisted_state():
    insp = norm.normalize_inspection({"ct_violations_count": 4}, "ohio")
    assert insp["violation_count"] == 4  # the map is not state-scoped
    assert "violation_found" not in insp


# --------------------------------------------------------------------------- #
# regulation_cited
# --------------------------------------------------------------------------- #
REGULATION_SOURCES = [
    ("arizona", "az_regulation", ["AAC R9-5-208.C.", "A.R.S. § 36-897.02.C."]),
    ("maryland", "md_regulation", ["13A.15.03.04A(3)"]),
    ("indiana", "in_rule_code", ["470 IAC 3-4.5-4(1)", "IC 12-17.2-6-14(1)"]),
    ("wisconsin", "wi_rule_number", ["251.05(2)(a)2."]),
]
REGULATION_CASES = [(st, src, val) for st, src, vals in REGULATION_SOURCES for val in vals]


@pytest.mark.parametrize(("state", "source", "raw"), REGULATION_CASES)
def test_regulation_cited_kept_exactly(state, source, raw):
    insp = norm.normalize_inspection({source: raw}, state)
    assert insp["regulation_cited"] == raw
    assert insp[source] == raw  # source kept


@pytest.mark.parametrize("raw", ["R6-5-5203.4, R6-5-5203.4.B, R6-5-5203.4.C", "DES CONTRACT", "INCIDENT"])
def test_regulation_cited_arizona_multi_and_label_as_emitted(raw):
    insp = norm.normalize_inspection({"az_regulation": raw}, "arizona")
    assert insp["regulation_cited"] == raw


@pytest.mark.parametrize(("state", "source", "_vals"), REGULATION_SOURCES)
@pytest.mark.parametrize("raw", [None, "", "   ", "N/A"])
def test_regulation_cited_blank_and_sentinel_unset(state, source, _vals, raw):
    insp = norm.normalize_inspection({source: raw}, state)
    assert "regulation_cited" not in insp


def test_regulation_cited_trimmed():
    insp = norm.normalize_inspection({"md_regulation": "  13A.15.03.04A(3) "}, "maryland")
    assert insp["regulation_cited"] == "13A.15.03.04A(3)"


# --------------------------------------------------------------------------- #
# compliance_domains (MA, NH) and the shared ratio parser
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("12 / 12", (12, 12)),
        ("17/17", (17, 17)),
        ("0 / 1", (0, 1)),
        ("  19  /  20  ", (19, 20)),
        ("5 / 3", None),
        ("N/A", None),
        ("-1 / 2", None),
        ("1 / 2 / 3", None),
        ("", None),
        (None, None),
        (12, None),
    ],
)
def test_parse_ratio(raw, expected):
    assert cf.parse_ratio(raw) == expected


def test_compliance_domains_new_hampshire():
    domains = [
        {"domain": "Administration", "level_of_compliance": "23 / 23"},
        {"domain": "Medication", "level_of_compliance": "19 / 20"},
        {"domain": "Care of Children", "level_of_compliance": "41/41"},
    ]
    insp = norm.normalize_inspection({"nh_domains": domains}, "new_hampshire")
    assert insp["compliance_domains"] == [
        {"domain": "Administration", "compliant": 23, "reviewed": 23},
        {"domain": "Medication", "compliant": 19, "reviewed": 20},
        {"domain": "Care of Children", "compliant": 41, "reviewed": 41},
    ]
    assert insp["nh_domains"] == domains  # source kept


def test_compliance_domains_new_hampshire_bad_row_dropped_and_logged(caplog):
    domains = [
        {"domain": "Administration", "level_of_compliance": "23 / 23"},
        {"domain": "Medication", "level_of_compliance": "N/A"},
        {"domain": "Safety", "level_of_compliance": "5 / 3"},
        {"domain": "Care of Children", "level_of_compliance": "1 / 2"},
    ]
    with caplog.at_level(logging.WARNING):
        insp = norm.normalize_inspection({"nh_domains": domains}, "new_hampshire")
    assert [r["domain"] for r in insp["compliance_domains"]] == ["Administration", "Care of Children"]
    assert "Medication" in caplog.text
    assert "Safety" in caplog.text


@pytest.mark.parametrize("domains", [None, [], [{"domain": "Medication", "level_of_compliance": "bad"}]])
def test_compliance_domains_new_hampshire_empty_unset(domains):
    insp = norm.normalize_inspection({"nh_domains": domains}, "new_hampshire")
    assert "compliance_domains" not in insp


def test_compliance_domains_massachusetts_rollup_and_order():
    counts = {
        "Medication": {"Non-Compliant": 1, "Compliant": 19},
        "Administration": {"Compliant": 10, "Complete": 2, "Not Complete": 1},
        "Care of Children": {"Unknown": 2},
        "Safety": {"Compliant": 4, "Unknown": 1},
    }
    insp = norm.normalize_inspection({"ma_domain_counts": counts}, "massachusetts")
    assert insp["compliance_domains"] == [
        {"domain": "Medication", "compliant": 19, "reviewed": 20},
        {"domain": "Administration", "compliant": 12, "reviewed": 13},
        # "Care of Children" is all Unknown: nothing reported, so no row.
        {"domain": "Safety", "compliant": 4, "reviewed": 4},
    ]
    assert insp["ma_domain_counts"] == counts  # source kept


def test_compliance_domains_massachusetts_drops_unnamed_domain(caplog):
    counts = {None: {"Compliant": 3}, "Health": {"Compliant": 2}}
    with caplog.at_level("WARNING"):
        insp = norm.normalize_inspection({"ma_domain_counts": counts}, "massachusetts")
    assert insp["compliance_domains"] == [{"domain": "Health", "compliant": 2, "reviewed": 2}]
    assert "no domain name" in caplog.text


def test_compliance_domains_massachusetts_all_unknown_unset():
    insp = norm.normalize_inspection({"ma_domain_counts": {"Health": {"Unknown": 4}}}, "massachusetts")
    assert "compliance_domains" not in insp


def test_compliance_domains_massachusetts_old_fields_only_unset():
    raw = {
        "ma_domain_compliance_counts": {"Compliant": 40, "Non-Compliant": 2},
        "ma_domains": [{"domain": "Medication", "level_of_compliance": "Non-Compliant"}],
    }
    insp = norm.normalize_inspection(raw, "massachusetts")
    assert "compliance_domains" not in insp


def test_compliance_domains_not_derived_for_other_states():
    insp = norm.normalize_inspection({"nh_domains": [{"domain": "A", "level_of_compliance": "1 / 1"}]}, "ohio")
    assert "compliance_domains" not in insp


# --------------------------------------------------------------------------- #
# items_compliant / items_reviewed (MA, RI, NH, OK, KS)
# --------------------------------------------------------------------------- #
ITEMS_RATIO_SOURCES = [
    ("massachusetts", "ma_level_of_compliance", "17/17", (17, 17)),
    ("rhode_island", "ri_compliance", "87/87", (87, 87)),
    ("new_hampshire", "nh_level_of_compliance", "221 / 222", (221, 222)),
]


@pytest.mark.parametrize(("state", "source", "raw", "expected"), ITEMS_RATIO_SOURCES)
def test_items_ratio_split(state, source, raw, expected):
    insp = norm.normalize_inspection({source: raw}, state)
    assert (insp["items_compliant"], insp["items_reviewed"]) == expected
    assert insp[source] == raw  # source kept


@pytest.mark.parametrize(("state", "source", "_raw", "_expected"), ITEMS_RATIO_SOURCES)
@pytest.mark.parametrize("bad", ["N/A", "5 / 3", "-1 / 2", "1/2/3", "", None, 17])
def test_items_ratio_bad_values_leave_both_unset(state, source, _raw, _expected, bad):
    insp = norm.normalize_inspection({source: bad}, state)
    assert "items_compliant" not in insp
    assert "items_reviewed" not in insp


def test_items_ratio_zero_over_zero_kept():
    insp = norm.normalize_inspection({"ma_level_of_compliance": "0/0"}, "massachusetts")
    assert (insp["items_compliant"], insp["items_reviewed"]) == (0, 0)


def test_items_ratio_bad_value_logged(caplog):
    with caplog.at_level(logging.WARNING):
        norm.normalize_inspection({"ri_compliance": "5 / 3"}, "rhode_island")
    assert "ri_compliance" in caplog.text


def test_items_ratio_does_not_overwrite_existing():
    raw = {"ma_level_of_compliance": "17/17", "items_compliant": 1, "items_reviewed": 2}
    insp = norm.normalize_inspection(raw, "massachusetts")
    assert (insp["items_compliant"], insp["items_reviewed"]) == (1, 2)


def test_items_oklahoma_copy():
    raw = {"ok_compliance_count": 9, "ok_compliance_total": 10}
    insp = norm.normalize_inspection(raw, "oklahoma")
    assert (insp["items_compliant"], insp["items_reviewed"]) == (9, 10)
    assert insp["ok_compliance_count"] == 9
    assert insp["ok_compliance_total"] == 10


def test_items_oklahoma_zero_kept():
    insp = norm.normalize_inspection({"ok_compliance_count": 0, "ok_compliance_total": 0}, "oklahoma")
    assert (insp["items_compliant"], insp["items_reviewed"]) == (0, 0)


@pytest.mark.parametrize(
    ("count", "total"),
    [(11, 10), (-1, 10), (3, -1), ("9", 10), (9, "10"), (True, 10), (9.0, 10), (None, 10), (9, None)],
)
def test_items_oklahoma_invalid_pair_unset(count, total):
    insp = norm.normalize_inspection({"ok_compliance_count": count, "ok_compliance_total": total}, "oklahoma")
    assert "items_compliant" not in insp
    assert "items_reviewed" not in insp


def test_items_kansas_reviewed_only():
    raw = {"ks_regulations_reviewed": 568, "ks_findings_count": 3}
    insp = norm.normalize_inspection(raw, "kansas")
    assert insp["items_reviewed"] == 568
    assert "items_compliant" not in insp
    assert insp["ks_regulations_reviewed"] == 568  # source kept


def test_items_kansas_findings_exceeding_reviewed_never_derives_compliant():
    insp = norm.normalize_inspection({"ks_regulations_reviewed": 1, "ks_findings_count": 2}, "kansas")
    assert insp["items_reviewed"] == 1
    assert "items_compliant" not in insp


@pytest.mark.parametrize("bad", [-1, "568", True, 1.5, None])
def test_items_kansas_invalid_unset(bad):
    insp = norm.normalize_inspection({"ks_regulations_reviewed": bad}, "kansas")
    assert "items_reviewed" not in insp


def test_items_kansas_does_not_overwrite_existing():
    insp = norm.normalize_inspection({"ks_regulations_reviewed": 5, "items_reviewed": 9}, "kansas")
    assert insp["items_reviewed"] == 9


def test_items_not_derived_for_other_states():
    raw = {"ma_level_of_compliance": "1/1", "ok_compliance_count": 1, "ok_compliance_total": 1}
    insp = norm.normalize_inspection(raw, "ohio")
    assert "items_compliant" not in insp
    assert "items_reviewed" not in insp


# --------------------------------------------------------------------------- #
# correction_date (AZ, IN, WV)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("state", "source"),
    [("arizona", "az_date_resolved"), ("indiana", "in_correction_date")],
)
def test_correction_date_copied_as_iso(state, source):
    insp = norm.normalize_inspection({source: "2024-08-09"}, state)
    assert insp["correction_date"] == "2024-08-09"
    assert insp[source] == "2024-08-09"  # source kept


@pytest.mark.parametrize(
    ("state", "source"),
    [("arizona", "az_date_resolved"), ("indiana", "in_correction_date")],
)
@pytest.mark.parametrize("empty", [None, "", "   ", "N/A"])
def test_correction_date_empty_source_unset(state, source, empty):
    insp = norm.normalize_inspection({source: empty}, state)
    assert not insp.get("correction_date")


def test_correction_date_unparseable_kept_as_emitted():
    insp = norm.normalize_inspection({"az_date_resolved": "soon"}, "arizona")
    assert insp["correction_date"] == "soon"


@pytest.mark.parametrize(
    ("state", "source"),
    [("arizona", "az_date_resolved"), ("indiana", "in_correction_date")],
)
def test_correction_date_does_not_overwrite_existing(state, source):
    insp = norm.normalize_inspection({source: "2024-08-09", "correction_date": "2020-01-02"}, state)
    assert insp["correction_date"] == "2020-01-02"


def _wv(outcome, completed):
    return {"wv_outcome_code": outcome, "wv_issue_completed_date": completed}


def test_correction_date_wv_achieved_real_date():
    insp = norm.normalize_inspection(_wv("Achieved", "8/9/2024"), "west_virginia")
    assert insp["correction_date"] == "2024-08-09"
    assert insp["wv_issue_completed_date"] == "8/9/2024"  # source kept
    assert insp["wv_outcome_code"] == "Achieved"


@pytest.mark.parametrize("completed", ["1/1/1753", " 1/1/1753 ", "", None])
def test_correction_date_wv_achieved_placeholder_or_empty_unset(completed):
    insp = norm.normalize_inspection(_wv("Achieved", completed), "west_virginia")
    assert not insp.get("correction_date")


@pytest.mark.parametrize("outcome", ["Pending", "Not Achieved", "", None, "achieved", "ACHIEVED"])
def test_correction_date_wv_other_outcomes_unset_even_with_date(outcome):
    insp = norm.normalize_inspection(_wv(outcome, "8/9/2024"), "west_virginia")
    assert not insp.get("correction_date")


def test_correction_date_wv_missing_outcome_key_unset():
    insp = norm.normalize_inspection({"wv_issue_completed_date": "8/9/2024"}, "west_virginia")
    assert not insp.get("correction_date")


def test_correction_date_wv_does_not_overwrite_existing():
    raw = {**_wv("Achieved", "8/9/2024"), "correction_date": "2020-01-02"}
    assert norm.normalize_inspection(raw, "west_virginia")["correction_date"] == "2020-01-02"


def test_correction_date_wv_unparseable_kept_as_emitted():
    insp = norm.normalize_inspection(_wv("Achieved", "someday"), "west_virginia")
    assert insp["correction_date"] == "someday"


def test_correction_date_new_hampshire_never_set():
    raw = {"nh_corrective_action_accepted": "06/25/2025"}
    insp = norm.normalize_inspection(raw, "new_hampshire")
    assert "correction_date" not in insp
    assert insp["nh_corrective_action_accepted"] == "06/25/2025"


def test_correction_date_wv_adapter_not_applied_to_other_states():
    assert "correction_date" not in norm.normalize_inspection(_wv("Achieved", "8/9/2024"), "ohio")


# --------------------------------------------------------------------------- #
# year_round / operating_months
# --------------------------------------------------------------------------- #
ALL_MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def test_months_georgia_all_twelve():
    raw = ",".join(m.upper() for m in ALL_MONTHS)
    item = norm.normalize_item({"ga_operating_months": raw}, "georgia")
    assert item["operating_months"] == ALL_MONTHS
    assert item["year_round"] is True
    assert item["ga_operating_months"] == raw  # additive: source kept


def test_months_georgia_ten_months_is_not_year_round():
    item = norm.normalize_item({"ga_operating_months": "JAN,FEB,MAR,APR,MAY,AUG,SEP,OCT,NOV,DEC"}, "georgia")
    assert item["operating_months"] == ["Jan", "Feb", "Mar", "Apr", "May", "Aug", "Sep", "Oct", "Nov", "Dec"]
    assert item["year_round"] is False


def test_months_georgia_other_school_break_dropped_silently(caplog):
    raw = ",".join(m.upper() for m in ALL_MONTHS) + ",OtherSchoolBreak"
    with caplog.at_level(logging.WARNING):
        item = norm.normalize_item({"ga_operating_months": raw}, "georgia")
    assert item["operating_months"] == ALL_MONTHS
    assert item["year_round"] is True
    assert "OtherSchoolBreak" not in caplog.text


def test_months_georgia_unknown_token_logged_and_unsorted_input_ordered(caplog):
    with caplog.at_level(logging.WARNING):
        item = norm.normalize_item({"ga_operating_months": "DEC, JAN,JAN,Smarch"}, "georgia")
    assert item["operating_months"] == ["Jan", "Dec"]
    assert item["year_round"] is False
    assert "Smarch" in caplog.text


def test_months_mississippi_list():
    item = norm.normalize_item({"ms_months_of_operation": list(ALL_MONTHS)}, "mississippi")
    assert item["operating_months"] == ALL_MONTHS
    assert item["year_round"] is True
    partial = norm.normalize_item({"ms_months_of_operation": ["Aug", "Sep", "Oct"]}, "mississippi")
    assert partial["operating_months"] == ["Aug", "Sep", "Oct"]
    assert partial["year_round"] is False
    assert partial["ms_months_of_operation"] == ["Aug", "Sep", "Oct"]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Jan - Dec", ALL_MONTHS),
        ("Sep - Jun", ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Sep", "Oct", "Nov", "Dec"]),
        ("Aug - May", ["Jan", "Feb", "Mar", "Apr", "May", "Aug", "Sep", "Oct", "Nov", "Dec"]),
        ("Jun - Aug", ["Jun", "Jul", "Aug"]),
    ],
)
def test_months_wisconsin_ranges(raw, expected):
    item = norm.normalize_item({"wi_months_open": raw}, "wisconsin")
    assert item["operating_months"] == expected
    assert item["year_round"] is (len(expected) == 12)
    assert item["wi_months_open"] == raw


def test_months_wisconsin_unparseable_logged_and_unset(caplog):
    with caplog.at_level(logging.WARNING):
        item = norm.normalize_item({"wi_months_open": "Varies"}, "wisconsin")
    assert "operating_months" not in item
    assert "year_round" not in item
    assert "Varies" in caplog.text


@pytest.mark.parametrize(
    ("state", "field", "raw"),
    [
        ("georgia", "ga_operating_months", ""),
        ("georgia", "ga_operating_months", "OtherSchoolBreak"),
        ("mississippi", "ms_months_of_operation", []),
        ("wisconsin", "wi_months_open", None),
    ],
)
def test_months_empty_leave_both_unset(state, field, raw):
    item = norm.normalize_item({field: raw}, state)
    assert "operating_months" not in item
    assert "year_round" not in item


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (["12 Months"], True),
        (["School Year Only"], False),
        (["Summer Only"], False),
        (["Other"], None),
        ([], None),
    ],
)
def test_year_round_south_dakota(raw, expected):
    item = norm.normalize_item({"sd_months_of_operation": list(raw)}, "south_dakota")
    if expected is None:
        assert "year_round" not in item
    else:
        assert item["year_round"] is expected
    assert "operating_months" not in item
    assert item["sd_months_of_operation"] == raw


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Full calendar year", True),
        ("School year only (typically September-May)", False),
    ],
)
def test_year_round_north_dakota(raw, expected):
    item = norm.normalize_item({"nd_enrollment_schedule": raw}, "north_dakota")
    assert item["year_round"] is expected
    assert item["nd_enrollment_schedule"] == raw


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("School Year Only", False), ("Summer Only", False), ("In-Service Days", None), ("", None), (None, None)],
)
def test_year_round_vermont(raw, expected):
    item = norm.normalize_item({"vt_special_schedule": raw}, "vermont")
    if expected is None:
        assert "year_round" not in item
    else:
        assert item["year_round"] is expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (["Year Round", "Daytime Hours"], True),
        (["Year Round", "Summer Only"], True),
        (["School Year Only", "Daytime Hours"], False),
        (["Summer Only"], False),
        (["School Vacation Only"], False),
        (["Daytime Hours", "Evening Hours"], None),
        ([], None),
    ],
)
def test_year_round_oklahoma(raw, expected):
    item = norm.normalize_item({"ok_care_types": list(raw)}, "oklahoma")
    if expected is None:
        assert "year_round" not in item
    else:
        assert item["year_round"] is expected
    assert item["ok_care_types"] == raw if raw else True


def test_ok_care_type_set_is_reusable_for_other_ok_tokens():
    labels = cf.ok_care_type_set(["Year Round", " Evening Hours "])
    assert labels == {"year round", "evening hours"}
    assert cf.ok_care_type_set("Weekend Hours, Overnight Hours") == {"weekend hours", "overnight hours"}
    assert cf.ok_care_type_set(None) == set()


@pytest.mark.parametrize(
    ("state", "field", "value"),
    [
        ("north_dakota", "nd_enrollment_schedule", "Sometimes"),
        ("vermont", "vt_special_schedule", "Weird Value"),
        ("south_dakota", "sd_months_of_operation", ["Weird Value"]),
    ],
)
def test_year_round_unknown_label_logged_and_unset(state, field, value, caplog):
    with caplog.at_level(logging.WARNING):
        item = norm.normalize_item({field: value}, state)
    assert "year_round" not in item
    assert "Weird Value" in caplog.text or "Sometimes" in caplog.text


def test_year_round_adapters_never_overwrite():
    item = norm.normalize_item({"ga_operating_months": "JAN,FEB", "year_round": True}, "georgia")
    assert item["year_round"] is True
    assert item["operating_months"] == ["Jan", "Feb"]


# --------------------------------------------------------------------------- #
# serves_special_needs / special_needs_types
# --------------------------------------------------------------------------- #
SPECIAL_NEEDS_CASES = [
    ("Autism Spectrum Disorders", "Autism"),
    ("Autism", "Autism"),
    ("Social Emotional Behaviors", "Behavioral/Social-Emotional"),
    ("Social/Emotional/Behavior Concerns", "Behavioral/Social-Emotional"),
    ("Behavioral", "Behavioral/Social-Emotional"),
    ("Emotional/Social", "Behavioral/Social-Emotional"),
    ("Emotional/Behavioral", "Behavioral/Social-Emotional"),
    ("ADD/ADHD", "ADHD"),
    ("ADHD/ADD", "ADHD"),
    ("Cognitive Delays", "Developmental/Cognitive"),
    ("Developmental Delays/Learning Disabilities", "Developmental/Cognitive"),
    ("Intellectual Disabilities", "Developmental/Cognitive"),
    ("Developmental", "Developmental/Cognitive"),
    ("Developmental Delay", "Developmental/Cognitive"),
    ("Speech/Communication", "Speech/Language"),
    ("Speech/Language Impairment", "Speech/Language"),
    ("Speech/Language", "Speech/Language"),
    ("Physical Delays/Limitations", "Physical"),
    ("Physical Disabilities", "Physical"),
    ("Physical", "Physical"),
    ("Hearing/Visual Impairment", "Hearing/Vision"),
    ("Visual", "Hearing/Vision"),
    ("Hearing/Sign Language", "Hearing/Vision"),
    ("Medical/Special Procedures", "Medical/Health"),
    ("Respiratory Illness", "Medical/Health"),
    ("Diabetes", "Medical/Health"),
    ("Seizure Disorders", "Medical/Health"),
    ("Health Impairment", "Medical/Health"),
    ("Special Health Needs", "Medical/Health"),
    ("Seizures", "Medical/Health"),
    ("Monitors", "Medical/Health"),
    ("HIV/Hepatitis B", "Medical/Health"),
    ("Asthma/Allergies", "Medical/Health"),
    ("Asthma/Severe Allergies", "Medical/Health"),
    ("Food/Dietary", "Dietary"),
    ("Special Diet", "Dietary"),
    ("Sensory Integration", "Sensory"),
    ("At risk", "At Risk"),
]


def test_special_needs_table_covers_all_categories_and_cases():
    assert set(cf.SPECIAL_NEEDS_LABELS.values()) == set(cf.SPECIAL_NEEDS_CATEGORIES)
    assert len(cf.SPECIAL_NEEDS_CATEGORIES) == 11
    assert {label for label, _ in SPECIAL_NEEDS_CASES} == set(cf.SPECIAL_NEEDS_LABELS)


@pytest.mark.parametrize(("label", "category"), SPECIAL_NEEDS_CASES)
def test_special_needs_label_maps_to_category(label, category):
    # Case-insensitive and whitespace-trimmed; the same table serves every state.
    for variant in (label, label.upper(), f"  {label.lower()} "):
        item = norm.normalize_item({"ma_special_needs": variant}, "massachusetts")
        assert item["special_needs_types"] == [category]
        assert item["serves_special_needs"] is True


def test_special_needs_iowa():
    assert norm.normalize_item({"ia_serves_special_needs": "Yes"}, "iowa")["serves_special_needs"] is True
    item = norm.normalize_item({"ia_serves_special_needs": "No"}, "iowa")
    assert item["serves_special_needs"] is False
    assert item["ia_serves_special_needs"] == "No"
    assert "special_needs_types" not in item
    assert "serves_special_needs" not in norm.normalize_item({"ia_serves_special_needs": None}, "iowa")


def test_special_needs_nebraska():
    assert norm.normalize_item({"ne_serves_special_needs": True}, "nebraska")["serves_special_needs"] is True
    item = norm.normalize_item({"ne_serves_special_needs": False}, "nebraska")
    assert item["serves_special_needs"] is False
    assert item["ne_serves_special_needs"] is False
    assert "special_needs_types" not in item


def test_special_needs_north_carolina_token():
    features = ["Provides transportation", "Will enroll children with special needs"]
    item = norm.normalize_item({"nc_special_features": list(features)}, "north_carolina")
    assert item["serves_special_needs"] is True
    assert "special_needs_types" not in item
    assert item["nc_special_features"] == features


@pytest.mark.parametrize("raw", [["Provides transportation"], [], None])
def test_special_needs_north_carolina_token_absent_is_unset(raw):
    item = norm.normalize_item({"nc_special_features": raw}, "north_carolina")
    assert "serves_special_needs" not in item
    assert "special_needs_types" not in item


# --------------------------------------------------------------------------- #
# transportation
# --------------------------------------------------------------------------- #
def test_transportation_north_carolina_token():
    features = ["Provides transportation", "Will enroll children with special needs"]
    item = norm.normalize_item({"nc_special_features": list(features)}, "north_carolina")
    assert item["transportation"] is True
    assert item["nc_special_features"] == features  # additive: source kept


@pytest.mark.parametrize(
    "raw", [["Will enroll children with special needs"], ["Provides transportation services"], [], None]
)
def test_transportation_north_carolina_token_absent_is_unset(raw):
    item = norm.normalize_item({"nc_special_features": raw}, "north_carolina")
    assert "transportation" not in item


def test_transportation_north_carolina_does_not_overwrite_spider_value():
    item = norm.normalize_item(
        {"nc_special_features": ["Provides transportation"], "transportation": "Yes"}, "north_carolina"
    )
    assert item["transportation"] == "Yes"


def test_transportation_other_states_still_collapse():
    item = norm.normalize_item({"ga_transportation": ["School Pickup"]}, "georgia")
    assert item["transportation"] == ["School Pickup"]


def test_special_needs_colorado_mapped_labels():
    raw = "Social Emotional Behaviors; Food/Dietary"
    item = norm.normalize_item({"co_special_needs": raw}, "colorado")
    assert item["serves_special_needs"] is True
    assert item["special_needs_types"] == ["Behavioral/Social-Emotional", "Dietary"]
    assert item["co_special_needs"] == raw


def test_special_needs_colorado_none_is_false():
    item = norm.normalize_item({"co_special_needs": "None"}, "colorado")
    assert item["serves_special_needs"] is False
    assert "special_needs_types" not in item


def test_special_needs_colorado_willing_to_support_is_true_without_types():
    item = norm.normalize_item({"co_special_needs": "Willing to Support"}, "colorado")
    assert item["serves_special_needs"] is True
    assert "special_needs_types" not in item


def test_special_needs_colorado_willing_plus_label():
    item = norm.normalize_item({"co_special_needs": "Willing to Support;Diabetes"}, "colorado")
    assert item["serves_special_needs"] is True
    assert item["special_needs_types"] == ["Medical/Health"]


def test_special_needs_colorado_only_unmapped_is_unset_and_logged(caplog):
    with caplog.at_level(logging.WARNING):
        item = norm.normalize_item({"co_special_needs": "Mystery Condition"}, "colorado")
    assert "serves_special_needs" not in item
    assert "special_needs_types" not in item
    assert "Mystery Condition" in caplog.text


@pytest.mark.parametrize("raw", ["", None])
def test_special_needs_colorado_blank_is_unset(raw):
    item = norm.normalize_item({"co_special_needs": raw}, "colorado")
    assert "serves_special_needs" not in item
    assert "special_needs_types" not in item


def test_special_needs_connecticut_list():
    raw = ["Autism Spectrum Disorders", "Intellectual Disabilities", "Developmental Delays/Learning Disabilities"]
    item = norm.normalize_item({"ct_special_needs": list(raw)}, "connecticut")
    assert item["serves_special_needs"] is True
    assert item["special_needs_types"] == ["Autism", "Developmental/Cognitive"]
    assert item["ct_special_needs"] == raw


def test_special_needs_north_dakota_list():
    raw = ["Special Health Needs", "ADHD/ADD", "Autism"]
    item = norm.normalize_item({"nd_special_populations": list(raw)}, "north_dakota")
    assert item["serves_special_needs"] is True
    assert item["special_needs_types"] == ["Autism", "ADHD", "Medical/Health"]
    assert item["nd_special_populations"] == raw


@pytest.mark.parametrize(
    ("state", "field", "raw"),
    [
        ("connecticut", "ct_special_needs", []),
        ("connecticut", "ct_special_needs", None),
        ("north_dakota", "nd_special_populations", []),
        ("north_dakota", "nd_special_populations", ["N/A"]),
        ("north_dakota", "nd_special_populations", ["None"]),
    ],
)
def test_special_needs_list_states_empty_or_sentinel_are_unset(state, field, raw):
    item = norm.normalize_item({field: raw}, state)
    assert "serves_special_needs" not in item
    assert "special_needs_types" not in item


def test_special_needs_massachusetts_mixed():
    raw = "Experience;Behavioral;Emotional/Social;ADD/ADHD;Special Diet"
    item = norm.normalize_item({"ma_special_needs": raw}, "massachusetts")
    assert item["serves_special_needs"] is True
    assert item["special_needs_types"] == ["Behavioral/Social-Emotional", "ADHD", "Dietary"]
    assert item["ma_special_needs"] == raw


@pytest.mark.parametrize(
    ("state", "field", "raw"),
    [
        ("massachusetts", "ma_special_needs", "Experience"),
        ("massachusetts", "ma_special_needs", "EXPERIENCE"),
        ("new_hampshire", "nh_special_needs", "EXPERIENCE"),
        ("new_hampshire", "nh_special_needs", "Experience"),
    ],
)
def test_special_needs_experience_alone_is_true_without_types(state, field, raw):
    item = norm.normalize_item({field: raw}, state)
    assert item["serves_special_needs"] is True
    assert "special_needs_types" not in item


def test_special_needs_new_hampshire_uppercase_split():
    raw = "EXPERIENCE, BEHAVIORAL, DEVELOPMENTAL"
    item = norm.normalize_item({"nh_special_needs": raw}, "new_hampshire")
    assert item["serves_special_needs"] is True
    assert item["special_needs_types"] == ["Behavioral/Social-Emotional", "Developmental/Cognitive"]
    assert item["nh_special_needs"] == raw


def test_special_needs_new_hampshire_unmapped_label_logged_and_dropped(caplog):
    with caplog.at_level(logging.WARNING):
        item = norm.normalize_item({"nh_special_needs": "BEHAVIORAL, SOMETHING ODD"}, "new_hampshire")
    assert item["special_needs_types"] == ["Behavioral/Social-Emotional"]
    assert "SOMETHING ODD" in caplog.text


def test_special_needs_duplicate_categories_collapse_in_canonical_order():
    raw = ["Seizures", "Physical", "Autism", "Monitors", "Autism", "Visual", "Hearing/Sign Language"]
    item = norm.normalize_item({"nd_special_populations": raw}, "north_dakota")
    assert item["special_needs_types"] == ["Autism", "Physical", "Hearing/Vision", "Medical/Health"]


def test_special_needs_adapters_never_overwrite():
    item = norm.normalize_item(
        {"ma_special_needs": "Behavioral", "serves_special_needs": False, "special_needs_types": ["Autism"]},
        "massachusetts",
    )
    assert item["serves_special_needs"] is False
    assert item["special_needs_types"] == ["Autism"]


# --------------------------------------------------------------------------- #
# USDA food program
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("state", "field", "raw"),
    [
        ("hawaii", "hi_usda_food_program", True),
        ("idaho", "id_usda_food_program", "Yes"),
        ("washington", "wa_food_program", "Yes"),
        ("delaware", "de_financial_arrangements", "Child Care Food Program;Nonprofit;Purchase of Care"),
        ("vermont", "vt_program_participation", "Food Program, Pre-K Program"),
        ("wyoming", "wy_services", ["Child and Adult Food Program", "Transportation"]),
        ("utah", "ut_meals", "On USDA Food Program,Breakfast,Lunch"),
        ("pennsylvania", "pa_meal_options", "Child and Adult Care Food Program (CACFP) Meals"),
    ],
)
def test_food_program_true_and_source_kept(state, field, raw):
    item = norm.normalize_item({field: raw}, state)
    assert item["food_program"] is True
    assert item[field] == raw


def test_food_program_washington_dc_meals():
    item = norm.normalize_item({"meals": "Child and Adult Care Food Program"}, "washington_dc")
    assert item["food_program"] is True
    assert item["meals"] == "Child and Adult Care Food Program"


@pytest.mark.parametrize(
    ("state", "field", "raw"),
    [
        ("delaware", "de_financial_arrangements", "Nonprofit;Purchase of Care"),
        ("vermont", "vt_program_participation", "Pre-K Program"),
        ("wyoming", "wy_services", ["Transportation"]),
        ("wyoming", "wy_services", []),
        ("utah", "ut_meals", "Breakfast,Lunch"),
        ("pennsylvania", "pa_meal_options", "Breakfast"),
        ("washington_dc", "meals", "Lunch"),
    ],
)
def test_food_program_phrase_absent_is_unset(state, field, raw):
    item = norm.normalize_item({field: raw}, state)
    assert "food_program" not in item


@pytest.mark.parametrize(("raw", "expected"), [(True, True), (False, False)])
def test_food_program_hawaii_bool(raw, expected):
    assert norm.normalize_item({"hi_usda_food_program": raw}, "hawaii")["food_program"] is expected


@pytest.mark.parametrize(("raw", "expected"), [("Yes", True), ("No", False)])
def test_food_program_idaho_yes_no(raw, expected):
    assert norm.normalize_item({"id_usda_food_program": raw}, "idaho")["food_program"] is expected


@pytest.mark.parametrize("raw", [None, ""])
def test_food_program_washington_blank_is_unset(raw):
    item = norm.normalize_item({"wa_food_program": raw}, "washington")
    assert "food_program" not in item


def test_food_program_meals_in_other_state_is_not_read():
    item = norm.normalize_item({"meals": "Child and Adult Care Food Program"}, "georgia")
    assert "food_program" not in item
    assert item["meals"] == "Child and Adult Care Food Program"


def test_food_program_meals_unchanged_for_scoped_states():
    item = norm.normalize_item({"ut_meals": "On USDA Food Program,Breakfast"}, "utah")
    assert item["meals"] == "On USDA Food Program,Breakfast"


@pytest.mark.parametrize("text", ["CHILD CARE FOOD PROGRAM", "food program", "cacfp", "Participates in CACFP"])
def test_food_program_phrase_is_case_insensitive(text):
    assert cf.food_program_token(text) is True
    assert norm.normalize_item({"de_financial_arrangements": text}, "delaware")["food_program"] is True


def test_food_program_phrase_absent_token_is_none():
    assert cf.food_program_token("Breakfast") is None
    assert cf.food_program_token(None) is None


def test_food_program_adapters_never_overwrite():
    item = norm.normalize_item({"ut_meals": "On USDA Food Program", "food_program": False}, "utah")
    assert item["food_program"] is False


# --------------------------------------------------------------------------- #
# State pre-K participation
# --------------------------------------------------------------------------- #
PREK_BOOL_CASES = [
    ("florida", "fl_is_vpk", True, False, "VPK"),
    ("rhode_island", "ri_state_prek", "Yes", "No", "RI Pre-K"),
    ("washington", "wa_eceap", "Yes", "No", "ECEAP"),
    ("washington_dc", "dc_prek_enhancement", True, False, "Pre-K Enhancement and Expansion"),
    ("kentucky", "ky_prek_partnership", True, False, "Preschool Partnership"),
]
PREK_TOKEN_CASES = [
    ("indiana", "in_programs", ["CCDF Provider", "On My Way Pre-K"], ["CCDF Provider"], "On My Way Pre-K"),
    ("georgia", "ga_other_care_type", "Summer Camp; Georgia's Pre-K", "Summer Camp", "Georgia's Pre-K"),
    ("south_carolina", "sc_program_participation", ["First Steps 4K"], ["ABC Quality"], "First Steps 4K"),
    ("vermont", "vt_program_participation", "Food Program, Pre-K Program", "Food Program", "Vermont Universal Pre-K"),
    ("nevada", "nv_site_characteristic", "Nevada Ready Pre-K", "Montessori", "Nevada Ready! Pre-K"),
]


@pytest.mark.parametrize(("state", "field", "yes", "no", "brand"), PREK_BOOL_CASES)
def test_state_prek_bool_states(state, field, yes, no, brand):
    item = norm.normalize_item({field: yes}, state)
    assert item["state_prek"] is True
    assert item["state_prek_program"] == brand
    assert item[field] == yes
    item = norm.normalize_item({field: no}, state)
    assert item["state_prek"] is False
    assert "state_prek_program" not in item
    assert item[field] == no
    for blank in (None, ""):
        item = norm.normalize_item({field: blank}, state)
        assert "state_prek" not in item
        assert "state_prek_program" not in item


@pytest.mark.parametrize(("state", "field", "yes", "no", "brand"), PREK_TOKEN_CASES)
def test_state_prek_token_states(state, field, yes, no, brand):
    item = norm.normalize_item({field: yes}, state)
    assert item["state_prek"] is True
    assert item["state_prek_program"] == brand
    assert item[field] == yes
    for absent in (no, None, "", []):
        item = norm.normalize_item({field: absent}, state)
        assert "state_prek" not in item
        assert "state_prek_program" not in item


def test_state_prek_georgia_transition_alone_is_unset():
    item = norm.normalize_item({"ga_other_care_type": "Georgia's Pre-K Transition"}, "georgia")
    assert "state_prek" not in item
    assert "state_prek_program" not in item


def test_state_prek_georgia_both_entries_is_true():
    raw = "Georgia's Pre-K Transition; Summer Camp; Georgia's Pre-K"
    item = norm.normalize_item({"ga_other_care_type": raw}, "georgia")
    assert item["state_prek"] is True
    assert item["state_prek_program"] == "Georgia's Pre-K"


def test_state_prek_vermont_head_start_alone_is_unset():
    item = norm.normalize_item({"vt_program_participation": "Head Start Program"}, "vermont")
    assert "state_prek" not in item


def test_state_prek_rhode_island_none_is_unset():
    item = norm.normalize_item({"ri_state_prek": None}, "rhode_island")
    assert "state_prek" not in item
    assert "state_prek_program" not in item


@pytest.mark.parametrize(
    ("state", "field", "raw"),
    [
        ("georgia", "ga_other_care_type", "GEORGIA'S PRE-K"),
        ("vermont", "vt_program_participation", "pre-k program"),
        ("nevada", "nv_site_characteristic", "nevada ready pre-k"),
        ("indiana", "in_programs", ["on my way pre-k"]),
        ("rhode_island", "ri_state_prek", "yes"),
    ],
)
def test_state_prek_is_case_insensitive(state, field, raw):
    assert norm.normalize_item({field: raw}, state)["state_prek"] is True


def test_state_prek_adapters_never_overwrite():
    item = norm.normalize_item({"fl_is_vpk": True, "state_prek": False}, "florida")
    assert item["state_prek"] is False
    assert "state_prek_program" not in item


def test_state_prek_other_state_is_not_read():
    item = norm.normalize_item({"fl_is_vpk": True}, "georgia")
    assert "state_prek" not in item


# --------------------------------------------------------------------------- #
# Night, evening and weekend care
# --------------------------------------------------------------------------- #
def _hours(state, **raw):
    return norm.normalize_item(dict(raw), state)


def _flags(item):
    return tuple(item.get(f) for f in ("night_care", "evening_care", "weekend_care", "nontraditional_hours"))


@pytest.mark.parametrize(
    ("value", "expected"),
    [("8", True), ("0", False), (8, True), (0, False), ("0 0", False), ("0  0  0", False), (None, None), ("", None)],
)
def test_capacity_to_bool(value, expected):
    assert cf.capacity_to_bool(value) is expected


def test_capacity_to_bool_unparseable_is_unset(caplog):
    with caplog.at_level(logging.WARNING):
        assert cf.capacity_to_bool("8 0") is None
        assert cf.capacity_to_bool("many") is None
        assert cf.capacity_to_bool("-3") is None


def test_nontraditional_louisiana():
    assert _flags(_hours("louisiana", la_night_care=True)) == (True, None, None, True)
    item = _hours("louisiana", la_night_care=False)
    assert _flags(item) == (False, None, None, None)  # roll-up stays unset
    assert item["la_night_care"] is False


def test_nontraditional_wyoming():
    item = _hours("wyoming", wy_evening_care=True, wy_weekend_care=False)
    assert _flags(item) == (None, True, False, True)
    assert _flags(_hours("wyoming", wy_evening_care=False, wy_weekend_care=False)) == (None, False, False, None)
    assert _flags(_hours("wyoming", wy_evening_care=False, wy_weekend_care=True)) == (None, False, True, True)


@pytest.mark.parametrize(
    ("raw", "night", "rollup"),
    [
        ("07:00 PM - 11:00 PM", True, True),
        ("10:00 PM - 06:00 AM", True, True),
        ("N/A - N/A", False, None),
        ("", None, None),
        (None, None, None),
    ],
)
def test_nontraditional_alabama(raw, night, rollup):
    item = _hours("alabama", al_nighttime_hours=raw, al_nighttime_ages="0-5")
    assert _flags(item) == (night, None, None, rollup)
    assert item.get("al_nighttime_hours") in (raw, None)
    assert item["al_nighttime_ages"] == "0-5"
    if raw:
        assert item["al_nighttime_hours"] == raw


@pytest.mark.parametrize("state", ["illinois", "wisconsin"])
@pytest.mark.parametrize(
    ("raw", "night", "rollup"),
    [("8", True, True), ("0", False, None), ("0 0", False, None), (None, None, None)],
)
def test_nontraditional_night_capacity_states(state, raw, night, rollup):
    field = "il_night_capacity" if state == "illinois" else "wi_night_capacity"
    item = _hours(state, **{field: raw})
    assert _flags(item) == (night, None, None, rollup)
    assert item[field] == raw


def test_nontraditional_wisconsin_junk_is_unset():
    assert _flags(_hours("wisconsin", wi_night_capacity="abc")) == (None, None, None, None)


def test_nontraditional_north_carolina():
    item = _hours("north_carolina", nc_capacity_second_shift=5, nc_capacity_third_shift=0)
    assert _flags(item) == (False, True, None, True)
    item = _hours("north_carolina", nc_capacity_second_shift=0, nc_capacity_third_shift=3)
    assert _flags(item) == (True, False, None, True)
    item = _hours("north_carolina", nc_capacity_second_shift=0, nc_capacity_third_shift=0)
    assert _flags(item) == (False, False, None, None)
    assert item["nc_capacity_third_shift"] == 0


def test_nontraditional_oklahoma():
    labels = ["Year Round", "Daytime Hours", "Evening Hours", "Weekend Hours", "Overnight Hours"]
    item = _hours("oklahoma", ok_care_types=labels)
    assert _flags(item) == (True, True, True, True)
    assert item["ok_care_types"] == labels
    assert _flags(_hours("oklahoma", ok_care_types=["Evening Hours"])) == (None, True, None, True)
    assert _flags(_hours("oklahoma", ok_care_types=["Year Round", "Daytime Hours"])) == (None, None, None, None)
    assert _flags(_hours("oklahoma", ok_care_types=[])) == (None, None, None, None)


def test_nontraditional_dc_rollup_only():
    item = _hours("washington_dc", dc_nontraditional_hours=True)
    assert _flags(item) == (None, None, None, True)
    item = _hours("washington_dc", dc_nontraditional_hours=False)
    assert _flags(item) == (None, None, None, False)
    assert item["dc_nontraditional_hours"] is False
    assert _flags(_hours("washington_dc", dc_nontraditional_hours=None)) == (None, None, None, None)


def test_nontraditional_does_not_overwrite_populated_and_ignores_other_states():
    item = _hours("louisiana", la_night_care=True, night_care=False)
    assert item["night_care"] is False
    assert _flags(_hours("georgia", la_night_care=True)) == (None, None, None, None)


# --------------------------------------------------------------------------- #
# care_schedules
# --------------------------------------------------------------------------- #
CARE_SCHEDULE_CASES = [
    (raw, tokens)
    for raw, tokens in {
        "Before School": ["before_school"],
        "Before school care": ["before_school"],
        "Before School Care provided.": ["before_school"],
        "Before and After School": ["before_school", "after_school"],
        "Before/After School": ["before_school", "after_school"],
        "After School": ["after_school"],
        "School Age After School": ["after_school"],
        "After school care": ["after_school"],
        "After School Care provided.": ["after_school"],
        "Drop In": ["drop_in"],
        "Drop-in": ["drop_in"],
        "Drop In Care": ["drop_in"],
        "Drop-in Care": ["drop_in"],
        "Drop-In Care": ["drop_in"],
        "Drop In Care provided.": ["drop_in"],
        "Drop-in/Hourly": ["drop_in"],
        "Hourly Care (Only)": ["drop_in"],
        "Drop in": ["drop_in"],
        "Parent’s night out": ["drop_in"],
        "Parent's night out": ["drop_in"],
        "Parent’s morning out": ["drop_in"],
        "Parent's morning out": ["drop_in"],
        "Drop-in care": ["drop_in"],
        "Full Day": ["full_day"],
        "Full day care": ["full_day"],
        "Full day": ["full_day"],
        "Full Day Care": ["full_day"],
        "Half Day": ["half_day"],
        "Half Day Morning": ["half_day"],
        "Half Day Afternoon": ["half_day"],
        "Half Day Morning & Afternoon": ["half_day"],
        "Half-day care (defined as 4 hours or less of care per day)": ["half_day"],
        "Part Day": ["half_day"],
        "Part day": ["half_day"],
        "Half Day Care": ["half_day"],
        "Full-time": ["full_time"],
        "Full-Time": ["full_time"],
        "Full Time": ["full_time"],
        "Full week": ["full_time"],
        "Part-time": ["part_time"],
        "Part-Time": ["part_time"],
        "Part Time": ["part_time"],
        "Partial week (less than 5 days a week care)": ["part_time"],
        "Part week": ["part_time"],
        "Summer Day Camp": ["summer"],
        "Summer Camp": ["summer"],
        "Before Camp Care": ["summer"],
        "After Camp Care": ["summer"],
        "School age care - summer vacation": ["summer"],
        "Summer Vacation": ["summer"],
        "Summer Only": ["summer"],
        "Days when school is not in session": ["school_holidays"],
        "School age care - school vacations": ["school_holidays"],
        "School age care - holidays/professional development days/snow day care": ["school_holidays"],
        "Open school breaks": ["school_holidays"],
        "Open school holidays": ["school_holidays"],
        "Open school vacation week": ["school_holidays"],
        "Open Holidays": ["school_holidays"],
        "Holidays": ["school_holidays"],
        "School Vacation Only": ["school_holidays"],
        "Temporary/Emergency": ["emergency"],
        "Temp/Emergency": ["emergency"],
        "Temporary Care": ["emergency"],
        "Emergency Care": ["emergency"],
        "Flexible Schedule": ["flexible"],
        "Flexible Opening Hours": ["flexible"],
        "Flexible Closing Hours": ["flexible"],
        "Rotating": ["flexible"],
        "Rotating Care provided.": ["flexible"],
        "Rotating Shifts": ["flexible"],
        "Intermittent Care": ["flexible"],
        "Sick Care": ["sick_care"],
        "Sick Care provided.": ["sick_care"],
        "Sick care": ["sick_care"],
    }.items()
]


def _schedules(state, **fields):
    return norm.normalize_item(dict(fields), state).get("care_schedules")


def test_care_schedule_table_covers_all_tokens_and_cases():
    assert {t for tokens in cf.CARE_SCHEDULE_LABELS.values() for t in tokens} == set(cf.CARE_SCHEDULE_TOKENS)
    assert cf.CARE_SCHEDULE_TOKENS == (
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
    for label in cf.CARE_SCHEDULE_LABELS:
        assert cf.care_schedule_tokens([label]) == list(cf.CARE_SCHEDULE_LABELS[label])
    assert not set(cf.CARE_SCHEDULE_LABELS) & cf.CARE_SCHEDULE_IGNORED


@pytest.mark.parametrize(("raw", "tokens"), CARE_SCHEDULE_CASES)
def test_care_schedule_label_maps_to_tokens(raw, tokens, caplog):
    with caplog.at_level(logging.WARNING):
        assert cf.care_schedule_tokens([raw]) == tokens
        assert cf.care_schedule_tokens([f"  {raw.upper()} "]) == tokens
    assert "unmapped" not in caplog.text


CARE_SCHEDULE_IGNORED_VALUES = [
    "Night Care",
    "Night care (care after 7pm on a consistent basis)",
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
    "No Additional Services Offered",
    "None of the additional care schedules apply",
]


@pytest.mark.parametrize("raw", CARE_SCHEDULE_IGNORED_VALUES)
def test_care_schedule_ignored_value_gives_nothing_and_is_not_logged(raw, caplog):
    with caplog.at_level(logging.WARNING):
        assert cf.care_schedule_tokens([raw]) == []
        assert _schedules("florida", fl_services=[raw]) is None
    assert caplog.text == ""


def test_care_schedule_ignored_set_includes_every_oklahoma_label():
    from provider_scrape.spiders.oklahoma import CARE_TYPE_LABELS

    for label in CARE_TYPE_LABELS.values():
        key = cf._care_schedule_key(label)
        assert key in cf._CARE_SCHEDULE_LOOKUP or key in cf._CARE_SCHEDULE_IGNORED_KEYS


def test_care_schedule_unknown_value_is_logged_and_dropped(caplog):
    with caplog.at_level(logging.WARNING):
        assert cf.care_schedule_tokens(["Mystery Care", "Drop In"]) == ["drop_in"]
    assert "unmapped value 'Mystery Care'" in caplog.text


def test_care_schedule_canonical_order_and_dedup():
    labels = ["Sick Care", "Part-time", "Before and After School", "After School", "Drop In", "drop in", "Full Day"]
    assert cf.care_schedule_tokens(labels) == [
        "before_school",
        "after_school",
        "drop_in",
        "full_day",
        "part_time",
        "sick_care",
    ]
    assert cf.care_schedule_tokens(["Evening", "N/A", None, ""]) == []


@pytest.mark.parametrize(
    ("state", "field"),
    [
        ("florida", "fl_services"),
        ("mississippi", "ms_services"),
        ("south_dakota", "sd_services_offered"),
        ("rhode_island", "ri_services_offered"),
        ("wisconsin", "wi_special_care_types"),
        ("north_dakota", "nd_supplemental_care"),
        ("massachusetts", "ma_schedule_options"),
        ("wyoming", "wy_services"),
        ("new_jersey", "nj_special_schedules_labels"),
    ],
)
def test_care_schedules_list_states(state, field):
    raw = ["Drop In", "Evening", "Mystery Care", "Before School Care provided.", "Drop-in Care"]
    item = norm.normalize_item({field: list(raw)}, state)
    assert item["care_schedules"] == ["before_school", "drop_in"]
    assert item[field] == raw
    assert _schedules(state, **{field: None}) is None
    assert _schedules(state, **{field: []}) is None
    assert _schedules(state, **{field: ["Infant Care"]}) is None


def test_care_schedules_florida_and_mississippi_examples():
    assert _schedules("florida", fl_services=["After School", "Before School", "Drop In", "Infant Care"]) == [
        "before_school",
        "after_school",
        "drop_in",
    ]
    assert _schedules("mississippi", ms_services=["School Age After School", "Full Day"]) == [
        "after_school",
        "full_day",
    ]


def test_care_schedules_rhode_island_example():
    raw = ["Half-day care (defined as 4 hours or less of care per day)", "Before school care", "Snacks included"]
    assert _schedules("rhode_island", ri_services_offered=raw) == ["before_school", "half_day"]


def test_care_schedules_new_jersey_labels_only_not_codes():
    assert _schedules("new_jersey", nj_special_schedules=[4, 6]) is None
    item = norm.normalize_item(
        {"nj_special_schedules": [1, 4, 6, 12], "nj_special_schedules_labels": ["Evening", "Holidays", "Drop-in care"]},
        "new_jersey",
    )
    assert item["care_schedules"] == ["drop_in", "school_holidays"]
    assert item["nj_special_schedules"] == [1, 4, 6, 12]


def test_care_schedules_iowa_comma_string():
    raw = "24-Hour, After School, Before School, Drop-in, Evening, Full-time, Open Holidays, Overnight, Rotating"
    item = norm.normalize_item({"ia_care_types": raw}, "iowa")
    assert item["care_schedules"] == [
        "before_school",
        "after_school",
        "drop_in",
        "full_time",
        "school_holidays",
        "flexible",
    ]
    assert item["ia_care_types"] == raw


def test_care_schedules_vermont_before_and_after():
    item = norm.normalize_item({"vt_type_of_care": "Before and After School, Daytime, Full-Time"}, "vermont")
    assert item["care_schedules"] == ["before_school", "after_school", "full_time"]


def test_care_schedules_michigan_semicolon_string():
    raw = "Night Time;Pre-School;School Age;Infant/Toddler;Food Service;Before/After School"
    item = norm.normalize_item({"mi_services_provided": raw}, "michigan")
    assert item["care_schedules"] == ["before_school", "after_school"]
    assert item["mi_services_provided"] == raw


def test_care_schedules_new_hampshire_uppercase():
    raw = "PART WEEK;PART DAY;FULL DAY;FULL WEEK;OPEN SCHOOL VACATION WEEK;FLEXIBLE SCHEDULE"
    assert _schedules("new_hampshire", nh_schedule_options=raw) == [
        "full_day",
        "half_day",
        "full_time",
        "part_time",
        "school_holidays",
        "flexible",
    ]


def test_care_schedules_oklahoma_subset():
    labels = ["Year Round", "Daytime Hours", "Evening Hours", "Drop In", "Sick Care", "Summer Only"]
    item = norm.normalize_item({"ok_care_types": labels}, "oklahoma")
    assert item["care_schedules"] == ["drop_in", "summer", "sick_care"]
    assert item["ok_care_types"] == labels
    assert _schedules("oklahoma", ok_care_types=["School Vacation Only"]) == ["school_holidays"]
    assert _schedules("oklahoma", ok_care_types=["Year Round", "Daytime Hours"]) is None


def test_care_schedules_georgia_merges_three_sources_with_curly_apostrophe():
    item = norm.normalize_item(
        {
            "ga_special_hours": "Drop in; Open school breaks; Parent’s night out",
            "ga_accepts_children_type": "Full Time; Part Time",
            "ga_summer_camp": "Summer Camp",
        },
        "georgia",
    )
    assert item["care_schedules"] == ["drop_in", "full_time", "part_time", "summer", "school_holidays"]
    assert item["ga_special_hours"] == "Drop in; Open school breaks; Parent’s night out"
    assert _schedules("georgia", ga_summer_camp="Summer Camp") == ["summer"]
    assert _schedules("georgia", ga_special_hours="Parent's morning out") == ["drop_in"]
    assert _schedules("georgia", ga_special_hours=None, ga_summer_camp=None) is None


def test_care_schedules_louisiana_bools():
    item = norm.normalize_item({"la_before_care": True, "la_after_care": True}, "louisiana")
    assert item["care_schedules"] == ["before_school", "after_school"]
    assert item["la_before_care"] is True
    assert _schedules("louisiana", la_before_care=False, la_after_care=True) == ["after_school"]
    assert _schedules("louisiana", la_before_care=True, la_after_care=False) == ["before_school"]
    item = norm.normalize_item({"la_before_care": False, "la_after_care": False}, "louisiana")
    assert "care_schedules" not in item
    assert item["la_before_care"] is False
    assert _schedules("louisiana", la_before_care=None, la_after_care=None) is None


def test_care_schedules_does_not_overwrite_and_ignores_other_states():
    assert _schedules("florida", fl_services=["Drop In"], care_schedules=["summer"]) == ["summer"]
    assert _schedules("texas", fl_services=["Drop In"]) is None


# --------------------------------------------------------------------------- #
# discounts
# --------------------------------------------------------------------------- #
def _discounts(state, **fields):
    return norm.normalize_item(dict(fields), state).get("discounts")


def test_discount_table_covers_all_tokens():
    assert set(cf.DISCOUNT_LABELS.values()) == set(cf.DISCOUNT_TOKENS)


def test_discounts_georgia_labels_and_source_kept():
    raw = "Multi-Child Discount; Scholarship; Sliding Scale Fee"
    item = norm.normalize_item({"ga_financial_info": raw}, "georgia")
    assert item["discounts"] == ["sibling", "sliding_scale"]
    assert item["ga_financial_info"] == raw


def test_discounts_massachusetts_labels_and_source_kept():
    raw = ["EEC Subsidies", "Sibling Discounts", "Employee Discounts", "Sliding Fee Scale", "Other", "Head Start"]
    item = norm.normalize_item({"ma_financial_assistance": list(raw)}, "massachusetts")
    assert item["discounts"] == ["sibling", "sliding_scale", "employee", "other"]
    assert item["ma_financial_assistance"] == raw


def test_discounts_new_hampshire_uppercase_comma_string():
    raw = "MULTI-CHILD DISCOUNT, SLIDING FEE SCALE, STATE CC ASSISTANCE (SCHOLARSHIP), PREVENTIVE AND PROTECTIVE CARE"
    item = norm.normalize_item({"nh_financial_assistance": raw}, "new_hampshire")
    assert item["discounts"] == ["sibling", "sliding_scale"]
    assert item["nh_financial_assistance"] == raw


def test_discounts_new_jersey_labels():
    labels = [
        "Discounts provided for parents in military service",
        "Discounts provided for employees",
        "Discount provided for siblings",
        "Discounts provided on a sliding scale",
    ]
    item = norm.normalize_item({"nj_discounts": [1, 2, 3, 4], "nj_discounts_labels": labels}, "new_jersey")
    assert item["discounts"] == ["sibling", "sliding_scale", "employee", "military"]
    assert item["nj_discounts"] == [1, 2, 3, 4]
    assert _discounts("new_jersey", nj_discounts=[1]) is None


def test_discounts_vermont_yes_no():
    item = norm.normalize_item({"vt_sibling_discount": "Yes"}, "vermont")
    assert item["discounts"] == ["sibling"]
    assert item["vt_sibling_discount"] == "Yes"
    assert _discounts("vermont", vt_sibling_discount="No") is None
    assert _discounts("vermont", vt_sibling_discount=None) is None


def test_discounts_skipped_subsidies_do_not_warn(caplog):
    with caplog.at_level(logging.WARNING):
        assert _discounts("georgia", ga_financial_info="Scholarship") is None
        assert (
            _discounts("massachusetts", ma_financial_assistance=["EEC Subsidies", "Private Scholarships/Grants"])
            is None
        )
        assert _discounts("new_hampshire", nh_financial_assistance="STATE CC ASSISTANCE (SCHOLARSHIP)") is None
    assert "unmapped" not in caplog.text


def test_discounts_unknown_label_logged_and_dropped(caplog):
    with caplog.at_level(logging.WARNING):
        result = _discounts("georgia", ga_financial_info="Multi-Child Discount; Pet Rock Rebate")
    assert result == ["sibling"]
    assert "discounts: unmapped value 'Pet Rock Rebate'" in caplog.text


def test_discounts_dedup_order_and_case_insensitive():
    labels = [" sliding fee scale ", "SIBLING DISCOUNTS", "sibling discounts", "Other", "employee discounts"]
    assert _discounts("massachusetts", ma_financial_assistance=labels) == [
        "sibling",
        "sliding_scale",
        "employee",
        "other",
    ]


def test_discounts_empty_sources_are_none_and_no_overwrite():
    assert _discounts("georgia", ga_financial_info="") is None
    assert _discounts("massachusetts", ma_financial_assistance=[]) is None
    assert _discounts("new_hampshire", nh_financial_assistance=None) is None
    assert _discounts("georgia", ga_financial_info="Sliding Scale Fee", discounts=["military"]) == ["military"]
    assert _discounts("texas", ga_financial_info="Sliding Scale Fee") is None


# --------------------------------------------------------------------------- #
# rates
# --------------------------------------------------------------------------- #
def _rates(state, **record):
    return norm.normalize_item(dict(record), state).get("rates")


def test_rates_connecticut_unpivots_both_columns_and_labels():
    rates = _rates(
        "connecticut",
        ct_rates=[
            {"age_group": "Infant", "label": "0-17 Months", "full_time_weekly": "225.00", "full_time_hourly": "6.50"},
            {"age_group": "Toddler", "part_time_daily": "30"},
        ],
    )
    assert rates == [
        {"age_group": "Infant (0-17 Months)", "unit": "full_time", "period": "week", "amount": 225.0},
        {"age_group": "Infant (0-17 Months)", "unit": "full_time", "period": "hour", "amount": 6.5},
        {"age_group": "Toddler", "unit": "part_time", "period": "day", "amount": 30.0},
    ]


def test_rates_connecticut_unknown_column_logged(caplog):
    with caplog.at_level(logging.WARNING):
        rates = _rates("connecticut", ct_rates=[{"age_group": "Infant", "bogus": "5", "full_time_weekly": "9"}])
    assert [r["amount"] for r in rates] == [9.0]
    assert "unknown rate column 'bogus'" in caplog.text


def test_rates_georgia_strips_dollar_and_comma():
    rates = _rates("georgia", ga_weekly_rates=[{"age": "1 year", "weekly_full_day": "$1,150.00"}])
    assert rates == [{"age_group": "1 year", "unit": "full_time", "period": "week", "amount": 1150.0}]
    assert _rates("georgia", ga_weekly_rates=[{"age": "2 years", "weekly_full_day": None}]) is None


def test_rates_kentucky_and_pennsylvania_period_is_day():
    ky = _rates("kentucky", ky_service_costs=[{"age_group": "Infant", "full_time_cost": 40.0, "part_time_cost": 20.0}])
    assert ky == [
        {"age_group": "Infant", "unit": "full_time", "period": "day", "amount": 40.0},
        {"age_group": "Infant", "unit": "part_time", "period": "day", "amount": 20.0},
    ]
    pa = _rates(
        "pennsylvania",
        pa_cost_table=[{"age_group": "Toddler", "full_time_rate": "$34.00", "part_time_rate": None, "openings": "x"}],
    )
    assert pa == [{"age_group": "Toddler", "unit": "full_time", "period": "day", "amount": 34.0}]


def test_rates_tennessee_every_unit_of_care_class_and_zero_skipped():
    def row(descriptor, rate="$144.00"):
        return {"age_group": "Infant", "weekly_rate": rate, "unit_of_care": descriptor, "vacancy": "Yes"}

    rates = _rates(
        "tennessee",
        tn_age_group_rates=[
            row("Full-time"),
            row("Part-time"),
            row("Weekly"),
            row("Daily"),
            row("Monthly"),
            row("Before/After"),
            row("Summer", "$0.00"),
        ],
    )
    assert [(r["unit"], r["period"], r.get("care_type")) for r in rates] == [
        ("full_time", "week", None),
        ("part_time", "week", None),
        (None, "week", None),
        (None, "day", None),
        (None, "month", None),
        (None, "week", "Before/After"),
    ]
    assert all(r["amount"] == 144.0 for r in rates)


def test_rates_tennessee_non_weekly_descriptors():
    rates = _rates(
        "tennessee",
        tn_age_group_rates=[
            {"age_group": "Infant", "weekly_rate": "$12.00", "unit_of_care": "Hourly care"},
            {"age_group": "Infant", "weekly_rate": "$15.00", "unit_of_care": "Drop In"},
            {"age_group": "Infant", "weekly_rate": "$345.00", "unit_of_care": "Yearly"},
        ],
    )
    assert [(r["period"], r.get("care_type")) for r in rates] == [
        ("hour", "Hourly care"),
        (None, "Drop In"),
        ("week", "Yearly"),  # a year-round descriptor; the amount is weekly
    ]


@pytest.mark.parametrize(
    ("rate_type", "unit", "period"),
    [
        ("Hourly Full Time", "full_time", "hour"),
        ("Hourly Part Time", "part_time", "hour"),
        ("Daily Full Time", "full_time", "day"),
        ("Daily Part Time", "part_time", "day"),
        ("Weekly Full Time", "full_time", "week"),
        ("Weekly Part Time", "part_time", "week"),
        ("Monthly Full Time", "full_time", "month"),
        ("Monthly Part Time", "part_time", "month"),
        ("Per Fortnight", None, None),
    ],
)
def test_rates_massachusetts_rate_type(rate_type, unit, period, caplog):
    table = [
        {"schedule_type": "Full Year Schedule", "age_group": "Preschool", "rate_type": rate_type, "amount": "$360.81"}
    ]
    with caplog.at_level(logging.WARNING):
        rates = _rates("massachusetts", ma_cost_table=table)
    assert rates == [
        {
            "age_group": "Preschool",
            "unit": unit,
            "period": period,
            "amount": 360.81,
            "schedule_type": "Full Year Schedule",
        }
    ]
    assert ("unrecognised rate_type" in caplog.text) == (unit is None)


@pytest.mark.parametrize(
    ("present", "expected"),
    [
        (["Summer Schedule", "School Year Schedule", "Full Year Schedule"], "Full Year Schedule"),
        (["Summer Schedule", "School Year Schedule"], "School Year Schedule"),
        (["Other Schedule", "Summer Schedule"], "Summer Schedule"),
        (["Other Schedule", "Odd Schedule"], "Other Schedule"),
    ],
)
def test_rates_massachusetts_schedule_priority(present, expected):
    table = [
        {"schedule_type": s, "age_group": "Infant", "rate_type": "Daily Full Time", "amount": "10.00"} for s in present
    ]
    rates = _rates("massachusetts", ma_cost_table=table)
    assert [r["schedule_type"] for r in rates] == [expected]


def test_rates_new_jersey_skips_zero_and_none():
    rates = _rates(
        "new_jersey",
        nj_tuition_infant_weekly=250.0,
        nj_tuition_infant_hourly=0.0,
        nj_tuition_school_age_monthly=800.5,
        nj_tuition_toddler_daily=None,
    )
    assert rates == [
        {"age_group": "Infant", "unit": None, "period": "week", "amount": 250.0},
        {"age_group": "School Age", "unit": None, "period": "month", "amount": 800.5},
    ]
    assert _rates("new_jersey", nj_tuition_infant_weekly=0.0) is None


def test_rates_wyoming_maps_period_and_type(caplog):
    programs = [
        {
            "name": "Infant Care",
            "type": "Infant Care",
            "rates": [
                {"rate": 36.96, "period": "DAILY", "type": "FULL_TIME"},
                {"rate": 18.48, "period": "DAILY", "type": "PART_TIME"},
                {"rate": 5, "period": "HOURLY", "type": "DROP_IN"},
                {"rate": 9, "period": "FORTNIGHTLY", "type": "FULL_TIME"},
            ],
        }
    ]
    with caplog.at_level(logging.WARNING):
        rates = _rates("wyoming", wy_programs=programs)
    assert rates == [
        {"age_group": "Infant Care", "unit": "full_time", "period": "day", "amount": 36.96},
        {"age_group": "Infant Care", "unit": "part_time", "period": "day", "amount": 18.48},
        {"age_group": "Infant Care", "unit": None, "period": "hour", "amount": 5.0, "care_type": "DROP_IN"},
        {"age_group": "Infant Care", "unit": "full_time", "period": None, "amount": 9.0},
    ]
    assert "unknown period 'FORTNIGHTLY'" in caplog.text


def test_rates_new_hampshire_produces_none_and_source_kept():
    text = "Please contact the child care provider for the rates."
    item = norm.normalize_item({"nh_rates": text}, "new_hampshire")
    assert "rates" not in item
    assert item["nh_rates"] == text


def test_rates_unparseable_logged_and_nonpositive_dropped(caplog):
    with caplog.at_level(logging.WARNING):
        rates = _rates(
            "georgia",
            ga_weekly_rates=[
                {"age": "a", "weekly_full_day": "call us"},
                {"age": "b", "weekly_full_day": "$0.00"},
                {"age": "c", "weekly_full_day": "-5"},
                {"age": "d", "weekly_full_day": " $12.50 "},
            ],
        )
    assert rates == [{"age_group": "d", "unit": "full_time", "period": "week", "amount": 12.5}]
    assert "could not parse number 'call us'" in caplog.text


def test_rates_keeps_sources_and_never_overwrites():
    source = [{"age": "1 year", "weekly_full_day": "$150.00"}]
    item = norm.normalize_item({"ga_weekly_rates": source}, "georgia")
    assert item["ga_weekly_rates"] == source
    existing = [{"age_group": "x", "unit": None, "period": None, "amount": 1.0}]
    assert _rates("georgia", ga_weekly_rates=source, rates=existing) == existing


# --------------------------------------------------------------------------- #
# registration_fee
# --------------------------------------------------------------------------- #
def _registration_fee(state, **record):
    return norm.normalize_item(dict(record), state).get("registration_fee")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("$100.00", 100.0), ("$0.00", 0.0), ("$1,250.00", 1250.0)],
)
def test_registration_fee_georgia_parses_dollars(raw, expected):
    fee = _registration_fee("georgia", ga_registration_fee=raw)
    assert fee == expected
    assert isinstance(fee, float)


def test_registration_fee_georgia_none_is_unset():
    assert _registration_fee("georgia", ga_registration_fee=None) is None


def test_registration_fee_wyoming_copies_amount():
    assert _registration_fee("wyoming", wy_registration_fee=50.0) == 50.0


def test_registration_fee_wyoming_missing_is_none():
    assert _registration_fee("wyoming") is None


def test_registration_fee_keeps_sources():
    ga = norm.normalize_item({"ga_registration_fee": "$100.00"}, "georgia")
    assert ga["ga_registration_fee"] == "$100.00"
    wy = norm.normalize_item({"wy_registration_fee": 50.0}, "wyoming")
    assert wy["wy_registration_fee"] == 50.0


def test_registration_fee_new_jersey_not_mapped():
    item = norm.normalize_item({"nj_fees": [1, 2]}, "new_jersey")
    assert item.get("registration_fee") is None
    assert item["nj_fees"] == [1, 2]


# --------------------------------------------------------------------------- #
# environment_features
# --------------------------------------------------------------------------- #
def _environment(state, **fields):
    return norm.normalize_item(dict(fields), state).get("environment_features")


def test_environment_table_covers_all_tokens():
    assert set(cf.ENVIRONMENT_LABELS.values()) == set(cf.ENVIRONMENT_TOKENS)


def test_environment_georgia_labels_and_source_kept():
    raw = (
        "No pets; Outdoor Play area; Smoke Free Environment; Secure Grounds/Keypad Access; Video surveillance; "
        "Webcam for parents; Pool; Sports fields; Tennis Courts; Provide your own equipment"
    )
    item = norm.normalize_item({"ga_environment": raw}, "georgia")
    assert item["environment_features"] == [
        "smoke_free",
        "outdoor_play",
        "secure_entry",
        "video_surveillance",
        "parent_webcam",
        "pool",
        "sports_facilities",
    ]
    assert item["ga_environment"] == raw


def test_environment_massachusetts_labels_and_source_kept():
    raw = [
        "Wheelchair Accessible",
        "Adult Pool",
        "Peanut Free",
        "Field Trips Taken",
        "Air Conditioned",
        "Fenced Yard",
        "Smoke Free",
        "Approved Assistant",
    ]
    item = norm.normalize_item({"ma_environment": list(raw)}, "massachusetts")
    assert item["environment_features"] == [
        "smoke_free",
        "fenced_yard",
        "air_conditioned",
        "peanut_free",
        "pool",
        "field_trips",
    ]
    assert item["ma_environment"] == raw


def test_environment_new_hampshire_uppercase_comma_string():
    raw = (
        "WHEELCHAIR ACCESSIBLE, NO CAT, NO TV USE, OUTDOOR PLAY EQUIPMENT, MIXED AGE (3-5), NO DOG, "
        "PUBLIC TRANSPORTATION, WADING POOL, FENCED IN YARD"
    )
    item = norm.normalize_item({"nh_environment": raw}, "new_hampshire")
    assert item["environment_features"] == [
        "fenced_yard",
        "outdoor_play",
        "pool",
        "near_public_transit",
        "no_tv",
    ]
    assert item["nh_environment"] == raw
    other = "SMOKE FREE, AIR CONDITIONED, PEANUT FREE, FIELD TRIPS TAKEN, SCHOOL BUS ROUTE, NOT WHEELCHAIR ACCESSIBLE"
    assert _environment("new_hampshire", nh_environment=other) == [
        "smoke_free",
        "air_conditioned",
        "peanut_free",
        "field_trips",
    ]


def test_environment_utah_comma_string_without_space():
    raw = "Smoke-free Environment,No Pets,Near Public Transportation"
    item = norm.normalize_item({"ut_environment": raw}, "utah")
    assert item["environment_features"] == ["smoke_free", "near_public_transit"]
    assert item["ut_environment"] == raw
    assert _environment("utah", ut_environment="Pets") is None


def test_environment_vermont_area_description():
    item = norm.normalize_item({"vt_area_description": "Fenced Yard, Smoke Free"}, "vermont")
    assert item["environment_features"] == ["smoke_free", "fenced_yard"]
    assert item["vt_area_description"] == "Fenced Yard, Smoke Free"


def test_environment_new_jersey_labels_and_unknown_codes_silent(caplog):
    labels = [
        "Smoke-free environment",
        "Separated play areas",
        "Fenced-in play area",
        "No pets",
        "Handicap accessible",
        "Unknown (code 10)",
    ]
    with caplog.at_level(logging.WARNING):
        item = norm.normalize_item(
            {"nj_environmental_features": [5, 4, 2, 1, 3, 10], "nj_environmental_features_labels": labels},
            "new_jersey",
        )
        assert _environment("new_jersey", nj_environmental_features_labels=["Unknown (code 10)"]) is None
    assert item["environment_features"] == ["smoke_free", "fenced_yard", "separate_play_areas"]
    assert item["nj_environmental_features"] == [5, 4, 2, 1, 3, 10]
    assert caplog.text == ""
    assert _environment("new_jersey", nj_environmental_features=[2]) is None


def test_environment_skipped_labels_do_not_warn(caplog):
    with caplog.at_level(logging.WARNING):
        assert (
            _environment(
                "new_hampshire",
                nh_environment="CAT, DOG, INDOOR PETS, OUTDOOR PETS, NO PETS, MIXED AGE (0-3), SCHOOL BUS ROUTE",
            )
            is None
        )
        assert _environment("georgia", ga_environment="No pets; Provide your own equipment") is None
        assert _environment("massachusetts", ma_environment=["Wheelchair Accessible", "Approved Assistant"]) is None
    assert "unmapped" not in caplog.text


def test_environment_unknown_label_logged_and_dropped(caplog):
    with caplog.at_level(logging.WARNING):
        result = _environment("georgia", ga_environment="Pool; Trampoline Park")
    assert result == ["pool"]
    assert "environment_features: unmapped value 'Trampoline Park'" in caplog.text


def test_environment_dedup_order_and_case_insensitive():
    labels = [" wading pool ", "SMOKE FREE", "smoke free", "Fenced Yard", "Adult Pool"]
    assert _environment("massachusetts", ma_environment=labels) == ["smoke_free", "fenced_yard", "pool"]


def test_environment_empty_sources_are_none_and_no_overwrite():
    assert _environment("georgia", ga_environment="") is None
    assert _environment("massachusetts", ma_environment=[]) is None
    assert _environment("georgia", ga_environment="Pool", environment_features=["no_tv"]) == ["no_tv"]
    assert _environment("texas", ga_environment="Pool") is None


# --------------------------------------------------------------------------- #
# MA "Other" rate types
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("rate_type", "unit"),
    [("Other Full Time", "full_time"), ("other part time ", "part_time")],
)
def test_rates_massachusetts_other_rate_type_sets_unit_only(rate_type, unit, caplog):
    table = [{"schedule_type": "Full Year Schedule", "age_group": "Infant", "rate_type": rate_type, "amount": "$24.00"}]
    with caplog.at_level(logging.WARNING):
        rates = _rates("massachusetts", ma_cost_table=table)
    assert [(r["unit"], r["period"]) for r in rates] == [(unit, None)]
    assert "unrecognised rate_type" not in caplog.text


def test_rates_massachusetts_unknown_rate_type_still_warns(caplog):
    table = [
        {"schedule_type": "Full Year Schedule", "age_group": "Infant", "rate_type": "Other Quarter", "amount": "5"}
    ]
    with caplog.at_level(logging.WARNING):
        rates = _rates("massachusetts", ma_cost_table=table)
    assert [(r["unit"], r["period"]) for r in rates] == [(None, None)]
    assert "unrecognised rate_type" in caplog.text


# --------------------------------------------------------------------------- #
# has_pets / pet_types / pets_excluded
# --------------------------------------------------------------------------- #
def _pets(state, **fields):
    item = norm.normalize_item(dict(fields), state)
    return item.get("has_pets"), item.get("pet_types"), item.get("pets_excluded")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Pet Free", (False, None, None)),
        (" pet free ", (False, None, None)),
        ("Dogs only", (True, ["dogs"], None)),
        ("CATS ONLY", (True, ["cats"], None)),
        ("Other", (True, ["other"], None)),
    ],
)
def test_pets_idaho(raw, expected):
    assert _pets("idaho", id_pet_policy=raw) == expected


def test_pets_idaho_unknown_logged_and_none(caplog):
    with caplog.at_level(logging.WARNING):
        assert _pets("idaho", id_pet_policy="Goldfish") == (None, None, None)
    assert "unmapped id_pet_policy" in caplog.text
    assert _pets("idaho", id_pet_policy="") == (None, None, None)


def test_pets_idaho_source_kept():
    item = norm.normalize_item({"id_pet_policy": "Dogs only"}, "idaho")
    assert item["id_pet_policy"] == "Dogs only"


def test_pets_vermont_types_ordered_and_deduped():
    raw = "Other Furry Animal, cat, Dog, Other Non-Furry Animal"
    assert _pets("vermont", vt_pets=raw) == (True, ["dogs", "cats", "other"], None)
    assert norm.normalize_item({"vt_pets": raw}, "vermont")["vt_pets"] == raw


def test_pets_vermont_unknown_dropped_and_logged(caplog):
    with caplog.at_level(logging.WARNING):
        assert _pets("vermont", vt_pets="Dog, Dragon") == (True, ["dogs"], None)
        assert _pets("vermont", vt_pets="Dragon") == (None, None, None)
    assert "unmapped vt_pets entry 'Dragon'" in caplog.text


@pytest.mark.parametrize("value", [None, "", "   "])
def test_pets_vermont_absence_is_none(value):
    assert _pets("vermont", vt_pets=value) == (None, None, None)
    assert _pets("vermont") == (None, None, None)


def test_pets_georgia():
    assert _pets("georgia", ga_environment="Pool; no pets; Smoke Free Environment") == (False, None, None)
    assert _pets("georgia", ga_environment="Pool; Smoke Free Environment") == (None, None, None)
    assert _pets("georgia", ga_environment="") == (None, None, None)


def test_pets_utah():
    assert _pets("utah", ut_environment="Smoke-free Environment,No Pets") == (False, None, None)
    assert _pets("utah", ut_environment="Pets,Near Public Transportation") == (True, None, None)
    assert _pets("utah", ut_environment="Near Public Transportation") == (None, None, None)


def test_pets_new_jersey():
    assert _pets("new_jersey", nj_environmental_features_labels=["Smoke-free Environment", "No pets"]) == (
        False,
        None,
        None,
    )
    assert _pets("new_jersey", nj_environmental_features_labels=["Smoke-free Environment"]) == (None, None, None)
    assert _pets("new_jersey", nj_environmental_features_labels=[]) == (None, None, None)


def test_pets_new_hampshire_positive_and_negative():
    env = "WHEELCHAIR ACCESSIBLE, NO CAT, NO TV USE, INDOOR PETS, NO DOG"
    assert _pets("new_hampshire", nh_environment=env) == (True, None, ["dogs", "cats"])
    item = norm.normalize_item({"nh_environment": env}, "new_hampshire")
    assert item["nh_environment"] == env
    assert item["environment_features"] == ["no_tv"]
    assert _pets("new_hampshire", nh_environment="NO PETS, SMOKE FREE") == (False, None, None)
    assert _pets("new_hampshire", nh_environment="OUTDOOR PETS") == (True, None, None)
    assert _pets("new_hampshire", nh_environment="PETS") == (True, None, None)
    assert _pets("new_hampshire", nh_environment="DOGS, cat") == (True, ["dogs", "cats"], None)
    assert _pets("new_hampshire", nh_environment="NO DOGS, NO CATS") == (None, None, ["dogs", "cats"])


def test_pets_new_hampshire_negatives_alone_do_not_set_has_pets():
    assert _pets("new_hampshire", nh_environment="NO DOG, NO CAT") == (None, None, ["dogs", "cats"])
    assert _pets("new_hampshire", nh_environment="NO CAT") == (None, None, ["cats"])
    assert _pets("new_hampshire", nh_environment="NO PETS, NO DOG") == (False, None, ["dogs"])
    assert _pets("new_hampshire", nh_environment="SMOKE FREE") == (None, None, None)


def test_pets_new_hampshire_no_pets_conflict_logged(caplog):
    with caplog.at_level(logging.WARNING):
        assert _pets("new_hampshire", nh_environment="NO PETS, INDOOR PETS") == (None, None, None)
        assert _pets("new_hampshire", nh_environment="NO PETS, DOG") == (None, None, None)
    assert "'no pets' together with pets" in caplog.text


def test_pets_new_hampshire_dog_conflict_logged(caplog):
    with caplog.at_level(logging.WARNING):
        assert _pets("new_hampshire", nh_environment="DOG, NO DOG") == (None, None, None)
        assert _pets("new_hampshire", nh_environment="DOG, NO DOG, CAT") == (True, ["cats"], None)
    assert "dogs both present and excluded" in caplog.text


def test_pets_other_states_untouched():
    assert _pets("texas", ga_environment="No pets") == (None, None, None)


# --------------------------------------------------------------------------- #
# Age range in months
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("0 Months", 0),
        ("6 Weeks", 1),
        ("4 Weeks", 0),
        ("1 Week", 0),
        ("12 Years", 144),
        ("1 Year", 12),
        ("18 Months", 18),
        ("2 Years 0 Months", 24),
        ("0 Years 1 Month", 1),
        ("13 Years 0 Months", 156),
        ("6W", 1),
        ("15M", 15),
        ("12Y", 144),
        ("0", 0),
        ("624 Weeks", 144),
        (36, 36),
        (None, None),
        ("", None),
        (True, None),
    ],
)
def test_age_to_months(raw, expected):
    assert cf.age_to_months(raw) == expected


@pytest.mark.parametrize("raw", ["garbage", "5", "3 fortnights", "Years"])
def test_age_to_months_garbage_logged(raw, caplog):
    with caplog.at_level(logging.WARNING):
        assert cf.age_to_months(raw) is None
    assert "could not parse age" in caplog.text


def test_weeks_to_months_floor():
    assert [cf.weeks_to_months(w) for w in (6, 4, 156, 624)] == [1, 0, 36, 144]


def _age_range(state, **record):
    item = norm.normalize_item(dict(record), state)
    return item.get("age_min_months"), item.get("age_max_months")


def test_age_range_connecticut():
    item = norm.normalize_item(
        {"ct_age_range_min_weeks": "6", "ct_age_range_max_weeks": "624", "ages_served": "x"}, "connecticut"
    )
    assert (item["age_min_months"], item["age_max_months"]) == (1, 144)
    assert item["ct_age_range_min_weeks"] == "6"
    assert item["ages_served"] == "x"
    assert _age_range("connecticut", ct_age_range_min_weeks="0", ct_age_range_max_weeks=None) == (0, None)
    assert _age_range("connecticut", ct_age_range_min_weeks=None, ct_age_range_max_weeks="") == (None, None)


def test_age_range_north_dakota():
    assert _age_range("north_dakota", nd_min_age="6 Weeks", nd_max_age="12 Years") == (1, 144)
    assert _age_range("north_dakota", nd_min_age="0 Months", nd_max_age="18 Months") == (0, 18)
    assert _age_range("north_dakota", nd_min_age="3 Years", nd_max_age=None) == (36, None)


def test_age_range_west_virginia():
    assert _age_range("west_virginia", wv_age_from="2 Years 0 Months", wv_age_to="13 Years 0 Months") == (24, 156)
    assert _age_range("west_virginia", wv_age_from="0 Years 1 Month", wv_age_to=None) == (1, None)


def test_age_range_illinois_day_only():
    assert _age_range("illinois", il_day_age_range="6W TO 12Y") == (1, 144)
    assert _age_range("illinois", il_day_age_range="0 TO 12Y") == (0, 144)
    assert _age_range("illinois", il_day_age_range="15m to 6y") == (15, 72)


def test_age_range_illinois_widest_of_day_and_night():
    item = norm.normalize_item({"il_day_age_range": "2M TO 6Y", "il_night_age_range": "6W TO 12Y"}, "illinois")
    assert (item["age_min_months"], item["age_max_months"]) == (1, 144)
    assert item["il_night_age_range"] == "6W TO 12Y"
    assert _age_range("illinois", il_day_age_range="15M TO 6Y", il_night_age_range="2Y TO 12Y") == (15, 144)


def test_age_range_illinois_bare_zero_and_blank_ignored():
    assert _age_range("illinois", il_day_age_range="0") == (None, None)
    assert _age_range("illinois", il_day_age_range="0", il_night_age_range="2M TO 12Y") == (2, 144)
    assert _age_range("illinois", il_day_age_range="6W TO 12Y", il_night_age_range="0") == (1, 144)
    assert _age_range("illinois", il_day_age_range="", il_night_age_range=None) == (None, None)


def test_age_range_wyoming_min_max_with_none_entries():
    programs = [
        {"min_age_months": 6, "max_age_months": 24},
        {"min_age_months": None, "max_age_months": None},
        {"min_age_months": 0, "max_age_months": 144},
        {"min_age_months": 30, "max_age_months": None},
    ]
    assert _age_range("wyoming", wy_programs=programs) == (0, 144)
    assert _age_range("wyoming", wy_programs=[{"min_age_months": None, "max_age_months": None}]) == (None, None)
    assert _age_range("wyoming", wy_programs=[]) == (None, None)
    assert _age_range("wyoming", wy_programs=[{"min_age_months": 12, "max_age_months": None}]) == (12, None)


def test_age_range_max_zero_dropped_keeps_min(caplog):
    with caplog.at_level(logging.WARNING):
        assert _age_range("north_dakota", nd_min_age="6 Weeks", nd_max_age="0 Years") == (1, None)
    assert "implausible max" in caplog.text


def test_age_range_max_below_min_dropped_keeps_min(caplog):
    with caplog.at_level(logging.WARNING):
        assert _age_range("west_virginia", wv_age_from="5 Years 0 Months", wv_age_to="2 Years 0 Months") == (60, None)
    assert "implausible max" in caplog.text


def test_age_range_never_overwrites_existing_and_other_states_untouched():
    item = norm.normalize_item({"nd_min_age": "1 Year", "nd_max_age": "5 Years", "age_min_months": 3}, "north_dakota")
    assert (item["age_min_months"], item["age_max_months"]) == (3, 60)
    assert _age_range("texas", nd_min_age="1 Year", nd_max_age="5 Years") == (None, None)


# --------------------------------------------------------------------------- #
# capacity_by_age
# --------------------------------------------------------------------------- #
def _capacity_by_age(state, **record):
    return norm.normalize_item(dict(record), state).get("capacity_by_age")


def test_capacity_by_age_massachusetts_table_order_not_source_order():
    rows = _capacity_by_age(
        "massachusetts", ma_capacity_by_age={"preschool": 60, "toddlerPreschool": 9, "toddler": 18, "multiAge": 4}
    )
    assert rows == [
        {"age_group": "Toddler", "capacity": 18},
        {"age_group": "Toddler/Preschool", "capacity": 9},
        {"age_group": "Preschool", "capacity": 60},
        {"age_group": "Multi-Age", "capacity": 4},
    ]


def test_capacity_by_age_massachusetts_full_label_table():
    keys = ["infantBirth", "infantToddler", "preschoolSA", "kindergarten", "schoolAge"]
    rows = _capacity_by_age("massachusetts", ma_capacity_by_age={key: 1 for key in keys})
    assert [r["age_group"] for r in rows] == [
        "Infant",
        "Infant/Toddler",
        "Preschool/School Age",
        "Kindergarten",
        "School Age",
    ]


def test_capacity_by_age_massachusetts_unknown_key_logged_and_humanized(caplog):
    with caplog.at_level(logging.WARNING):
        rows = _capacity_by_age("massachusetts", ma_capacity_by_age={"schoolAge": 5, "afterSchoolCare": 7})
    assert rows == [{"age_group": "School Age", "capacity": 5}, {"age_group": "After School Care", "capacity": 7}]
    assert "unknown age key 'afterSchoolCare'" in caplog.text


def test_capacity_by_age_rhode_island_with_and_without_classrooms():
    rows = _capacity_by_age(
        "rhode_island",
        ri_age_group_capacity=[
            {"age_group": "Toddler (18 months up to 36 months)", "classrooms": 2, "capacity": 16},
            {"age_group": "Preschool (3 years)", "classrooms": None, "capacity": 20},
        ],
    )
    assert rows == [
        {"age_group": "Toddler (18 months up to 36 months)", "capacity": 16, "classrooms": 2},
        {"age_group": "Preschool (3 years)", "capacity": 20},
    ]


def test_capacity_by_age_connecticut_zero_kept_blank_skipped_full_time_row():
    rows = _capacity_by_age(
        "connecticut",
        ct_capacity_three_and_under="0",
        ct_capacity_school_aged="",
        ct_capacity_full_time="6",
    )
    assert rows == [
        {"age_group": "Three and Under", "capacity": 0},
        {"age_group": "Full Time (any age)", "capacity": 6},
    ]
    assert _capacity_by_age("connecticut", ct_capacity_school_aged="3") == [{"age_group": "School Aged", "capacity": 3}]


def test_capacity_by_age_indiana_label_with_and_without_end_age():
    rows = _capacity_by_age(
        "indiana",
        in_licensed_ages=[
            {"start_age": "Infant", "end_age": "Twelve", "quantity": 12},
            {"start_age": "Infant", "end_age": "35 Months", "quantity": 6},
            {"start_age": "Toddler", "end_age": None, "quantity": 4},
            {"start_age": "Preschool", "end_age": "  ", "quantity": 0},
        ],
    )
    assert rows == [
        {"age_group": "Infant – Twelve", "capacity": 12},
        {"age_group": "Infant – 35 Months", "capacity": 6},
        {"age_group": "Toddler", "capacity": 4},
        {"age_group": "Preschool", "capacity": 0},
    ]


def test_capacity_by_age_unparseable_capacity_dropped_and_logged(caplog):
    with caplog.at_level(logging.WARNING):
        rows = _capacity_by_age(
            "indiana",
            in_licensed_ages=[
                {"start_age": "Infant", "end_age": None, "quantity": "lots"},
                {"start_age": "Toddler", "end_age": None, "quantity": 3},
            ],
        )
    assert rows == [{"age_group": "Toddler", "capacity": 3}]
    assert "could not parse number 'lots'" in caplog.text


def test_capacity_by_age_no_rows_is_none():
    assert _capacity_by_age("massachusetts", ma_capacity_by_age={}) is None
    assert _capacity_by_age("rhode_island", ri_age_group_capacity=[]) is None
    assert _capacity_by_age("connecticut", ct_capacity_full_time="") is None
    assert _capacity_by_age("indiana", in_licensed_ages=[{"start_age": "Infant", "quantity": None}]) is None
    assert _capacity_by_age("massachusetts") is None


def test_capacity_by_age_new_york_not_parsed_and_sources_and_capacity_kept():
    assert _capacity_by_age("new_york", ny_capacity_description="Infant 8, Toddler 12") is None
    item = norm.normalize_item(
        {"ct_capacity_three_and_under": "6", "ct_capacity_school_aged": "3", "capacity": 9}, "connecticut"
    )
    assert item["capacity"] == 9
    assert item["ct_capacity_three_and_under"] == "6"
    assert item["ct_capacity_school_aged"] == "3"


def _license_conditions(state, **record):
    return norm.normalize_item(dict(record), state).get("license_conditions")


def test_license_conditions_minnesota_none_sentinel_is_unset():
    assert _license_conditions("minnesota", mn_restrictions="None") is None
    assert _license_conditions("minnesota", mn_restrictions=None) is None
    assert _license_conditions("minnesota") is None


def test_license_conditions_minnesota_semicolon_split_and_dedupe():
    text = "This location only operates on school release days.; This location only operates on school release days."
    assert _license_conditions("minnesota", mn_restrictions=text) == [
        "This location only operates on school release days."
    ]
    assert _license_conditions("minnesota", mn_restrictions="First limit; ;second limit;FIRST LIMIT") == [
        "First limit",
        "second limit",
    ]


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (
            "No infants in care No transporting of children under age nine.",
            ["No infants in care", "No transporting of children under age nine."],
        ),
        (
            "No transporting of children under age nine. Group E Occupancy Restrictions: "
            "Program is limited to serving no more than 5 children age 2 1/2 years of age or less.",
            [
                "No transporting of children under age nine.",
                "Group E Occupancy Restrictions: Program is limited to serving no more than 5 children "
                "age 2 1/2 years of age or less.",
            ],
        ),
        (
            "No transporting of children under age nine. EXTENSION—CANNOT OPERATE LICENSE UNDER INDEFINITE SUSPENSION",
            [
                "No transporting of children under age nine.",
                "EXTENSION—CANNOT OPERATE LICENSE UNDER INDEFINITE SUSPENSION",
            ],
        ),
        (
            "No infants in care No transporting of children under age nine. Group E Occupancy Restrictions: "
            "Limited to 5 children.",
            [
                "No infants in care",
                "No transporting of children under age nine.",
                "Group E Occupancy Restrictions: Limited to 5 children.",
            ],
        ),
        ("Hours limited. EXTENSION - see notes", ["Hours limited.", "EXTENSION - see notes"]),
        ("Hours limited. EXTENSION – see notes", ["Hours limited.", "EXTENSION – see notes"]),
        ("Day care only TEMPORARY PROVISIONAL LICENSE", ["Day care only", "TEMPORARY PROVISIONAL LICENSE"]),
        ("Day care only; temporary provisional license", ["Day care only", "temporary provisional license"]),
        ("Cap of 6. no infants in care", ["Cap of 6.", "no infants in care"]),
    ],
)
def test_license_conditions_minnesota_phrase_splits(text, expected):
    assert _license_conditions("minnesota", mn_restrictions=text) == expected


def test_license_conditions_minnesota_free_text_not_split():
    text = "There is no one allowed to enter. The license extension was approved, no infants over 12 months."
    assert _license_conditions("minnesota", mn_restrictions=text) == [text]
    # A phrase at the very start is not a split point either.
    assert _license_conditions("minnesota", mn_restrictions="EXTENSION—PENDING") == ["EXTENSION—PENDING"]


def test_license_conditions_north_carolina_prefix_stripped_and_bare_other_dropped():
    assert _license_conditions(
        "north_carolina",
        nc_license_restrictions=["Other - Meets reduced ratios", "other -", "Other -", "Other - Meets reduced ratios"],
    ) == ["Meets reduced ratios"]
    assert _license_conditions("north_carolina", nc_license_restrictions=["Other -"]) is None
    assert _license_conditions("north_carolina", nc_license_restrictions=[]) is None
    assert _license_conditions("north_carolina") is None


def test_license_conditions_missouri_split_keeps_case():
    assert _license_conditions("missouri", mo_limitations="16 CHILDREN UNDER 24 MONTHS; NO SCHOOL AGE CHILDREN;") == [
        "16 CHILDREN UNDER 24 MONTHS",
        "NO SCHOOL AGE CHILDREN",
    ]
    assert _license_conditions("missouri", mo_limitations="  ") is None


def test_license_conditions_delaware_split():
    assert _license_conditions("delaware", de_special_conditions="Variance") == ["Variance"]
    assert _license_conditions("delaware", de_special_conditions="Foster Care;Agreement of Understanding") == [
        "Foster Care",
        "Agreement of Understanding",
    ]
    assert _license_conditions("delaware", de_special_conditions=None) is None


def test_license_conditions_sources_kept():
    item = norm.normalize_item({"mn_restrictions": "None", "mo_limitations": "A;B"}, "minnesota")
    assert item["mn_restrictions"] == "None"
    item = norm.normalize_item({"mo_limitations": "A;B"}, "missouri")
    assert item["mo_limitations"] == "A;B"
    item = norm.normalize_item({"nc_license_restrictions": ["Other - X"]}, "north_carolina")
    assert item["nc_license_restrictions"] == ["Other - X"]


# --------------------------------------------------------------------------- #
# Original license date
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("state", "field", "raw", "expected"),
    [
        ("california", "ca_license_first_date", "6/18/2008", "2008-06-18"),
        ("new_york", "ny_facility_opened_date", "10/17/1991", "1991-10-17"),
        ("minnesota", "mn_original_license_date", "2014-07-01", "2014-07-01"),
        ("vermont", "vt_original_license_date", "2005-03-01", "2005-03-01"),
        ("massachusetts", "ma_original_license_date", "1999-11-05", "1999-11-05"),
        ("rhode_island", "ri_original_license_date", "1997-01-01", "1997-01-01"),
    ],
)
def test_original_license_date_map_states(state, field, raw, expected):
    item = norm.normalize_item({field: raw, "license_begin_date": "3/25/2024"}, state)
    assert item["original_license_date"] == expected
    assert item[field] == raw  # additive: source kept
    assert item["license_begin_date"] == "2024-03-25"  # current term untouched


@pytest.mark.parametrize(
    ("state", "field"),
    [
        ("connecticut", "ct_date_established"),
        ("colorado", "co_license_issue_date"),
    ],
)
def test_original_license_date_excluded_states(state, field):
    item = norm.normalize_item({field: "04/04/2003"}, state)
    assert item.get("original_license_date") is None
    assert item[field] == "04/04/2003"


@pytest.mark.parametrize(
    ("state", "field"),
    [("california", "ca_license_first_date"), ("new_york", "ny_facility_opened_date")],
)
def test_original_license_date_missing_is_none(state, field):
    for raw in (None, "", "N/A"):
        assert norm.normalize_item({field: raw}, state).get("original_license_date") is None


# --------------------------------------------------------------------------- #
# Last renewed date
# --------------------------------------------------------------------------- #
RENEWED_STATES = [
    ("minnesota", "mn_last_renewed_date", "mn_original_license_date"),
    ("rhode_island", "ri_most_recently_renewed", "ri_original_license_date"),
    ("massachusetts", "ma_last_issue_date", "ma_original_license_date"),
]


@pytest.mark.parametrize(("state", "current", "original"), RENEWED_STATES)
def test_last_renewed_date_renewed_is_current_term(state, current, original):
    record = {current: "2025-11-01", original: "2010-03-15", "license_begin_date": "11/01/2025"}
    item = norm.normalize_item(record, state)
    assert item["last_renewed_date"] == "2025-11-01"
    assert item["last_renewed_date"] == item["license_begin_date"]
    assert item[current] == "2025-11-01"  # additive: sources kept
    assert item[original] == "2010-03-15"
    assert item["original_license_date"] == "2010-03-15"  # untouched


@pytest.mark.parametrize(("state", "current", "original"), RENEWED_STATES)
def test_last_renewed_date_raw_mdy_values_compared_as_dates(state, current, original):
    # raw M/D/YYYY values are parsed before comparing
    item = norm.normalize_item({current: "1/5/2026", original: "12/20/2025"}, state)
    assert item["last_renewed_date"] == "2026-01-05"


@pytest.mark.parametrize(("state", "current", "original"), RENEWED_STATES)
def test_last_renewed_date_equal_is_none_first_term(state, current, original):
    item = norm.normalize_item(
        {current: "2024-05-01", original: "2024-05-01", "license_begin_date": "2024-05-01"}, state
    )
    assert item.get("last_renewed_date") is None
    assert item["license_begin_date"] == "2024-05-01"
    assert item["original_license_date"] == "2024-05-01"


@pytest.mark.parametrize(("state", "current", "original"), RENEWED_STATES)
def test_last_renewed_date_current_before_original_is_none_and_warns(state, current, original, caplog):
    with caplog.at_level(logging.WARNING):
        item = norm.normalize_item({current: "2010-01-01", original: "2020-01-01"}, state)
    assert item.get("last_renewed_date") is None
    assert f"{current} 2010-01-01 is before {original} 2020-01-01" in caplog.text
    assert item["original_license_date"] == "2020-01-01"


@pytest.mark.parametrize(("state", "current", "original"), RENEWED_STATES)
def test_last_renewed_date_original_missing_is_none(state, current, original):
    for raw in (None, "", "N/A"):
        item = norm.normalize_item({current: "2025-11-01", original: raw}, state)
        assert item.get("last_renewed_date") is None
    assert norm.normalize_item({current: "2025-11-01"}, state).get("last_renewed_date") is None


@pytest.mark.parametrize(("state", "current", "original"), RENEWED_STATES)
def test_last_renewed_date_current_missing_is_none(state, current, original):
    for raw in (None, "", "N/A"):
        item = norm.normalize_item({current: raw, original: "2010-03-15"}, state)
        assert item.get("last_renewed_date") is None
    assert norm.normalize_item({original: "2010-03-15"}, state).get("last_renewed_date") is None


def test_last_renewed_date_missouri_anniversary_not_mapped():
    item = norm.normalize_item({"mo_license_anniversary_date": "05/01"}, "missouri")
    assert item.get("last_renewed_date") is None
    assert item["mo_license_anniversary_date"] == "05/01"


# --------------------------------------------------------------------------- #
# temporarily_closed
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("state", "field", "raw", "expected"),
    [
        ("maine", "me_temporarily_closed", "Yes", True),
        ("maine", "me_temporarily_closed", "No", False),
        ("indiana", "in_is_temporarily_closed", True, True),
    ],
)
def test_temporarily_closed_collapse(state, field, raw, expected):
    item = norm.normalize_item({field: raw, "status": "active"}, state)
    assert item["temporarily_closed"] is expected
    assert item[field] == raw
    assert item["status"] == "active"


def test_temporarily_closed_indiana_missing_is_none():
    item = norm.normalize_item({"in_temporarily_closed_message": None}, "indiana")
    assert item.get("temporarily_closed") is None


@pytest.mark.parametrize(
    ("state", "field", "raw"),
    [
        ("new_hampshire", "nh_covid_closure", False),
        ("massachusetts", "ma_temporary_status", "Reopened"),
        ("massachusetts", "ma_temporary_status", "Provisionally Reopened"),
    ],
)
def test_temporarily_closed_covid_era_fields_not_mapped(state, field, raw):
    item = norm.normalize_item({field: raw}, state)
    assert item.get("temporarily_closed") is None
    assert item[field] == raw


# --------------------------------------------------------------------------- #
# complaint_count
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("state", "field", "raw", "expected"),
    [
        ("iowa", "ia_complaint_count", "0", 0),
        ("iowa", "ia_complaint_count", "3", 3),
        ("indiana", "in_complaints", [{"issue": "a"}, {"issue": "b"}], 2),
        ("california", "ca_complaint_info", "No Complaints", 0),
        ("california", "ca_complaint_info", "NO COMPLAINTS", 0),
    ],
)
def test_complaint_count_collapse(state, field, raw, expected):
    item = norm.normalize_item({field: raw}, state)
    assert item["complaint_count"] == expected
    assert item[field] == raw


def test_complaint_count_indiana_missing_is_zero():
    item = norm.normalize_item({"in_is_temporarily_closed": None}, "indiana")
    assert item["complaint_count"] == 0


def test_complaint_count_california_date_is_unknown():
    item = norm.normalize_item({"ca_complaint_info": "09/20/2022"}, "california")
    assert item.get("complaint_count") is None
    assert item["ca_complaint_info"] == "09/20/2022"


def test_complaint_count_alabama_substantiated_not_mapped():
    raw = ["Complaint 06-22-2026"]
    item = norm.normalize_item({"al_substantiated_complaints": raw}, "alabama")
    assert item.get("complaint_count") is None
    assert item["al_substantiated_complaints"] == raw


# --------------------------------------------------------------------------- #
# serious_injury_count / fatality_count
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("raw_injuries", "raw_fatalities", "injuries", "fatalities"),
    [("0", "0", 0, 0), ("2", "1", 2, 1), (None, None, None, None)],
)
def test_injury_counts_maryland(raw_injuries, raw_fatalities, injuries, fatalities):
    record = {"md_serious_injuries": raw_injuries, "md_fatalities": raw_fatalities}
    item = norm.normalize_item(dict(record), "maryland")
    assert item.get("serious_injury_count") == injuries
    assert item.get("fatality_count") == fatalities
    assert item["md_serious_injuries"] == raw_injuries
    assert item["md_fatalities"] == raw_fatalities


@pytest.mark.parametrize(
    ("report", "injuries", "fatalities"),
    [
        ("No facility injuries reported. No facility deaths reported.", 0, 0),
        ("2026: 1 facility injury. No facility deaths reported.", 1, 0),
        (
            "2026: 1 facility injury. No facility deaths reported.; "
            "2025: 2 facility injuries. No facility deaths reported.",
            1,
            0,
        ),
        # highest year wins even when it is not listed first
        (
            "2024: 1 facility injury. No facility deaths reported.; "
            "2025: 3 facility injuries. No facility deaths reported.",
            3,
            0,
        ),
        ("2022: 2 facility injuries. No facility deaths reported.", 2, 0),
        ("2025: No facility injuries reported. 1 facility death.", 0, 1),
        ("2025: No facility injuries reported. 2 facility deaths.", 0, 2),
    ],
)
def test_injury_counts_delaware(report, injuries, fatalities):
    item = norm.normalize_item({"de_injuries_report": report}, "delaware")
    assert item["serious_injury_count"] == injuries
    assert item["fatality_count"] == fatalities
    assert item["de_injuries_report"] == report


def test_injury_counts_delaware_unrecognised_logs_and_is_none(caplog):
    with caplog.at_level("WARNING"):
        item = norm.normalize_item({"de_injuries_report": "see facility"}, "delaware")
    assert item.get("serious_injury_count") is None
    assert item.get("fatality_count") is None
    assert item["de_injuries_report"] == "see facility"
    assert "unrecognised de_injuries_report" in caplog.text


def test_injury_counts_louisiana_url_not_mapped():
    url = "https://example.org/injuries"
    item = norm.normalize_item({"la_serious_injuries_url": url}, "louisiana")
    assert item.get("serious_injury_count") is None
    assert item.get("fatality_count") is None
    assert item["la_serious_injuries_url"] == url


# --------------------------------------------------------------------------- #
# license_history
# --------------------------------------------------------------------------- #
def test_license_history_hawaii():
    history = [
        {
            "effectiveDate": "2005-03-01",
            "expirationDate": "2007-03-01",
            "printNumber": 3,
            "licenseType": "R",
            "capacity": 12,
        },
        {
            "effectiveDate": "2002-02-13",
            "expirationDate": "2003-02-13",
            "printNumber": 1,
            "licenseType": "P",
            "capacity": 8,
        },
    ]
    item = norm.normalize_item({"hi_license_history": history}, "hawaii")
    assert item["license_history"] == [
        {"license_type": "Provisional", "effective_date": "2002-02-13", "expiration_date": "2003-02-13", "capacity": 8},
        {"license_type": "Regular", "effective_date": "2005-03-01", "expiration_date": "2007-03-01", "capacity": 12},
    ]
    assert item["hi_license_history"] == history


def test_license_history_hawaii_unknown_code_logged_and_kept(caplog):
    history = [{"effectiveDate": "2002-02-13", "licenseType": "X", "capacity": 5}]
    with caplog.at_level("WARNING"):
        item = norm.normalize_item({"hi_license_history": history}, "hawaii")
    assert item["license_history"] == [{"license_type": "X", "effective_date": "2002-02-13", "capacity": 5}]
    assert "unknown licenseType" in caplog.text


def test_license_history_washington():
    history = [
        {
            "license_id": "PL-65132",
            "license_type": "Non-Expiring",
            "license_status": "Closed",
            "issue_date": "09/01/2015",
            "closure_date": "03/15/2019",
        },
        {
            "license_id": "PL-70001",
            "license_type": "Unknown",
            "license_status": "Open",
            "issue_date": "",
            "closure_date": "",
        },
    ]
    item = norm.normalize_item({"wa_license_history": history}, "washington")
    assert item["license_history"] == [
        {
            "license_id": "PL-65132",
            "license_type": "Non-Expiring",
            "status": "Closed",
            "effective_date": "2015-09-01",
            "expiration_date": "2019-03-15",
        },
        {"license_id": "PL-70001", "status": "Open"},
    ]
    assert item["wa_license_history"] == history


def test_license_history_sorted_chronologically_undated_last():
    history = [
        {"license_id": "B", "issue_date": ""},
        {"license_id": "C", "issue_date": "01/01/2020"},
        {"license_id": "A", "issue_date": "01/01/2010"},
        {"license_id": "D", "issue_date": ""},
    ]
    item = norm.normalize_item({"wa_license_history": history}, "washington")
    assert [row["license_id"] for row in item["license_history"]] == ["A", "C", "B", "D"]


@pytest.mark.parametrize(
    ("state", "field", "value"),
    [
        ("washington", "wa_license_history", []),
        ("washington", "wa_license_history", [{"license_id": "", "license_type": "Unknown", "issue_date": ""}]),
        ("hawaii", "hi_license_history", [{"printNumber": 2, "licenseType": ""}]),
    ],
)
def test_license_history_empty_is_none(state, field, value):
    item = norm.normalize_item({field: value}, state)
    assert item.get("license_history") is None


def test_license_history_north_carolina_excluded():
    history = [{"license_type": "Center", "effective_date": "5/1/2024", "age_range": "0-12"}]
    item = norm.normalize_item({"nc_license_history": history}, "north_carolina")
    assert item.get("license_history") is None
    assert item["nc_license_history"] == history


@pytest.mark.parametrize(("raw", "expected"), [(12, 12), ("12", 12), ("0", None), (0, None), (None, None)])
def test_current_enrollment_north_dakota(raw, expected):
    record = {} if raw is None else {"nd_total_enrollment": raw}
    item = norm.normalize_item(record, "north_dakota")
    assert item.get("current_enrollment") == expected
    if raw is not None:
        assert item["nd_total_enrollment"] == raw


def _dc_enrollment(*values):
    labels = list(cf.DC_AGE_BANDS)
    return [{"age_group": labels[i], "current_enrollment": v, "openings": "2"} for i, v in enumerate(values)]


def test_current_enrollment_dc_sums_numeric_rows():
    rows = _dc_enrollment("3", "No Data Available", "Not Applicable", "4")
    item = norm.normalize_item({"dc_enrollment": rows}, "washington_dc")
    assert item["current_enrollment"] == 7
    assert item["dc_enrollment"] == rows
    assert item["vacancies_infant"] == 2


@pytest.mark.parametrize("values", [("No Data Available", "Not Applicable", None), ()])
def test_current_enrollment_dc_no_numeric_rows_is_none(values):
    item = norm.normalize_item({"dc_enrollment": _dc_enrollment(*values)}, "washington_dc")
    assert item.get("current_enrollment") is None


def test_current_enrollment_dc_numeric_zeros_stay_zero():
    item = norm.normalize_item({"dc_enrollment": _dc_enrollment("0", "0", "No Data Available")}, "washington_dc")
    assert item["current_enrollment"] == 0


def test_program_description_idaho_description_wins():
    record = {"id_program_description": " We teach. ", "id_philosophy_description": "Play-based"}
    item = norm.normalize_item(record, "idaho")
    assert item["program_description"] == "We teach."
    assert item["id_philosophy_description"] == "Play-based"


@pytest.mark.parametrize("description", [None, "", "x", "Please ask provider."])
def test_program_description_idaho_philosophy_fallback(description):
    record = {"id_philosophy_description": "Play-based", "id_philosophy_comment": "Curriculum notes"}
    if description is not None:
        record["id_program_description"] = description
    item = norm.normalize_item(record, "idaho")
    assert item["program_description"] == "Play-based"
    assert item["id_philosophy_comment"] == "Curriculum notes"


def test_program_description_idaho_comment_ignored():
    item = norm.normalize_item({"id_philosophy_comment": "Curriculum notes"}, "idaho")
    assert item.get("program_description") is None


def test_program_description_north_dakota_copy():
    item = norm.normalize_item({"nd_philosophy": " We love kids. "}, "north_dakota")
    assert item["program_description"] == "We love kids."
    assert item["nd_philosophy"].strip() == "We love kids."


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "   ",
        ".",
        "..",
        "-",
        "*",
        "x",
        "xxx",
        "XXXXX",
        "Please ask provider.",
        "please ask provider",
        "Ask provider",
        "Please ask program.",
        "Coming soon",
        "N/A",
    ],
)
def test_program_description_vermont_filler_is_none(raw):
    item = norm.normalize_item({"vt_program_description": raw}, "vermont")
    assert item.get("program_description") is None


@pytest.mark.parametrize("raw", ["Montessori", "Public PreK", "Preschool"])
def test_program_description_short_answers_kept(raw):
    item = norm.normalize_item({"vt_program_description": raw}, "vermont")
    assert item["program_description"] == raw
    assert item["vt_program_description"] == raw


@pytest.mark.parametrize("raw", [True, False])
def test_licensed_connecticut_copy(raw):
    item = norm.normalize_item({"ct_licensed": raw}, "connecticut")
    assert item["licensed"] is raw
    assert item["ct_licensed"] is raw


@pytest.mark.parametrize("raw, expected", [("Licensed", True), ("License Exempt", False)])
def test_licensed_louisiana_vocab(raw, expected):
    item = norm.normalize_item({"la_licensed": raw}, "louisiana")
    assert item["licensed"] is expected
    assert item["la_licensed"] == raw


def test_licensed_louisiana_blank_is_unset():
    item = norm.normalize_item({"la_licensed": ""}, "louisiana")
    assert item.get("licensed") is None


def test_licensed_new_hampshire_yes_and_absent():
    item = norm.normalize_item({"nh_licensed": "Yes"}, "new_hampshire")
    assert item["licensed"] is True
    assert item["nh_licensed"] == "Yes"
    assert norm.normalize_item({}, "new_hampshire").get("licensed") is None


@pytest.mark.parametrize("raw, expected", [("License", True), ("Approval", False), ("Exempt", False)])
def test_licensed_south_carolina_vocab(raw, expected):
    item = norm.normalize_item({"sc_license_category": raw}, "south_carolina")
    assert item["licensed"] is expected
    assert item["sc_license_category"] == raw


def test_licensed_south_carolina_unknown_logged(caplog):
    with caplog.at_level(logging.WARNING):
        item = norm.normalize_item({"sc_license_category": "Mystery"}, "south_carolina")
    assert item.get("licensed") is None
    assert "unmapped value 'Mystery'" in caplog.text


def test_licensed_exempt_category_fallback_false():
    item = norm.normalize_item({"provider_type": "Self-Declared Provider"}, "north_dakota")
    assert item["facility_category"] == "exempt"
    assert item["licensed"] is False


def test_licensed_fallback_does_not_override_source_true():
    record = {"provider_type": "Exempt Only", "ct_licensed": True}
    item = norm.normalize_item(record, "connecticut")
    assert item["facility_category"] == "exempt"
    assert item["licensed"] is True


def test_licensed_non_exempt_category_stays_unset():
    item = norm.normalize_item({"provider_type": "Child Care Center"}, "north_dakota")
    assert item["facility_category"] != "exempt"
    assert item.get("licensed") is None


@pytest.mark.parametrize("raw, expected", [("house", "residence"), ("building", "non_residential")])
def test_building_type_iowa_vocab(raw, expected):
    item = norm.normalize_item({"ia_business_type": raw}, "iowa")
    assert item["building_type"] == expected
    assert item["ia_business_type"] == raw


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Non-Residential", "non_residential"),
        ("House", "residence"),
        ("Workplace-Based", "workplace"),
        ("Mobile Home", "residence"),
        ("Apartment", "residence"),
    ],
)
def test_building_type_vermont_vocab(raw, expected):
    item = norm.normalize_item({"vt_building_type": raw}, "vermont")
    assert item["building_type"] == expected
    assert item["vt_building_type"] == raw


def test_building_type_unknown_logged(caplog):
    with caplog.at_level(logging.WARNING):
        item = norm.normalize_item({"vt_building_type": "Yurt"}, "vermont")
    assert item.get("building_type") is None
    assert "unmapped value 'Yurt'" in caplog.text


def test_building_type_minnesota_not_populated():
    item = norm.normalize_item({"mn_setting": "House"}, "minnesota")
    assert item.get("building_type") is None
    assert item["mn_setting"] == "House"


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Nonprofit", "nonprofit"),
        ("Profit", "for_profit"),
        ("Private", "for_profit"),
        ("Publicly Operated", "public"),
    ],
)
def test_profit_status_delaware_vocab(raw, expected):
    item = norm.normalize_item({"de_profit_status": raw}, "delaware")
    assert item["profit_status"] == expected
    assert item["de_profit_status"] == raw


def test_profit_status_unknown_logged(caplog):
    with caplog.at_level(logging.WARNING):
        item = norm.normalize_item({"de_profit_status": "Cooperative"}, "delaware")
    assert item.get("profit_status") is None
    assert "unmapped value 'Cooperative'" in caplog.text


def test_profit_status_georgia_not_populated():
    item = norm.normalize_item({"ga_profit_status": None}, "georgia")
    assert item.get("profit_status") is None
    item = norm.normalize_item({"ga_profit_status": "Nonprofit"}, "georgia")
    assert item.get("profit_status") is None


# --------------------------------------------------------------------------- #
# accreditation: negatives dropped, ID/NH added, KY left out
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("negative", ["NA", "na", "N/A", "None", "No", "NOT Nationally Accredited", "Not Accredited"])
def test_accreditation_maryland_negative_dropped(negative):
    item = norm.normalize_item({"md_accreditation": negative}, "maryland")
    assert item.get("accreditation") is None
    assert item["md_accreditation"] == negative


def test_accreditation_maryland_real_value_kept():
    item = norm.normalize_item({"md_accreditation": "NAEYC"}, "maryland")
    assert item["accreditation"] == "NAEYC"
    assert item["md_accreditation"] == "NAEYC"


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("NOT Nationally Accredited, NOT Nationally Accredited", None),
        ("NOT Nationally Accredited", None),
        ("NAEYC, Other, NAEYC, Other", ["NAEYC", "Other"]),
        ("Other, NOT Nationally Accredited", ["Other"]),
        ("NOT Nationally Accredited, Other", ["Other"]),
        ("Other", ["Other"]),
        ("NAEYC,NECPA", ["NAEYC", "NECPA"]),
    ],
)
def test_accreditation_idaho_split_dedupe_and_drop_negatives(raw, expected):
    item = norm.normalize_item({"id_national_accreditation": raw}, "idaho")
    assert item.get("accreditation") == expected
    assert item["id_national_accreditation"] == raw


def test_accreditation_new_hampshire():
    assert norm.normalize_item({"nh_accreditation": "No"}, "new hampshire").get("accreditation") is None
    item = norm.normalize_item({"nh_accreditation": "NAEYC"}, "new hampshire")
    assert item["accreditation"] == "NAEYC"
    assert item["nh_accreditation"] == "NAEYC"


def test_accreditation_kentucky_bare_bool_not_mapped():
    for value in (True, False):
        item = norm.normalize_item({"ky_accreditation_available": value}, "kentucky")
        assert item.get("accreditation") is None
        assert item["ky_accreditation_available"] is value


def test_accreditation_connecticut_list_drops_negative_entries():
    item = norm.normalize_item({"ct_accreditations": ["NAEYC", "Not Accredited"]}, "connecticut")
    assert item["accreditation"] == ["NAEYC"]
    assert item["ct_accreditations"] == ["NAEYC", "Not Accredited"]
    item = norm.normalize_item({"ct_accreditations": ["None"]}, "connecticut")
    assert item.get("accreditation") is None


def test_accreditation_existing_shapes_unchanged():
    item = norm.normalize_item({"ga_accreditation": "SACS|GAC"}, "georgia")
    assert item["accreditation"] == "SACS|GAC"
    item = norm.normalize_item({"ct_accreditations": ["Head Start", "NAEYC"]}, "connecticut")
    assert item["accreditation"] == ["Head Start", "NAEYC"]
    hawaii = [{"name": "NECPA", "effective_date": "2025-11-30", "expiration_date": "2028-11-30"}]
    item = norm.normalize_item({"hi_accreditations": list(hawaii)}, "hawaii")
    assert item["accreditation"] == hawaii
    assert item["hi_accreditations"] == hawaii


def test_accreditation_value_trims_before_matching():
    assert cf.accreditation_value("  n/a ") is None
    assert cf.accreditation_value(" NAEYC ") == "NAEYC"


# --------------------------------------------------------------------------- #
# licensing_period_start / licensing_period_end (HI, ME)
# --------------------------------------------------------------------------- #
_PERIOD_SOURCES = {
    "hawaii": ("hi_licensing_period_start", "hi_licensing_period_end"),
    "maine": ("me_licensed_from", "me_licensed_to"),
}


def _period(state, start, end, **extra):
    start_key, end_key = _PERIOD_SOURCES[state]
    return {start_key: start, end_key: end, **extra}


def test_licensing_period_hawaii_copied():
    raw = _period("hawaii", "2024-12-01", "2026-11-30")
    insp = norm.normalize_inspection(raw, "hawaii")
    assert insp["licensing_period_start"] == "2024-12-01"
    assert insp["licensing_period_end"] == "2026-11-30"


def test_licensing_period_maine_converted():
    insp = norm.normalize_inspection(_period("maine", "09/09/2024", "09/08/2026"), "maine")
    assert insp["licensing_period_start"] == "2024-09-09"
    assert insp["licensing_period_end"] == "2026-09-08"


@pytest.mark.parametrize("state", ["hawaii", "maine"])
def test_licensing_period_inspection_before_period_still_populated(state):
    raw = _period(state, "2024-12-01", "2026-11-30", date="2024-10-15")
    insp = norm.normalize_inspection(raw, state)
    assert insp["licensing_period_start"] == "2024-12-01"
    assert insp["licensing_period_end"] == "2026-11-30"


@pytest.mark.parametrize("state", ["hawaii", "maine"])
def test_licensing_period_end_before_start_both_unset(state):
    insp = norm.normalize_inspection(_period(state, "2024-12-01", "2024-11-30"), state)
    assert "licensing_period_start" not in insp
    assert "licensing_period_end" not in insp


@pytest.mark.parametrize("state", ["hawaii", "maine"])
def test_licensing_period_end_equal_start_kept(state):
    insp = norm.normalize_inspection(_period(state, "2024-12-01", "2024-12-01"), state)
    assert insp["licensing_period_start"] == insp["licensing_period_end"] == "2024-12-01"


@pytest.mark.parametrize("state", ["hawaii", "maine"])
@pytest.mark.parametrize("empty", [None, "", "   ", "N/A"])
def test_licensing_period_empty_values_unset(state, empty):
    insp = norm.normalize_inspection(_period(state, empty, empty), state)
    assert not insp.get("licensing_period_start")
    assert not insp.get("licensing_period_end")


@pytest.mark.parametrize("state", ["hawaii", "maine"])
def test_licensing_period_one_side_missing_fills_the_other(state):
    insp = norm.normalize_inspection(_period(state, "2024-12-01", None), state)
    assert insp["licensing_period_start"] == "2024-12-01"
    assert not insp.get("licensing_period_end")


@pytest.mark.parametrize("state", ["hawaii", "maine"])
def test_licensing_period_unparseable_kept_as_emitted(state):
    insp = norm.normalize_inspection(_period(state, "someday", "2026-11-30"), state)
    assert insp["licensing_period_start"] == "someday"
    assert insp["licensing_period_end"] == "2026-11-30"


@pytest.mark.parametrize("state", ["hawaii", "maine"])
def test_licensing_period_does_not_overwrite_existing(state):
    raw = _period(
        state, "2024-12-01", "2026-11-30", licensing_period_start="2020-01-02", licensing_period_end="2022-01-02"
    )
    insp = norm.normalize_inspection(raw, state)
    assert insp["licensing_period_start"] == "2020-01-02"
    assert insp["licensing_period_end"] == "2022-01-02"


@pytest.mark.parametrize("state", ["hawaii", "maine"])
def test_licensing_period_source_fields_kept(state):
    start_key, end_key = _PERIOD_SOURCES[state]
    raw = _period(state, "2024-12-01", "2026-11-30")
    insp = norm.normalize_inspection(raw, state)
    assert insp[start_key] == "2024-12-01"
    assert insp[end_key] == "2026-11-30"


def test_licensing_period_not_applied_to_other_states():
    insp = norm.normalize_inspection(_period("maine", "09/09/2024", "09/08/2026"), "ohio")
    assert "licensing_period_start" not in insp


# --------------------------------------------------------------------------- #
# visit_reason (MA, OK, KS)
# --------------------------------------------------------------------------- #
_VISIT_REASON_SOURCES = [
    ("massachusetts", "ma_visit_reason"),
    ("oklahoma", "ok_visit_purpose"),
    ("kansas", "ks_survey_reason"),
]


@pytest.mark.parametrize(("state", "source"), _VISIT_REASON_SOURCES)
def test_visit_reason_copied_trimmed(state, source):
    insp = norm.normalize_inspection({source: "  Renewal - Full Review ", "type": "Monitoring Visit"}, state)
    assert insp["visit_reason"] == "Renewal - Full Review"
    assert insp[source] == "Renewal - Full Review"  # source kept (whitespace-cleaned)
    assert insp["type"] == "Monitoring Visit"  # type unchanged


@pytest.mark.parametrize(
    ("state", "type_value"),
    [("massachusetts", "Investigation"), ("oklahoma", "Complaint")],
)
def test_visit_reason_falls_back_to_type_for_ma_ok(state, type_value):
    insp = norm.normalize_inspection({"type": type_value}, state)
    assert insp["visit_reason"] == type_value
    assert insp["type"] == type_value


@pytest.mark.parametrize(("state", "source"), _VISIT_REASON_SOURCES[:2])
@pytest.mark.parametrize("empty", [None, "", "   ", "N/A"])
def test_visit_reason_blank_reason_falls_back_to_type(state, source, empty):
    insp = norm.normalize_inspection({source: empty, "type": "Complaint"}, state)
    assert insp["visit_reason"] == "Complaint"


@pytest.mark.parametrize(("state", "source"), _VISIT_REASON_SOURCES)
def test_visit_reason_prefers_reason_over_type(state, source):
    insp = norm.normalize_inspection({source: "Periodic", "type": "Complaint"}, state)
    assert insp["visit_reason"] == "Periodic"


def test_visit_reason_kansas_has_no_type_fallback():
    insp = norm.normalize_inspection({"type": "Administrative Order"}, "kansas")
    assert "visit_reason" not in insp
    insp = norm.normalize_inspection({"ks_survey_reason": "", "type": "Complaint Survey"}, "kansas")
    assert "visit_reason" not in insp


@pytest.mark.parametrize("state", ["ohio", "virginia", "texas"])
def test_visit_reason_type_fallback_never_fires_for_other_states(state):
    insp = norm.normalize_inspection({"type": "Complaint"}, state)
    assert "visit_reason" not in insp


@pytest.mark.parametrize("empty", [None, "", "   ", "N/A"])
@pytest.mark.parametrize(("state", "source"), _VISIT_REASON_SOURCES)
def test_visit_reason_sentinels_and_blanks_unset(state, source, empty):
    insp = norm.normalize_inspection({source: empty, "type": empty}, state)
    assert not insp.get("visit_reason")


@pytest.mark.parametrize(("state", "source"), _VISIT_REASON_SOURCES)
def test_visit_reason_does_not_overwrite_existing(state, source):
    insp = norm.normalize_inspection({source: "Periodic", "type": "Complaint", "visit_reason": "Preset"}, state)
    assert insp["visit_reason"] == "Preset"


# --------------------------------------------------------------------------- #
# finding_text (MD, IN, WI, AZ, OK, DE)
# --------------------------------------------------------------------------- #
_FINDING_TEXT_SOURCES = [
    ("maryland", "md_finding"),
    ("indiana", "in_noncompliance"),
    ("wisconsin", "wi_description"),
    ("arizona", "az_decision_correction"),
    ("delaware", "de_investigation_conclusion"),
]
_NARRATIVE = "Findings: Menu lacking components; Staff Sam was not CPR certified."


@pytest.mark.parametrize(("state", "source"), _FINDING_TEXT_SOURCES)
def test_finding_text_copied_trimmed_and_verbatim(state, source):
    insp = norm.normalize_inspection({source: f"  {_NARRATIVE} "}, state)
    assert insp["finding_text"] == _NARRATIVE
    assert insp[source] == _NARRATIVE  # source kept


@pytest.mark.parametrize(("state", "source"), _FINDING_TEXT_SOURCES)
@pytest.mark.parametrize("empty", [None, "", "   ", "N/A", "None"])
def test_finding_text_blanks_and_sentinels_unset(state, source, empty):
    insp = norm.normalize_inspection({source: empty}, state)
    assert "finding_text" not in insp


@pytest.mark.parametrize(("state", "source"), _FINDING_TEXT_SOURCES)
def test_finding_text_does_not_overwrite_existing(state, source):
    insp = norm.normalize_inspection({source: _NARRATIVE, "finding_text": "Preset"}, state)
    assert insp["finding_text"] == "Preset"


def test_finding_text_keeps_case():
    insp = norm.normalize_inspection({"wi_description": "CHILD LEFT UNSUPERVISED"}, "wisconsin")
    assert insp["finding_text"] == "CHILD LEFT UNSUPERVISED"


@pytest.mark.parametrize(
    "sentinel",
    ["No Noncompliances Found", "  no noncompliances found  ", "No Noncompliances Found."],
)
def test_finding_text_md_sentinel_unset_but_source_kept(sentinel):
    insp = norm.normalize_inspection({"md_finding": sentinel}, "maryland")
    assert "finding_text" not in insp
    assert insp["md_finding"] == sentinel.strip()


def test_finding_text_md_other_text_kept():
    text = "Specialist observed no evidence of a posted emergency plan."
    insp = norm.normalize_inspection({"md_finding": text}, "maryland")
    assert insp["finding_text"] == text


def test_finding_text_in_copies_rule_text_as_published():
    rule = "Children must be supervised at all times."
    insp = norm.normalize_inspection({"in_noncompliance": rule, "in_rule_description": rule}, "indiana")
    assert insp["finding_text"] == rule
    assert insp["in_rule_description"] == rule


@pytest.mark.parametrize(
    "placeholder",
    [
        "MONITOR VISIT COMPLETED; NO ISSUES",
        "  MONITOR VISIT COMPLETED; NO ISSUES ",
        "Monitor Visit Completed; No Issues",
    ],
)
def test_finding_text_az_placeholder_unset_but_source_kept(placeholder):
    insp = norm.normalize_inspection({"az_decision_correction": placeholder}, "arizona")
    assert "finding_text" not in insp
    assert insp["az_decision_correction"] == placeholder.strip()


@pytest.mark.parametrize(
    "text",
    [
        "MONITOR VISIT COMPLETED; NO ISSUES FOUND",
        "MONITOR VISIT COMPLETED; NO ISSUES. Follow-up planned",
        "Monitor visit completed; no issues with staffing, but menu lacking",
    ],
)
def test_finding_text_az_near_miss_kept(text):
    insp = norm.normalize_inspection({"az_decision_correction": text}, "arizona")
    assert insp["finding_text"] == text


def test_finding_text_ok_joins_with_blank_line():
    raw = {"ok_noncompliances": ["First issue.", "Second issue."]}
    insp = norm.normalize_inspection(raw, "oklahoma")
    assert insp["finding_text"] == "First issue.\n\nSecond issue."
    assert insp["ok_noncompliances"] == ["First issue.", "Second issue."]  # source kept


def test_finding_text_ok_single_entry():
    insp = norm.normalize_inspection({"ok_noncompliances": ["Only one."]}, "oklahoma")
    assert insp["finding_text"] == "Only one."


@pytest.mark.parametrize("value", [[], ["", "   "], [None, "N/A"], None])
def test_finding_text_ok_empty_or_blank_list_unset(value):
    insp = norm.normalize_inspection({"ok_noncompliances": value}, "oklahoma")
    assert "finding_text" not in insp


def test_finding_text_ok_drops_blank_entries():
    insp = norm.normalize_inspection({"ok_noncompliances": ["A.", "  ", "B."]}, "oklahoma")
    assert insp["finding_text"] == "A.\n\nB."


def test_finding_text_unset_without_a_source_field():
    insp = norm.normalize_inspection({"type": "Complaint", "notes": "x"}, "ohio")
    assert "finding_text" not in insp


# --------------------------------------------------------------------------- #
# regulation_description (IN, WI, WV)
# --------------------------------------------------------------------------- #
_REGULATION_DESCRIPTION_SOURCES = [
    ("indiana", "in_rule_description"),
    ("wisconsin", "wi_rule_summary"),
    ("west_virginia", "wv_non_compliance_code"),
]
_RULE_TEXT = "Supervision Of Children"


@pytest.mark.parametrize(("state", "source"), _REGULATION_DESCRIPTION_SOURCES)
def test_regulation_description_copied_trimmed_and_verbatim(state, source):
    insp = norm.normalize_inspection({source: f"  {_RULE_TEXT} "}, state)
    assert insp["regulation_description"] == _RULE_TEXT
    assert insp[source] == _RULE_TEXT  # source kept


@pytest.mark.parametrize(("state", "source"), _REGULATION_DESCRIPTION_SOURCES)
@pytest.mark.parametrize("empty", [None, "", "   ", "N/A"])
def test_regulation_description_blanks_and_sentinels_unset(state, source, empty):
    insp = norm.normalize_inspection({source: empty}, state)
    assert "regulation_description" not in insp


@pytest.mark.parametrize(("state", "source"), _REGULATION_DESCRIPTION_SOURCES)
def test_regulation_description_does_not_overwrite_existing(state, source):
    insp = norm.normalize_inspection({source: _RULE_TEXT, "regulation_description": "Preset"}, state)
    assert insp["regulation_description"] == "Preset"


def test_regulation_description_keeps_case_and_long_text():
    text = "A CHILD CARE CENTER SHALL ENSURE THAT children are supervised at all times."
    insp = norm.normalize_inspection({"wv_non_compliance_code": text}, "west_virginia")
    assert insp["regulation_description"] == text


# --------------------------------------------------------------------------- #
# violations (CT, DE, MA, NH, MO, WY, NC, SC, OK)
# --------------------------------------------------------------------------- #
_VIOLATION_CASES = [
    (
        "connecticut",
        "ct_violations",
        {"regulation": " [19a-87b-10(b)(2)] ", "statute": " Health record required ", "category": " Child Health "},
        {
            "regulation": "19a-87b-10(b)(2)",
            "regulation_description": "Health record required",
            "category": "Child Health",
        },
    ),
    (
        "delaware",
        "de_violations",
        {
            "regulation_code": "R-1",
            "description": "Ratios",
            "corrective_action": "ignored",
            "correction_status": "Corrected",
            "corrected_date": "2024-05-01",
        },
        {
            "regulation": "R-1",
            "regulation_description": "Ratios",
            "status": "Corrected",
            "corrected_date": "2024-05-01",
        },
    ),
    (
        "massachusetts",
        "ma_violations",
        {"regulation": "7.10(05)(j)", "result": "Non Compliant", "statement": " Supervision lapsed "},
        {"regulation": "7.10(05)(j)", "finding_text": "Supervision lapsed"},
    ),
    (
        "missouri",
        "mo_violations",
        {"rule": "19 CSR 30-61.105", "description": "Sanitation"},
        {"regulation": "19 CSR 30-61.105", "regulation_description": "Sanitation"},
    ),
    (
        "wyoming",
        "wy_violations",
        {"regulation": "Rule 5 Section 2 - Staff ratio", "compliance_achieved_date": "3/4/2024"},
        {"regulation_description": "Rule 5 Section 2 - Staff ratio", "corrected_date": "2024-03-04"},
    ),
    (
        "south_carolina",
        "sc_deficiencies",
        {"severity": "High", "deficiency_type": "Staff Records", "resolved": "Yes", "date_resolved": "8/28/2024"},
        {"category": "Staff Records", "severity": "High", "status": "Yes", "corrected_date": "2024-08-28"},
    ),
    (
        "oklahoma",
        "ok_allegations",
        {
            "requirement": "110:30-3-1",
            "requirement_description": "Supervision",
            "noncompliance_observed": "Staff left room",
            "plan_to_correct": "ignored",
            "findings": "Substantiated",
        },
        {
            "regulation": "110:30-3-1",
            "regulation_description": "Supervision",
            "finding_text": "Staff left room",
            "status": "Substantiated",
        },
    ),
]


@pytest.mark.parametrize(("state", "source", "raw", "expected"), _VIOLATION_CASES)
def test_violations_mapping_per_state(state, source, raw, expected):
    insp = norm.normalize_inspection({source: [raw]}, state)
    assert insp["violations"] == [expected]
    assert insp[source] == [raw]  # source kept


def test_violations_nh_multiple_citations_joined():
    raw = {
        "domain": "Health",
        "item": "ignored",
        "result": "Non-Compliant",
        "regulations": [
            {"citation": " 7.1(a) ", "text": "First rule text"},
            {"citation": None, "text": ""},
            {"citation": "7.2(b)", "text": None},
            {"citation": "7.3", "text": "Third rule text"},
        ],
        "observations": " Seen at visit ",
    }
    insp = norm.normalize_inspection({"nh_violations": [raw]}, "new_hampshire")
    assert insp["violations"] == [
        {
            "regulation": "7.1(a); 7.2(b); 7.3",
            "regulation_description": "First rule text\n\nThird rule text",
            "finding_text": "Seen at visit",
            "category": "Health",
            "status": "Non-Compliant",
        }
    ]


def test_violations_nh_one_row_per_violation_without_regulations():
    insp = norm.normalize_inspection(
        {"nh_violations": [{"domain": "A", "observations": "x", "regulations": []}, {"domain": "B"}]},
        "new_hampshire",
    )
    assert insp["violations"] == [{"category": "A", "finding_text": "x"}, {"category": "B"}]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Rule Violated: Children were unsupervised.", "Children were unsupervised."),
        ("  rule violated:   lowercase prefix ", "lowercase prefix"),
        ("Staff were not trained.", "Staff were not trained."),
    ],
)
def test_violations_nc_prefix_stripped_only_when_present(raw, expected):
    insp = norm.normalize_inspection({"nc_violations": [raw]}, "north_carolina")
    assert insp["violations"] == [{"finding_text": expected}]


def test_violations_nc_bare_prefix_dropped():
    insp = norm.normalize_inspection({"nc_violations": ["Rule Violated:"]}, "north_carolina")
    assert "violations" not in insp


def test_violations_ct_only_surrounding_brackets_stripped():
    rows = [{"regulation": "[A(1)]"}, {"regulation": "B[2]"}, {"regulation": "[unclosed"}, {"regulation": "[ ]"}]
    insp = norm.normalize_inspection({"ct_violations": rows}, "connecticut")
    assert insp["violations"] == [{"regulation": "A(1)"}, {"regulation": "B[2]"}, {"regulation": "[unclosed"}]


def test_violations_sc_na_severity_unset():
    rows = [{"severity": "N/A", "deficiency_type": "Health", "resolved": "N/A"}]
    insp = norm.normalize_inspection({"sc_deficiencies": rows}, "south_carolina")
    assert insp["violations"] == [{"category": "Health"}]


def test_violations_unparseable_date_kept_as_emitted(caplog):
    with caplog.at_level(logging.WARNING):
        insp = norm.normalize_inspection({"wy_violations": [{"compliance_achieved_date": "soonish"}]}, "wyoming")
    assert insp["violations"] == [{"corrected_date": "soonish"}]


def test_violations_empty_rows_dropped_and_order_kept():
    rows = [{"rule": "B"}, {"rule": "N/A", "description": "  "}, {}, {"rule": "A"}]
    insp = norm.normalize_inspection({"mo_violations": rows}, "missouri")
    assert insp["violations"] == [{"regulation": "B"}, {"regulation": "A"}]


@pytest.mark.parametrize(
    ("state", "source"), [(c[0], c[1]) for c in _VIOLATION_CASES] + [("north_carolina", "nc_violations")]
)
@pytest.mark.parametrize("raw", [None, [], [{}], [None, 5, "x", []], "text", 7, {"a": 1}])
def test_violations_unset_when_nothing_usable(state, source, raw):
    insp = norm.normalize_inspection({source: raw}, state)
    if state == "north_carolina" and raw == [None, 5, "x", []]:
        assert insp["violations"] == [{"finding_text": "x"}]  # a bare string is NC's valid shape
    else:
        assert "violations" not in insp


def test_violations_garbage_entries_skipped_without_crash():
    rows = [None, "text", 3, ["list"], {"rule": "R1"}]
    insp = norm.normalize_inspection({"mo_violations": rows}, "missouri")
    assert insp["violations"] == [{"regulation": "R1"}]
    insp = norm.normalize_inspection(
        {"nh_violations": [{"regulations": "bad"}, {"regulations": [None, 1]}]}, "new_hampshire"
    )
    assert "violations" not in insp


def test_violations_do_not_overwrite_existing():
    insp = norm.normalize_inspection(
        {"mo_violations": [{"rule": "R1"}], "violations": [{"regulation": "Preset"}]}, "missouri"
    )
    assert insp["violations"] == [{"regulation": "Preset"}]


def test_violations_not_emitted_for_other_states():
    insp = norm.normalize_inspection({"mo_violations": [{"rule": "R1"}]}, "kansas")
    assert "violations" not in insp


# --------------------------------------------------------------------------- #
# enforcement_action (DE, KY, OK)
# --------------------------------------------------------------------------- #
def _enforcement(record, state):
    return norm.normalize_item(dict(record), state).get("enforcement_action")


@pytest.mark.parametrize(
    ("record", "expected"),
    [
        ({"de_enforcement_action": "Suspended"}, "Suspended"),
        ({"de_intent_to_revoke": "Intent to Revoke"}, "Intent to Revoke"),
        (
            {"de_enforcement_action": " Warning of Probation ", "de_intent_to_revoke": "Intent to Revoke"},
            "Warning of Probation",
        ),
        ({"de_enforcement_action": "Revoked"}, "Revoked"),
        ({"de_enforcement_action": "N/A", "de_intent_to_revoke": "Intent to Revoke"}, "Intent to Revoke"),
        ({"de_enforcement_action": "  ", "de_intent_to_revoke": None}, None),
        ({}, None),
    ],
)
def test_enforcement_action_delaware(record, expected):
    assert _enforcement(record, "delaware") == expected


def test_enforcement_action_delaware_revoked_keeps_status_closed():
    item = norm.normalize_item({"status": "Revoked", "de_enforcement_action": "Revoked"}, "delaware")
    assert item["enforcement_action"] == "Revoked"
    assert item["status"] == "closed"


@pytest.mark.parametrize(
    ("processes", "expected"),
    [
        ([{"process_type": "Adverse Action", "status": "On-going"}], "Adverse Action"),
        (
            [
                {"process_type": "Adverse Action", "status": "On-going"},
                {"process_type": "Directed Plan of Correction", "status": "On-going"},
            ],
            "Adverse Action; Directed Plan of Correction",
        ),
        (
            [
                {"process_type": "Adverse Action", "status": "Completed"},
                {"process_type": "Directed Plan of Correction", "status": "On-going"},
            ],
            "Directed Plan of Correction",
        ),
        (
            [
                {"process_type": "Adverse Action", "status": "On-going"},
                {"process_type": " Adverse Action ", "status": "On-going"},
            ],
            "Adverse Action",
        ),
        ([{"process_type": "Adverse Action", "status": "Completed"}], None),
        ([{"process_type": "N/A", "status": "On-going"}, None, "x"], None),
        ([], None),
        (None, None),
    ],
)
def test_enforcement_action_kentucky(processes, expected):
    assert _enforcement({"ky_ongoing_processes": processes}, "kentucky") == expected


def test_enforcement_action_kentucky_bool_without_list_is_unset_and_logged(caplog):
    with caplog.at_level(logging.WARNING):
        assert _enforcement({"ky_ongoing_process": True}, "kentucky") is None
    assert "ky_ongoing_process is True" in caplog.text


@pytest.mark.parametrize(
    ("flags", "expected"),
    [
        ({"ok_revocation_sent": True}, "Revocation sent"),
        ({"ok_denial_sent": True}, "Denial sent"),
        ({"ok_emergency_issued": True}, "Emergency order issued"),
        (
            {"ok_emergency_issued": True, "ok_revocation_sent": True},
            "Revocation sent; Emergency order issued",
        ),
        (
            {"ok_emergency_issued": True, "ok_denial_sent": True, "ok_revocation_sent": True},
            "Revocation sent; Denial sent; Emergency order issued",
        ),
        ({"ok_revocation_sent": False, "ok_denial_sent": False, "ok_emergency_issued": False}, None),
        ({}, None),
    ],
)
def test_enforcement_action_oklahoma(flags, expected):
    assert _enforcement(flags, "oklahoma") == expected


def test_enforcement_action_alabama_never_set():
    record = {"al_adverse_actions": ["Adverse Action CORRECTIVE ACTION 1/1/2020 1/1/2021"]}
    assert _enforcement(record, "alabama") is None


def test_enforcement_action_leaves_status_unchanged():
    base = {"status": "Active", "ky_ongoing_processes": []}
    with_action = {"status": "Active", "ky_ongoing_processes": [{"process_type": "X", "status": "On-going"}]}
    out = norm.normalize_item(dict(with_action), "kentucky")
    assert out["enforcement_action"] == "X"
    assert out["status"] == norm.normalize_item(dict(base), "kentucky")["status"]


def test_enforcement_action_does_not_overwrite_existing_value():
    out = norm.normalize_item({"enforcement_action": "Preset", "de_enforcement_action": "Suspended"}, "delaware")
    assert out["enforcement_action"] == "Preset"


def test_enforcement_action_keeps_source_fields():
    record = {"de_enforcement_action": "Suspended", "de_intent_to_revoke": "Intent"}
    out = norm.normalize_item(dict(record), "delaware")
    assert out["de_enforcement_action"] == "Suspended"
    assert out["de_intent_to_revoke"] == "Intent"


@pytest.mark.parametrize(
    ("state", "record"),
    [
        ("kentucky", {"de_enforcement_action": "Suspended"}),
        ("oklahoma", {"de_enforcement_action": "Suspended"}),
        ("delaware", {"ok_revocation_sent": True}),
        ("delaware", {"ky_ongoing_processes": [{"process_type": "X", "status": "On-going"}]}),
        ("oklahoma", {"ky_ongoing_processes": [{"process_type": "X", "status": "On-going"}]}),
        ("kentucky", {"ok_denial_sent": True}),
    ],
)
def test_enforcement_action_does_not_cross_fire(state, record):
    assert _enforcement(record, state) is None


# --------------------------------------------------------------------------- #
# faith_based
# --------------------------------------------------------------------------- #
def _faith(state, **fields):
    item = norm.normalize_item(dict(fields), state)
    return item.get("faith_based"), item


@pytest.mark.parametrize(("state", "field"), [("florida", "fl_is_faith_based"), ("vermont", "vt_religious_activity")])
@pytest.mark.parametrize(("raw", "expected"), [(True, True), (False, False), ("Yes", True), ("No", False)])
def test_faith_based_fl_vt_copy_true_and_false(state, field, raw, expected):
    value, item = _faith(state, **{field: raw})
    assert value is expected
    assert item[field] == raw  # source kept in place


@pytest.mark.parametrize("raw", ["", None, "  "])
def test_faith_based_vt_blank_is_unset(raw):
    value, _ = _faith("vermont", vt_religious_activity=raw)
    assert value is None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Religious", True),
        ("Montessori Religious", True),
        ("Cognitive Based Philosophy Religious", True),
        ("religious", True),
        (["Montessori", "Religious"], True),
        ("None Reported.", None),
        ("Montessori", None),
        ("Irreligious", None),
        ("Religiously", None),
        ("", None),
        (None, None),
    ],
)
def test_faith_based_wi_whole_word_religious(raw, expected):
    value, item = _faith("wisconsin", wi_program_philosophy=raw)
    assert value is expected  # never False
    assert item["wi_program_philosophy"] == raw or raw in ("", None)


@pytest.mark.parametrize(
    ("state", "faith_type"),
    [("virginia", "Religious Exempt Child Day Center"), ("indiana", "Unlicensed Registered Ministry")],
)
def test_faith_based_va_in_provider_type(state, faith_type):
    value, item = _faith(state, provider_type=faith_type)
    assert value is True
    assert item["provider_type"] == faith_type  # provider_type unchanged
    value, _ = _faith(state, provider_type=f"  {faith_type} ")
    assert value is True
    for other in ("Licensed Child Day Center", "Religious Exempt", faith_type.lower(), "", None):
        value, _ = _faith(state, provider_type=other)
        assert value is None  # true-only, never False


def test_faith_based_provider_type_is_state_scoped():
    for state, faith_type in cf.FAITH_BASED_PROVIDER_TYPES.items():
        other = "indiana" if state == "virginia" else "virginia"
        assert _faith(other, provider_type=faith_type)[0] is None
        assert _faith("ohio", provider_type=faith_type)[0] is None


def test_faith_based_never_overwrites_existing_value():
    value, _ = _faith("virginia", provider_type="Religious Exempt Child Day Center", faith_based=False)
    assert value is False
    value, _ = _faith("florida", fl_is_faith_based=True, faith_based=False)
    assert value is False

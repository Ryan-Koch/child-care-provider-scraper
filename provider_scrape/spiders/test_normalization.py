"""Unit tests for provider_scrape.normalization and NormalizationPipeline.

Run with the project virtualenv: ``.venv/bin/pytest``.

These tests use small inline dict fixtures (not full state files) and a tiny
fake spider, so they are fast and framework-light.
"""

import copy
import logging

from provider_scrape import normalization
from provider_scrape.items import ProviderItem
from provider_scrape.pipelines import NormalizationPipeline


class FakeSettings:
    def __init__(self, values=None):
        self._values = values or {}

    def getbool(self, key, default=False):
        return bool(self._values.get(key, default))


class FakeSpider:
    """Minimal stand-in exposing the attributes the pipeline touches."""

    def __init__(self, name="test_state", settings_values=None):
        self.name = name
        self.settings = FakeSettings(settings_values)
        self.logger = logging.getLogger(f"fake.{name}")


# --------------------------------------------------------------------------- #
# Task 02 — scaffold / pipeline wiring (already-clean data is left unchanged)
# --------------------------------------------------------------------------- #
def test_normalize_item_leaves_clean_data_unchanged():
    item = {"provider_name": "Bright Beginnings", "capacity": 30}
    original = copy.deepcopy(item)
    assert normalization.normalize_item(item, "montana") == original


def test_normalize_inspection_leaves_clean_data_unchanged():
    inspection = {"date": "2024-01-01", "type": "Routine"}
    original = copy.deepcopy(inspection)
    assert normalization.normalize_inspection(inspection, "montana") == original


def test_pipeline_disabled_passes_item_through_untouched():
    spider = FakeSpider(settings_values={"NORMALIZE_ENABLED": False})
    pipeline = NormalizationPipeline()
    pipeline.open_spider(spider)

    item = ProviderItem()
    item["provider_name"] = "  Untouched Name  "
    item["status"] = "LICENSED"
    before = dict(item)

    result = pipeline.process_item(item, spider)

    assert result is item
    assert dict(result) == before


def test_pipeline_enabled_is_noop_at_scaffold_stage():
    # With the scaffold-only helpers, an enabled run still leaves data
    # unchanged (behavior is added incrementally in later tasks).
    spider = FakeSpider(settings_values={"NORMALIZE_ENABLED": True})
    pipeline = NormalizationPipeline()
    pipeline.open_spider(spider)

    item = ProviderItem()
    item["provider_name"] = "Bright Beginnings"
    item["capacity"] = 30
    before = dict(item)

    result = pipeline.process_item(item, spider)
    assert dict(result) == before


# --------------------------------------------------------------------------- #
# Task 03 — whitespace & string hygiene
# --------------------------------------------------------------------------- #
def test_clean_whitespace_strips_and_collapses():
    assert normalization.clean_whitespace("  Foo   Bar  ") == "Foo Bar"


def test_clean_whitespace_handles_nbsp_and_doubles():
    assert normalization.clean_whitespace("Foo\xa0\xa0Bar") == "Foo Bar"


def test_clean_whitespace_empty_becomes_none():
    assert normalization.clean_whitespace("   ") is None
    assert normalization.clean_whitespace("") is None


def test_clean_whitespace_list_drops_empty_elements():
    assert normalization.clean_whitespace(["  Pre-K ", "", "  "]) == ["Pre-K"]


def test_clean_whitespace_leaves_non_strings_alone():
    assert normalization.clean_whitespace(30) == 30
    assert normalization.clean_whitespace(True) is True
    assert normalization.clean_whitespace({"a": 1}) == {"a": 1}


def test_title_case_name_basic_allcaps():
    assert normalization.title_case_name("  A CHILD'S WORLD ".strip()) == "A Child's World"


def test_title_case_name_preserves_llc_and_roman_numerals():
    assert normalization.title_case_name("SMITH FAMILY DAYCARE LLC") == "Smith Family Daycare LLC"
    assert normalization.title_case_name("JOHN SMITH III") == "John Smith III"


def test_title_case_name_leaves_mixed_case_alone():
    assert normalization.title_case_name("Bright Beginnings LLC") == "Bright Beginnings LLC"


def test_title_case_name_handles_hyphenated_token():
    assert normalization.title_case_name("WELL-BEING CENTER") == "Well-Being Center"


def test_normalize_item_cleans_and_titlecases_name_fields():
    item = {
        "provider_name": "  A CHILD'S WORLD  ",
        "license_holder": "LEE, YVONNE ZERITHIA ",
        "license_number": "10003537",  # not a name field -> casing untouched
        "languages": ["  English ", ""],  # list field -> whitespace cleanup
    }
    out = normalization.normalize_item(item, "alabama")
    assert out["provider_name"] == "A Child's World"
    assert out["license_holder"] == "Lee, Yvonne Zerithia"
    assert out["license_number"] == "10003537"
    assert out["languages"] == ["English"]


def test_normalize_inspection_cleans_whitespace():
    inspection = {"type": "  Routine  ", "original_status": "Open\xa0Item"}
    out = normalization.normalize_inspection(inspection, "alabama")
    assert out["type"] == "Routine"
    assert out["original_status"] == "Open Item"


# --------------------------------------------------------------------------- #
# Task 04 — date normalization (ISO 8601)
# --------------------------------------------------------------------------- #
def test_normalize_date_zero_pads_m_d_yyyy():
    assert normalization.normalize_date("3/1/2025") == "2025-03-01"


def test_normalize_date_mm_dd_yyyy():
    assert normalization.normalize_date("06/01/2024") == "2024-06-01"


def test_normalize_date_iso_passthrough():
    assert normalization.normalize_date("2024-10-30") == "2024-10-30"


def test_normalize_date_iso_with_time_drops_time():
    assert normalization.normalize_date("2025-10-01T06:00:00.000Z") == "2025-10-01"


def test_normalize_date_drops_trailing_clock_time():
    # Space-separated timestamps (e.g. Tennessee visit dates) drop the time.
    assert normalization.normalize_date("05/07/2026 08:42 AM") == "2026-05-07"
    assert normalization.normalize_date("04/08/2026 09:45 PM") == "2026-04-08"
    assert normalization.normalize_date("2025-10-01 14:30:00") == "2025-10-01"


def test_normalize_date_full_month_name():
    assert normalization.normalize_date("January 5, 2024") == "2024-01-05"


def test_normalize_date_ap_abbreviated_month():
    # AP-style "Sept." (4 letters, with period) seen in inspection dates.
    assert normalization.normalize_date("Sept. 23, 2025") == "2025-09-23"
    assert normalization.normalize_date("Dec. 10, 2025") == "2025-12-10"


def test_normalize_date_ordinal_day_suffix():
    # Idaho (idahochildcarecheck.org) renders inspection/incident dates with
    # an ordinal suffix on the day -- st/nd/rd/th -- for every day of month.
    assert normalization.normalize_date("May 29th, 2026") == "2026-05-29"
    assert normalization.normalize_date("March 1st, 2026") == "2026-03-01"
    assert normalization.normalize_date("December 22nd, 2025") == "2025-12-22"
    assert normalization.normalize_date("August 3rd, 2025") == "2025-08-03"


def test_normalize_date_garbage_unchanged_and_logged(caplog):
    with caplog.at_level("WARNING"):
        assert normalization.normalize_date("N/A") == "N/A"
        assert normalization.normalize_date("see notes") == "see notes"
    assert "could not parse date" in caplog.text


def test_normalize_date_non_string_unchanged():
    assert normalization.normalize_date(None) is None
    assert normalization.normalize_date(20240101) == 20240101


def test_normalize_item_converts_common_date_fields():
    item = {
        "status_date": "3/1/2025",
        "license_begin_date": "2025-10-01T06:00:00.000Z",
        "license_expiration": "06/30/2026",
    }
    out = normalization.normalize_item(item, "utah")
    assert out["status_date"] == "2025-03-01"
    assert out["license_begin_date"] == "2025-10-01"
    assert out["license_expiration"] == "2026-06-30"


def test_normalize_inspection_converts_date_fields():
    inspection = {"date": "Sept. 23, 2025", "status_updated": "3/1/2025"}
    out = normalization.normalize_inspection(inspection, "west_virginia")
    assert out["date"] == "2025-09-23"
    assert out["status_updated"] == "2025-03-01"


def test_date_fields_is_common_only_not_state_specific():
    """bug_11: DATE_FIELDS is documented as common-(provider-level)-fields
    only. mn_original_license_date / mn_last_renewed_date / vt_original_
    license_date are state-specific, so the spiders normalize them directly
    via normalize_date rather than routing them through this shared list --
    guard against a future change quietly adding them here instead (which
    would be harmless, normalize_date is idempotent on an already-ISO value,
    but would duplicate normalization logic the spiders already do)."""
    for field in ("mn_original_license_date", "mn_last_renewed_date", "vt_original_license_date"):
        assert field not in normalization.DATE_FIELDS


def test_normalize_item_leaves_mn_vt_date_fields_untouched():
    # These are normalized by the spiders themselves (see minnesota.py /
    # vermont.py), not by the shared pipeline -- normalize_item must leave a
    # raw, non-ISO value on these fields exactly as given.
    item = {
        "mn_original_license_date": "07/01/2014",
        "mn_last_renewed_date": "01/01/2026",
        "vt_original_license_date": "07/01/2005",
    }
    out = normalization.normalize_item(item, "minnesota")
    assert out["mn_original_license_date"] == "07/01/2014"
    assert out["mn_last_renewed_date"] == "01/01/2026"
    assert out["vt_original_license_date"] == "07/01/2005"


# --------------------------------------------------------------------------- #
# Task 05 — numeric & type consistency
# --------------------------------------------------------------------------- #
def test_normalize_capacity_string_to_int():
    assert normalization.normalize_capacity("8") == 8
    assert normalization.normalize_capacity("85") == 85


def test_normalize_capacity_int_passthrough():
    assert normalization.normalize_capacity(80) == 80


def test_normalize_capacity_range_unchanged_and_logged(caplog):
    with caplog.at_level("WARNING"):
        assert normalization.normalize_capacity("6-12") == "6-12"
        assert normalization.normalize_capacity("up to 50") == "up to 50"
    assert "non-integer capacity" in caplog.text


def test_normalize_deficiencies_digit_string_to_int():
    # TX (bug_10): CSV "Deficiencies" column passed through as a string.
    assert normalization.normalize_deficiencies("0") == 0
    assert normalization.normalize_deficiencies("1") == 1
    assert normalization.normalize_deficiencies("2") == 2


def test_normalize_deficiencies_int_passthrough():
    # CT/KS/WI already compute a plain int (sum/len) in the spider.
    assert normalization.normalize_deficiencies(0) == 0
    assert normalization.normalize_deficiencies(47) == 47


def test_normalize_deficiencies_none_and_blank_stay_none():
    assert normalization.normalize_deficiencies(None) is None
    assert normalization.normalize_deficiencies("") is None
    assert normalization.normalize_deficiencies("   ") is None


def test_normalize_deficiencies_comma_grouped_string():
    assert normalization.normalize_deficiencies("1,234") == 1234


def test_normalize_deficiencies_integral_float_and_string():
    assert normalization.normalize_deficiencies(2.0) == 2
    assert normalization.normalize_deficiencies("2.0") == 2
    assert isinstance(normalization.normalize_deficiencies("2.0"), int)


def test_normalize_deficiencies_non_integral_float_becomes_none_and_logged(caplog):
    with caplog.at_level("DEBUG"):
        assert normalization.normalize_deficiencies(2.5) is None
        assert normalization.normalize_deficiencies("2.5") is None
    assert "non-integer value" in caplog.text


def test_normalize_deficiencies_negative_values_become_none_and_logged(caplog):
    with caplog.at_level("DEBUG"):
        assert normalization.normalize_deficiencies(-1) is None
        assert normalization.normalize_deficiencies(-1.0) is None
        assert normalization.normalize_deficiencies("-1") is None
        assert normalization.normalize_deficiencies("-1,234") is None
    assert "negative value" in caplog.text


def test_normalize_deficiencies_non_numeric_becomes_none_and_logged(caplog):
    with caplog.at_level("DEBUG"):
        assert normalization.normalize_deficiencies("N/A") is None
        assert normalization.normalize_deficiencies("see report") is None
        assert normalization.normalize_deficiencies("1,2,3") is None  # malformed grouping
    assert "non-numeric value" in caplog.text


def test_normalize_deficiencies_bool_becomes_none(caplog):
    # isinstance(True, int) is True in Python -- a stray bool must not be
    # read as a count of 0/1, and the field stays strictly int-or-None.
    with caplog.at_level(logging.DEBUG, logger="provider_scrape.normalization"):
        assert normalization.normalize_deficiencies(True) is None
        assert normalization.normalize_deficiencies(False) is None
    assert "boolean value" in caplog.text


def test_normalize_ages_served_list_to_string():
    assert normalization.normalize_ages_served(["Pre-K (4 - 5 yrs)"]) == "Pre-K (4 - 5 yrs)"


def test_normalize_ages_served_joins_multiple():
    assert normalization.normalize_ages_served(["Infant", "Toddler"]) == "Infant, Toddler"


def test_normalize_ages_served_string_passthrough():
    assert normalization.normalize_ages_served("0 to 12") == "0 to 12"


def test_normalize_coordinate_string_trimmed_and_stays_string():
    result = normalization.normalize_coordinate("  33.514100000000000  ")
    assert result == "33.514100000000000"
    assert isinstance(result, str)


def test_normalize_coordinate_float_becomes_string():
    result = normalization.normalize_coordinate(33.5141)
    assert isinstance(result, str)
    assert result == "33.5141"


def test_normalize_item_applies_numeric_and_type_rules():
    item = {
        "capacity": "8",
        "ages_served": ["Pre-K (4 - 5 yrs)"],
        "latitude": 33.5141,
        "longitude": "-112.109100000000000",
    }
    out = normalization.normalize_item(item, "rhode_island")
    assert out["capacity"] == 8
    assert out["ages_served"] == "Pre-K (4 - 5 yrs)"
    assert isinstance(out["latitude"], str) and out["latitude"] == "33.5141"
    assert out["longitude"] == "-112.109100000000000"


def test_normalize_item_coerces_texas_deficiencies_string_to_int(caplog):
    # bug_10: TX-shaped item -- numeric string from the CSV "Deficiencies"
    # column -- is coerced to a plain int by the shared pipeline.
    item = {"deficiencies": "2"}
    out = normalization.normalize_item(item, "texas")
    assert out["deficiencies"] == 2
    assert isinstance(out["deficiencies"], int) and not isinstance(out["deficiencies"], bool)


def test_normalize_item_leaves_int_emitting_states_deficiencies_unchanged():
    # CT/KS/WI already compute a plain int (sum/len) in the spider -- the
    # pipeline must not alter an already-correct value.
    out_ct = normalization.normalize_item({"deficiencies": 47}, "connecticut")
    out_ks = normalization.normalize_item({"deficiencies": 0}, "kansas")
    out_wi = normalization.normalize_item({"deficiencies": 55}, "wisconsin")
    assert out_ct["deficiencies"] == 47
    assert out_ks["deficiencies"] == 0
    assert out_wi["deficiencies"] == 55


def test_normalize_item_skips_absent_deficiencies():
    # A state that never sets this key is left alone -- no key is invented.
    item = {"provider_name": "Example Center"}
    out = normalization.normalize_item(item, "montana")
    assert "deficiencies" not in out


# --------------------------------------------------------------------------- #
# Task 09 — languages de-dupe + casing (bug_04)
# --------------------------------------------------------------------------- #
def test_normalize_languages_illinois_regression():
    # bug_04: Illinois' Language1..3 columns repeat the same ALL-CAPS value.
    assert normalization.normalize_languages(["ENGLISH", "ENGLISH", "SPANISH"]) == [
        "English",
        "Spanish",
    ]


def test_normalize_languages_dedupe_ignores_case_and_whitespace():
    assert normalization.normalize_languages(["English", " english ", "ENGLISH"]) == ["English"]


def test_normalize_languages_dedupe_keeps_first_occurrence_order():
    # First-seen casing/order wins; a later differently-cased repeat is
    # dropped, not swapped in.
    assert normalization.normalize_languages(["Spanish", "English", "spanish"]) == [
        "Spanish",
        "English",
    ]


def test_normalize_languages_drops_empty_and_whitespace_entries():
    assert normalization.normalize_languages(["English", "", "   ", "Spanish"]) == [
        "English",
        "Spanish",
    ]


def test_normalize_languages_all_empty_becomes_none():
    assert normalization.normalize_languages(["", "   "]) is None
    assert normalization.normalize_languages([]) is None


def test_normalize_languages_acronym_preserved():
    # "ASL" must not be title-cased down to "Asl".
    assert normalization.normalize_languages(["ASL", "ENGLISH"]) == ["ASL", "English"]


def test_normalize_languages_mixed_case_entries_untouched():
    # Already-intentional mixed case (CT/MA/NJ-style) is left exactly as is,
    # including parenthetical annotations and multi-word entries.
    value = ["American Sign Language (ASL)", "Punjabi (Gurmukhi)", "French Creole"]
    assert normalization.normalize_languages(value) == value


def test_normalize_languages_multi_word_allcaps_titlecased():
    assert normalization.normalize_languages(["OTHER ASIAN", "UNKNOWN"]) == [
        "Other Asian",
        "Unknown",
    ]


def test_normalize_languages_string_passthrough_unchanged():
    # Delimited strings (AZ ';', CO/IA/ND/RI/VT/WA/DC ',', GA '; ' with
    # annotations, NM boilerplate) are intentionally left alone -- see
    # normalize_languages docstring and bug_04 follow-ups.
    value = "English (Taught and Spoken by Staff); Spanish (Taught)"
    assert normalization.normalize_languages(value) == value


def test_normalize_languages_non_list_non_string_unchanged():
    assert normalization.normalize_languages(None) is None


def test_title_case_language_leaves_mixed_case_alone():
    assert normalization.title_case_language("American Sign Language (ASL)") == ("American Sign Language (ASL)")


def test_normalize_item_dedupes_languages():
    item = {"languages": ["ENGLISH", "ENGLISH"]}
    out = normalization.normalize_item(item, "illinois")
    assert out["languages"] == ["English"]


def test_normalize_item_leaves_string_languages_unchanged():
    item = {"languages": "English, Spanish"}
    out = normalization.normalize_item(item, "colorado")
    assert out["languages"] == "English, Spanish"


# --------------------------------------------------------------------------- #
# Task 06 — status + facility_category controlled vocabularies
# --------------------------------------------------------------------------- #
def test_canonical_status_active_variants():
    for raw in ("LICENSED", "Licensed", "License issued (IL)", "Open"):
        assert normalization.canonical_status(raw) == "active"


def test_canonical_status_closed_and_enforcement():
    assert normalization.canonical_status("CLOSED") == "closed"
    assert normalization.canonical_status("INACTIVE") == "closed"
    assert normalization.canonical_status("Refuse to Renew (RR)") == "enforcement"


def test_canonical_status_case_and_whitespace_tolerant():
    assert normalization.canonical_status("  licensed ") == "active"


def test_canonical_status_unmapped_is_unknown_and_logged(caplog):
    with caplog.at_level("WARNING"):
        assert normalization.canonical_status("Banana Status") == "unknown"
    assert "unmapped status" in caplog.text


def test_facility_category_mappings():
    assert normalization.facility_category_from_type("FAMILY DAY CARE HOME") == "family_home"
    assert normalization.facility_category_from_type("Child Care Center") == "center"
    assert normalization.facility_category_from_type("GFDC") == "group_home"
    assert normalization.facility_category_from_type("SACC") == "school_age"
    assert normalization.facility_category_from_type("Exempt Only") == "exempt"


def test_canonical_status_maryland_values():
    assert normalization.canonical_status("Continuing - Full") == "active"
    assert normalization.canonical_status("Initial - Full") == "provisional"
    assert normalization.canonical_status("Provisional") == "provisional"
    assert normalization.canonical_status("Suspended - Emergency") == "enforcement"
    assert normalization.canonical_status("Closed") == "closed"
    assert normalization.canonical_status("Revoked") == "closed"


def test_facility_category_maryland_codes():
    assert normalization.facility_category_from_type("CTR") == "center"
    assert normalization.facility_category_from_type("LOC") == "center"
    assert normalization.facility_category_from_type("FCCH") == "family_home"
    assert normalization.facility_category_from_type("LFCCH") == "family_home"


def test_facility_category_ohio_program_types():
    fc = normalization.facility_category_from_type
    assert fc("Licensed Type A Family Child Care Home") == "family_home"
    assert fc("Licensed Type B Family Child Care Home") == "family_home"
    assert fc("Licensed Child Care Center") == "center"
    assert fc("Licensed School-Based Preschool") == "center"
    assert fc("Licensed School-Age Child Care") == "school_age"
    assert fc("Registered Day Camp or Approved Day Camp") == "other"
    assert fc("Certified In Home Aide") == "other"


def test_facility_category_wisconsin_regulation_types():
    fc = normalization.facility_category_from_type
    assert fc("Licensed Group") == "center"
    assert fc("Licensed Family") == "family_home"
    assert fc("Regular Certified") == "family_home"
    assert fc("Provisional Certified") == "family_home"
    assert fc("Licensed Camp") == "other"
    # The "(Probational)" licence-status suffix is stripped before lookup.
    assert fc("Licensed Group (Probational)") == "center"
    assert fc("Licensed Family (Probational)") == "family_home"
    assert fc("Licensed Camp (Probational)") == "other"


def test_facility_category_unmapped_is_other_and_logged(caplog):
    with caplog.at_level("WARNING"):
        assert normalization.facility_category_from_type("Spaceship Care") == "other"
    assert "unmapped provider_type" in caplog.text


def test_facility_category_delaware_types():
    # delaware_plan.md Sec 8: two of Delaware's three provider_type values
    # were previously unmapped (falling to "other"); "Licensed Child Care
    # Center" already matched the existing "center" bucket.
    fc = normalization.facility_category_from_type
    assert fc("Licensed Family Child Care") == "family_home"
    assert fc("Licensed Large Family Child Care") == "group_home"
    assert fc("Licensed Child Care Center") == "center"


def test_canonical_status_delaware_enforcement_values():
    # delaware_plan.md Sec 7.3/8: nine probation / intent-to-revoke stages,
    # derived onto `status` by delaware.derive_status.
    for raw in (
        "Probation",
        "Probation Extension",
        "Warning of Probation",
        "Warning of Probation Extension",
        "Intent to Revoke",
        "Intent to Place on Probation",
        "Intent to Place on Probation Extension",
        "Intent to Place on Warning of Probation",
        "Intent to Place on Warning of Probation Extension",
    ):
        assert normalization.canonical_status(raw) == "enforcement"


def test_normalize_item_status_in_place_and_facility_category_additive():
    item = {"status": "LICENSED", "provider_type": "Child Care Center"}
    out = normalization.normalize_item(item, "alabama")
    assert out["status"] == "active"  # replaced in place (D4)
    assert out["facility_category"] == "center"  # new additive facet (D2)
    assert out["provider_type"] == "Child Care Center"  # raw value preserved


# --------------------------------------------------------------------------- #
# Task 07 — field collapse -> common fields (additive, D2)
# --------------------------------------------------------------------------- #
def test_collapse_copies_single_source_and_keeps_it():
    item = {"ut_license_type": "Center"}
    out = normalization.collapse_state_fields(item)
    assert out["license_type"] == "Center"
    assert out["ut_license_type"] == "Center"  # source preserved (D2)


def test_collapse_does_not_overwrite_populated_common_field():
    item = {"license_type": "Already Set", "ut_license_type": "Center"}
    out = normalization.collapse_state_fields(item)
    assert out["license_type"] == "Already Set"


def test_collapse_school_district_from_any_single_source():
    out = normalization.collapse_state_fields({"wa_school_district": "Seattle"})
    assert out["school_district"] == "Seattle"


def test_collapse_curriculum_excludes_vpk_curriculum():
    # ga_curriculum/nj_curriculum feed curriculum; fl_vpk_curriculum must not.
    out = normalization.collapse_state_fields({"ga_curriculum": "Creative"})
    assert out["curriculum"] == "Creative"

    out2 = normalization.collapse_state_fields({"fl_vpk_curriculum": "VPK Set"})
    assert out2.get("curriculum") is None


def test_collapse_head_start_to_boolean():
    assert normalization.collapse_state_fields({"fl_is_head_start": True})["head_start"] is True
    assert normalization.collapse_state_fields({"co_head_start": "Yes"})["head_start"] is True
    assert normalization.collapse_state_fields({"co_head_start": "No"})["head_start"] is False
    # A descriptive program name reads as affirmative presence.
    assert normalization.collapse_state_fields({"wa_head_start": "Region X Head Start"})["head_start"] is True


def test_collapse_head_start_false_source_is_preserved_as_false():
    out = normalization.collapse_state_fields({"fl_is_head_start": False})
    assert out["head_start"] is False


def test_collapse_no_sources_leaves_common_unset():
    out = normalization.collapse_state_fields({"provider_name": "X"})
    assert out.get("license_type") is None
    assert out.get("head_start") is None


def test_collapse_accepting_new_children_from_single_source():
    # bug_02: a real Yes/No source (e.g. Colorado) still collapses normally.
    out = normalization.collapse_state_fields({"co_accepting_new_children": "Yes"})
    assert out["accepting_new_children"] == "Yes"


def test_collapse_accepting_new_children_ignores_none_source():
    # bug_02 regression: GA's ga_accepting_new_children is None whenever the
    # source carries no signal (see georgia.py's _accepting_new_children); a
    # None source must not be treated as "present" and must not fabricate a
    # collapsed value.
    out = normalization.collapse_state_fields({"ga_accepting_new_children": None})
    assert out.get("accepting_new_children") is None


def test_collapse_does_not_touch_quality_ratings():
    # Quality ratings are intentionally never collapsed.
    assert "ut_quality_rating" not in str(normalization.FIELD_COLLAPSE_MAP)
    assert not hasattr(ProviderItem, "quality_rating")
    assert "quality_rating" not in ProviderItem.fields


def test_collapse_mailing_address_excludes_owner_and_licensee():
    flat = str(normalization.FIELD_COLLAPSE_MAP["mailing_address"])
    assert "nc_owner_mailing_address" not in flat
    assert "mi_licensee_address" not in flat


# --------------------------------------------------------------------------- #
# Task 08 — address cleanup + component parse
# --------------------------------------------------------------------------- #
def test_clean_address_strips_trailing_country():
    assert (
        normalization.clean_address("849 Centerville Road Warwick, Rhode Island 02886, United States")
        == "849 Centerville Road Warwick, Rhode Island 02886"
    )


def test_clean_address_normalizes_comma_spacing():
    assert normalization.clean_address("Foo ,Bar") == "Foo, Bar"
    assert normalization.clean_address("Foo,Bar") == "Foo, Bar"


def test_parse_components_clean_full_address():
    city, state, zip_code = normalization.parse_address_components("460 A County Rd 27, Continental Divide, NM 87312")
    assert (city, state, zip_code) == ("Continental Divide", "NM", "87312")


def test_parse_components_spelled_out_state_to_usps():
    city, state, zip_code = normalization.parse_address_components("849 Centerville Road Warwick, Rhode Island 02886")
    assert state == "RI"
    assert zip_code == "02886"
    # City is mashed into the street field here -> not guessed.
    assert city is None


def test_parse_components_west_virginia_not_mismatched_to_virginia():
    _, state, _ = normalization.parse_address_components("100 Main St, Charleston, West Virginia 25301")
    assert state == "WV"


def test_parse_components_ambiguous_address_left_none_and_logged(caplog):
    with caplog.at_level("WARNING"):
        result = normalization.parse_address_components("Lewis and Clark, Helena, 59602")
    assert result == (None, None, None)
    assert "no recognizable state" in caplog.text


def test_parse_components_no_zip_left_none():
    assert normalization.parse_address_components("Somewhere, USA") == (None, None, None)


def test_normalize_item_address_cleanup_and_components():
    item = {
        "address": "849 Centerville Road Warwick, Rhode Island 02886, United States",
    }
    out = normalization.normalize_item(item, "rhode_island")
    assert out["address"] == "849 Centerville Road Warwick, Rhode Island 02886"
    assert out["state"] == "RI"
    assert out["zip"] == "02886"
    assert out.get("city") is None


def test_normalize_item_messy_address_keeps_original_components_none():
    item = {"address": "Lewis and Clark, Helena, 59602"}
    out = normalization.normalize_item(item, "montana")
    assert out["address"] == "Lewis and Clark, Helena, 59602"
    assert out.get("city") is None
    assert out.get("state") is None
    assert out.get("zip") is None


def test_normalize_item_skips_parse_when_components_present(caplog):
    # Scraper supplied a street-only address plus structured city/state/zip;
    # the component parse should be skipped (no spurious "no ZIP" warning) and
    # the pre-set components left intact.
    item = {"address": "303 1st Ave W", "city": "Carson", "state": "ND", "zip": "58529"}
    with caplog.at_level("WARNING"):
        out = normalization.normalize_item(item, "north_dakota")
    assert out["city"] == "Carson"
    assert out["state"] == "ND"
    assert out["zip"] == "58529"
    assert "no ZIP" not in caplog.text


def test_normalize_item_cleans_mailing_address_but_skips_component_parse():
    # bug_09: mailing_address gets the same whitespace/country-suffix/comma
    # cleanup as address, but must never feed city/state/zip -- those are the
    # provider's physical location and a mailing address can legitimately
    # point somewhere else (a PO box, a different city, etc.).
    item = {
        "al_mailing_address": "1909 Central Parkway SW,  Decatur, AL 35601, United States",
    }
    out = normalization.normalize_item(item, "alabama")
    assert out["mailing_address"] == "1909 Central Parkway SW, Decatur, AL 35601"
    assert out.get("city") is None
    assert out.get("state") is None
    assert out.get("zip") is None


def test_normalize_item_blank_mailing_address_stays_none():
    # bug_09: collapse_state_fields only copies a *present* source value, so
    # a bare "," (or None) from the spider must not produce a collapsed
    # mailing_address at all.
    out = normalization.normalize_item({"al_mailing_address": None}, "alabama")
    assert out.get("mailing_address") is None


def test_normalize_item_still_parses_when_a_component_missing():
    # Only city/state present (no zip) -> the parse still runs and fills zip.
    item = {"address": "849 Centerville Road Warwick, RI 02886", "city": "Warwick", "state": "RI"}
    out = normalization.normalize_item(item, "rhode_island")
    assert out["zip"] == "02886"


# --------------------------------------------------------------------------- #
# Task 10 — age-served flags: infant/toddler/preschool/school -> bool-or-None
# (bug_05)
# --------------------------------------------------------------------------- #
def test_coerce_age_served_flag_bool_and_none_passthrough():
    assert normalization.coerce_age_served_flag(True) is True
    assert normalization.coerce_age_served_flag(False) is False
    assert normalization.coerce_age_served_flag(None) is None


def test_coerce_age_served_flag_ny_digit_strings():
    # NY's per-age capacity counts, emitted as strings.
    assert normalization.coerce_age_served_flag("0") is False
    assert normalization.coerce_age_served_flag("8") is True
    assert normalization.coerce_age_served_flag("16") is True


def test_coerce_age_served_flag_ny_comma_grouped_count():
    # NY's two "1,100"/"1,305" outliers are thousands-grouped counts.
    assert normalization.coerce_age_served_flag("1,100") is True
    assert normalization.coerce_age_served_flag("0,000") is False


def test_coerce_age_served_flag_malformed_commas_are_unparseable(caplog):
    with caplog.at_level(logging.DEBUG, logger="provider_scrape.normalization"):
        assert normalization.coerce_age_served_flag("1,10") is None
        assert normalization.coerce_age_served_flag("2, 3") is None
    assert "unparseable flag" in caplog.text


def test_coerce_age_served_flag_vt_int_counts():
    # VT's per-age capacity counts, emitted as ints (already coerced by the
    # spider's _num() helper before the pipeline runs).
    assert normalization.coerce_age_served_flag(0) is False
    assert normalization.coerce_age_served_flag(2) is True


def test_coerce_age_served_flag_tx_y_n_and_stray_tokens():
    # TX's Y/N plus the two stray "1" values and one stray "0".
    assert normalization.coerce_age_served_flag("Y") is True
    assert normalization.coerce_age_served_flag("N") is False
    assert normalization.coerce_age_served_flag("1") is True
    assert normalization.coerce_age_served_flag("0") is False


def test_coerce_age_served_flag_ga_yes_and_absent():
    # GA only ever emits "Yes" (a falsy age group is left unset -> None).
    assert normalization.coerce_age_served_flag("Yes") is True
    assert normalization.coerce_age_served_flag(None) is None


def test_coerce_age_served_flag_negative_tokens_never_become_true():
    # None of these may ever coerce to True (plan §bug_05 criterion 4).
    for token in ("N", "No", "no", "0", "false", "False", "", "   ", "n/a"):
        assert normalization.coerce_age_served_flag(token) is not True


def test_coerce_age_served_flag_unknown_token_logs_debug_and_returns_none(caplog):
    with caplog.at_level(logging.DEBUG, logger="provider_scrape.normalization"):
        result = normalization.coerce_age_served_flag("Sometimes")
    assert result is None
    assert "unparseable flag" in caplog.text


def test_normalize_item_coerces_age_flags_for_every_state_shape():
    # One representative item per affected state (bug_05 evidence), run
    # through the full pipeline, confirming all four fields land bool-or-None.
    ny_item = {
        "infant": "8",
        "toddler": "0",
        "preschool": "16",
        "school": "1,305",  # thousands-grouped count -> True
    }
    vt_item = {"infant": 2, "toddler": 0, "preschool": None, "school": 4}
    tx_item = {"infant": "Y", "toddler": "N", "preschool": "1", "school": "0"}
    ga_item = {"infant": "Yes", "toddler": None, "preschool": "Yes", "school": None}

    out_ny = normalization.normalize_item(dict(ny_item), "new_york")
    out_vt = normalization.normalize_item(dict(vt_item), "vermont")
    out_tx = normalization.normalize_item(dict(tx_item), "texas")
    out_ga = normalization.normalize_item(dict(ga_item), "georgia")

    for out in (out_ny, out_vt, out_tx, out_ga):
        for field in normalization.AGE_SERVED_FLAG_FIELDS:
            assert out.get(field) is None or isinstance(out.get(field), bool), (
                f"{field} not bool-or-None: {out.get(field)!r}"
            )

    assert (out_ny["infant"], out_ny["toddler"], out_ny["preschool"], out_ny["school"]) == (
        True,
        False,
        True,
        True,
    )
    assert (out_vt["infant"], out_vt["toddler"], out_vt["preschool"], out_vt["school"]) == (
        True,
        False,
        None,
        True,
    )
    assert (out_tx["infant"], out_tx["toddler"], out_tx["preschool"], out_tx["school"]) == (
        True,
        False,
        True,
        False,
    )
    assert (out_ga["infant"], out_ga["toddler"], out_ga["preschool"], out_ga["school"]) == (
        True,
        None,
        True,
        None,
    )


def test_normalize_item_skips_already_absent_age_flags():
    # A state that never sets these keys at all (e.g. most states) is left
    # alone -- no key is invented.
    item = {"provider_name": "Example Center"}
    out = normalization.normalize_item(item, "montana")
    for field in normalization.AGE_SERVED_FLAG_FIELDS:
        assert field not in out


# --------------------------------------------------------------------------- #
# bug_06 -- scholarships_accepted: bool / Yes-No / Accepted-Not Accepted ->
# bool-or-None
# --------------------------------------------------------------------------- #
def test_coerce_scholarships_accepted_flag_bool_and_none_passthrough():
    assert normalization.coerce_scholarships_accepted_flag(True) is True
    assert normalization.coerce_scholarships_accepted_flag(False) is False
    assert normalization.coerce_scholarships_accepted_flag(None) is None


def test_coerce_scholarships_accepted_flag_yes_no():
    # AK/CO/GA/RI/TN/TX/UT/VT/DC all emit this shape.
    assert normalization.coerce_scholarships_accepted_flag("Yes") is True
    assert normalization.coerce_scholarships_accepted_flag("No") is False
    assert normalization.coerce_scholarships_accepted_flag("yes") is True
    assert normalization.coerce_scholarships_accepted_flag("no") is False


def test_coerce_scholarships_accepted_flag_accepted_not_accepted():
    # NM's vocabulary. "Not Accepted" is the regression case from bug_06: a
    # plain _coerce_bool-style fallback would read it as truthy text.
    assert normalization.coerce_scholarships_accepted_flag("Accepted") is True
    assert normalization.coerce_scholarships_accepted_flag("Not Accepted") is False
    assert normalization.coerce_scholarships_accepted_flag("not accepted") is False


def test_coerce_scholarships_accepted_flag_not_accepted_never_becomes_true():
    # Regression test (bug_06 acceptance criterion): across a few casings/
    # whitespace variants, "Not Accepted" must never slip through as True.
    for token in ("Not Accepted", "NOT ACCEPTED", " Not Accepted ", "not accepted"):
        assert normalization.coerce_scholarships_accepted_flag(token) is False


def test_coerce_scholarships_accepted_flag_none_and_empty():
    assert normalization.coerce_scholarships_accepted_flag(None) is None
    assert normalization.coerce_scholarships_accepted_flag("") is None
    assert normalization.coerce_scholarships_accepted_flag("   ") is None


def test_coerce_scholarships_accepted_flag_unknown_token_logs_debug_and_returns_none(caplog):
    with caplog.at_level(logging.DEBUG, logger="provider_scrape.normalization"):
        result = normalization.coerce_scholarships_accepted_flag("Pending Review")
    assert result is None
    assert "unparseable flag" in caplog.text


def test_coerce_scholarships_accepted_flag_negative_tokens_never_become_true():
    # Mirrors the age-served-flag regression test (bug_05 criterion 4): none
    # of these may ever coerce to True.
    for token in ("No", "no", "0", "false", "False", "", "   ", "n/a", "Not Accepted"):
        assert normalization.coerce_scholarships_accepted_flag(token) is not True


def test_normalize_item_coerces_scholarships_accepted_for_every_state_shape():
    # One representative item per affected-state shape (bug_06 evidence),
    # run through the full pipeline.
    bool_item = {"scholarships_accepted": True}  # e.g. CT/IN/IA/KY/...
    yes_no_item = {"scholarships_accepted": "Yes"}  # AK/CO/GA/RI/TN/TX/UT/VT/DC
    no_item = {"scholarships_accepted": "No"}
    accepted_item = {"scholarships_accepted": "Accepted"}  # NM
    not_accepted_item = {"scholarships_accepted": "Not Accepted"}  # NM
    absent_item = {"provider_name": "Example Center"}  # most states never set it

    out_bool = normalization.normalize_item(dict(bool_item), "connecticut")
    out_yes = normalization.normalize_item(dict(yes_no_item), "alaska")
    out_no = normalization.normalize_item(dict(no_item), "colorado")
    out_accepted = normalization.normalize_item(dict(accepted_item), "new_mexico")
    out_not_accepted = normalization.normalize_item(dict(not_accepted_item), "new_mexico")
    out_absent = normalization.normalize_item(dict(absent_item), "montana")

    assert out_bool["scholarships_accepted"] is True
    assert out_yes["scholarships_accepted"] is True
    assert out_no["scholarships_accepted"] is False
    assert out_accepted["scholarships_accepted"] is True
    assert out_not_accepted["scholarships_accepted"] is False
    assert "scholarships_accepted" not in out_absent

    for out in (out_bool, out_yes, out_no, out_accepted, out_not_accepted):
        assert isinstance(out["scholarships_accepted"], bool)


def test_normalize_item_scholarships_accepted_runs_after_collapse():
    # scholarships_accepted has a few collapse fallbacks, but the final value
    # must still come out bool-or-None, and a spider-set value is never
    # overwritten by the collapse.
    assert "scholarships_accepted" in normalization.FIELD_COLLAPSE_MAP
    item = {"scholarships_accepted": "Not Accepted"}
    out = normalization.normalize_item(item, "new_mexico")
    assert out["scholarships_accepted"] is False

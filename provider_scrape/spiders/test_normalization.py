"""Unit tests for provider_scrape.normalization and NormalizationPipeline.

Run with the project virtualenv: ``.venv/bin/pytest``.

These tests use small inline dict fixtures (not full state files) and a tiny
fake spider, so they are fast and framework-light.
"""
import copy
import logging

from provider_scrape import normalization
from provider_scrape.pipelines import NormalizationPipeline
from provider_scrape.items import ProviderItem


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
        self.logger = logging.getLogger("fake.%s" % name)


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
    assert normalization.title_case_name("  A CHILD'S WORLD ".strip()) \
        == "A Child's World"


def test_title_case_name_preserves_llc_and_roman_numerals():
    assert normalization.title_case_name("SMITH FAMILY DAYCARE LLC") \
        == "Smith Family Daycare LLC"
    assert normalization.title_case_name("JOHN SMITH III") == "John Smith III"


def test_title_case_name_leaves_mixed_case_alone():
    assert normalization.title_case_name("Bright Beginnings LLC") \
        == "Bright Beginnings LLC"


def test_title_case_name_handles_hyphenated_token():
    assert normalization.title_case_name("WELL-BEING CENTER") \
        == "Well-Being Center"


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
    assert normalization.normalize_date("2025-10-01T06:00:00.000Z") \
        == "2025-10-01"


def test_normalize_date_full_month_name():
    assert normalization.normalize_date("January 5, 2024") == "2024-01-05"


def test_normalize_date_ap_abbreviated_month():
    # AP-style "Sept." (4 letters, with period) seen in inspection dates.
    assert normalization.normalize_date("Sept. 23, 2025") == "2025-09-23"
    assert normalization.normalize_date("Dec. 10, 2025") == "2025-12-10"


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


def test_normalize_ages_served_list_to_string():
    assert normalization.normalize_ages_served(["Pre-K (4 - 5 yrs)"]) \
        == "Pre-K (4 - 5 yrs)"


def test_normalize_ages_served_joins_multiple():
    assert normalization.normalize_ages_served(["Infant", "Toddler"]) \
        == "Infant, Toddler"


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


# --------------------------------------------------------------------------- #
# Task 06 — status + facility_category controlled vocabularies
# --------------------------------------------------------------------------- #
def test_canonical_status_active_variants():
    for raw in ("LICENSED", "Licensed", "License issued (IL)", "Open"):
        assert normalization.canonical_status(raw) == "active"


def test_canonical_status_closed_and_enforcement():
    assert normalization.canonical_status("CLOSED") == "closed"
    assert normalization.canonical_status("INACTIVE") == "closed"
    assert normalization.canonical_status("Refuse to Renew (RR)") \
        == "enforcement"


def test_canonical_status_case_and_whitespace_tolerant():
    assert normalization.canonical_status("  licensed ") == "active"


def test_canonical_status_unmapped_is_unknown_and_logged(caplog):
    with caplog.at_level("WARNING"):
        assert normalization.canonical_status("Banana Status") == "unknown"
    assert "unmapped status" in caplog.text


def test_facility_category_mappings():
    assert normalization.facility_category_from_type("FAMILY DAY CARE HOME") \
        == "family_home"
    assert normalization.facility_category_from_type("Child Care Center") \
        == "center"
    assert normalization.facility_category_from_type("GFDC") == "group_home"
    assert normalization.facility_category_from_type("SACC") == "school_age"
    assert normalization.facility_category_from_type("Exempt Only") == "exempt"


def test_facility_category_unmapped_is_other_and_logged(caplog):
    with caplog.at_level("WARNING"):
        assert normalization.facility_category_from_type("Spaceship Care") \
            == "other"
    assert "unmapped provider_type" in caplog.text


def test_normalize_item_status_in_place_and_facility_category_additive():
    item = {"status": "LICENSED", "provider_type": "Child Care Center"}
    out = normalization.normalize_item(item, "alabama")
    assert out["status"] == "active"            # replaced in place (D4)
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
    assert normalization.collapse_state_fields(
        {"fl_is_head_start": True})["head_start"] is True
    assert normalization.collapse_state_fields(
        {"co_head_start": "Yes"})["head_start"] is True
    assert normalization.collapse_state_fields(
        {"co_head_start": "No"})["head_start"] is False
    # A descriptive program name reads as affirmative presence.
    assert normalization.collapse_state_fields(
        {"wa_head_start": "Region X Head Start"})["head_start"] is True


def test_collapse_head_start_false_source_is_preserved_as_false():
    out = normalization.collapse_state_fields({"fl_is_head_start": False})
    assert out["head_start"] is False


def test_collapse_no_sources_leaves_common_unset():
    out = normalization.collapse_state_fields({"provider_name": "X"})
    assert out.get("license_type") is None
    assert out.get("head_start") is None


def test_collapse_does_not_touch_quality_ratings():
    # Quality ratings are intentionally never collapsed.
    assert "ut_quality_rating" not in str(normalization.FIELD_COLLAPSE_MAP)
    assert not hasattr(ProviderItem, "quality_rating")
    assert "quality_rating" not in ProviderItem.fields


def test_collapse_mailing_address_excludes_owner_and_licensee():
    flat = str(normalization.FIELD_COLLAPSE_MAP)
    assert "nc_owner_mailing_address" not in flat
    assert "mi_licensee_address" not in flat


# --------------------------------------------------------------------------- #
# Task 08 — address cleanup + component parse
# --------------------------------------------------------------------------- #
def test_clean_address_strips_trailing_country():
    assert normalization.clean_address(
        "849 Centerville Road Warwick, Rhode Island 02886, United States") \
        == "849 Centerville Road Warwick, Rhode Island 02886"


def test_clean_address_normalizes_comma_spacing():
    assert normalization.clean_address("Foo ,Bar") == "Foo, Bar"
    assert normalization.clean_address("Foo,Bar") == "Foo, Bar"


def test_parse_components_clean_full_address():
    city, state, zip_code = normalization.parse_address_components(
        "460 A County Rd 27, Continental Divide, NM 87312")
    assert (city, state, zip_code) == ("Continental Divide", "NM", "87312")


def test_parse_components_spelled_out_state_to_usps():
    city, state, zip_code = normalization.parse_address_components(
        "849 Centerville Road Warwick, Rhode Island 02886")
    assert state == "RI"
    assert zip_code == "02886"
    # City is mashed into the street field here -> not guessed.
    assert city is None


def test_parse_components_west_virginia_not_mismatched_to_virginia():
    _, state, _ = normalization.parse_address_components(
        "100 Main St, Charleston, West Virginia 25301")
    assert state == "WV"


def test_parse_components_ambiguous_address_left_none_and_logged(caplog):
    with caplog.at_level("WARNING"):
        result = normalization.parse_address_components(
            "Lewis and Clark, Helena, 59602")
    assert result == (None, None, None)
    assert "no recognizable state" in caplog.text


def test_parse_components_no_zip_left_none():
    assert normalization.parse_address_components("Somewhere, USA") \
        == (None, None, None)


def test_normalize_item_address_cleanup_and_components():
    item = {
        "address": "849 Centerville Road Warwick, Rhode Island 02886, "
                   "United States",
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

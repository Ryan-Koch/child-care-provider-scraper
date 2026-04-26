from pathlib import Path

import pytest
from parsel import Selector

from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.spiders.north_carolina import (
    NC_COUNTIES,
    SEARCH_URL,
    NorthCarolinaSpider,
    _join_address,
    _read_obfuscated_email,
    _to_int,
    _yes_no_to_bool,
    build_item,
    count_rows_on_page,
    parse_basic,
    parse_licenses,
    parse_owner,
    parse_pagination_total,
    parse_special_features,
    parse_visits,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DETAILS_HTML = (PROJECT_ROOT / "north_carolina_details.html").read_text(
    encoding="utf-8"
)
RESULTS_HTML = (PROJECT_ROOT / "north_carolina_results.html").read_text(
    encoding="utf-8"
)


# ---- Tiny helpers ------------------------------------------------------------


def test_to_int_handles_strings():
    assert _to_int("224") == 224
    assert _to_int(" 15 ") == 15
    assert _to_int(None) is None
    assert _to_int("") is None
    assert _to_int("  ") is None
    assert _to_int("not-a-number") is None


def test_to_int_extracts_digits_from_mixed_string():
    assert _to_int("184 children") == 184


def test_yes_no_to_bool():
    assert _yes_no_to_bool("Yes") is True
    assert _yes_no_to_bool("yes") is True
    assert _yes_no_to_bool("No") is False
    assert _yes_no_to_bool("no") is False
    assert _yes_no_to_bool("") is None
    assert _yes_no_to_bool(None) is None
    assert _yes_no_to_bool("maybe") is None


def test_join_address_full():
    assert (
        _join_address("981 KIRKPATRICK RD", "BURLINGTON", "NC", "27215")
        == "981 KIRKPATRICK RD, BURLINGTON, NC 27215"
    )


def test_join_address_missing_zip():
    assert (
        _join_address("123 Main St", "Raleigh", "NC", None)
        == "123 Main St, Raleigh, NC"
    )


def test_join_address_only_street():
    assert (
        _join_address("100 Oak Ln", None, None, None)
        == "100 Oak Ln"
    )


def test_join_address_all_empty():
    assert _join_address(None, None, None, None) is None


# ---- Email obfuscation -------------------------------------------------------


def test_read_obfuscated_email_prefers_mailto_href():
    sel = Selector(text="""
        <span id="x">
            <a href="mailto:foo@example.com">foo<i class="fas fa-at"></i>example.com</a>
        </span>
    """)
    assert _read_obfuscated_email(sel, "x") == "foo@example.com"


def test_read_obfuscated_email_rebuilds_from_fa_at_when_no_anchor():
    sel = Selector(text="""
        <span id="x">armc<i class="fa fas fa-at"></i>brighthorizons.com</span>
    """)
    assert _read_obfuscated_email(sel, "x") == "armc@brighthorizons.com"


def test_read_obfuscated_email_returns_none_when_blank():
    sel = Selector(text='<span id="x"></span>')
    assert _read_obfuscated_email(sel, "x") is None


def test_read_obfuscated_email_returns_none_when_no_at():
    sel = Selector(text='<span id="x">just plain text</span>')
    assert _read_obfuscated_email(sel, "x") is None


# ---- parse_basic --------------------------------------------------------------


@pytest.fixture
def details_sel():
    return Selector(text=DETAILS_HTML)


def test_parse_basic_golden_path(details_sel):
    basic = parse_basic(details_sel)
    assert basic["license_number"] == "01000324"
    assert basic["provider_name"] == "ARMC FAMILY ENRICHMENT CENTER"
    assert basic["address"] == "981 KIRKPATRICK RD, BURLINGTON, NC 27215"
    assert basic["county"] == "Alamance"
    assert basic["email"] == "armc@brighthorizons.com"
    assert basic["provider_website"] is None
    assert basic["provider_type"] == "Child Care Center"
    assert basic["phone"] == "(336) 586-9767"
    assert basic["scholarships_accepted"] is True
    assert basic["nc_sanitation_inspection_date"] == "1/21/2026"
    assert basic["nc_sanitation_classification"] == "Superior"
    assert basic["nc_sanitation_score"] == 224


# ---- parse_licenses -----------------------------------------------------------


def test_parse_licenses_returns_two_entries(details_sel):
    licenses = parse_licenses(details_sel)
    assert len(licenses) == 2


def test_parse_licenses_current_entry(details_sel):
    licenses = parse_licenses(details_sel)
    current = licenses[0]
    assert current["license_type"] == "Five Star Center License"
    assert current["effective_date"] == "3/31/2026"
    assert current["age_range"] == "0 through 3"
    assert current["capacity_first_shift"] == 184
    assert current["capacity_second_shift"] == 0
    assert current["capacity_third_shift"] == 0
    assert current["license_restrictions"] == [
        "Other - Meets all enhanced requirements and reduced ratios"
    ]
    # Current license has no star rating in this example.
    assert current["star_rating_total_points"] is None
    assert current["program_standards_points"] is None
    assert current["educational_standards_points"] is None


def test_parse_licenses_previous_entry_with_star_rating(details_sel):
    licenses = parse_licenses(details_sel)
    previous = licenses[1]
    assert previous["license_type"] == "Five Star Center License"
    assert previous["effective_date"] == "7/11/2017"
    assert previous["age_range"] == "0 through 6"
    assert previous["capacity_first_shift"] == 184
    assert previous["program_standards_points"] == 7
    assert previous["program_standards_max_points"] == 7
    assert previous["educational_standards_points"] == 7
    assert previous["educational_standards_max_points"] == 7
    assert previous["star_rating_total_points"] == 15
    assert previous["star_rating_max_points"] == 15
    assert previous["license_restrictions"] == [
        "Other - Meets all enhanced requirements and reduced ratios"
    ]


# ---- parse_special_features ---------------------------------------------------


def test_parse_special_features_services(details_sel):
    services, _ = parse_special_features(details_sel)
    assert "Provides transportation" in services
    assert "Accredited by a national organization*" in services


def test_parse_special_features_ratios(details_sel):
    _, ratios = parse_special_features(details_sel)
    # The example facility lists 0/0 for most age groups but populates 3/4yo.
    assert ratios.get("3 Year Olds") == "0 Adult(s)/10 Children"
    assert ratios.get("4 Year Olds") == "0 Adult(s)/13 Children"
    assert ratios.get("Infants") == "0 Adult(s)/0 Children"


# ---- parse_owner --------------------------------------------------------------


def test_parse_owner_golden_path(details_sel):
    owner = parse_owner(details_sel)
    assert owner["name"] == "BRIGHT HORIZONS CHILDREN CENTERS, INC."
    assert "200 TALCOTT AVE SOUTH" in owner["mailing_address"]
    assert "WATERTOWN" in owner["mailing_address"]
    assert owner["phone"] == "(336) 586-9759"
    assert owner["fax"] == "(336) 586-9744"
    assert owner["email"] == "armc@brighthorizons.com"


# ---- parse_visits -------------------------------------------------------------


def test_parse_visits_returns_inspection_items(details_sel):
    visits = parse_visits(details_sel)
    assert len(visits) >= 5
    assert all(isinstance(v, InspectionItem) for v in visits)


def test_parse_visits_first_entry_shape(details_sel):
    visits = parse_visits(details_sel)
    first = visits[0]
    assert first["date"] == "12/16/2025"
    assert first["type"] == "Unannounced"
    # First visit has no violations.
    assert first["nc_violations"] is None


def test_parse_visits_captures_violation_text(details_sel):
    visits = parse_visits(details_sel)
    # Find the second 12/16/2025 entry which has violations.
    with_violations = [v for v in visits if v["nc_violations"]]
    assert with_violations, "Expected at least one visit with violation details"
    sample = with_violations[0]
    flat = " ".join(sample["nc_violations"])
    assert "Rule Violated" in flat


# ---- parse_pagination_total / count_rows_on_page ------------------------------


def test_parse_pagination_total_from_results_page():
    record_count, total_pages = parse_pagination_total(RESULTS_HTML)
    assert record_count == 79
    assert total_pages == 8


def test_count_rows_on_page_from_results_page():
    assert count_rows_on_page(RESULTS_HTML) == 10


def test_parse_pagination_total_handles_missing_pager():
    html = "<html><body></body></html>"
    record_count, total_pages = parse_pagination_total(html)
    assert record_count is None
    assert total_pages is None


def test_parse_pagination_total_single_page_no_pager():
    html = """
    <html><body>
        <span id="dnn_ctr1464_View_lblRecordCount">3</span>
    </body></html>
    """
    record_count, total_pages = parse_pagination_total(html)
    assert record_count == 3
    assert total_pages == 1


# ---- build_item: golden path --------------------------------------------------


def test_build_item_core_fields():
    item = build_item(DETAILS_HTML)
    assert isinstance(item, ProviderItem)
    assert item["source_state"] == "North Carolina"
    assert item["provider_url"] == SEARCH_URL
    assert item["provider_name"] == "ARMC FAMILY ENRICHMENT CENTER"
    assert item["license_number"] == "01000324"
    assert item["address"] == "981 KIRKPATRICK RD, BURLINGTON, NC 27215"
    assert item["county"] == "Alamance"
    assert item["email"] == "armc@brighthorizons.com"
    assert item["phone"] == "(336) 586-9767"
    assert item["provider_type"] == "Child Care Center"
    assert item["scholarships_accepted"] is True


def test_build_item_current_license_fields():
    item = build_item(DETAILS_HTML)
    assert item["nc_license_type"] == "Five Star Center License"
    assert item["nc_license_effective_date"] == "3/31/2026"
    assert item["ages_served"] == "0 through 3"
    assert item["nc_capacity_first_shift"] == 184
    assert item["nc_capacity_second_shift"] == 0
    assert item["nc_capacity_third_shift"] == 0
    # Sum of shifts.
    assert item["capacity"] == 184


def test_build_item_star_rating_falls_back_to_previous_license():
    """The current license entry has no star rating; the rating should roll
    forward from the most-recent previous license that has it."""
    item = build_item(DETAILS_HTML)
    assert item["nc_star_rating_total_points"] == 15
    assert item["nc_star_rating_max_points"] == 15
    assert item["nc_program_standards_points"] == 7
    assert item["nc_educational_standards_points"] == 7


def test_build_item_license_history_contains_previous_entries():
    item = build_item(DETAILS_HTML)
    history = item["nc_license_history"]
    assert isinstance(history, list)
    assert len(history) == 1
    assert history[0]["license_type"] == "Five Star Center License"
    assert history[0]["effective_date"] == "7/11/2017"
    assert history[0]["star_rating_total_points"] == 15


def test_build_item_owner_fields():
    item = build_item(DETAILS_HTML)
    assert item["nc_owner_name"] == "BRIGHT HORIZONS CHILDREN CENTERS, INC."
    assert item["nc_owner_phone"] == "(336) 586-9759"
    assert item["nc_owner_fax"] == "(336) 586-9744"
    assert item["nc_owner_email"] == "armc@brighthorizons.com"


def test_build_item_sanitation_fields():
    item = build_item(DETAILS_HTML)
    assert item["nc_sanitation_inspection_date"] == "1/21/2026"
    assert item["nc_sanitation_classification"] == "Superior"
    assert item["nc_sanitation_score"] == 224


def test_build_item_inspections_attached_with_violations():
    item = build_item(DETAILS_HTML)
    inspections = item["inspections"]
    assert isinstance(inspections, list)
    assert len(inspections) >= 5
    assert any(i["nc_violations"] for i in inspections)


def test_build_item_special_features_and_ratios():
    item = build_item(DETAILS_HTML)
    assert "Provides transportation" in item["nc_special_features"]
    assert item["nc_staff_child_ratios"]["3 Year Olds"] == "0 Adult(s)/10 Children"


def test_build_item_uses_county_hint_when_basic_county_missing():
    minimal = """
    <html><body>
      <span id="dnn_ctr1464_View_FacilityDetail_rptBasicFacilityInfo_LicenseNumberLabel_0">L1</span>
      <span id="dnn_ctr1464_View_FacilityDetail_rptBasicFacilityInfo_FacilityNameLabel_0">A Place</span>
    </body></html>
    """
    item = build_item(minimal, county_hint="WAKE")
    assert item["county"] == "Wake"


# ---- build_item: missing-fields variant ---------------------------------------


SPARSE_HTML = """
<html><body>
  <span id="dnn_ctr1464_View_FacilityDetail_rptBasicFacilityInfo_LicenseNumberLabel_0">99999</span>
  <span id="dnn_ctr1464_View_FacilityDetail_rptBasicFacilityInfo_FacilityNameLabel_0">Sparse Center</span>
  <span id="dnn_ctr1464_View_FacilityDetail_rptBasicFacilityInfo_FacilityStreetLabel_0">1 Main St</span>
  <span id="dnn_ctr1464_View_FacilityDetail_rptBasicFacilityInfo_FacilityCityLabel_0">Raleigh</span>
  <span id="dnn_ctr1464_View_FacilityDetail_rptBasicFacilityInfo_FacilityStateLabel_0">NC</span>
  <span id="dnn_ctr1464_View_FacilityDetail_rptBasicFacilityInfo_FacilityZipLabel_0">27601</span>
</body></html>
"""


def test_build_item_sparse_record_keeps_present_fields():
    item = build_item(SPARSE_HTML)
    assert item["provider_name"] == "Sparse Center"
    assert item["license_number"] == "99999"
    assert item["address"] == "1 Main St, Raleigh, NC 27601"


def test_build_item_sparse_record_collapses_missing_fields_to_none():
    item = build_item(SPARSE_HTML)
    assert item["email"] is None
    assert item["provider_website"] is None
    assert item["phone"] is None
    assert item["provider_type"] is None
    assert item["scholarships_accepted"] is None
    assert item["nc_sanitation_inspection_date"] is None
    assert item["nc_sanitation_score"] is None
    # No license rows — all license-derived fields collapse.
    assert item["nc_license_type"] is None
    assert item["nc_license_effective_date"] is None
    assert item["capacity"] is None
    assert item["nc_star_rating_total_points"] is None
    assert item["nc_license_history"] is None
    assert item["nc_special_features"] is None
    assert item["nc_staff_child_ratios"] is None
    assert item["nc_owner_name"] is None
    assert item["inspections"] is None


# ---- Spider construction ------------------------------------------------------


def test_counties_default_to_full_list():
    spider = NorthCarolinaSpider()
    assert spider.counties == NC_COUNTIES
    assert len(spider.counties) == 100


def test_counties_arg_filters_to_known_subset():
    spider = NorthCarolinaSpider(counties="WAKE,DURHAM,bogus")
    assert spider.counties == ["WAKE", "DURHAM"]


def test_counties_arg_normalizes_case_and_whitespace():
    spider = NorthCarolinaSpider(counties=" wake , durham ")
    assert spider.counties == ["WAKE", "DURHAM"]


def test_concurrency_arg_overrides_settings():
    spider = NorthCarolinaSpider(concurrency=8)
    assert spider.concurrency == 8
    assert spider.custom_settings["CONCURRENT_REQUESTS"] == 8
    assert spider.custom_settings["CONCURRENT_REQUESTS_PER_DOMAIN"] == 8


def test_start_requests_emits_one_request_per_county():
    spider = NorthCarolinaSpider(counties="WAKE,DURHAM")
    requests = list(spider.start_requests())
    assert len(requests) == 2
    counties = [r.meta["county"] for r in requests]
    assert counties == ["WAKE", "DURHAM"]
    contexts = [r.meta["playwright_context"] for r in requests]
    assert contexts == ["nc-WAKE", "nc-DURHAM"]
    assert all(r.meta["playwright"] for r in requests)
    assert all(r.meta["playwright_include_page"] for r in requests)


def test_nc_counties_list_has_no_duplicates():
    assert len(set(NC_COUNTIES)) == len(NC_COUNTIES)

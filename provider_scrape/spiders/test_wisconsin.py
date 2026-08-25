import os

import pytest
import scrapy
from scrapy.http import HtmlResponse, Request

from provider_scrape.spiders.wisconsin import (
    COUNTIES,
    WisconsinSpider,
    _rows_from_selector,
    _showing,
    _split_city_state_zip,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")
BASE = "https://childcarefinder.wisconsin.gov/"
RESULTS_URL = BASE + "SearchResults?CCF=Y&UserSessionId=abc&SearchId=1&Distance=5"
DETAIL_URL = BASE + "ProviderDetails?ProviderNumber=555710&LocationNumber=16"


def _read(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return fh.read()


@pytest.fixture
def spider():
    return WisconsinSpider()


@pytest.fixture
def results_selector():
    # The captured fixture is a bare <thead>/<tbody> fragment; wrap it in a
    # <table> as the live SearchResults page serves it.
    frag = _read("wisconsin_results.html")
    return scrapy.Selector(text=f"<html><body><table>{frag}</table></body></html>")


@pytest.fixture
def detail_response():
    body = _read("wisconsin_detail.html")
    stub = {
        "county": "Dane County",
        "provider_name": "Kindercare Learning Ctr-Old Sauk (stub)",
        "provider_type": "Licensed Group",
        "wi_youngstar_rating": "5 Stars",
        "address": "stub address",
    }
    return HtmlResponse(
        url=DETAIL_URL,
        body=body.encode("utf-8"),
        request=Request(DETAIL_URL, meta={"stub": stub}),
    )


# --------------------------------------------------------------------------- #
# Results list
# --------------------------------------------------------------------------- #
def test_results_row_count(results_selector):
    stubs = _rows_from_selector(results_selector, RESULTS_URL, "Dane County")
    assert len(stubs) == 50


def test_results_first_row_fields(results_selector):
    stubs = _rows_from_selector(results_selector, RESULTS_URL, "Dane County")
    first = stubs[0]
    assert first["provider_name"] == "Kindercare Learning Centers Inc"
    assert first["provider_type"] == "Licensed Group"
    assert first["wi_youngstar_rating"] == "5 Stars"
    assert first["address"] == "3327 E Milwaukee St, Janesville WI 53546-1631"
    assert first["county"] == "Dane County"


def test_results_detail_urls_are_absolute(results_selector):
    stubs = _rows_from_selector(results_selector, RESULTS_URL, "Dane County")
    assert all(s["detail_url"].startswith(BASE + "ProviderDetails?") for s in stubs)
    assert "ProviderNumber=555710&LocationNumber=1&" in stubs[0]["detail_url"]


# --------------------------------------------------------------------------- #
# Detail — golden path
# --------------------------------------------------------------------------- #
def test_detail_core_fields(spider, detail_response):
    item = next(spider.parse_detail(detail_response))
    assert item["source_state"] == "Wisconsin"
    assert item["provider_url"] == DETAIL_URL
    assert item["county"] == "Dane County"
    assert item["provider_name"] == "Kindercare Learning Ctr-Old Sauk"
    assert item["license_number"] == "0000555710"
    assert item["wi_location_number"] == "016"
    assert item["wi_facility_number"] == "120162"
    assert item["provider_type"] == "Licensed Group"
    assert item["license_holder"] == "April Carter"
    assert item["administrator"] == "Sarah Smith"
    assert item["phone"] == "(608) 831-1223"


def test_detail_address_split(spider, detail_response):
    item = next(spider.parse_detail(detail_response))
    assert item["address"] == "7126 Old Sauk Rd, Madison WI 53717-1013"
    assert item["city"] == "Madison"
    assert item["state"] == "WI"
    assert item["zip"] == "53717-1013"


def test_detail_care_fields(spider, detail_response):
    item = next(spider.parse_detail(detail_response))
    assert item["ages_served"] == "6 weeks - 13 years"
    assert item["wi_months_open"] == "Jan - Dec"
    assert item["capacity"] == "135"
    assert item["wi_night_capacity"] == "0"
    assert item["hours"] == "Mon-Fri 6:00AM - 6:00PM; Sat-Sun Closed"


def test_detail_youngstar_and_accreditation(spider, detail_response):
    item = next(spider.parse_detail(detail_response))
    assert item["wi_youngstar_rating"] == "5 Stars"
    assert item["wi_unique_services"] == ["This program provides Infant Child Care."]
    assert "National Association For The Education Of Young Children" in item["accreditation"]


def test_detail_provider_reported(spider, detail_response):
    item = next(spider.parse_detail(detail_response))
    assert item["wi_special_care_types"] == [
        "Before School Care provided.",
        "After School Care provided.",
        "Rotating Care provided.",
    ]
    assert item["wi_program_philosophy"] == "None Reported."
    assert item["wi_vacancies"] == "None Reported."
    assert item["wi_waitlist"] == "This provider does not have a waitlist."


# --------------------------------------------------------------------------- #
# Detail — inspections
# --------------------------------------------------------------------------- #
def test_inspection_counts(spider, detail_response):
    item = next(spider.parse_detail(detail_response))
    by_type = {}
    for insp in item["inspections"]:
        by_type.setdefault(insp["type"], 0)
        by_type[insp["type"]] += 1
    assert by_type["Enforcement"] == 4
    assert by_type["Monitoring"] == 12
    assert by_type["Violation"] == 55
    assert item["deficiencies"] == 55


def test_enforcement_row(spider, detail_response):
    item = next(spider.parse_detail(detail_response))
    enf = [i for i in item["inspections"] if i["type"] == "Enforcement"]
    first = enf[0]
    assert first["date"] == "9/11/2025"
    assert first["wi_enforcement_type"] == "Orders Letter"
    assert first["wi_appeal"] == "No"
    assert first["wi_decision"] is None  # whitespace-only cell
    assert "251.04(3)(c)" in first["wi_description"]


def test_monitoring_row(spider, detail_response):
    item = next(spider.parse_detail(detail_response))
    mon = [i for i in item["inspections"] if i["type"] == "Monitoring"]
    first = mon[0]
    assert first["date"] == "2/13/2026"
    assert first["original_status"] == "See Violations below"
    assert first["report_url"].startswith(BASE + "MonitoringResults?")
    assert first["wi_correction_plan_url"].startswith(BASE + "Documents/")


def test_violation_row(spider, detail_response):
    item = next(spider.parse_detail(detail_response))
    vio = [i for i in item["inspections"] if i["type"] == "Violation"]
    first = vio[0]
    assert first["date"] == "2/13/2026"
    assert first["wi_rule_number"] == "251.055(1)(a)"
    assert first["wi_rule_summary"] == "Supervision Of Children"
    assert first["wi_description"].startswith("A preschooler was unsupervised")
    assert first["report_url"].startswith("https://docs.legis.wisconsin.gov/")


# --------------------------------------------------------------------------- #
# Detail — missing fields resilience
# --------------------------------------------------------------------------- #
MINIMAL_DETAIL = """
<div id="providerDetailsCollapsible"><div class="accordion-body"><div class="row">
  <div class="col-12 col-md-3 mb-3">
    <div class="row"><div class="col-12 Bold">Tiny Tots Family Home</div></div>
    <div class="row"><div class="col-12">123 Main St</div></div>
    <div class="row"><div class="col-12 mb-3">Madison WI 53703</div></div>
  </div>
  <div class="col-12 col-md-4 mb-3">
    <div class="row"><div class="col-7 Bold">Provider #</div>
        <div class="col-5">0000999999</div></div>
    <div class="row"><div class="col-7 Bold">Regulation Type</div>
        <div class="col-5">Licensed Family</div></div>
  </div>
</div></div></div>
"""


def test_detail_missing_fields(spider):
    resp = HtmlResponse(
        url=DETAIL_URL,
        body=MINIMAL_DETAIL.encode("utf-8"),
        request=Request(DETAIL_URL, meta={"stub": {"county": "Iron County"}}),
    )
    item = next(spider.parse_detail(resp))
    assert item["provider_name"] == "Tiny Tots Family Home"
    assert item["license_number"] == "0000999999"
    assert item["provider_type"] == "Licensed Family"
    assert item["city"] == "Madison"
    assert item["state"] == "WI"
    assert item["zip"] == "53703"
    # Absent sections degrade to None / empty, not exceptions.
    assert item["phone"] is None
    assert item["capacity"] is None
    assert item["hours"] is None
    assert item["wi_youngstar_rating"] is None
    assert item["wi_special_care_types"] is None
    assert item["inspections"] == []
    assert item["deficiencies"] is None


CERTIFIED_DETAIL = """
<div id="providerDetailsCollapsible"><div class="accordion-body"><div class="row">
  <div class="col-12 col-md-3 mb-3">
    <div class="row"><div class="col-12 Bold">Paraiso Infantil</div></div>
    <div class="row"><div class="col-12">1109 Macarthur Dr</div></div>
    <div class="row"><div class="col-12 mb-3">Janesville WI 53548-1410</div></div>
  </div>
  <div class="col-12 col-md-4 mb-3">
    <div class="row"><div class="col-7 Bold">Provider #</div>
        <div class="col-5">1000590051</div></div>
    <div class="row"><div class="col-7 Bold">Facility #</div>
        <div class="col-5">N/A</div></div>
    <div class="row"><div class="col-7 Bold">Regulation Type</div>
        <div class="col-5">Regular Certified</div></div>
  </div>
</div></div></div>
"""


def test_detail_certified_facility_number_na_is_none(spider):
    # Certified providers render Facility # as "N/A"; normalise to None.
    resp = HtmlResponse(
        url=DETAIL_URL,
        body=CERTIFIED_DETAIL.encode("utf-8"),
        request=Request(DETAIL_URL, meta={"stub": {"county": "Rock County"}}),
    )
    item = next(spider.parse_detail(resp))
    assert item["license_number"] == "1000590051"
    assert item["wi_facility_number"] is None
    assert item["provider_type"] == "Regular Certified"


NO_LICENSE_DETAIL = """
<div id="providerDetailsCollapsible"><div class="accordion-body"><div class="row">
  <div class="col-12 col-md-3 mb-3">
    <div class="row"><div class="col-12 Bold">Closed Provider</div></div>
    <div class="row"><div class="col-12">1 Main St</div></div>
    <div class="row"><div class="col-12 mb-3">Madison WI 53703</div></div>
  </div>
  <div class="col-12 col-md-4 mb-3">
    <div class="row"><div class="col-7 Bold">Provider #</div>
        <div class="col-5">1000000001</div></div>
    <div class="row"><div class="col-7 Bold">Regulation Type</div>
        <div class="col-5">No active license or certificate found for this provider!</div></div>
  </div>
</div></div></div>
"""


def test_detail_no_active_license_goes_to_status(spider):
    # The "no active license" notice must not be treated as a provider_type
    # (it would pollute facility_category); it lands in status, and
    # provider_type falls back to the results-list type.
    resp = HtmlResponse(
        url=DETAIL_URL,
        body=NO_LICENSE_DETAIL.encode("utf-8"),
        request=Request(
            DETAIL_URL,
            meta={"stub": {"county": "Dane County", "provider_type": "Licensed Group"}},
        ),
    )
    item = next(spider.parse_detail(resp))
    assert item["status"] == "No active license or certificate found for this provider!"
    assert item["provider_type"] == "Licensed Group"  # from stub fallback


def test_detail_falls_back_to_stub_rating(spider):
    # A detail page with no YoungStar block should fall back to the stub value
    # captured from the results list.
    resp = HtmlResponse(
        url=DETAIL_URL,
        body=MINIMAL_DETAIL.encode("utf-8"),
        request=Request(
            DETAIL_URL,
            meta={"stub": {"county": "Iron County", "wi_youngstar_rating": "3 Stars"}},
        ),
    )
    item = next(spider.parse_detail(resp))
    assert item["wi_youngstar_rating"] == "3 Stars"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def test_showing_caption():
    # Live pager markup: "<p>Showing 1 - 50 out of 93<br><button>Next ></button>".
    sel = scrapy.Selector(
        text="<html><body><p>Showing 1 - 50 out of 93<br><button>Next &gt;</button></p></body></html>"
    )
    assert _showing(sel) == (1, 50, 93)
    # Last page.
    sel2 = scrapy.Selector(text="<p>Showing 51 - 93 out of 93</p>")
    assert _showing(sel2) == (51, 93, 93)
    # No caption (e.g. no-results page).
    assert _showing(scrapy.Selector(text="<p>No matches</p>")) is None


def test_split_city_state_zip():
    assert _split_city_state_zip("Madison WI 53717-1013") == ("Madison", "WI", "53717-1013")
    assert _split_city_state_zip("Green Bay WI 54304") == ("Green Bay", "WI", "54304")
    assert _split_city_state_zip("S Milwaukee WI 53172-1005") == ("S Milwaukee", "WI", "53172-1005")
    assert _split_city_state_zip("nonsense") == (None, None, None)
    assert _split_city_state_zip(None) == (None, None, None)


# --------------------------------------------------------------------------- #
# County map sanity
# --------------------------------------------------------------------------- #
def test_county_map():
    ids = [cid for cid, _ in COUNTIES]
    assert len(ids) == len(set(ids)), "duplicate county ids"
    assert (40, "Milwaukee County") in COUNTIES
    assert (90, "") not in COUNTIES  # id 90 does not exist
    assert len(COUNTIES) == 83  # 72 counties + 11 tribal nations

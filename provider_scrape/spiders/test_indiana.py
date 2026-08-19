import json
import os

import pytest
from scrapy.http import Request, TextResponse

from provider_scrape import normalization as norm
from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.spiders.indiana import (
    DETAIL_URL,
    PAGE_SIZE,
    SEARCH_URL,
    IndianaSpider,
    ages_from_bands,
    format_schedule,
    join_names,
    title_county,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def _load_fixture(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return json.load(fh)


@pytest.fixture
def spider():
    return IndianaSpider()


# --- response builders ------------------------------------------------- #

def detail_response(payload, pid="x", lid="y"):
    req = Request(DETAIL_URL, method="POST",
                  meta={"provider_id": pid, "location_id": lid})
    return TextResponse(url=DETAIL_URL, body=json.dumps(payload).encode(),
                        encoding="utf-8", request=req)


def search_response(payload, page=1):
    req = Request(SEARCH_URL, method="POST", meta={"page": page})
    return TextResponse(url=SEARCH_URL, body=json.dumps(payload).encode(),
                        encoding="utf-8", request=req)


def split_requests(outputs):
    """Partition parse_search output into (detail_reqs, search_reqs)."""
    details = [r for r in outputs if r.url == DETAIL_URL]
    searches = [r for r in outputs if r.url == SEARCH_URL]
    return details, searches


# --- helper unit tests ------------------------------------------------- #

def test_join_names():
    assert join_names([{"name": "Erica Ryan"}, {"name": "Sam Lee"}]) == \
        "Erica Ryan, Sam Lee"
    assert join_names([]) is None
    assert join_names(None) is None


def test_title_county():
    assert title_county("MARION") == "Marion"
    assert title_county("ST JOSEPH") == "St Joseph"
    assert title_county(None) is None
    assert title_county("  ") is None


def test_format_schedule_collapses_uniform_week():
    sched = [
        {"dayOfWeek": "Monday", "openTime": "6:30 AM", "closeTime": "6:00 PM"},
        {"dayOfWeek": "Tuesday", "openTime": "6:30 AM", "closeTime": "6:00 PM"},
        {"dayOfWeek": "Friday", "openTime": "6:30 AM", "closeTime": "6:00 PM"},
    ]
    assert format_schedule(sched) == "Monday-Friday 6:30 AM-6:00 PM"


def test_format_schedule_lists_mixed_days():
    sched = [
        {"dayOfWeek": "Monday", "openTime": "6:30 AM", "closeTime": "6:00 PM"},
        {"dayOfWeek": "Saturday", "openTime": "8:00 AM", "closeTime": "1:00 PM"},
    ]
    out = format_schedule(sched)
    assert "Monday 6:30 AM-6:00 PM" in out and "Saturday 8:00 AM-1:00 PM" in out
    assert format_schedule(None) is None
    assert format_schedule([]) is None


@pytest.mark.parametrize("bands,expected_flags,expected_ages", [
    # single infant-only band
    ([{"startAge": "Infant", "endAge": None, "quantity": 8}],
     {"infant"}, "Infant"),
    # infant through six -> all four groups
    ([{"startAge": "Infant", "endAge": "Six", "quantity": 12}],
     {"infant", "toddler", "preschool", "school"}, "Infant-Six"),
    # two through twelve -> preschool + school (no infant/toddler)
    ([{"startAge": "Two", "endAge": "Twelve", "quantity": 40}],
     {"preschool", "school"}, "Two-Twelve"),
    # None / empty -> nothing
    (None, set(), None),
])
def test_ages_from_bands(bands, expected_flags, expected_ages):
    ages, flags = ages_from_bands(bands)
    assert set(flags) == expected_flags
    assert all(v is True for v in flags.values())
    assert ages == expected_ages


# --- parse_search: pagination + fan-out -------------------------------- #

def test_parse_search_page1_fans_out_pages_and_details(spider):
    page = _load_fixture("in_search_page.json")  # totalResults 3978, 3 providers
    details, searches = split_requests(list(spider.parse_search(
        search_response(page, page=1))))
    # 3978 / 250 = 16 pages -> pages 2..16 == 15 follow-up search requests.
    assert len(searches) == 15
    assert {r.meta["page"] for r in searches} == set(range(2, 17))
    for r in searches:
        assert r.method == "POST"
    # one detail POST per provider on the page.
    assert len(details) == 3
    for r in details:
        assert r.method == "POST"
        body = json.loads(r.body)
        assert body["providerId"] and body["locationId"]
        assert "LAT" in body["coordinates"] and "LNG" in body["coordinates"]
    assert len(spider.seen) == 3


def test_parse_search_later_page_only_details(spider):
    page = _load_fixture("in_search_page.json")
    _, searches = split_requests(list(spider.parse_search(
        search_response(page, page=2))))
    assert len(searches) == 0  # no re-fan-out off a non-first page


def test_parse_search_dedupes(spider):
    page = _load_fixture("in_search_page.json")
    list(spider.parse_search(search_response(page, page=1)))
    seen_after_first = len(spider.seen)
    # feeding the same providers again yields no new detail requests.
    details, _ = split_requests(list(spider.parse_search(
        search_response(page, page=3))))
    assert details == []
    assert len(spider.seen) == seen_after_first


def test_pagesize_constant_matches_fanout():
    # Guards the arithmetic in test_parse_search_* against a PAGE_SIZE change.
    import math
    assert math.ceil(3978 / PAGE_SIZE) == 16


# --- parse_detail: golden path (rich center) --------------------------- #

def test_parse_detail_center_golden(spider):
    item = next(spider.parse_detail(detail_response(
        _load_fixture("in_detail_center.json"))))
    assert isinstance(item, ProviderItem)
    assert item["source_state"] == "Indiana"
    assert item["provider_name"] == "Abacus Childcare Center"
    assert item["license_number"] == "165836"
    assert item["in_provider_id"] == "165836"
    assert item["in_location_id"] == "22917"
    assert item["provider_type"] == "Licensed Center"
    assert item["status"] == "Open"
    assert item["state"] == "IN"
    assert item["latitude"] and item["longitude"]
    assert item["scholarships_accepted"] is True
    assert item["in_is_ccdf"] is True
    assert item["license_type"] == "Regular"
    assert item["license_begin_date"]  # normalized by the pipeline, not here
    assert item["in_ptq_level"] == "4"
    # capacity is the SUM of the licensedAges quantities.
    expected_cap = sum(
        b["quantity"] for b in
        _load_fixture("in_detail_center.json")["provider"]["location"]["licensedAges"])
    assert item["capacity"] == expected_cap
    assert isinstance(item["in_licensed_ages"], list)
    # inspections -> InspectionItem list; at least one carries a rule citation.
    assert item["inspections"] and all(
        isinstance(i, InspectionItem) for i in item["inspections"])
    assert any(i.get("in_rule_code") for i in item["inspections"])
    assert any(i.get("in_correction_date") for i in item["inspections"])
    # complaints captured separately from inspections.
    assert item["in_complaints"] and "issue" in item["in_complaints"][0]


def test_center_facility_category_via_pipeline(spider):
    item = next(spider.parse_detail(detail_response(
        _load_fixture("in_detail_center.json"))))
    norm.normalize_item(dict(item), "indiana")  # smoke: no raise
    assert norm.facility_category_from_type(item["provider_type"]) == "center"


def test_golden_item_has_no_undefined_fields(spider):
    item = next(spider.parse_detail(detail_response(
        _load_fixture("in_detail_center.json"))))
    assert dict(item)  # constructing/serializing raises on an undefined field


# --- parse_detail: sparse home (address suppressed) -------------------- #

def test_parse_detail_home_suppressed_address(spider):
    item = next(spider.parse_detail(detail_response(
        _load_fixture("in_detail_home.json"))))
    assert item["provider_type"] == "Licensed Home"
    assert norm.facility_category_from_type(item["provider_type"]) == "family_home"
    # homes omit line1/city/state -> address/city absent; zip/county/coords kept.
    assert "address" not in item
    assert "city" not in item
    assert item["state"] == "IN"
    assert item["zip"] == "46268"
    assert item["county"] == "Marion"
    assert item["latitude"] and item["longitude"]
    # single band -> exact capacity, all-ages flags set.
    assert item["capacity"] == 12
    assert item["infant"] and item["school"]


# --- parse_detail: ministry (licensedAges null, org applicant) --------- #

def test_parse_detail_ministry_null_ages(spider):
    item = next(spider.parse_detail(detail_response(
        _load_fixture("in_detail_ministry.json"))))
    assert item["provider_type"] == "Unlicensed Registered Ministry"
    assert norm.facility_category_from_type(item["provider_type"]) == "exempt"
    # licensedAges is null -> no capacity, no age flags, no ages_served.
    assert "capacity" not in item
    assert "ages_served" not in item
    for f in ("infant", "toddler", "preschool", "school"):
        assert f not in item
    # organization applicant flows into license_holder.
    assert item["license_holder"] == \
        "Anglican Cathedral Church of the Resurrection"
    assert item["license_type"] == "Registration"


# --- parse_detail: temporarily closed --------------------------------- #

def test_parse_detail_temporarily_closed(spider):
    payload = {"provider": {"id": "1", "name": "Closed Place", "location": {
        "id": "2", "providerType": "Licensed Center", "status": "Open",
        "isTemporarilyClosed": True,
        "temporarilyClosedMessage": "Closed for the season.",
        "coordinates": {"lat": 39.0, "lng": -86.0}}}}
    item = next(spider.parse_detail(detail_response(payload)))
    assert item["status"] == "Temporary Closure"
    assert item["in_is_temporarily_closed"] is True
    assert item["in_temporarily_closed_message"] == "Closed for the season."
    # pipeline maps the temporary-closure status to the `closed` bucket.
    assert norm.canonical_status(item["status"]) == "closed"


# --- parse_detail: minimal / missing data ------------------------------ #

def test_parse_detail_minimal(spider):
    item = next(spider.parse_detail(detail_response(
        {"provider": {"id": "abc", "name": "Bare Home",
                      "location": {"id": "loc1"}}})))
    assert item["provider_name"] == "Bare Home"
    assert item["license_number"] == "abc"
    assert item["state"] == "IN"
    assert item["scholarships_accepted"] is False  # isCcdf falsy
    for absent in ("phone", "address", "city", "zip", "county", "capacity",
                   "ages_served", "license_type", "in_ptq_level", "inspections",
                   "in_complaints", "hours"):
        assert absent not in item


# --- facility_category mapping (all 5 IN types) ------------------------ #

def test_indiana_status_mapping():
    # "Open" -> active; the enforcement-pending variant -> enforcement.
    assert norm.canonical_status("Open") == "active"
    assert norm.canonical_status("Open - Enforcement Pending") == "enforcement"


@pytest.mark.parametrize("provider_type,category", [
    ("Licensed Center", "center"),
    ("Licensed Home", "family_home"),
    ("Unlicensed CCDF Certified Center/School", "exempt"),
    ("Unlicensed Registered Ministry", "exempt"),
    ("Unlicensed CCDF Certified Home", "exempt"),
])
def test_indiana_facility_category_mapping(provider_type, category):
    assert norm.facility_category_from_type(provider_type) == category

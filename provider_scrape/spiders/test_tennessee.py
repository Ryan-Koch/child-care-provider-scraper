import os

import pytest
from scrapy.http import Request, TextResponse

from provider_scrape import normalization as norm
from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.spiders.tennessee import (
    DETAIL_PAGE_ID,
    PAGE_API,
    TennesseeSpider,
    build_ages_served,
    clean,
    find_dict_with,
    flatten_hours,
    to_int,
    visit_report_url,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def _load(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return fh.read()


@pytest.fixture
def spider():
    return TennesseeSpider()


# --- response builders --------------------------------------------------- #

def county_response(spider, county="Pickett", fixture="tn_county_pickett.json"):
    req = spider._county_request(county)
    return TextResponse(url=req.url, body=_load(fixture).encode(),
                        encoding="utf-8", request=req, status=200)


def detail_response(sysid, fixture, meta=None):
    m = {"handle_httpstatus_list": [401], "sysid": sysid,
         "latitude": "36.5", "longitude": "-85.1", "name": "From County"}
    if meta:
        m.update(meta)
    url = f"{PAGE_API}?id={DETAIL_PAGE_ID}&sysid={sysid}"
    req = Request(url, meta=m)
    return TextResponse(url=url, body=_load(fixture).encode(),
                        encoding="utf-8", request=req, status=200)


# --- helper unit tests --------------------------------------------------- #

def test_clean():
    assert clean("  hi  ") == "hi"
    assert clean("No Information Available") is None
    assert clean("") is None
    assert clean(None) is None


def test_to_int():
    assert to_int("38") == 38
    assert to_int("No Information Available") is None
    assert to_int("N/A") == "N/A"


def test_flatten_hours():
    hours = [{"name": "Monday", "hours": ["06:30 AM -- 05:30 PM"]},
             {"name": "Saturday", "hours": []}]
    assert flatten_hours(hours) == "Monday: 06:30 AM -- 05:30 PM"
    assert flatten_hours([]) is None
    assert flatten_hours(None) is None


def test_build_ages_served():
    assert build_ages_served("6 Week(s)", "12 Year(s)") == \
        "6 Week(s) to 12 Year(s)"
    assert build_ages_served("6 Week(s)", None) == "6 Week(s)"
    assert build_ages_served(None, None) is None


def test_find_dict_with():
    tree = {"a": [{"b": 1}, {"prvId": "9"}]}
    assert find_dict_with(tree, "prvId") == {"prvId": "9"}
    assert find_dict_with(tree, "zzz") is None


def test_visit_report_url_trims_cruft():
    raw = ("?id=cp_visit_details_maps&sysId1=abc123&name1=null&parent=X"
           "&provList=?id=cp_provider_search_results&visitsListURL=?id=y")
    assert visit_report_url(raw) == \
        "https://onedhs.tn.gov/csp?id=cp_visit_details_maps&sysId1=abc123"
    assert visit_report_url(None) is None
    # No sysId1 -> fall back to absolutizing the relative source URL.
    assert visit_report_url("?id=cp_monitoring_visits").startswith(
        "https://onedhs.tn.gov/csp?id=cp_monitoring_visits")


# --- parse_county -------------------------------------------------------- #

def test_parse_county_schedules_details(spider):
    outputs = list(spider.parse_county(county_response(spider)))
    # The Pickett fixture has 2 providers -> 2 detail requests.
    assert len(outputs) == 2
    for r in outputs:
        assert "sysid=" in r.url
        assert r.callback == spider.parse_detail
    assert len(spider.seen) == 2


def test_parse_county_dedupes(spider):
    list(spider.parse_county(county_response(spider)))
    again = list(spider.parse_county(county_response(spider)))
    assert again == []  # same sysids already seen -> nothing new


# --- parse_detail: rated golden ------------------------------------------ #

def test_parse_detail_rated(spider):
    item = next(spider.parse_detail(detail_response(
        "a1e297b5dbc10110c21143d913961921", "tn_detail_rated.json")))
    assert isinstance(item, ProviderItem)
    assert item["source_state"] == "Tennessee"
    assert "Generations" in item["provider_name"]
    assert item["license_number"] == "125878048"
    assert item["provider_type"] == "Child Care"
    assert item["status"] == "Active"          # raw; the pipeline buckets it
    assert item["county"] == "Davidson"
    assert item["capacity"] == 86
    assert item["latitude"] == "36.5"          # threaded from county meta
    assert item["tn_provider_id"] == "84171"
    assert item["tn_regulatory_agency"] == "DHS Child Care"
    assert item["tn_quality_rating"] == "91/100"
    assert "health_and_safety" in item["tn_rating_scorecard"]
    assert item["tn_rating_scorecard"]["health_and_safety"]["score"]
    assert len(item["tn_age_group_rates"]) >= 1
    assert item["tn_age_group_rates"][0]["age_group"]
    assert "AM" in item["hours"]
    assert len(item["inspections"]) >= 1
    assert isinstance(item["inspections"][0], InspectionItem)
    assert item["inspections"][0]["report_url"].startswith(
        "https://onedhs.tn.gov/csp?id=cp_visit_details_maps&sysId1=")
    assert item["provider_url"].endswith(
        "sysid=a1e297b5dbc10110c21143d913961921")


def test_rated_item_serializes(spider):
    item = next(spider.parse_detail(detail_response(
        "a1e297b5dbc10110c21143d913961921", "tn_detail_rated.json")))
    assert dict(item)  # raises if an undefined field was set


# --- parse_detail: unrated (DOE) ----------------------------------------- #

def test_parse_detail_unrated(spider):
    item = next(spider.parse_detail(detail_response(
        "12c04846dbdf0590ee34266e139619d5", "tn_detail_unrated.json")))
    assert item["provider_type"] == "DOE"
    # No rating block for an unrated provider.
    for absent in ("tn_quality_rating", "tn_rating_scorecard",
                   "tn_rating_effective_date"):
        assert absent not in item


def test_parse_detail_missing_data(spider):
    # A page with no provider data (no prvId anywhere) yields nothing.
    resp = detail_response("deadbeef", "tn_detail_rated.json")
    resp = resp.replace(body=b'{"result": {"containers": []}}')
    assert list(spider.parse_detail(resp)) == []


# --- normalization mapping ----------------------------------------------- #

@pytest.mark.parametrize("ptype,category", [
    ("Child Care", "center"),
    ("DOE", "center"),
    ("Exempt", "exempt"),
])
def test_facility_category(ptype, category):
    assert norm.facility_category_from_type(ptype) == category

import json
import os

import pytest
from scrapy.http import Request, TextResponse

from provider_scrape import normalization as norm
from provider_scrape.items import ProviderItem
from provider_scrape.spiders.north_dakota import (
    DETAIL_URL,
    MAX_DENSIFY_DEPTH,
    SEARCH_URL,
    NorthDakotaSpider,
    age_range,
    iso_date,
    join_labels,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def _load_fixture(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return json.load(fh)


@pytest.fixture
def spider():
    return NorthDakotaSpider()


# --- response builders ------------------------------------------------- #

def detail_response(profile, pid=None):
    pid = pid or profile.get("id")
    req = Request(DETAIL_URL.format(pid), meta={"id": pid})
    return TextResponse(url=req.url, body=json.dumps(profile).encode(),
                        encoding="utf-8", request=req)


def search_response(results, lat=47.5, lon=-100.0, half_mi=6.0, depth=0):
    body = json.dumps({"results": results, "limit": 100, "pageSize": 20,
                       "sort": "distance"})
    req = Request(SEARCH_URL, method="POST",
                  meta={"lat": lat, "lon": lon, "half_mi": half_mi,
                        "depth": depth})
    return TextResponse(url=SEARCH_URL, body=body.encode(),
                        encoding="utf-8", request=req)


def make_results(n, distance, start=0):
    return [
        {"id": f"id{start + i:04d}", "orgName": f"Program {start + i}",
         "locationFilterDistance": distance}
        for i in range(n)
    ]


def split_requests(outputs):
    """Partition parse_search output into (detail_requests, search_requests)."""
    details = [r for r in outputs if "search/publicSearch" not in r.url]
    searches = [r for r in outputs if "search/publicSearch" in r.url]
    return details, searches


# --- helper unit tests ------------------------------------------------- #

def test_iso_date_trims_timestamp():
    assert iso_date("2025-08-11T00:00:00Z") == "2025-08-11"
    assert iso_date("2025-08-11") == "2025-08-11"
    assert iso_date(None) is None


def test_join_labels():
    assert join_labels(["English", "Spanish"]) == "English, Spanish"
    assert join_labels([]) is None
    assert join_labels(None) is None


def test_age_range():
    assert age_range(0, "Months") == "0 Months"
    assert age_range(12, "Years") == "12 Years"
    assert age_range(None, "Years") is None


# --- parse_detail: golden path ----------------------------------------- #

def test_parse_detail_golden(spider):
    item = next(spider.parse_detail(detail_response(_load_fixture("nd_profile.json"))))
    assert isinstance(item, ProviderItem)
    assert item["source_state"] == "North Dakota"
    assert item["provider_name"] == "701 Monkey Business"
    assert item["license_number"] == "4-19-00384-H"
    assert item["provider_type"] == "HHS-Licensed Group Child Care Facility"
    # parse_detail emits the raw status; the pipeline maps it to a bucket.
    assert item["status"] == "Active"
    assert item["address"] == "303 1st Ave W"
    assert item["city"] == "Carson"
    assert item["state"] == "ND"
    assert item["zip"] == "58529"
    assert item["county"] == "Grant"
    assert item["latitude"] == "46.4198158"
    assert item["longitude"] == "-101.5680527"
    assert item["phone"] == "701-622-3785"
    assert item["email"] == "hill@westriv.com"
    assert item["administrator"] == "Evelyn Alt"
    assert item["capacity"] == 20
    assert item["hours"] == "Mon-Fri"
    assert item["infant"] and item["toddler"] and item["preschool"] and item["school"]
    assert item["scholarships_accepted"] is True
    assert item["license_begin_date"] == "2025-08-11"
    assert item["license_expiration"] == "2026-08-10"
    assert item["languages"] == "English"
    assert item["head_start"] is False
    assert item["accepting_new_children"] is False  # totalVacancies == 0
    assert item["provider_url"].endswith(
        "(slide-full:68484324fa1ed3f5f772600a/profile)")
    # nd_ fields
    assert item["nd_quality_rating"] == "Step 1"
    assert item["nd_total_vacancies"] == 0
    assert len(item["nd_vacancies_by_age"]) == 4
    assert item["nd_vacancies_by_age"][0] == {
        "age_group": "Infant (0-17 months)", "vacancies": 0}
    assert item["nd_min_age"] == "0 Months"
    assert item["nd_max_age"] == "12 Years"
    assert item["nd_program_id"] == "68484324fa1ed3f5f772600a"
    assert item["nd_org_id"] == "27035"


def test_golden_item_has_no_undefined_fields(spider):
    # Constructing/serializing the item would raise if an undefined field were set.
    item = next(spider.parse_detail(detail_response(_load_fixture("nd_profile.json"))))
    assert dict(item)  # no KeyError on unknown field


# --- parse_detail: missing / null data --------------------------------- #

def test_parse_detail_minimal(spider):
    item = next(spider.parse_detail(detail_response(
        {"id": "abc123", "orgName": "Tiny Home Daycare"})))
    assert item["provider_name"] == "Tiny Home Daycare"
    assert item["status"] == "Active"          # default when not deactivated
    assert item["head_start"] is False         # always set (boolean field)
    assert item["nd_program_id"] == "abc123"
    # Optional fields must be absent (not set to None) when the source lacks them.
    for absent in ("phone", "email", "capacity", "ages_served", "infant",
                   "toddler", "license_begin_date", "accepting_new_children",
                   "nd_quality_rating", "nd_total_vacancies"):
        assert absent not in item


def test_parse_detail_null_values_do_not_set(spider):
    profile = {"id": "x", "orgName": "N", "website": None, "address2": None,
               "qualityRating": None, "qualityRatingLabel": None,
               "ageGroupsServed": [], "contactPhone": "  "}
    item = next(spider.parse_detail(detail_response(profile)))
    assert "provider_website" not in item
    assert "phone" not in item           # whitespace-only -> not set
    assert "nd_quality_rating" not in item
    assert "infant" not in item          # empty ageGroupsServed


def test_status_deactivated(spider):
    item = next(spider.parse_detail(detail_response(
        {"id": "x", "orgName": "Gone", "deactivated": True})))
    assert item["status"] == "Closed"


@pytest.mark.parametrize("profile,expected", [
    ({"id": "x", "orgName": "n", "facilityTypeLabel": "Head Start Site"}, True),
    ({"id": "x", "orgName": "n", "headStartGranteeId": "G123"}, True),
    ({"id": "x", "orgName": "n"}, False),
])
def test_head_start(spider, profile, expected):
    item = next(spider.parse_detail(detail_response(profile)))
    assert item["head_start"] is expected


@pytest.mark.parametrize("total,expected_key,expected_val", [
    (5, "accepting_new_children", True),
    (0, "accepting_new_children", False),
])
def test_accepting_new_children(spider, total, expected_key, expected_val):
    item = next(spider.parse_detail(detail_response(
        {"id": "x", "orgName": "n", "totalVacancies": total})))
    assert item[expected_key] is expected_val
    assert item["nd_total_vacancies"] == total


def test_age_group_booleans_partial(spider):
    # ageGroupsServed [1, 4] -> infant + school only.
    item = next(spider.parse_detail(detail_response(
        {"id": "x", "orgName": "n", "ageGroupsServed": [1, 4]})))
    assert item["infant"] is True
    assert item["toddler"] is False
    assert item["preschool"] is False
    assert item["school"] is True


# --- parse_search: enumeration + densification ------------------------- #

def test_search_unsaturated_no_densify(spider):
    outputs = list(spider.parse_search(search_response(make_results(30, 4.0))))
    details, searches = split_requests(outputs)
    assert len(details) == 30
    assert len(searches) == 0
    assert len(spider.seen) == 30


def test_search_saturated_densifies(spider):
    # 100 results within 3mi at half=6 (corner 8.49mi) -> not covered -> split 4.
    outputs = list(spider.parse_search(search_response(
        make_results(100, 3.0), half_mi=6.0, depth=0)))
    details, searches = split_requests(outputs)
    assert len(details) == 100
    assert len(searches) == 4
    for s in searches:
        assert s.method == "POST"
        assert s.meta["depth"] == 1
        assert s.meta["half_mi"] == 3.0


def test_search_saturated_but_covered_no_densify(spider):
    # 100 results but the farthest (9mi) exceeds the cell corner (8.49mi):
    # the cell is fully covered, so no subdivision.
    outputs = list(spider.parse_search(search_response(
        make_results(100, 9.0), half_mi=6.0, depth=0)))
    details, searches = split_requests(outputs)
    assert len(details) == 100
    assert len(searches) == 0


def test_search_depth_cap_stops_densifying(spider):
    outputs = list(spider.parse_search(search_response(
        make_results(100, 1.0), half_mi=4.0, depth=MAX_DENSIFY_DEPTH)))
    _, searches = split_requests(outputs)
    assert len(searches) == 0  # at the depth cap, do not subdivide further


def test_search_dedupes_across_nodes(spider):
    r1 = make_results(30, 4.0, start=0)
    r2 = make_results(30, 4.0, start=15)  # ids 15-29 overlap
    first = split_requests(list(spider.parse_search(search_response(r1))))[0]
    second = split_requests(list(spider.parse_search(search_response(r2))))[0]
    assert len(first) == 30
    assert len(second) == 15   # only the new ids
    assert len(spider.seen) == 45


# --- ND facility_category mapping (normalization) ---------------------- #

@pytest.mark.parametrize("facility_type,category", [
    ("HHS-Licensed Child Care Center", "center"),
    ("HHS-Licensed Group Child Care Facility", "center"),
    ("HHS-Licensed Preschool", "center"),
    ("HHS Four-Year Old Program", "center"),
    ("Head Start Site", "center"),
    ("HHS-Licensed Group Child Care Home", "group_home"),
    ("HHS-Licensed Family Child Care", "family_home"),
    ("HHS-Licensed School Age Child Care", "school_age"),
    ("Self-Declared Provider", "exempt"),
    ("HHS-Licensed Multiple License", "other"),
    ("Tribal Subsidy Recipient", "other"),
])
def test_nd_facility_category_mapping(facility_type, category):
    assert norm.facility_category_from_type(facility_type) == category

import json
import os

import pytest
from scrapy.http import Request, TextResponse

from provider_scrape import normalization as norm
from provider_scrape.items import ProviderItem
from provider_scrape.spiders.louisiana import (
    DETAIL_URL,
    SEARCH_URL,
    LouisianaSpider,
    age_flags_from_grades,
    compose_provider_type,
    format_hours,
    is_affirmative,
    is_real_coordinate,
    normalize_parish,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def _load_fixture(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return json.load(fh)


@pytest.fixture
def spider():
    return LouisianaSpider()


@pytest.fixture(scope="module")
def search_fixture():
    return _load_fixture("la_search.json")


def _summary_by_id(search_fixture, unique_id):
    for record in search_fixture["result"]:
        if record["uniqueId"] == unique_id:
            return record
    raise KeyError(unique_id)


# --- response builders ------------------------------------------------- #


def search_response(payload):
    req = Request(SEARCH_URL)
    return TextResponse(url=SEARCH_URL, body=json.dumps(payload).encode(), encoding="utf-8", request=req)


def detail_response(payload, unique_id, summary):
    url = DETAIL_URL.format(unique_id=unique_id)
    req = Request(url, meta={"summary": summary})
    return TextResponse(url=url, body=json.dumps(payload).encode(), encoding="utf-8", request=req)


def detail_fixture_response(detail_fixture_name, unique_id, search_fixture):
    """Build a detail response wiring a detail fixture to its real search summary."""
    payload = _load_fixture(detail_fixture_name)
    summary = _summary_by_id(search_fixture, unique_id)
    return detail_response(payload, unique_id, summary)


# --- helper unit tests --------------------------------------------------- #


@pytest.mark.parametrize(
    "sa,expected",
    [
        ({"earlyEdLicenseType": "I"}, "Type I"),
        ({"earlyEdLicenseType": "II"}, "Type II"),
        ({"earlyEdLicenseType": "III"}, "Type III"),
        ({"earlyEdLicenseType": "R (Family Home)"}, "Family Child Care Home"),
        ({"earlyEdLicenseType": "", "earlyEdPublicSchoolStatus": "Public School"}, "Public School"),
        # school (entityType), no license, no public-school status = a private/
        # parochial/special school with a PreK program -> "Private School"
        # (-> center). Covers both the BOTH private schools and the lone EE
        # "special school". Ryan 2026-09-08.
        ({"earlyEdLicenseType": "", "earlyEdPublicSchoolStatus": "", "entityType": "school"}, "Private School"),
        # malformed/empty record with no entityType -> unset (the guard path).
        ({"earlyEdLicenseType": "", "earlyEdPublicSchoolStatus": ""}, None),
        # unmapped license type -> passed through verbatim so the pipeline's
        # "other" fallback + warning surfaces it (Sec 4.2).
        ({"earlyEdLicenseType": "IV"}, "Type IV"),
    ],
)
def test_compose_provider_type(sa, expected):
    assert compose_provider_type(sa) == expected


@pytest.mark.parametrize(
    "grades,expected",
    [
        ("Infant/Toddler/PreK", {"infant", "toddler", "preschool"}),
        ("Toddler/PreK", {"toddler", "preschool"}),
        ("PreK", {"preschool"}),
        ("Infant/Toddler/PreK/School Age", {"infant", "toddler", "preschool", "school"}),
        ("", set()),
        (None, set()),
    ],
)
def test_age_flags_from_grades(grades, expected):
    flags = age_flags_from_grades(grades)
    assert set(flags) == expected
    assert all(v is True for v in flags.values())


def test_format_hours_joins_populated_days_only():
    sa = {
        "earlyEdMonday": "",
        "earlyEdTuesday": "Tue: 07:30am to 04:00pm",
        "earlyEdWednesday": "Wed: 07:30am to 04:00pm",
        "earlyEdThursday": "Thu: 07:30am to 04:00pm",
        "earlyEdFriday": "Fri: 07:30am to 04:00pm",
        "earlyEdSaturday": "",
        "earlyEdSunday": "",
    }
    hours = format_hours(sa)
    assert hours == (
        "Tue: 07:30am to 04:00pm; Wed: 07:30am to 04:00pm; Thu: 07:30am to 04:00pm; Fri: 07:30am to 04:00pm"
    )


def test_format_hours_all_closed_is_none():
    assert format_hours({}) is None


@pytest.mark.parametrize(
    "value,expected",
    [
        ("YES", True),
        ("Yes", True),
        ("yes", True),
        ("NO", False),
        ("No", False),
        ("", False),
        (None, False),
    ],
)
def test_is_affirmative_case_insensitive(value, expected):
    assert is_affirmative(value) is expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Acadia Parish", "Acadia"),  # suffixed -> bare
        ("Acadia", "Acadia"),  # already bare -> unchanged
        ("acadia parish", "acadia"),  # case-insensitive suffix strip
        ("West Baton Rouge Parish", "West Baton Rouge"),  # multi-word parish
        ("Zachary", "Zachary"),  # municipal district, not a parish -> kept bare
        ("Zachary Parish", "Zachary"),  # bogus source parish -> normalized to bare
        ("", None),  # empty -> None
        (None, None),  # None -> None
    ],
)
def test_normalize_parish(raw, expected):
    assert normalize_parish(raw) == expected


@pytest.mark.parametrize(
    "latitude,longitude,expected",
    [
        ("30.412230", "-92.224190", True),
        ("", "", False),
        (None, None, False),
        ("30.1", "", False),
        # the (0, 0) null-island sentinel observed live on one record
        # (VIR001) -- a non-empty but bogus placeholder, distinct from an
        # empty string.
        ("0.000000", "0.000000", False),
        ("0", "0", False),
    ],
)
def test_is_real_coordinate(latitude, longitude, expected):
    assert is_real_coordinate(latitude, longitude) is expected


# --- case 1: parse_search filter ---------------------------------------- #


def test_parse_search_filters_ee_both_and_skips_k12(spider, search_fixture):
    requests = list(spider.parse_search(search_response(search_fixture)))
    kept_ids = {r.url.rsplit("/", 1)[1] for r in requests}
    # la_search.json: EE 001030/001031/ZHW001, BOTH 001002/003011 kept;
    # K12 001/Nation/LA skipped.
    assert kept_ids == {"001030", "001031", "001002", "003011", "ZHW001"}
    assert spider.kept == 5
    assert spider.skipped == 3
    for r in requests:
        assert r.dont_filter is True
        assert "summary" in r.meta
        assert r.meta["summary"]["uniqueId"] in kept_ids


def test_parse_search_dedupes_on_repeat(spider, search_fixture):
    list(spider.parse_search(search_response(search_fixture)))
    seen_after_first = len(spider.seen)
    requests = list(spider.parse_search(search_response(search_fixture)))
    assert requests == []
    assert len(spider.seen) == seen_after_first


# --- case 2: golden path -- licensed Type III center w/ Head Start ------ #


def test_parse_detail_licensed_center_golden(spider, search_fixture):
    resp = detail_fixture_response("la_detail_center.json", "001030", search_fixture)
    item = next(spider.parse_detail(resp))
    assert isinstance(item, ProviderItem)
    assert item["source_state"] == "Louisiana"
    assert item["provider_name"] == "AP Church Point Head Start Center"
    assert item["license_number"] == "3962"
    assert item["provider_type"] == "Type III"
    assert norm.facility_category_from_type(item["provider_type"]) == "center"
    assert item["la_licensed"] == "Licensed"
    assert item["la_unique_id"] == "001030"
    assert item["la_profile_type"] == "EE"
    assert item["address"] == "693 N Main St, Church Point, LA 70525"
    assert item["city"] == "Church Point"
    assert item["state"] == "LA"
    assert item["zip"] == "70525"
    assert item["county"] == "Acadia"
    assert item["latitude"] == "30.412230"
    assert item["longitude"] == "-92.224190"
    assert item["geocode_source"] == "state"
    assert item["head_start"] is True
    assert item["administrator"] == "Cynthia Scott"
    assert item["hours"]
    # earlyEdGrades == "Toddler/PreK" -> toddler + preschool only.
    assert "infant" not in item
    assert item["toddler"] is True
    assert item["preschool"] is True
    assert "school" not in item
    assert item["provider_url"] == "https://louisianaschools.com/001030/ec-school-about"
    assert item["la_star_rating"] == "4 Stars"
    assert item["la_performance_rating"] == "High Proficient"
    assert item["la_inspection_url"] == "http://carefacility.doe.louisiana.gov/view.aspx?id=3962&type=B"
    # the static shared monitoring-checklist doc is never emitted.
    assert "la_inspection_list" not in item
    assert "inspections" not in item


def test_golden_item_has_no_undefined_fields(spider, search_fixture):
    resp = detail_fixture_response("la_detail_center.json", "001030", search_fixture)
    item = next(spider.parse_detail(resp))
    assert dict(item)  # constructing/serializing raises on an undefined field


# --- case 3: family home, License Exempt --------------------------------- #


def test_parse_detail_family_home(spider, search_fixture):
    resp = detail_fixture_response("la_detail_family_home.json", "ZHW001", search_fixture)
    item = next(spider.parse_detail(resp))
    assert item["provider_type"] == "Family Child Care Home"
    assert norm.facility_category_from_type(item["provider_type"]) == "family_home"
    assert item["la_licensed"] == "License Exempt"
    assert item["scholarships_accepted"] is True  # CCAP "YES"
    assert item["la_before_care"] is True
    assert item["la_after_care"] is False
    assert item["provider_name"] == "Selina Faye Willis"
    assert item["license_number"] == "60915"
    assert item["county"] == "Caddo"


# --- case 4: unlicensed public-school pre-K (BOTH, no license) ---------- #


def test_parse_detail_unlicensed_public_school(spider, search_fixture):
    resp = detail_fixture_response("la_detail_public_school.json", "001002", search_fixture)
    item = next(spider.parse_detail(resp))
    assert "license_number" not in item
    assert "la_licensed" not in item
    assert item["la_public_school_status"] == "Public School"
    assert item["provider_type"] == "Public School"
    assert norm.facility_category_from_type(item["provider_type"]) == "center"
    assert item["la_profile_type"] == "BOTH"
    assert item["la_unique_id"] == "001002"  # durable id survives with no license
    # still emitted (Sec 4.1) -- not dropped for lack of a license.
    assert item["provider_name"] == "Branch Elementary School"


# --- case 5: BOTH school operating a licensed Type III center ----------- #


def test_parse_detail_both_licensed_center(spider, search_fixture):
    resp = detail_fixture_response("la_detail_both_licensed.json", "003011", search_fixture)
    item = next(spider.parse_detail(resp))
    assert item["provider_type"] == "Type III"
    assert norm.facility_category_from_type(item["provider_type"]) == "center"
    assert item["la_profile_type"] == "BOTH"
    assert item["license_number"] == "51812"
    assert item["la_licensed"] == "Licensed"
    # earlyEdTipsNumber == "0" on this fixture -- treated as absent (Sec 6.3).
    assert "la_tips_number" not in item


# --- case 6: coordinates -- fallback + missing --------------------------- #


def test_coordinates_from_detail(spider, search_fixture):
    resp = detail_fixture_response("la_detail_center.json", "001030", search_fixture)
    item = next(spider.parse_detail(resp))
    assert item["latitude"] == "30.412230"
    assert item["longitude"] == "-92.224190"
    assert item["geocode_source"] == "state"


def test_coordinates_fallback_to_summary_when_detail_blank(spider):
    payload = {"result": {"schoolAbout": {"addressLatitude": "", "addressLongitude": ""}, "leftPanel": {}}}
    summary = {"uniqueId": "X1", "name": "Fallback Test", "profileType": "EE", "latitude": "30.1", "longitude": "-92.1"}
    item = next(spider.parse_detail(detail_response(payload, "X1", summary)))
    assert item["latitude"] == "30.1"
    assert item["longitude"] == "-92.1"
    assert item["geocode_source"] == "state"


def test_coordinates_unset_when_both_blank(spider):
    payload = {"result": {"schoolAbout": {"addressLatitude": "", "addressLongitude": ""}, "leftPanel": {}}}
    summary = {"uniqueId": "X2", "name": "No Coords", "profileType": "EE", "latitude": "", "longitude": ""}
    item = next(spider.parse_detail(detail_response(payload, "X2", summary)))
    assert "latitude" not in item
    assert "longitude" not in item
    assert "geocode_source" not in item


def test_coordinates_unset_on_null_island_sentinel(spider):
    # Live-observed on VIR001: a non-empty "0.000000"/"0.000000" placeholder
    # rather than an empty string -- must not be emitted as a real point.
    payload = {
        "result": {"schoolAbout": {"addressLatitude": "0.000000", "addressLongitude": "0.000000"}, "leftPanel": {}}
    }
    summary = {"uniqueId": "X4", "name": "Null Island", "profileType": "EE", "latitude": "", "longitude": ""}
    item = next(spider.parse_detail(detail_response(payload, "X4", summary)))
    assert "latitude" not in item
    assert "longitude" not in item
    assert "geocode_source" not in item


# --- case 7: age flags / ages_served (earlyEdGrades, not gradeservedcurrentsy) #


def test_ages_served_uses_early_ed_grades_not_whole_school_grades(spider):
    payload = {
        "result": {
            "schoolAbout": {
                "earlyEdGrades": "Toddler/PreK",
                "gradeservedcurrentsy": "PK (Ages 3-4)-Grade 8",  # whole-school span; must NOT leak
            },
            "leftPanel": {},
        }
    }
    summary = {"uniqueId": "X3", "name": "Both School", "profileType": "BOTH"}
    item = next(spider.parse_detail(detail_response(payload, "X3", summary)))
    assert item["ages_served"] == "Toddler/PreK"
    assert item["toddler"] is True
    assert item["preschool"] is True
    assert "school" not in item  # the whole-school grade span never leaks in
    assert "infant" not in item


# --- case 8: binary flags casing ----------------------------------------- #


def test_binary_flags_casing(spider, search_fixture):
    resp = detail_fixture_response("la_detail_family_home.json", "ZHW001", search_fixture)
    item = next(spider.parse_detail(resp))
    assert item["scholarships_accepted"] is True  # earlyEdCcapAvailableBinary "YES"
    assert item["transportation"] == "No"  # earlyEdTransportationBinaryFormatted


def test_binary_flags_casing_negative(spider, search_fixture):
    resp = detail_fixture_response("la_detail_center.json", "001030", search_fixture)
    item = next(spider.parse_detail(resp))
    assert item["scholarships_accepted"] is False  # earlyEdCcapAvailableBinary "NO"
    assert item["transportation"] == "Yes"


# --- case 9: inspectionList is a static doc, never emitted -------------- #


def test_inspection_list_never_emitted(spider, search_fixture):
    for fixture, unique_id in (
        ("la_detail_center.json", "001030"),
        ("la_detail_family_home.json", "ZHW001"),
        ("la_detail_both_licensed.json", "003011"),
    ):
        resp = detail_fixture_response(fixture, unique_id, search_fixture)
        item = next(spider.parse_detail(resp))
        assert "inspections" not in item
        assert all("checklist" not in str(v).lower() for k, v in dict(item).items() if k.startswith("la_"))
        assert item["la_inspection_url"]  # the real per-provider link IS kept


# --- case 10: facility_category mapping (parametrized) ------------------ #


@pytest.mark.parametrize(
    "provider_type,category",
    [
        ("Type I", "center"),
        ("Type II", "center"),
        ("Type III", "center"),
        ("Family Child Care Home", "family_home"),
        ("Public School", "center"),
    ],
)
def test_facility_category_mapping(provider_type, category):
    assert norm.facility_category_from_type(provider_type) == category


# --- case 11: no undefined item fields (guards la_* typos) -------------- #


def test_no_undefined_item_fields_across_archetypes(spider, search_fixture):
    for fixture, unique_id in (
        ("la_detail_center.json", "001030"),
        ("la_detail_family_home.json", "ZHW001"),
        ("la_detail_public_school.json", "001002"),
        ("la_detail_both_licensed.json", "003011"),
    ):
        resp = detail_fixture_response(fixture, unique_id, search_fixture)
        item = next(spider.parse_detail(resp))
        assert dict(item)  # raises on an undefined field name


def test_undefined_field_raises():
    item = ProviderItem()
    with pytest.raises(KeyError):
        item["la_not_a_real_field"] = "boom"


# --- closed() logging ----------------------------------------------------- #


def test_closed_licensed_vs_unlicensed_split(spider, search_fixture):
    list(spider.parse_search(search_response(search_fixture)))
    for fixture, unique_id in (
        ("la_detail_center.json", "001030"),
        ("la_detail_family_home.json", "ZHW001"),
        ("la_detail_public_school.json", "001002"),
        ("la_detail_both_licensed.json", "003011"),
    ):
        resp = detail_fixture_response(fixture, unique_id, search_fixture)
        list(spider.parse_detail(resp))
    assert spider.licensed_count == 3  # center, family_home, both_licensed
    assert spider.unlicensed_count == 1  # public_school
    spider.closed("finished")  # smoke: no raise

import os

import pytest
from scrapy.http import Request, TextResponse

from provider_scrape import normalization as norm
from provider_scrape.spiders.washington_dc import (
    DETAIL_URL,
    LIST_URL,
    WashingtonDcSpider,
    _clean,
    _has_value,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def _load(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return fh.read()


@pytest.fixture
def spider():
    return WashingtonDcSpider()


def list_response(body=None):
    body = body if body is not None else _load("dc_list.html")
    req = Request(LIST_URL, method="POST")
    return TextResponse(url=LIST_URL, body=body.encode(), encoding="utf-8",
                        request=req)


def detail_response(fixture, meta):
    req = Request(DETAIL_URL, method="POST", meta=meta)
    return TextResponse(url=DETAIL_URL, body=_load(fixture).encode(),
                        encoding="utf-8", request=req)


def raw_detail_response(body, meta):
    req = Request(DETAIL_URL, method="POST", meta=meta)
    return TextResponse(url=DETAIL_URL, body=body.encode(), encoding="utf-8",
                        request=req)


# A minimal meta as parse_list would produce, for driving parse_detail directly.
def meta_for(fid, **over):
    base = {
        "fid": fid,
        "name": "Test Center",
        "list_address": "1 Test ST, NW 20001",
        "phone": "(202) 555-0100",
        "latitude": "38.9",
        "longitude": "-77.0",
        "badges": set(),
    }
    base.update(over)
    return base


# --- helper unit tests ------------------------------------------------- #

def test_clean_collapses_whitespace():
    assert _clean("  a\n  b ") == "a b"
    assert _clean("   ") is None
    assert _clean(None) is None


def test_has_value_treats_placeholders_as_empty():
    assert _has_value("8") is True
    assert _has_value("No Data Available") is False
    assert _has_value("Not Applicable") is False
    assert _has_value("") is False
    assert _has_value(None) is False


def test_zip_extracts_five_digits():
    assert WashingtonDcSpider._zip("3414 18th ST, NE, DC 20018") == "20018"
    assert WashingtonDcSpider._zip("no zip here") is None
    assert WashingtonDcSpider._zip(None) is None


def test_age_field_maps_prefixes():
    m = WashingtonDcSpider._age_field
    assert m("Infant (0 - 12 Months)") == "infant"
    assert m("School Age (5 - 19 Years)") == "school"
    assert m("Nonsense") is None


# --- parse_list -------------------------------------------------------- #

def test_parse_list_yields_a_detail_request_per_card(spider):
    reqs = list(spider.parse_list(list_response()))
    assert len(reqs) == 4
    assert all(r.url == DETAIL_URL and r.method == "POST" for r in reqs)


def test_parse_list_extracts_card_fields_and_badges(spider):
    reqs = {r.meta["fid"]: r for r in spider.parse_list(list_response())}
    r = reqs["2899"]
    assert r.meta["name"] == "18th Street Early Learning Child Development Center"
    assert r.meta["phone"] == "(202) 921-9525"
    assert r.meta["list_address"] == "3414 18th ST, NE 20018"
    assert r.meta["latitude"] == "38.9325512"
    assert r.meta["longitude"] == "-76.9798796"
    assert "Accepts Subsidies" in r.meta["badges"]
    assert "Capital Quality Participant" in r.meta["badges"]
    # The POST body carries the facility id.
    assert b"facilityID=2899" in reqs["2899"].body


def test_parse_list_unescapes_entities_in_name(spider):
    reqs = {r.meta["fid"]: r for r in spider.parse_list(list_response())}
    assert reqs["2000"].meta["name"] == "Barbara Chambers Children's Center"


# --- parse_detail: golden path (center with full enrollment) ----------- #

def test_parse_detail_center_golden(spider):
    meta = meta_for(
        "2899",
        name="18th Street Early Learning Child Development Center",
        list_address="3414 18th ST, NE 20018",
        phone="(202) 921-9525",
        latitude="38.9325512", longitude="-76.9798796",
        badges={"Accepts Subsidies", "Capital Quality Participant",
                "Participating in Pay Equity Fund"},
    )
    item = next(spider.parse_detail(detail_response("dc_detail.html", meta)))

    assert item["source_state"] == "Washington DC"
    assert item["license_number"] == "2899"
    assert item["provider_url"].endswith("FacilityProfile?FacilityId=2899")
    assert item["provider_name"] == \
        "18th Street Early Learning Child Development Center"
    assert item["phone"] == "(202) 921-9525"
    assert item["latitude"] == "38.9325512"
    assert item["longitude"] == "-76.9798796"

    # Address components set explicitly (quadrant must not become city).
    assert item["city"] == "Washington"
    assert item["state"] == "DC"
    assert item["zip"] == "20018"

    assert item["provider_type"] == "CDC (Child Development Center)"
    assert item["administrator"] == "Anora Goldring"
    assert item["email"] == "mstearlylearningcdc@gmail.com"
    assert item["dc_capital_quality_designation"] == "Progressing"
    assert item["ages_served"] == "0 - 5 years"
    assert item["capacity"] == "27"
    assert item["scholarships_accepted"] == "Yes"
    assert item["languages"] == "English"  # "Not Applicable" dropped
    assert item["hours"].startswith("Mon 07:00 AM-06:00 PM")
    assert "Sun Closed" in item["hours"]

    # Enrollment table.
    enr = item["dc_enrollment"]
    assert len(enr) == 4
    assert enr[0] == {
        "age_group": "Infant (0 - 12 Months)", "openings": "0",
        "current_enrollment": "4", "desired_enrollment": "8",
        "monthly_tuition": "No Data Available",
    }
    # Age booleans derived from the enrollment rows.
    assert item["infant"] is True
    assert item["toddler"] is True
    assert item["preschool"] is True
    assert item["school"] is False  # all "Not Applicable"

    # Badges.
    assert item["dc_capital_quality_participant"] is True
    assert item["dc_pay_equity_fund"] is True
    assert item["dc_prek_enhancement"] is False
    assert item["dc_nontraditional_hours"] is False
    assert "meals" not in item or item.get("meals") is None
    assert item.get("curriculum") is None


# --- parse_detail: home type (CDX) ------------------------------------- #

def test_parse_detail_home_type(spider):
    meta = meta_for("2980", name="A Place to Grow Child Development Home",
                    badges=set())
    item = next(spider.parse_detail(detail_response("dc_detail_home.html", meta)))
    assert item["provider_type"] == "CDX (Child Development Home Expanded)"
    assert item["administrator"] == "Sharde Bushrod"
    assert item["capacity"] == "9"
    assert item["dc_capital_quality_designation"] == "Preliminary"


# --- badge mapping to common fields ------------------------------------ #

def test_food_and_montessori_badges_map_to_common_fields(spider):
    meta = meta_for("2899", badges={"Child and Adult Care Food Program",
                                     "Montessori", "Pre-K Enhancement",
                                     "Nontraditional"})
    item = next(spider.parse_detail(detail_response("dc_detail.html", meta)))
    assert item["meals"] == "Child and Adult Care Food Program"
    assert item["curriculum"] == "Montessori"
    assert item["dc_prek_enhancement"] is True
    assert item["dc_nontraditional_hours"] is True


# --- parse_detail: missing / sparse data ------------------------------- #

SPARSE_DETAIL = """
<div id="facilityNameDisplay" facilityid="9999">
  <span class="ProximaSoft-Semibold">Sparse Facility</span>
  <span>1 Nowhere RD, SE , DC 20099</span>
  <span><img/>&nbsp;Facility Type:&nbsp;CDC (Child Development Center)</span>
</div>
"""


def test_parse_detail_missing_sections_are_none(spider):
    meta = meta_for("9999", name="Sparse Facility",
                    list_address="1 Nowhere RD, SE 20099")
    item = next(spider.parse_detail(raw_detail_response(SPARSE_DETAIL, meta)))
    # Present.
    assert item["provider_name"] == "Sparse Facility"
    assert item["provider_type"] == "CDC (Child Development Center)"
    assert item["zip"] == "20099"
    # Absent detail sections must not raise and stay None/absent.
    assert item.get("dc_capital_quality_designation") is None
    assert item.get("ages_served") is None
    assert item.get("capacity") is None
    assert item.get("hours") is None
    assert item.get("dc_enrollment") is None
    assert item.get("languages") is None
    # Badges default to False when the card had none.
    assert item["dc_capital_quality_participant"] is False


# --- normalization integration ----------------------------------------- #

@pytest.mark.parametrize("ptype,expected", [
    ("CDC (Child Development Center)", "center"),
    ("CDH (Child Development Home)", "family_home"),
    ("CDX (Child Development Home Expanded)", "group_home"),
])
def test_facility_category_mapping(ptype, expected):
    assert norm.facility_category_from_type(ptype) == expected

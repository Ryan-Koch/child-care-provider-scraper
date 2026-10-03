import copy
import json
import os

import pytest
from scrapy.exceptions import CloseSpider
from scrapy.http import Request, TextResponse

from provider_scrape import normalization as norm
from provider_scrape.spiders.wyoming import (
    DEFAULT_LEAD_ID,
    WyomingSpider,
    build_item,
    find_centroid_points,
    fix_mojibake,
    format_age,
    format_ages,
    format_hours,
    hours_schedule,
    inspection_inspection,
    number,
    parse_directors,
    visit_inspection,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")

ECARES = "n1vovbbml5fc7ng"
CENTROID_PEER = "u246326dwwc2zlh"  # shares ECARES's point at a different street
RICH = "hmw3i8hym62mz6l"


def _load(name):
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as fh:
        return json.load(fh)


def _response(body, request=None, status=200):
    url = request.url if request else "https://childcare.dfs.wyo.gov/x"
    return TextResponse(
        url=url,
        body=json.dumps(body).encode("utf-8"),
        encoding="utf-8",
        status=status,
        request=request or Request(url=url),
    )


@pytest.fixture
def spider():
    return WyomingSpider()


@pytest.fixture
def search_records():
    return {f["facilityId"]: f for f in _load("wy_search_sample.json")["data"]["facilities"]}


@pytest.fixture
def centroids(search_records):
    return find_centroid_points(search_records.values())


def _facility(name):
    return copy.deepcopy(_load(name)["data"]["facility"])


def _drain(gen):
    return list(gen)


def _feed_search(spider, records):
    """Pretend the sweep finished with this union and return Phase 2 requests."""
    spider.facilities = dict(records)
    spider.zips_pending = 1
    return _drain(spider._zip_finished())


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #


def test_number_handles_both_shapes():
    assert number(25.0) == 25.0
    assert number({"source": "50.0", "parsedValue": 50}) == 50
    assert number(None) is None


@pytest.mark.parametrize(
    "months,expected",
    [(0, "0 months"), (12, "12 months"), (18, "18 months"), (24, "2 years"), (36, "3 years"), (144, "12 years")],
)
def test_format_age_boundaries(months, expected):
    assert format_age(months) == expected


def test_format_ages_and_missing():
    assert format_ages({"minAge": 0, "maxAge": 144}) == "0 months - 12 years"
    assert format_ages({"minAge": 36, "maxAge": 60}) == "3 years - 5 years"
    assert format_ages({"minAge": 0}) is None
    assert format_ages(None) is None


def test_hours_skip_closed_days():
    hours = [
        {"weekday": "Monday", "startTime": "06:00", "endTime": "18:00"},
        {"weekday": "Tuesday"},
        {"weekday": "Wednesday", "startTime": "07:00", "endTime": "12:00"},
    ]
    schedule = hours_schedule(hours)
    assert schedule == [
        {"day": "Monday", "start": "06:00", "end": "18:00"},
        {"day": "Wednesday", "start": "07:00", "end": "12:00"},
    ]
    assert format_hours(schedule) == "Monday 06:00-18:00; Wednesday 07:00-12:00"
    assert hours_schedule(None) == []


def test_directors_dedupe_and_titlecase():
    directors = parse_directors(
        [
            {"firstname": "jamie", "lastname": "shannon", "title": "Director"},
            {"firstname": "Jamie", "lastname": "Shannon", "title": "DIRECTOR"},
            {"firstname": "jamie", "lastname": "shannon", "title": "Director/Principal"},
            {"firstname": "McKay", "lastname": "SMITH", "title": "Director"},
            {"firstname": "", "lastname": "", "title": "Director"},
        ]
    )
    assert [d["name"] for d in directors] == ["Jamie Shannon", "McKay SMITH"]
    assert directors[0]["title"] == "Director"
    assert parse_directors(None) == []


def test_mojibake_fixed_and_clean_text_untouched():
    broken = "the â€œtime outâ€\x9d rule"
    assert fix_mojibake(broken) == "the “time out” rule"
    assert fix_mojibake("Chapter 4 (b) staff:child ratios") == "Chapter 4 (b) staff:child ratios"
    assert fix_mojibake("curly “ok”") == "curly “ok”"
    assert fix_mojibake(None) is None


def test_inspection_type_normalization():
    assert inspection_inspection({"inspectionType": "FIRE_INSPECTION"})["type"] == "Fire Inspection"
    assert (
        inspection_inspection({"inspectionType": "FOOD_SAFETY_AND_OR_SANITATION_INSPECTION"})["type"]
        == "Food Safety and/or Sanitation Inspection"
    )
    assert inspection_inspection({"inspectionType": "Fire Inspection"})["type"] == "Fire Inspection"
    assert inspection_inspection({"inspectionType": "NEW_THING"})["type"] == "New thing"


def test_visit_type_normalization_and_blank():
    assert visit_inspection({"visitType": "Renewal / Validation Visit"})["type"] == "Renewal/Validation Visit"
    assert visit_inspection({"visitType": "Renewal/Validation Visit"})["type"] == "Renewal/Validation Visit"
    blank = visit_inspection({"visitDate": "01/02/2024", "visitType": "", "violation": True})
    assert blank["type"] == "Visit"
    assert blank["wy_record_type"] == "visit"
    assert blank["wy_violation_found"] is True


# --------------------------------------------------------------------------- #
# Phase 1 -- sweep
# --------------------------------------------------------------------------- #


def test_start_requests_sweep_range_and_lead(spider):
    requests = list(spider.start_requests())
    assert len(requests) == 83199 - 82001 + 1
    body = json.loads(requests[0].body)
    assert body == {"memberId": "1", "zip": "82001", "isAnonymous": True, "externalId": DEFAULT_LEAD_ID}
    assert requests[0].meta["handle_httpstatus_list"] == [400]
    assert requests[0].headers.get("Content-Type") == b"application/json"


def test_lead_id_override():
    spider = WyomingSpider(lead_id="abc123")
    assert json.loads(next(iter(spider.start_requests())).body)["externalId"] == "abc123"


def test_dedupe_across_overlapping_zips(spider):
    sample = _load("wy_search_sample.json")
    spider.zips_pending = 10
    _drain(spider.parse_search(_response(sample), zip_code="82001"))
    _drain(spider.parse_search(_response(sample), zip_code="82002"))
    assert len(spider.facilities) == 7
    assert spider.zips_valid == 2


def test_invalid_zip_skipped_quietly(spider, caplog):
    spider.zips_pending = 10
    with caplog.at_level("WARNING"):
        out = _drain(spider.parse_search(_response(_load("wy_search_invalid_zip.json"), status=400), zip_code="82004"))
    assert out == []
    assert not spider.facilities
    assert spider.zips_valid == 0
    assert spider.zips_done == 1
    assert "82004" not in caplog.text


def test_other_400_warns(spider, caplog):
    spider.zips_pending = 10
    with caplog.at_level("WARNING"):
        _drain(spider.parse_search(_response({"errorMessage": "boom"}, status=400), zip_code="82004"))
    assert "82004" in caplog.text


def test_lead_not_found_closes_spider(spider):
    spider.zips_pending = 10
    with pytest.raises(CloseSpider) as exc:
        _drain(spider.parse_search(_response(_load("wy_search_lead_not_found.json"), status=400), zip_code="82001"))
    assert exc.value.reason == "wyoming_lead_expired"


def test_phase2_waits_for_last_sweep_request(spider, search_records):
    spider.facilities = dict(search_records)
    spider.zips_pending = 2
    assert _drain(spider._zip_finished()) == []
    requests = _drain(spider._zip_finished())
    assert len(requests) == 6  # 7 minus the test record


def test_search_errback_still_counts(spider):
    spider.zips_pending = 1

    class F:
        value = "boom"

    requests = _drain(spider.search_failed(F()))
    assert spider.zips_pending == 0
    assert requests == []  # empty union -> no detail requests


def test_test_facility_filtered(spider, search_records):
    requests = _feed_search(spider, search_records)
    ids = {json.loads(r.body)["facilityId"] for r in requests}
    assert ECARES not in ids
    assert RICH in ids
    assert spider.test_records_skipped == 1


# --------------------------------------------------------------------------- #
# Centroid rule
# --------------------------------------------------------------------------- #


def test_centroid_points(search_records, centroids):
    ecares = search_records[ECARES]["facilityAddress"]
    assert (ecares["lat"], ecares["lon"]) in centroids
    morrie = search_records["tgqx0ihnv659tnk"]["facilityAddress"]
    assert (morrie["lat"], morrie["lon"]) not in centroids  # same street, same point
    assert len(centroids) == 1


def test_centroid_coords_dropped_and_flagged(search_records, centroids):
    item = build_item(_facility("wy_detail_sparse.json"), search_records[CENTROID_PEER], centroids)
    assert "latitude" not in item and "longitude" not in item
    assert item["wy_coordinates_approximate"] is True


def test_same_street_coords_kept(search_records, centroids):
    facility = _facility("wy_detail_rich.json")
    facility["facilityAddress"].update(search_records["tgqx0ihnv659tnk"]["facilityAddress"])
    item = build_item(facility, search_records["tgqx0ihnv659tnk"], centroids)
    assert item["latitude"] == search_records["tgqx0ihnv659tnk"]["facilityAddress"]["lat"]
    assert "wy_coordinates_approximate" not in item


# --------------------------------------------------------------------------- #
# Item mapping
# --------------------------------------------------------------------------- #


def test_rich_item(search_records, centroids):
    item = build_item(_facility("wy_detail_rich.json"), search_records[RICH], centroids)
    assert item["wy_facility_id"] == RICH
    assert item["provider_name"] == "The Eagle Nest"
    assert item["provider_type"] == search_records[RICH]["facilityType"]
    assert item["accreditation"] == "National Association for the Education of Young Children (NAEYC)"
    assert item["meals"] == "Meals and Snacks Provided"
    assert "Accepts Subsidies" in item["wy_services"]
    assert len(item["wy_programs"]) == 6
    assert item["accepting_new_children"] in (True, False)
    assert item["wy_registration_fee"] == 25.0
    assert item["capacity"] == _facility("wy_detail_rich.json")["noOfChildrenServed"]
    assert item["administrator"] == "Ciley Andreen"
    assert item["wy_directors"] == [{"name": "Ciley Andreen", "title": "DIRECTOR"}]
    assert item["address"].endswith(f"{search_records[RICH]['facilityAddress']['zip']}")
    assert item["address"].count("WY") == 1
    assert item["latitude"] is not None
    assert item["transportation"] == search_records[RICH]["providesTransport"]
    assert item["wy_weekend_care"] == search_records[RICH]["weekendCare"]
    assert item["wy_evening_care"] == search_records[RICH]["eveningCare"]
    assert item["provider_url"] == "https://childcare.dfs.wyo.gov/shopping/"
    assert "status" not in item and "county" not in item
    assert item["wy_programs"][0]["rates"][0]["period"]


def test_sparse_item_missing_fields(search_records, centroids):
    facility = _facility("wy_detail_sparse.json")
    facility.pop("facilityWebsite")
    facility["licenseId"] = ""
    facility["facilityHours"] = []
    facility["facilityDirectors"] = []
    facility["services"] = []
    record = dict(search_records[CENTROID_PEER], licenseId="")
    item = build_item(facility, record, centroids)
    assert "license_number" not in item
    assert "provider_website" not in item
    assert "hours" not in item and "wy_schedule" not in item
    assert "administrator" not in item
    assert "wy_programs" not in item and "accepting_new_children" not in item
    assert "languages" not in item
    assert "meals" not in item
    # address2 key absent -> no empty segment
    assert item["address"] == "7505 US Hwy 30, Cheyenne, WY 82001"
    assert item["phone"] == "3077786431"


def test_sparse_directors_deduped(search_records, centroids):
    item = build_item(_facility("wy_detail_sparse.json"), search_records[CENTROID_PEER], centroids)
    assert item["administrator"] == (
        "Cheyenne Hills Christian Academy, Heidi Griffin, Jamie Shannon, Amanda Pospischil, Front Office General Contact"
    )


def test_compound_number_shapes(search_records, centroids):
    facility = _facility("wy_detail_compound_numbers.json")
    item = build_item(facility, search_records[ECARES], centroids)
    assert item["wy_registration_fee"] == 50
    rates = [r["rate"] for p in item["wy_programs"] for r in p["rates"]]
    assert rates and all(isinstance(r, (int, float)) for r in rates)
    assert item["languages"] == "English"


def test_no_registration_fee_when_not_charged(search_records, centroids):
    facility = _facility("wy_detail_rich.json")
    facility["chargingRegistrationFee"] = False
    facility["registrationFee"] = 25.0
    assert "wy_registration_fee" not in build_item(facility, search_records[RICH], centroids)


def test_transport_from_service_when_search_silent(centroids):
    facility = {"services": [{"serviceName": "Provides Transportation "}]}
    assert build_item(facility, {"facilityId": "x"}, centroids)["transportation"] is True


def test_partial_item_from_search_only(search_records, centroids):
    item = build_item({}, search_records[RICH], centroids)
    assert item["wy_facility_id"] == RICH
    assert item["provider_name"]
    assert "wy_programs" not in item


@pytest.mark.parametrize(
    "raw,category",
    [
        ("Child Care Center", "center"),
        ("Family Child Care Home", "family_home"),
        ("Family Child Care Center", "group_home"),
    ],
)
def test_facility_category(raw, category):
    assert norm.facility_category_from_type(raw) == category


# --------------------------------------------------------------------------- #
# Phases 2 and 3 end to end
# --------------------------------------------------------------------------- #


def _run_chain(spider, record, detail_body, reports):
    """Drive detail -> visits -> inspections -> violations; return emitted items."""
    detail_req = Request("https://childcare.dfs.wyo.gov/d")
    out = _drain(spider.parse_detail(_response(detail_body, detail_req), record=record))
    for kind, callback in (
        ("visits", spider.parse_visits),
        ("inspections", spider.parse_inspections),
        ("violations", spider.parse_violations),
    ):
        if not out or not isinstance(out[0], Request):
            break
        request = out[0]
        assert f"/reports/{kind}/" in request.url
        out = _drain(callback(_response(reports[kind], request), **request.cb_kwargs))
    return out


def test_golden_path_end_to_end(spider, search_records):
    spider.centroid_points = find_centroid_points(search_records.values())
    spider.details_total = 1
    reports = _load("wy_reports_notices.json")
    out = _run_chain(spider, search_records[RICH], _load("wy_detail_rich.json"), reports)
    assert len(out) == 1
    item = out[0]
    inspections = item["inspections"]
    kinds = [i["wy_record_type"] for i in inspections]
    assert kinds.count("visit") == 18
    assert kinds.count("inspection") == 7
    assert kinds.count("compliance_notice") == 5
    assert item["deficiencies"] == 3 + 1 + 3 + 1 + 1
    notice = next(i for i in inspections if i["wy_record_type"] == "compliance_notice")
    assert notice["type"] == "Compliance Notice"
    assert notice["report_url"].startswith("https://www.wylicensing.org/")
    assert set(notice["wy_violations"][0]) == {
        "regulation",
        "compliance_due_date",
        "compliance_achieved_date",
        "action_plan_due_date",
        "action_plan_completed_date",
    }
    assert spider.items_emitted == 1


def test_blank_dates_in_violations_become_none(spider):
    from provider_scrape.spiders.wyoming import notice_inspection

    inspection, count, declared = notice_inspection(
        {
            "visitDate": "04/30/2024",
            "complianceNoticeUrl": "u",
            "numberOfNonComplianceViolations": "2",
            "violationDetails": [{"regulation": "r", "complianceDueDate": "04/30/2024", "complianceAchievedDate": ""}],
        }
    )
    assert inspection["wy_violations"][0]["compliance_achieved_date"] is None
    assert count == 1 and declared == 2  # list wins; mismatch is logged by the caller


def test_count_mismatch_warns(spider, caplog):
    item = build_item({}, {"facilityId": "x"}, set())
    item["inspections"] = []
    body = {
        "data": {
            "violations": [
                {
                    "visitDate": "01/01/2024",
                    "numberOfNonComplianceViolations": "4",
                    "violationDetails": [{"regulation": "r"}],
                }
            ]
        }
    }
    with caplog.at_level("WARNING"):
        out = _drain(spider.parse_violations(_response(body), item=item))
    assert out[0]["deficiencies"] == 1
    assert "declares 4" in caplog.text


def test_mojibake_repaired_in_reports(spider, search_records):
    reports = _load("wy_reports_mojibake.json")
    item = build_item({}, {"facilityId": "m42"}, set())
    item["inspections"] = []
    out = _drain(spider.parse_violations(_response(reports["violations"]), item=item))
    regs = [v["regulation"] for i in out[0]["inspections"] for v in i["wy_violations"]]
    assert regs and not any("â€" in r for r in regs)
    assert any("“" in r or "”" in r for r in regs)


def test_empty_reports(spider, search_records):
    spider.details_total = 1
    out = _run_chain(spider, search_records[RICH], _load("wy_detail_rich.json"), _load("wy_reports_empty.json"))
    assert out[0]["inspections"] == []
    assert out[0]["deficiencies"] == 0


def test_reports_disabled_skips_phase3(search_records):
    spider = WyomingSpider(reports=0)
    out = _drain(spider.parse_detail(_response(_load("wy_detail_rich.json")), record=search_records[RICH]))
    assert len(out) == 1
    assert not isinstance(out[0], Request)
    assert "inspections" not in out[0]
    assert "deficiencies" not in out[0]


def test_report_errback_still_yields_item(spider, search_records):
    item = build_item({}, search_records[RICH], set())
    request = Request("https://childcare.dfs.wyo.gov/r", cb_kwargs={"item": item})

    class F:
        value = "boom"

    F.request = request
    out = _drain(spider.report_failed(F()))
    assert out == [item]
    assert spider.report_failures == 1


def test_detail_errback_yields_partial_item(spider, search_records):
    class F:
        value = "boom"
        request = Request("https://childcare.dfs.wyo.gov/d", cb_kwargs={"record": search_records[RICH]})

    out = _drain(spider.detail_failed(F()))
    assert out[0]["wy_facility_id"] == RICH
    assert spider.detail_failures == 1


def test_detail_lead_expired_closes(spider, search_records):
    with pytest.raises(CloseSpider):
        _drain(
            spider.parse_detail(
                _response(_load("wy_search_lead_not_found.json"), status=400), record=search_records[RICH]
            )
        )

"""Unit tests for the Delaware spider.

Fixtures are copied (unmodified) from tasks/delaware_story/captures/ into
provider_scrape/spiders/fixtures/ with a de_ prefix, per delaware_plan.md
Sec 9 -- no network is used in these tests.
"""

import json
import logging
import os
from urllib.parse import parse_qs, urlparse

import pytest
import scrapy
from scrapy.http import Request, TextResponse

import provider_scrape.spiders.delaware as delaware
from provider_scrape.items import ProviderItem
from provider_scrape.spiders.delaware import (
    PORTAL_LIST_URL,
    DelawareSpider,
    age_flags_from_group,
    build_complaint_items,
    build_inspection_index,
    build_visit_items,
    compose_address,
    compose_hours,
    derive_status,
    parse_portal_ids,
    split_financial_arrangements,
    strip_trailing_period,
    violation_from_row,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")
LOG = logging.getLogger("test.delaware")


def _load_fixture(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return json.load(fh)


def _by_id(rows):
    return {r["resource_id"]: r for r in rows}


@pytest.fixture
def spider():
    return DelawareSpider()


class _Failure:
    """Minimal stand-in for a Scrapy/Twisted Failure (connecticut precedent)."""

    def __init__(self, request, value):
        self.request = request
        self.value = value


def page_response(phase, rows, offset, page_num):
    cfg = delaware.DATASETS[phase]
    url = delaware._socrata_page_url(cfg["url"], cfg["order"], delaware.PAGE_SIZE, offset)
    req = Request(url, meta={"phase": phase, "offset": offset, "page_num": page_num})
    return TextResponse(url=url, body=json.dumps(rows).encode(), encoding="utf-8", request=req)


def portal_response(payload):
    req = Request(PORTAL_LIST_URL)
    body = json.dumps(payload).encode()
    return TextResponse(url=PORTAL_LIST_URL, body=body, encoding="utf-8", request=req)


# --------------------------------------------------------------------------- #
# Pure helper unit tests
# --------------------------------------------------------------------------- #


def test_socrata_page_url_params():
    url = delaware._socrata_page_url(delaware.PROVIDERS_URL, "resource_id", 5000, 1000)
    qs = parse_qs(urlparse(url).query)
    assert qs["$limit"] == ["5000"]
    assert qs["$offset"] == ["1000"]
    assert qs["$order"] == ["resource_id"]


def test_compose_address_full():
    assert (
        compose_address("501 WEST 11TH STREET", "WILMINGTON", "19801") == "501 WEST 11TH STREET, WILMINGTON, DE 19801"
    )


def test_compose_address_missing_pieces():
    assert compose_address(None, None, None) == "DE"
    assert compose_address("1 Main St", None, None) == "1 Main St, DE"


def test_compose_hours_both_present():
    assert compose_hours("7:00 AM", "6:00 PM") == "7:00 AM - 6:00 PM"


def test_compose_hours_missing_one_or_both():
    assert compose_hours("7:00 AM", None) is None
    assert compose_hours(None, "6:00 PM") is None
    assert compose_hours(None, None) is None


def test_strip_trailing_period():
    assert strip_trailing_period("6 weeks through 6 years.") == "6 weeks through 6 years"
    assert strip_trailing_period("6 weeks through 6 years") == "6 weeks through 6 years"
    assert strip_trailing_period(None) is None


@pytest.mark.parametrize(
    "age_group,expected",
    [
        ("Infant through School-Age", {"infant": True, "toddler": True, "preschool": True, "school": True}),
        ("Toddler through School-Age", {"toddler": True, "preschool": True, "school": True}),
        ("School-Age", {"school": True}),
        ("Infant through Pre-School", {"infant": True, "toddler": True, "preschool": True}),
        ("Toddler through Pre-School", {"toddler": True, "preschool": True}),
        ("Pre-School", {"preschool": True}),
        ("Pre-School through School-Age", {"preschool": True, "school": True}),
        ("Infant through Toddler", {"infant": True, "toddler": True}),
    ],
)
def test_age_flags_from_group_all_eight_values(age_group, expected):
    assert age_flags_from_group(age_group) == expected


def test_age_flags_from_group_unmapped_value_logs_and_sets_no_flags(caplog):
    with caplog.at_level("WARNING"):
        assert age_flags_from_group("Teenager", LOG) == {}
    assert "unmapped age_group" in caplog.text


def test_age_flags_from_group_empty_is_no_flags():
    assert age_flags_from_group(None) == {}
    assert age_flags_from_group("") == {}


@pytest.mark.parametrize(
    "enforcement,intent,expected",
    [
        (None, None, "Licensed"),
        ("Suspended", None, "Suspended"),
        (None, "Intent to Revoke", "Intent to Revoke"),
        ("Probation Extension", "Intent to Place on Probation Extension", "Probation Extension"),
    ],
)
def test_derive_status_four_branches(enforcement, intent, expected):
    assert derive_status(enforcement, intent) == expected


def test_split_financial_arrangements_three_tokens():
    scholarships, meals, profit = split_financial_arrangements("Child Care Food Program;Nonprofit;Purchase of Care")
    assert (scholarships, meals, profit) == (True, True, "Nonprofit")


def test_split_financial_arrangements_single_token():
    scholarships, meals, profit = split_financial_arrangements("Child Care Food Program")
    assert (scholarships, meals, profit) == (False, True, None)


def test_split_financial_arrangements_empty():
    assert split_financial_arrangements(None) == (False, False, None)
    assert split_financial_arrangements("") == (False, False, None)


def test_split_financial_arrangements_unrecognized_token_logged(caplog):
    with caplog.at_level("WARNING"):
        result = split_financial_arrangements("Sliding Scale", LOG)
    assert result == (False, False, None)
    assert "unrecognized financial_arrangements token" in caplog.text


def test_violation_from_row_full():
    row = {
        "regulation_code": "49U",
        "regulation_short_desc": "Indoor Space",
        "regulation_corrective_action": "Paint the ceiling.",
        "regulation_correction_status": "Completed",
        "regulation_correction_due": "2023-03-15T00:00:00.000",
        "regulation_corrected_date": "2023-03-23T00:00:00.000",
        "regulation_how_corrected": "Document",
    }
    assert violation_from_row(row) == {
        "regulation_code": "49U",
        "description": "Indoor Space",
        "corrective_action": "Paint the ceiling.",
        "correction_status": "Completed",
        "correction_due": "2023-03-15",
        "corrected_date": "2023-03-23",
        "how_corrected": "Document",
    }


def test_violation_from_row_missing_dates_are_none_not_empty_string():
    row = {"regulation_code": "22P", "regulation_correction_status": "Pending"}
    result = violation_from_row(row)
    assert result["correction_due"] is None
    assert result["corrected_date"] is None


# --------------------------------------------------------------------------- #
# Test 4/5/12/16 -- compliance grouping (build_visit_items)
# --------------------------------------------------------------------------- #


def test_build_visit_items_multi_visit_groups_by_license_and_date():
    rows = _load_fixture("de_compliance_multi_visit.json")
    by_license = build_visit_items(rows)
    items = by_license["651764"]
    assert len(items) == 2
    by_date = {i["date"]: i["de_violation_count"] for i in items}
    assert by_date == {"2023-02-21": 2, "2025-02-03": 2}


def test_build_visit_items_20_violations_sorted_by_regulation_code():
    rows = _load_fixture("de_compliance_20_violations.json")
    by_license = build_visit_items(rows)
    license_number = rows[0]["license_number"]
    items = by_license[license_number]
    assert len(items) == 1
    item = items[0]
    assert item["de_violation_count"] == 20
    codes = [v["regulation_code"] for v in item["de_violations"]]
    assert codes == sorted(codes)


def test_build_visit_items_pending_correction_no_corrected_date():
    rows = _load_fixture("de_compliance_pending_correction.json")
    by_license = build_visit_items(rows)
    items = by_license["564685"]
    assert len(items) == 1
    violation = items[0]["de_violations"][0]
    assert violation["correction_status"] == "Pending"
    assert violation["corrected_date"] is None


def test_build_visit_items_all_seven_visit_types_pass_through_unchanged():
    rows = _load_fixture("de_compliance_visit_types.json")
    by_license = build_visit_items(rows)
    all_items = [item for items in by_license.values() for item in items]
    got_types = {item["type"] for item in all_items}
    expected_types = {r["facility_visit_type"] for r in rows}
    assert len(expected_types) == 7
    assert got_types == expected_types


def test_build_visit_items_skips_rows_missing_license_or_date(caplog):
    rows = [{"provider_action_date_of_visit": "2023-01-01T00:00:00.000"}, {"license_number": "123"}]
    with caplog.at_level("WARNING"):
        by_license = build_visit_items(rows, LOG)
    assert by_license == {}
    assert "missing license_number/visit date" in caplog.text


# --------------------------------------------------------------------------- #
# Test 6/10/11 -- complaints (build_complaint_items)
# --------------------------------------------------------------------------- #


def test_build_complaint_items_all_kept_one_per_row():
    rows = _load_fixture("de_complaints_provider_mixed.json")
    by_license = build_complaint_items(rows)
    items = by_license["27399"]
    assert len(items) == 6
    results = [i["original_status"] for i in items]
    assert "Substantiated" in results
    for item in items:
        assert item["type"] == "OCCL Standards Complaint"
        assert "date" in item
        assert "de_investigation_conclusion" in item


def test_build_complaint_items_missing_fields_parse_cleanly():
    rows = _load_fixture("de_complaints_missing_fields.json")
    by_license = build_complaint_items(rows)

    with_date = by_license["48731"][0]
    assert with_date["date"] == "2024-04-18"
    assert "original_status" not in with_date

    no_date = by_license["580492"][0]
    assert "date" not in no_date
    assert "original_status" not in no_date


def test_build_complaint_items_ia_investigation_has_no_conclusion():
    rows = _load_fixture("de_complaints_ia_investigation.json")
    by_license = build_complaint_items(rows)
    for items in by_license.values():
        item = items[0]
        assert item["type"] == "IA Investigation"
        assert "date" in item
        assert "original_status" in item
        assert "de_investigation_conclusion" not in item


def test_build_complaint_items_skips_rows_missing_resource_id(caplog):
    with caplog.at_level("WARNING"):
        by_license = build_complaint_items([{"investigation_type": "OCCL Standards Complaint"}], LOG)
    assert by_license == {}
    assert "missing resource_id" in caplog.text


def test_build_inspection_index_sorts_newest_first():
    rows = _load_fixture("de_compliance_multi_visit.json")
    index, visit_count, complaint_count = build_inspection_index(rows, [])
    assert [i["date"] for i in index["651764"]] == ["2025-02-03", "2023-02-21"]
    assert visit_count == 2
    assert complaint_count == 0


# --------------------------------------------------------------------------- #
# Test 8/15 -- the advisory portal list (parse_portal_ids)
# --------------------------------------------------------------------------- #


def test_parse_portal_ids_valid_response():
    data = [{"resource_id": str(i)} for i in range(600)]
    ids = parse_portal_ids(data)
    assert ids == {str(i) for i in range(600)}


def test_parse_portal_ids_too_short_is_none():
    assert parse_portal_ids([{"resource_id": "1"}] * 10) is None


def test_parse_portal_ids_non_list_is_none():
    assert parse_portal_ids({"error": "nope"}) is None
    assert parse_portal_ids(None) is None


# --------------------------------------------------------------------------- #
# Test 1/2/3/7/9/13 -- full provider mapping (spider._build_provider_item)
# --------------------------------------------------------------------------- #


def test_full_provider_mapping_golden_path(spider):
    row = _by_id(_load_fixture("de_facilities_shapes.json"))["27399"]
    item = spider._build_provider_item(row)

    assert item["source_state"] == "Delaware"
    assert item["license_number"] == "27399"
    assert item["provider_name"] == "YMCA OF DELAWARE / CENTRAL BRANCH YMCA CHILDREN'S CORNER 1"
    assert item["provider_type"] == "Licensed Child Care Center"
    assert item["county"] == "New Castle"
    assert item["address"] == "501 WEST 11TH STREET, WILMINGTON, DE 19801"
    assert item["phone"] == "(302) 778-9009"
    assert item["capacity"] == "99"
    assert item["longitude"] == "-75.551365"
    assert item["latitude"] == "39.747981"
    assert item["hours"] == "7:00 AM - 6:00 PM"
    assert item["ages_served"] == "6 weeks through 6 years"
    assert item["infant"] is True
    assert item["toddler"] is True
    assert item["preschool"] is True
    assert item["school"] is True
    assert item["status"] == "Licensed"
    assert item["de_financial_arrangements"] == "Child Care Food Program;Nonprofit;Purchase of Care"
    assert item["scholarships_accepted"] is True
    assert item["meals"] == "Child Care Food Program"
    assert item["de_profit_status"] == "Nonprofit"
    assert "2024: 5 facility injuries" in item["de_injuries_report"]
    assert item["provider_url"] == (
        "https://education.delaware.gov/families/birth-age-5/child_care_search/facility-details/?license_number=27399"
    )
    assert item["inspections"] == []


def test_status_derivation_enforcement_action(spider):
    row = _by_id(_load_fixture("de_facilities_shapes.json"))["28751"]
    item = spider._build_provider_item(row)
    assert item["status"] == "Suspended"
    assert item["de_enforcement_action"] == "Suspended"
    assert "de_intent_to_revoke" not in item


def test_status_derivation_intent_to_revoke(spider):
    row = _by_id(_load_fixture("de_facilities_shapes.json"))["641907"]
    item = spider._build_provider_item(row)
    assert item["status"] == "Intent to Revoke"
    assert item["de_intent_to_revoke"] == "Intent to Revoke"


def test_special_conditions_and_large_family_type(spider):
    row = _by_id(_load_fixture("de_facilities_shapes.json"))["27615"]
    item = spider._build_provider_item(row)
    assert item["de_special_conditions"] == "Foster Care;Agreement of Understanding"
    assert item["provider_type"] == "Licensed Family Child Care"


def test_absent_geocoded_location_leaves_coords_unset(spider):
    row = _by_id(_load_fixture("de_facilities_shapes.json"))["27473"]
    item = spider._build_provider_item(row)
    assert "latitude" not in item
    assert "longitude" not in item


def test_absent_financial_arrangements_leaves_related_fields_unset(spider):
    row = _by_id(_load_fixture("de_facilities_shapes.json"))["27411"]
    item = spider._build_provider_item(row)
    assert "de_financial_arrangements" not in item
    assert "scholarships_accepted" not in item
    assert "meals" not in item
    assert "de_profit_status" not in item


def test_absent_phone_number_leaves_phone_unset_capacity_zero_kept(spider):
    row = _by_id(_load_fixture("de_facilities_shapes.json"))["254719"]
    item = spider._build_provider_item(row)
    assert "phone" not in item
    assert item["capacity"] == "0"


def test_absent_hours_leaves_hours_unset(spider):
    row = _by_id(_load_fixture("de_facilities_shapes.json"))["1606914"]
    item = spider._build_provider_item(row)
    assert "hours" not in item


def test_provider_with_no_inspections_still_emitted(spider):
    row = _by_id(_load_fixture("de_facilities_shapes.json"))["27390"]
    item = spider._build_provider_item(row)
    assert isinstance(item, ProviderItem)
    assert item["inspections"] == []


def test_build_provider_item_missing_resource_id_returns_none(spider, caplog):
    with caplog.at_level("WARNING"):
        result = spider._build_provider_item({"resource_name": "No Id LLC"})
    assert result is None
    assert "no resource_id" in caplog.text


def test_de_portal_listed_true_and_false(spider):
    spider.portal_ids = set(_load_fixture("de_wp_portal_ids.json"))
    rows = _by_id(_load_fixture("de_facilities_shapes.json"))

    listed = spider._build_provider_item(rows["27399"])
    assert listed["de_portal_listed"] is True

    unlisted = spider._build_provider_item(rows["1720470"])
    assert unlisted["de_portal_listed"] is False


def test_de_portal_listed_unset_when_portal_ids_none(spider):
    spider.portal_ids = None
    row = _by_id(_load_fixture("de_facilities_shapes.json"))["27399"]
    item = spider._build_provider_item(row)
    assert "de_portal_listed" not in item


def test_provider_item_carries_attached_inspections(spider):
    compliance_rows = _load_fixture("de_compliance_multi_visit.json")
    spider.inspections_by_license, spider.visit_item_count, spider.complaint_item_count = build_inspection_index(
        compliance_rows, [], spider.logger
    )
    row = _by_id(_load_fixture("de_facilities_shapes.json"))["27399"]
    row = dict(row)
    row["resource_id"] = "651764"  # reuse the golden row under the visit's license
    item = spider._build_provider_item(row)
    assert len(item["inspections"]) == 2
    assert item["inspections"][0]["date"] == "2025-02-03"


# --------------------------------------------------------------------------- #
# Test 14 -- pagination (Socrata paging, default 1000/no-limit truncation trap)
# --------------------------------------------------------------------------- #


def test_pagination_full_page_requests_next_page(spider, monkeypatch):
    monkeypatch.setattr(delaware, "PAGE_SIZE", 3)
    rows = [{"license_number": str(i), "provider_action_date_of_visit": "2024-01-01T00:00:00.000"} for i in range(3)]
    resp = page_response("compliance", rows, offset=0, page_num=1)
    outputs = list(spider.parse_page(resp))
    requests = [o for o in outputs if isinstance(o, scrapy.Request)]
    assert len(requests) == 1
    assert requests[0].meta == {"phase": "compliance", "offset": 3, "page_num": 2}
    assert len(spider.compliance_rows) == 3


def test_pagination_short_page_stops_and_advances_to_next_phase(spider, monkeypatch):
    monkeypatch.setattr(delaware, "PAGE_SIZE", 5)
    spider._complaints_done = True
    spider._portal_done = True
    rows = [{"license_number": "1", "provider_action_date_of_visit": "2024-01-01T00:00:00.000"}]
    resp = page_response("compliance", rows, offset=0, page_num=1)
    outputs = list(spider.parse_page(resp))
    requests = [o for o in outputs if isinstance(o, scrapy.Request)]
    assert spider._compliance_done is True
    # all three phases now done -> Phase 2 (providers) kicks off.
    assert len(requests) == 1
    assert requests[0].meta["phase"] == "providers"
    assert requests[0].meta["offset"] == 0


def test_pagination_max_pages_cap_warns_and_stops(spider, monkeypatch, caplog):
    monkeypatch.setattr(delaware, "PAGE_SIZE", 2)
    monkeypatch.setattr(delaware, "MAX_PAGES", 2)
    rows = [{"license_number": "1"}, {"license_number": "2"}]
    resp = page_response("compliance", rows, offset=2, page_num=2)
    with caplog.at_level("WARNING"):
        outputs = list(spider.parse_page(resp))
    requests = [o for o in outputs if isinstance(o, scrapy.Request)]
    assert requests == []
    assert "MAX_PAGES" in caplog.text
    assert "compliance" in spider.page_cap_hit_phases


def test_providers_phase_emits_items_not_accumulated_rows(spider):
    spider._compliance_done = True
    spider._complaints_done = True
    spider._portal_done = True
    rows = _load_fixture("de_facilities_shapes.json")
    resp = page_response("providers", rows, offset=0, page_num=1)
    outputs = list(spider.parse_page(resp))
    items = [o for o in outputs if isinstance(o, ProviderItem)]
    assert len(items) == len(rows)
    assert spider.providers_emitted == len(rows)


def test_page_errback_counts_failure_and_finishes_phase(spider):
    req = Request(delaware.COMPLAINTS_URL, meta={"phase": "complaints"})
    failure = _Failure(req, TimeoutError("boom"))
    spider._compliance_done = True
    spider._portal_done = True
    outputs = list(spider.page_errback(failure))
    assert spider.non_200_count == 1
    assert spider._complaints_done is True
    requests = [o for o in outputs if isinstance(o, scrapy.Request)]
    assert len(requests) == 1
    assert requests[0].meta["phase"] == "providers"


def test_page_errback_on_providers_phase_does_not_finish_a_review_phase(spider):
    req = Request(delaware.PROVIDERS_URL, meta={"phase": "providers"})
    failure = _Failure(req, TimeoutError("boom"))
    outputs = list(spider.page_errback(failure))
    assert outputs == []
    assert spider.non_200_count == 1


# --------------------------------------------------------------------------- #
# Test 15 -- Phase 1 (portal list) resilience
# --------------------------------------------------------------------------- #


def test_parse_portal_list_malformed_response_does_not_abort_run(spider, caplog):
    spider._compliance_done = True
    spider._complaints_done = True
    resp = portal_response({"unexpected": "shape"})
    with caplog.at_level("WARNING"):
        outputs = list(spider.parse_portal_list(resp))
    assert spider.portal_ids is None
    assert spider.portal_failed is True
    assert spider._portal_done is True
    requests = [o for o in outputs if isinstance(o, scrapy.Request)]
    assert len(requests) == 1
    assert requests[0].meta["phase"] == "providers"


def test_portal_list_errback_does_not_abort_run(spider):
    req = Request(PORTAL_LIST_URL)
    failure = _Failure(req, TimeoutError("boom"))
    spider._compliance_done = True
    spider._complaints_done = True
    outputs = list(spider.portal_list_errback(failure))
    assert spider.portal_ids is None
    assert spider.portal_failed is True
    requests = [o for o in outputs if isinstance(o, scrapy.Request)]
    assert len(requests) == 1


# --------------------------------------------------------------------------- #
# closed() guardrail smoke tests
# --------------------------------------------------------------------------- #


def test_closed_warns_when_below_baselines(spider, caplog):
    spider.providers_emitted = 5
    spider.visit_item_count = 1
    spider.complaint_item_count = 1
    spider.non_200_count = 1
    spider.portal_failed = True
    with caplog.at_level("WARNING"):
        spider.closed("finished")
    assert "possible incomplete crawl" in caplog.text
    assert "portal list was unavailable" in caplog.text


def test_closed_quiet_when_baselines_met(spider, caplog):
    spider.providers_emitted = delaware.EXPECTED_MIN_PROVIDERS
    spider.visit_item_count = delaware.EXPECTED_MIN_INSPECTIONS
    spider.complaint_item_count = 0
    with caplog.at_level("WARNING"):
        spider.closed("finished")
    assert "possible incomplete crawl" not in caplog.text

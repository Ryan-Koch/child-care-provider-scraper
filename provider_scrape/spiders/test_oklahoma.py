import asyncio
import copy
import json
import logging
import os

import pytest
from scrapy.http import Request, TextResponse

from provider_scrape import normalization as norm
from provider_scrape.items import InspectionItem
from provider_scrape.spiders.oklahoma import (
    MAX_DETAIL_ATTEMPTS,
    OklahomaSpider,
    apply_ages,
    build_search_item,
    care_type_labels,
    enrich_item,
    flag,
    format_hours,
    page_props,
    retry_delay,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")

CENTER = "K830057556"
HOME = "K820058108"
HOME_NO_DBA = "K820014904"
UNKNOWN_TYPE = "K830004978"


def _read(name):
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as fh:
        return fh.read()


def _response(html, url="https://childcarefind.okdhs.org/providers", status=200):
    return TextResponse(url=url, body=html.encode("utf-8"), encoding="utf-8", status=status, request=Request(url))


def _detail_html(props, page="/providers/[vendorId]"):
    data = {"props": {"pageProps": props}, "page": page}
    return f'<html><body><script id="__NEXT_DATA__" type="application/json">{json.dumps(data)}</script></body></html>'


def _detail_props(name):
    return copy.deepcopy(page_props(_response(_read(name))))


@pytest.fixture(autouse=True)
def sleeps(monkeypatch):
    """Record asyncio.sleep calls instead of really waiting."""
    calls = []

    async def fake_sleep(seconds):
        calls.append(seconds)

    monkeypatch.setattr("provider_scrape.spiders.oklahoma.asyncio.sleep", fake_sleep)
    return calls


@pytest.fixture
def spider():
    return OklahomaSpider()


@pytest.fixture
def records():
    props = page_props(_response(_read("ok_search_sample.html")))
    return {r["vendorId"]: r for r in props["childcareProviders"]}


def _parse(spider, record, html, attempt=1, status=200):
    response = _response(html, url=f"https://childcarefind.okdhs.org/providers/{record['vendorId']}", status=status)

    async def collect():
        return [out async for out in spider.parse_detail(response, record=record, attempt=attempt)]

    return asyncio.run(collect())


# --------------------------------------------------------------------------- #
# Search page
# --------------------------------------------------------------------------- #


def test_search_yields_one_detail_request_per_unique_provider(spider, records):
    response = _response(_read("ok_search_sample.html"))
    requests = list(spider.parse_search(response))
    assert [r.url for r in requests] == [f"https://childcarefind.okdhs.org/providers/{v}" for v in records]
    assert len(requests) == 5
    first = requests[0]
    assert first.cb_kwargs["record"]["vendorId"] == CENTER
    assert first.cb_kwargs["attempt"] == 1
    assert first.dont_filter is True
    assert first.meta["handle_httpstatus_list"] == [404]
    assert spider.details_total == 5


def test_search_dedupes_duplicate_vendor_ids(spider, records):
    data = {"props": {"pageProps": {"childcareProviders": [records[CENTER], records[HOME], records[CENTER]]}}}
    html = f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(data)}</script>'
    requests = list(spider.parse_search(_response(html)))
    assert len(requests) == 2


@pytest.mark.parametrize(
    "html",
    [
        "<html><body>no data</body></html>",
        '<script id="__NEXT_DATA__" type="application/json">{not json</script>',
        '<script id="__NEXT_DATA__" type="application/json">{"props": {"pageProps": {}}}</script>',
    ],
)
def test_search_without_usable_next_data_logs_error_and_yields_nothing(spider, caplog, html):
    with caplog.at_level(logging.ERROR):
        assert list(spider.parse_search(_response(html))) == []
    assert "no childcareProviders" in caplog.text


# --------------------------------------------------------------------------- #
# Detail page: golden paths
# --------------------------------------------------------------------------- #


def test_center_detail_golden_path(spider, records):
    items = _parse(spider, records[CENTER], _read("ok_detail_center.html"))
    assert len(items) == 1
    item = items[0]
    assert item["source_state"] == "Oklahoma"
    assert item["provider_url"] == f"https://childcarefind.okdhs.org/providers/{CENTER}"
    assert item["license_number"] == CENTER
    assert item["provider_name"] == "PRIMROSE SCHOOL OF WEST HEFNER POINTE"
    assert item["ok_doing_business_as"] == "Primrose School of West Hefner Pointe"
    assert item["provider_type"] == "Child Care Center"
    assert item["address"] == "9700 W HEFNER ROAD, YUKON, OK 73099"
    assert item["latitude"] == "35.5797389"
    assert item["longitude"] == "-97.684477"
    assert item["phone"] == "(405) 792-2300"
    assert item["email"] == "kwaterman@primrosewesthefnerpointe.com"
    assert item["administrator"] == "Valerie Smith"
    assert item["ok_administrator_title"] == "Director"
    assert item["capacity"] == 220
    assert item["hours"] == (
        "Monday 6:30AM-6:00PM; Tuesday 6:30AM-6:00PM; Wednesday 6:30AM-6:00PM; "
        "Thursday 6:30AM-6:00PM; Friday 6:30AM-6:00PM"
    )
    assert len(item["ok_schedule"]) == 7
    assert item["ok_schedule"][0] == {"day": "Monday", "hours": "6:30AM - 6:00PM"}
    assert item["ok_schedule"][5] == {"day": "Saturday", "hours": None}
    assert item["ok_care_types"] == ["Year Round", "Daytime Hours"]
    assert item["ages_served"] == (
        "Infants (0-11 months), Toddlers (12-23 months), Preschool (24-48 months; 2-4 yrs.), School-age (5 years-older)"
    )
    assert (item["infant"], item["toddler"], item["preschool"], item["school"]) == (True, True, True, True)
    assert item["scholarships_accepted"] is False
    assert "ok_subsidy_contract_number" not in item
    assert item["ok_star_level"] == 2
    assert item["ok_licensing_specialist"] == "Brooke Horn"
    assert item["ok_licensing_specialist_phone"] == "(405) 550-3764"
    assert item["ok_denial_sent"] is False
    assert item["ok_revocation_sent"] is False
    assert item["ok_emergency_issued"] is False
    assert "status" not in item
    assert "ok_detail_unavailable" not in item
    assert spider.ok_first_try == 1


def test_center_inspections_visits_and_complaint_sorted_newest_first(spider, records):
    item = _parse(spider, records[CENTER], _read("ok_detail_center.html"))[0]
    inspections = item["inspections"]
    dates = [i["date"] for i in inspections]
    assert dates == sorted(dates, reverse=True)
    # 1 complaint allegation, no non-compliances on any visit.
    assert item["deficiencies"] == 1

    complaint = next(i for i in inspections if i["type"] == "Complaint")
    assert isinstance(complaint, InspectionItem)
    assert complaint["date"] == "2026-07-31"
    assert complaint["original_status"] == "Substantiated"
    allegation = complaint["ok_allegations"][0]
    assert allegation["requirement"] == "340:110-3-280(d)(1)(E)"
    assert allegation["occurrence_date"] == "7/29/2026"
    assert allegation["plan_to_correct"] is None
    assert allegation["noncompliance_observed"].startswith("Reporting- Parent was not notified")

    visit = inspections[0]
    assert visit["date"] == "2026-08-05"
    assert visit["type"] == "Follow Up (Partial)"
    assert visit["original_status"] == "2 of 2 areas in compliance"
    assert visit["report_url"] == f"https://childcarefind.okdhs.org/licensing-history/{CENTER}/2026-08-05"
    assert visit["ok_visit_type"] == "Partial"
    assert visit["ok_visit_purpose"] == "Follow Up"
    assert visit["ok_compliance_count"] == 2
    assert visit["ok_compliance_total"] == 2
    assert visit["ok_noncompliances"] == []


def test_home_detail_golden_path(spider, records):
    item = _parse(spider, records[HOME], _read("ok_detail_home.html"))[0]
    assert item["provider_type"] == "Family Child Care Home"
    assert item["provider_name"] == "KING-HUGHES, SHANTEL"
    assert item["ok_doing_business_as"] == "Lyric's Lil Learning Tots LLC"
    assert item["ok_administrator_title"] == "Primary Caregiver"
    assert item["ok_subsidy_contract_number"] == "40649"
    assert item["scholarships_accepted"] is True
    assert item["capacity"] == 7
    assert item["deficiencies"] == sum(len(i.get("ok_noncompliances", [])) for i in item["inspections"])
    assert item["deficiencies"] >= 1
    with_issues = [i for i in item["inspections"] if i.get("ok_noncompliances")]
    assert with_issues
    assert with_issues[0]["ok_compliance_total"] - with_issues[0]["ok_compliance_count"] == len(
        with_issues[0]["ok_noncompliances"]
    )


def test_administrator_whitespace_is_collapsed(spider, records):
    props = _detail_props("ok_detail_home.html")
    props["directorFullName"] = "Elaine  Dean "
    item = _parse(spider, records[HOME], _detail_html(props))[0]
    assert item["administrator"] == "Elaine Dean"


def test_normalization_maps_provider_types(spider, records):
    center = _parse(spider, records[CENTER], _read("ok_detail_center.html"))[0]
    home = _parse(spider, records[HOME], _read("ok_detail_home.html"))[0]
    assert norm.normalize_item(dict(center), "oklahoma")["facility_category"] == "center"
    assert norm.normalize_item(dict(home), "oklahoma")["facility_category"] == "family_home"


# --------------------------------------------------------------------------- #
# Detail page: missing data
# --------------------------------------------------------------------------- #


def test_sparse_detail_has_no_empty_fields_and_does_not_crash(spider, records):
    props = _detail_props("ok_detail_center.html")
    props.update(
        {
            "emailAddress": None,
            "starLevelCode": None,
            "complaints": [],
            "monitoringVisits": [],
            "agesAccepted": [],
            "hoursOfOperation": [],
            "phoneNumber": "",
            "workerFullName": None,
            "workerPhoneNumberFormatted": None,
            "directorFullName": None,
            "directorPosition": None,
            "licenseCapacity": None,
            "contractNumber": "",
            "officialDoingBusinessAs": "",
            "denialSent": None,
        }
    )
    record = dict(records[CENTER], officialDoingBusinessAs="")
    item = _parse(spider, record, _detail_html(props))[0]

    for value in item.values():
        assert value not in ("", [], {}, None)
    for key in (
        "email",
        "ok_star_level",
        "inspections",
        "ages_served",
        "infant",
        "hours",
        "ok_schedule",
        "phone",
        "capacity",
        "administrator",
        "ok_doing_business_as",
        "ok_denial_sent",
        "ok_licensing_specialist",
    ):
        assert key not in item
    assert item["deficiencies"] == 0
    # Search-level data still present.
    assert item["address"] == "9700 W HEFNER ROAD, YUKON, OK 73099"
    assert item["ok_care_types"] == ["Year Round", "Daytime Hours"]


def test_search_record_without_dba_has_no_dba_field(spider, records):
    item = build_search_item(records[HOME_NO_DBA])
    assert "ok_doing_business_as" not in item
    assert item["provider_type"] == "Family Child Care Home"


# --------------------------------------------------------------------------- #
# 404 retry handling
# --------------------------------------------------------------------------- #


def test_stale_404_requeues_with_lower_priority(spider, records):
    out = _parse(spider, records[CENTER], _read("ok_detail_404.html"), attempt=1, status=404)
    assert len(out) == 1
    retry = out[0]
    assert isinstance(retry, Request)
    assert retry.url.endswith(CENTER)
    assert retry.dont_filter is True
    assert retry.cb_kwargs["attempt"] == 2
    assert retry.cb_kwargs["record"]["vendorId"] == CENTER
    assert retry.priority < 0
    assert retry.priority < spider.detail_request(CENTER, records[CENTER], 1).priority
    assert retry.meta["handle_httpstatus_list"] == [404]
    assert spider.retries == 1


def test_http_200_with_empty_page_props_is_also_retried(spider, records):
    out = _parse(spider, records[CENTER], _detail_html({}), attempt=3)
    assert isinstance(out[0], Request)
    assert out[0].cb_kwargs["attempt"] == 4


def test_success_after_retry_is_counted_separately(spider, records):
    _parse(spider, records[CENTER], _read("ok_detail_center.html"), attempt=4)
    assert (spider.ok_first_try, spider.ok_after_retry, spider.gave_up) == (0, 1, 0)


def test_give_up_yields_search_only_item(spider, records, caplog):
    with caplog.at_level(logging.WARNING):
        out = _parse(spider, records[CENTER], _read("ok_detail_404.html"), attempt=MAX_DETAIL_ATTEMPTS, status=404)
    assert len(out) == 1
    item = out[0]
    assert item["ok_detail_unavailable"] is True
    assert "deficiencies" not in item
    assert "inspections" not in item
    assert item["license_number"] == CENTER
    assert item["provider_name"] == "PRIMROSE SCHOOL OF WEST HEFNER POINTE"
    assert item["ok_doing_business_as"] == "Primrose School of West Hefner Pointe"
    assert item["provider_type"] == "Child Care Center"
    assert item["address"] == "9700 W HEFNER ROAD, YUKON, OK 73099"
    assert item["latitude"] and item["longitude"]
    assert item["ok_care_types"] == ["Year Round", "Daytime Hours"]
    assert item["scholarships_accepted"] is False
    assert item["provider_url"].endswith(CENTER)
    assert spider.gave_up == 1
    assert f"giving up on detail {CENTER}" in caplog.text


# --------------------------------------------------------------------------- #
# Misc
# --------------------------------------------------------------------------- #


def test_unknown_facility_type_keeps_raw_slug_and_warns(spider, records, caplog):
    record = dict(records[UNKNOWN_TYPE], facilityType="childcare-mystery")
    with caplog.at_level(logging.WARNING):
        item = build_search_item(record, spider.logger)
    assert item["provider_type"] == "childcare-mystery"
    assert "unknown facilityType 'childcare-mystery'" in caplog.text


def test_helpers():
    assert flag("True") is True
    assert flag("False") is False
    assert flag(None) is None
    assert flag("maybe") is None
    assert care_type_labels(["school-year", "sick-care", "new-tag", "school-year"]) == [
        "School Year Only",
        "Sick Care",
        "New Tag",
    ]
    assert format_hours([{"day": "Monday", "hours": "1:00AM - 2:00PM"}, {"day": "Sunday", "hours": None}]) == (
        "Monday 1:00AM-2:00PM"
    )
    item = {}
    apply_ages(item, ["three-year", "five-year"])
    assert item["ages_served"] == "Preschool (24-48 months; 2-4 yrs.), School-age (5 years-older)"
    assert (item["infant"], item["toddler"], item["preschool"], item["school"]) == (False, False, True, True)


def test_enrich_item_requires_vendor_id_only_from_ready_pages(records):
    item = build_search_item(records[CENTER])
    enrich_item(item, _detail_props("ok_detail_center.html"))
    assert item["ok_star_level"] == 2


def test_retry_delay_schedule():
    assert [retry_delay(a) for a in range(1, 8)] == [2, 4, 8, 16, 30, 30, 30]
    assert retry_delay(MAX_DETAIL_ATTEMPTS - 1) == 30


def test_requeue_waits_for_backoff_before_yielding(spider, records, sleeps):
    out = _parse(spider, records[CENTER], _read("ok_detail_404.html"), attempt=3, status=404)
    assert sleeps == [8]
    assert isinstance(out[0], Request)
    assert out[0].cb_kwargs["attempt"] == 4


def test_no_sleep_on_success_or_give_up(spider, records, sleeps):
    _parse(spider, records[CENTER], _read("ok_detail_center.html"))
    _parse(spider, records[CENTER], _read("ok_detail_404.html"), attempt=MAX_DETAIL_ATTEMPTS, status=404)
    assert sleeps == []


def test_first_attempt_requests_are_not_delayed(spider, records):
    request = next(iter(spider.parse_search(_response(_read("ok_search_sample.html")))))
    assert "download_delay" not in request.meta

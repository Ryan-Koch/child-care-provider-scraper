import json
import logging
import os
from urllib.parse import parse_qs

import pytest
from scrapy.http import Request, TextResponse

from provider_scrape import normalization as norm
from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.spiders.kentucky import (
    AURA_URL,
    MAX_ERROR_RETRIES,
    KentuckySpider,
    _num,
    ages_from_flags,
    format_hours,
    format_phone,
    title_county,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def _load_fixture(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return json.load(fh)


def _decode_records(search_payload):
    """Pull the provider summary list out of a raw search fixture (test-only
    unwrap; not the code under test)."""
    action = search_payload["actions"][0]
    raw = action["returnValue"]["returnValue"]
    return json.loads(raw)["sspChildCareProviderDetails"]


def _decode_message(request):
    """Recover the Aura ``message`` JSON envelope from a request body."""
    fields = parse_qs(request.body.decode())
    return json.loads(fields["message"][0])


@pytest.fixture
def spider():
    return KentuckySpider()


# --- response builders --------------------------------------------------- #


def search_response(payload, zip5=42101, attempt=1):
    req = Request(AURA_URL, method="POST", meta={"zip": zip5, "attempt": attempt})
    return TextResponse(url=AURA_URL, body=json.dumps(payload).encode(), encoding="utf-8", request=req)


def detail_response(payload, item=None, provider_id=1):
    if item is None:
        item = ProviderItem()
    req = Request(AURA_URL, method="POST", meta={"item": item, "provider_id": provider_id})
    return TextResponse(url=AURA_URL, body=json.dumps(payload).encode(), encoding="utf-8", request=req)


def split_search_outputs(outputs):
    """Partition parse_search's yielded values into (search_requests,
    detail_requests, items)."""
    searches, details, items = [], [], []
    for out in outputs:
        if isinstance(out, ProviderItem):
            items.append(out)
            continue
        method = _decode_message(out)["actions"][0]["params"]["method"]
        if method == "getChildCareProviderDetails":
            searches.append(out)
        else:
            details.append(out)
    return searches, details, items


# --- helper unit tests (§8.1-5) ------------------------------------------ #


def test_format_phone():
    assert format_phone("2707834484") == "(270) 783-4484"
    assert format_phone("12345") == "12345"  # not 10 digits -> bare digits
    assert format_phone(None) is None
    assert format_phone("") is None


def test_format_hours_collapses_uniform_week():
    hours = [
        {"Day": "Monday", "ServiceTime": "7:30 AM - 3:30 PM"},
        {"Day": "Tuesday", "ServiceTime": "7:30 AM - 3:30 PM"},
        {"Day": "Wednesday", "ServiceTime": "7:30 AM - 3:30 PM"},
        {"Day": "Thursday", "ServiceTime": "7:30 AM - 3:30 PM"},
        {"Day": "Friday", "ServiceTime": "7:30 AM - 3:30 PM"},
        {"Day": "Saturday", "ServiceTime": "No Information Available"},
        {"Day": "Sunday", "ServiceTime": "No Information Available"},
    ]
    assert format_hours(hours) == "Monday-Friday 7:30 AM - 3:30 PM"


def test_format_hours_lists_mixed_days():
    hours = [
        {"Day": "Monday", "ServiceTime": "7:00 AM - 5:00 PM"},
        {"Day": "Saturday", "ServiceTime": "8:00 AM - 12:00 PM"},
        {"Day": "Sunday", "ServiceTime": "No Information Available"},
    ]
    out = format_hours(hours)
    assert "Monday 7:00 AM - 5:00 PM" in out
    assert "Saturday 8:00 AM - 12:00 PM" in out


def test_format_hours_all_closed_is_none():
    hours = [
        {"Day": d, "ServiceTime": "No Information Available"}
        for d in ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
    ]
    assert format_hours(hours) is None
    assert format_hours(None) is None
    assert format_hours([]) is None


@pytest.mark.parametrize(
    "record,expected_flags,expected_ages",
    [
        (
            {"Infant": "Y", "Toddler": "Y", "PreSchool": "Y", "SchoolAge": "Y"},
            {"infant": True, "toddler": True, "preschool": True, "school": True},
            "Infant, Toddler, Preschool, School Age",
        ),
        (
            {"Infant": "N", "Toddler": "N", "PreSchool": "N", "SchoolAge": "N"},
            {"infant": False, "toddler": False, "preschool": False, "school": False},
            None,
        ),
        ({"Infant": None, "Toddler": None, "PreSchool": None, "SchoolAge": None}, {}, None),
        (
            {"Infant": "Y", "Toddler": "N", "PreSchool": None, "SchoolAge": "Y"},
            {"infant": True, "toddler": False, "school": True},
            "Infant, School Age",
        ),
    ],
)
def test_ages_from_flags(record, expected_flags, expected_ages):
    ages, flags = ages_from_flags(record)
    assert flags == expected_flags
    assert ages == expected_ages


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("WARREN", "Warren"),
        ("MCCRACKEN", "McCracken"),
        ("MCCREARY", "McCreary"),
        ("MCLEAN", "McLean"),
    ],
)
def test_title_county(raw, expected):
    assert title_county(raw) == expected


def test_title_county_none_and_blank():
    assert title_county(None) is None
    assert title_county("  ") is None


def test_num_helper_both_shapes():
    # bare float (the real wire shape, §5.4)
    assert _num(290673.0) == 290673
    assert _num(84.0) == 84
    # {"source", "parsedValue"} wrapper (the epic's DevTools-captured shape,
    # which never occurs on the real wire but must still be accepted)
    assert _num({"source": "342257.0", "parsedValue": 342257}) == 342257
    assert _num(None) is None


def test_zip_and_provider_id_float_coercion(spider):
    record = _decode_records(_load_fixture("ky_search_42101.json"))[0]
    provider_id = _num(record["ProviderId"])
    assert provider_id == 84
    item = spider._item_from_summary(record, provider_id)
    assert item["zip"] == "42101"  # not "42101.0"


def test_parse_zips_ranges_and_singles():
    assert KentuckySpider._parse_zips("42101,40216") == [42101, 40216]
    assert KentuckySpider._parse_zips("40200-40202") == [40200, 40201, 40202]
    assert KentuckySpider._parse_zips(None) == list(range(40000, 42800))


@pytest.mark.parametrize("arg,expected", [(None, 2), ("2", 2), ("8", 8)])
def test_concurrency_arg_reaches_the_crawler_settings(arg, expected):
    """`-a concurrency=N` must change the settings the ENGINE reads.

    Regression guard: setting ``self.custom_settings`` from ``__init__`` looks
    like it works (the instance dict updates) but is a silent no-op --
    ``Crawler.__init__`` reads ``custom_settings`` off the *class* before any
    instance exists. Assert against ``crawler.settings``, never the spider's
    own dict, or this test would pass while the argument does nothing.
    """
    from scrapy.crawler import Crawler
    from scrapy.utils.project import get_project_settings

    crawler = Crawler(KentuckySpider, get_project_settings())
    kwargs = {"zips": "42101"}
    if arg is not None:
        kwargs["concurrency"] = arg
    KentuckySpider.from_crawler(crawler, **kwargs)

    assert crawler.settings.getint("CONCURRENT_REQUESTS") == expected
    assert crawler.settings.getint("CONCURRENT_REQUESTS_PER_DOMAIN") == expected


# --- parse_search (§8.6-9) ------------------------------------------------ #


def test_parse_search_golden_yields_24_detail_requests(spider):
    payload = _load_fixture("ky_search_42101.json")
    outputs = list(spider.parse_search(search_response(payload, zip5=42101)))
    searches, details, items = split_search_outputs(outputs)
    assert searches == []
    assert items == []
    assert len(details) == 24
    for req in details:
        params = _decode_message(req)["actions"][0]["params"]["params"]
        assert isinstance(params["providerId"], int)
        assert params["licenseNumber"]
    assert len(spider.seen) == 24
    assert spider.zips_with_hits == 1
    assert spider.zips_done == 1


def test_parse_search_empty_zip_is_quiet(spider, caplog):
    payload = _load_fixture("ky_search_empty.json")
    with caplog.at_level(logging.WARNING):
        outputs = list(spider.parse_search(search_response(payload, zip5=49999)))
    assert outputs == []
    assert not any(r.levelno >= logging.WARNING for r in caplog.records)
    assert spider.zips_done == 1
    assert spider.zips_with_hits == 0


def test_parse_search_error_state_retries_then_gives_up(spider, caplog):
    payload = _load_fixture("ky_search_error.json")
    with caplog.at_level(logging.WARNING):
        outputs = list(spider.parse_search(search_response(payload, zip5=40999, attempt=1)))
    assert spider.error_state_count == 1
    assert len(outputs) == 1
    retry_req = outputs[0]
    assert _decode_message(retry_req)["actions"][0]["params"]["method"] == "getChildCareProviderDetails"
    assert retry_req.meta["attempt"] == 2
    assert any("Aura state" in r.getMessage() for r in caplog.records)

    outputs2 = list(spider.parse_search(search_response(payload, zip5=40999, attempt=MAX_ERROR_RETRIES + 1)))
    assert outputs2 == []
    assert 40999 in spider.zips_failed


def test_parse_search_dedupes_across_zips(spider):
    payload = _load_fixture("ky_search_42101.json")
    list(spider.parse_search(search_response(payload, zip5=42101)))
    seen_after_first = len(spider.seen)
    outputs = list(spider.parse_search(search_response(payload, zip5=42101)))
    assert outputs == []
    assert len(spider.seen) == seen_after_first == 24


# --- parse_detail (§8.10-13, 17-19) --------------------------------------- #


def test_parse_detail_center_golden(spider):
    record = _decode_records(_load_fixture("ky_search_42101.json"))[0]
    provider_id = _num(record["ProviderId"])
    item = spider._item_from_summary(record, provider_id)
    payload = _load_fixture("ky_detail_center.json")
    result = next(spider.parse_detail(detail_response(payload, item=item, provider_id=provider_id)))

    assert isinstance(result, ProviderItem)
    assert result["provider_name"] == "Warren County Head Start & Child Care Center"
    assert result["license_number"] == "L350746"
    assert result["provider_type"] == "Licensed"
    assert result["status"] == "APPROVED"
    assert result["address"] == "200 East Fourth Street"
    assert result["city"] == "Bowling Green"
    assert result["state"] == "KY"
    assert result["zip"] == "42101"
    assert result["county"] == "Warren"
    assert result["latitude"] and result["longitude"]
    assert result["phone"] == "(270) 783-4484"
    assert result["ky_stars_rating"] == 4
    assert result["infant"] is True
    assert result["capacity"] == 335
    assert result["hours"]
    assert result["ky_service_costs"]
    assert len(result["inspections"]) == 45
    assert all(isinstance(i, InspectionItem) for i in result["inspections"])
    dates = [i["date"] for i in result["inspections"] if i.get("date")]
    assert dates == sorted(dates, reverse=True)


def test_golden_item_has_no_undefined_fields(spider):
    record = _decode_records(_load_fixture("ky_search_42101.json"))[0]
    provider_id = _num(record["ProviderId"])
    item = spider._item_from_summary(record, provider_id)
    payload = _load_fixture("ky_detail_center.json")
    result = next(spider.parse_detail(detail_response(payload, item=item, provider_id=provider_id)))
    assert dict(result)  # constructing/serializing raises on an undefined field


def test_parse_detail_home_golden(spider):
    # No live search-summary capture exists for this provider (only the
    # detail capture, ky_detail_home.json); build a minimal, plausible
    # summary the way a real Certified-home record looks.
    record = {
        "ProviderId": 103038.0,
        "ProviderCLRNumber": "C68494",
        "ProviderName": "Tammy Caldera's Day Care",
        "ProviderType": "Certified",
        "ProviderStatus": "APPROVED",
        "LocationAddressLine1": "100 Main St",
        "LocationAddressLine2": None,
        "LocationCity": "Somewhere",
        "LocationCountyDescription": "FAYETTE",
        "LocationStateDescription": "KY",
        "LocationZipCode5": 40502.0,
        "AddressLatitude": "38.0",
        "AddressLongitude": "-84.5",
        "PhoneNumber": "8595551212",
        "NumberOfStars": 0,
        "Infant": "Y",
        "Toddler": None,
        "PreSchool": None,
        "SchoolAge": None,
        "IsSubsidyAccepted": "Y",
        "Transportation": "N",
        "PreKPartnershipFlag": "N",
        "IsOngoingProcess": "N",
        "HoursOfOperationList": [],
    }
    item = spider._item_from_summary(record, 103038)
    payload = _load_fixture("ky_detail_home.json")
    result = next(spider.parse_detail(detail_response(payload, item=item, provider_id=103038)))

    assert result["license_number"] == "C68494"
    assert norm.facility_category_from_type(result["provider_type"]) == "family_home"
    assert result["capacity"] == 6
    assert len(result["inspections"]) == 25


def test_parse_detail_empty_keeps_summary_item(spider):
    item = ProviderItem()
    item["provider_name"] = "Some Provider"
    item["license_number"] = "L999999"
    payload = _load_fixture("ky_detail_empty.json")
    result = next(spider.parse_detail(detail_response(payload, item=item, provider_id=999)))
    assert result["license_number"] == "L999999"
    assert "capacity" not in result
    assert "inspections" not in result
    assert spider.detail_failures == 1


def test_detail_errback_emits_summary_only_item(spider):
    item = ProviderItem()
    item["provider_name"] = "Timeout Provider"
    item["license_number"] = "L123456"

    class _Failure:
        request = Request(AURA_URL, meta={"item": item, "provider_id": 42})
        value = TimeoutError("connection reset")

    out = list(spider.detail_errback(_Failure()))
    assert out == [item]
    assert spider.detail_failures == 1


def test_inspection_join_poc(spider):
    item = ProviderItem()
    item["license_number"] = "L353576"
    payload = _load_fixture("ky_detail_poc.json")
    result = next(spider.parse_detail(detail_response(payload, item=item, provider_id=403)))

    inspections = result["inspections"]
    assert len(inspections) == 20
    with_poc = [i for i in inspections if i.get("ky_poc_id")]
    without_poc = [i for i in inspections if not i.get("ky_poc_id")]
    assert with_poc and without_poc
    for i in with_poc:
        assert i.get("status_updated")  # ApprovalDate joined in
    for i in without_poc:
        assert "status_updated" not in i

    # The two lists' InspectionIds must be the exact same set (§5.4/§6.2) --
    # verified directly against the raw fixture, independent of parse logic.
    kiccs = payload["actions"][0]["returnValue"]["returnValue"]["mapResponse"]["KICCSDataDetails"]
    history_ids = {row["InspectionId"] for row in json.loads(kiccs["InspectionHistoryList"])["inspections"]}
    updated_ids = {_num(row["InspectionId"]) for row in kiccs["InspectionHistoryListUpdated"]}
    assert history_ids == updated_ids


def test_ongoing_processes_and_max_inspections(spider):
    item = ProviderItem()
    item["license_number"] = "L356054"
    payload = _load_fixture("ky_detail_ongoing.json")
    result = next(spider.parse_detail(detail_response(payload, item=item, provider_id=1500)))

    assert result["ky_ongoing_processes"] == [
        {"process_type": "Adverse Action", "status": "On-going"},
        {"process_type": "Directed Plan of Correction", "status": "On-going"},
    ]
    assert len(result["inspections"]) == 109  # the statewide max seen


@pytest.mark.parametrize(
    "source_field,item_field",
    [
        ("IsAcceditationsAvailable", "ky_accreditation_available"),
        ("IsFoodPermitAvailable", "ky_food_permit"),
    ],
)
def test_tristate_flags(spider, source_field, item_field):
    for raw, expected in (("Y", True), ("N", False), (None, None)):
        payload = _load_fixture("ky_detail_center.json")
        kiccs = payload["actions"][0]["returnValue"]["returnValue"]["mapResponse"]["KICCSDataDetails"]
        kiccs[source_field] = raw
        item = ProviderItem()
        result = next(spider.parse_detail(detail_response(payload, item=item, provider_id=84)))
        if expected is None:
            assert item_field not in result  # null -> unset, not False
        else:
            assert result[item_field] is expected


# --- normalization pipeline (§8.15-16) ------------------------------------ #


def test_kentucky_status_mapping():
    assert norm.canonical_status("APPROVED") == "active"
    assert norm.canonical_status("SUSPENDED") == "enforcement"


@pytest.mark.parametrize(
    "provider_type,category",
    [
        ("Licensed", "center"),
        ("Certified", "family_home"),
    ],
)
def test_kentucky_facility_category_mapping(provider_type, category):
    assert norm.facility_category_from_type(provider_type) == category

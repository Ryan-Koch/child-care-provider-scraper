import json
import logging
import os
from urllib.parse import parse_qs

import pytest
from scrapy.http import Request, TextResponse

from provider_scrape import normalization as norm
from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.spiders.massachusetts import (
    AGE_GROUP_KEYS,
    DETAIL_URL,
    MAX_ERROR_RETRIES,
    MAX_ZIP,
    MIN_ZIP,
    SEARCH_URL,
    MassachusettsSpider,
    _date_sort_key,
    _normalize_last_issue_date,
    age_flags_from_counts,
    format_hours,
    split_semicolon,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def _load_fixture(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return json.load(fh)


def _decode_records(search_payload):
    """Pull the accountData list out of a raw search fixture (test-only
    unwrap; not the code under test -- unlike Kentucky, MA's inner
    returnValue is already a dict, no second json.loads)."""
    action = search_payload["actions"][0]
    return action["returnValue"]["returnValue"]["accountData"]


def _find_record(records, account_name_substring):
    for record in records:
        if account_name_substring in (record.get("accountName") or ""):
            return record
    raise AssertionError(f"no search record containing {account_name_substring!r}")


def _decode_message(request):
    """Recover the Aura ``message`` JSON envelope from a request body."""
    fields = parse_qs(request.body.decode())
    return json.loads(fields["message"][0])


@pytest.fixture
def spider():
    return MassachusettsSpider()


# --- response builders --------------------------------------------------- #


def search_response(payload, zip5=2301, attempt=1):
    req = Request(SEARCH_URL, method="POST", meta={"zip": zip5, "attempt": attempt})
    return TextResponse(url=SEARCH_URL, body=json.dumps(payload).encode(), encoding="utf-8", request=req)


def detail_response(payload, item=None, encrypted_id="enc-1"):
    if item is None:
        item = ProviderItem()
    req = Request(DETAIL_URL, method="POST", meta={"item": item, "encrypted_id": encrypted_id})
    return TextResponse(url=DETAIL_URL, body=json.dumps(payload).encode(), encoding="utf-8", request=req)


def split_search_outputs(outputs):
    """Partition parse_search's yielded values into (search_requests,
    detail_requests, items)."""
    searches, details, items = [], [], []
    for out in outputs:
        if isinstance(out, ProviderItem):
            items.append(out)
            continue
        method = _decode_message(out)["actions"][0]["params"]["method"]
        if method == "callApex":
            searches.append(out)
        else:
            details.append(out)
    return searches, details, items


# --- helper unit tests ----------------------------------------------------- #


def test_split_semicolon():
    assert split_semicolon("Program Provides Breakfast;Program Provides Lunch;Afternoon Snack") == [
        "Program Provides Breakfast",
        "Program Provides Lunch",
        "Afternoon Snack",
    ]
    assert split_semicolon("") is None
    assert split_semicolon(None) is None
    assert split_semicolon("  ;  ;") is None


def test_format_hours_collapses_uniform_week():
    schedule_list = [
        {
            "scheduleType": "Full Year Schedule",
            "daysOfServiceList": [
                {"dayOfTheWeek": "Monday", "startTime": "06:00 AM", "endTime": "11:30 PM"},
                {"dayOfTheWeek": "Tuesday", "startTime": "06:00 AM", "endTime": "11:30 PM"},
                {"dayOfTheWeek": "Wednesday", "startTime": "06:00 AM", "endTime": "11:30 PM"},
                {"dayOfTheWeek": "Thursday", "startTime": "06:00 AM", "endTime": "11:30 PM"},
                {"dayOfTheWeek": "Friday", "startTime": "06:00 AM", "endTime": "11:30 PM"},
            ],
        }
    ]
    assert format_hours(schedule_list) == "Monday-Friday 06:00 AM - 11:30 PM"


def test_format_hours_lists_mixed_days():
    schedule_list = [
        {
            "scheduleType": "Full Year Schedule",
            "daysOfServiceList": [
                {"dayOfTheWeek": "Monday", "startTime": "07:00 AM", "endTime": "05:00 PM"},
                {"dayOfTheWeek": "Saturday", "startTime": "08:00 AM", "endTime": "12:00 PM"},
            ],
        }
    ]
    out = format_hours(schedule_list)
    assert "Monday 07:00 AM - 05:00 PM" in out
    assert "Saturday 08:00 AM - 12:00 PM" in out


def test_format_hours_all_empty_is_none():
    assert format_hours(None) is None
    assert format_hours([]) is None
    assert format_hours([{"scheduleType": "Full Year Schedule", "daysOfServiceList": []}]) is None


def test_format_hours_prefers_full_year_schedule():
    schedule_list = [
        {
            "scheduleType": "Temporary Schedule",
            "daysOfServiceList": [{"dayOfTheWeek": "Monday", "startTime": "06:00 AM", "endTime": "06:00 PM"}],
        },
        {
            "scheduleType": "Full Year Schedule",
            "daysOfServiceList": [{"dayOfTheWeek": "Monday", "startTime": "07:00 AM", "endTime": "05:00 PM"}],
        },
    ]
    assert format_hours(schedule_list) == "Monday 07:00 AM - 05:00 PM"


def _counts(**overrides):
    base = {key: 0 for key in AGE_GROUP_KEYS}
    base.update(overrides)
    return base


def test_age_flags_from_counts_all_present():
    counts = _counts(
        infantBirth=7,
        infantToddler=9,
        toddler=9,
        toddlerPreschool=9,
        preschool=20,
        preschoolSA=20,
        schoolAge=26,
    )
    ages_served, flags, capacity_by_age = age_flags_from_counts(counts)
    assert ages_served == "Infant, Toddler, Preschool, School Age"
    assert flags == {"infant": True, "toddler": True, "preschool": True, "school": True}
    assert capacity_by_age == {
        "infantBirth": 7,
        "infantToddler": 9,
        "toddler": 9,
        "toddlerPreschool": 9,
        "preschool": 20,
        "preschoolSA": 20,
        "schoolAge": 26,
    }


def test_age_flags_from_counts_all_zero_is_unset():
    ages_served, flags, capacity_by_age = age_flags_from_counts(_counts())
    assert ages_served is None
    assert flags == {}
    assert capacity_by_age is None


def test_age_flags_from_counts_missing_keys_is_unset():
    ages_served, flags, capacity_by_age = age_flags_from_counts({})
    assert ages_served is None
    assert flags == {}
    assert capacity_by_age is None


def test_age_flags_from_counts_mixed():
    counts = _counts(schoolAge=26)
    ages_served, flags, capacity_by_age = age_flags_from_counts(counts)
    assert ages_served == "School Age"
    assert flags == {"infant": False, "toddler": False, "preschool": False, "school": True}
    assert capacity_by_age == {"schoolAge": 26}


def test_normalize_last_issue_date():
    assert _normalize_last_issue_date("2024-03-25") == "2024-03-25"  # already ISO (search)
    assert _normalize_last_issue_date("3/25/2024") == "2024-03-25"  # M/D/YYYY (detail)
    assert _normalize_last_issue_date("7/6/2026") == "2026-07-06"
    assert _normalize_last_issue_date("") == ""  # blank passes through (_put then drops it)
    assert _normalize_last_issue_date(None) is None


def test_date_sort_key_orders_iso_and_mdy_correctly():
    # An ISO visit date and an M/D/YYYY investigation date must compare
    # correctly against each other, not just within their own format.
    assert _date_sort_key("2026-07-09") > _date_sort_key("10/15/2025")
    assert _date_sort_key("10/15/2025") > _date_sort_key("06/13/2025")
    assert _date_sort_key(None) == (0, 0, 0)
    assert _date_sort_key("") == (0, 0, 0)


def test_parse_zips_ranges_and_singles():
    assert MassachusettsSpider._parse_zips("02301,01844") == [2301, 1844]
    assert MassachusettsSpider._parse_zips("02100-02102") == [2100, 2101, 2102]
    assert MassachusettsSpider._parse_zips(None) == list(range(MIN_ZIP, MAX_ZIP + 1))


@pytest.mark.parametrize("arg,expected", [(None, 4), ("2", 2), ("8", 8)])
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

    crawler = Crawler(MassachusettsSpider, get_project_settings())
    kwargs = {"zips": "02301"}
    if arg is not None:
        kwargs["concurrency"] = arg
    MassachusettsSpider.from_crawler(crawler, **kwargs)

    assert crawler.settings.getint("CONCURRENT_REQUESTS") == expected
    assert crawler.settings.getint("CONCURRENT_REQUESTS_PER_DOMAIN") == expected


# --- parse_search ----------------------------------------------------------- #


def test_parse_search_golden_yields_107_detail_requests(spider):
    payload = _load_fixture("ma_search_02301.json")
    outputs = list(spider.parse_search(search_response(payload, zip5=2301)))
    searches, details, items = split_search_outputs(outputs)
    assert searches == []
    assert items == []
    assert len(details) == 107
    for req in details:
        params = _decode_message(req)["actions"][0]["params"]["params"]
        assert isinstance(params["providerId"], str) and params["providerId"]
    assert len(spider.seen) == 107
    assert spider.zips_with_hits == 1
    assert spider.zips_done == 1


def test_parse_search_empty_zip_is_quiet(spider, caplog):
    payload = _load_fixture("ma_search_empty.json")
    with caplog.at_level(logging.WARNING):
        outputs = list(spider.parse_search(search_response(payload, zip5=99999)))
    assert outputs == []
    assert not any(r.levelno >= logging.WARNING for r in caplog.records)
    assert spider.zips_done == 1
    assert spider.zips_with_hits == 0


def test_parse_search_error_state_retries_then_gives_up(spider, caplog):
    payload = _load_fixture("ma_search_error.json")
    with caplog.at_level(logging.WARNING):
        outputs = list(spider.parse_search(search_response(payload, zip5=999, attempt=1)))
    assert spider.error_state_count == 1
    assert len(outputs) == 1
    retry_req = outputs[0]
    assert _decode_message(retry_req)["actions"][0]["params"]["method"] == "callApex"
    assert retry_req.meta["attempt"] == 2
    assert any("Aura state" in r.getMessage() for r in caplog.records)

    outputs2 = list(spider.parse_search(search_response(payload, zip5=999, attempt=MAX_ERROR_RETRIES + 1)))
    assert outputs2 == []
    assert 999 in spider.zips_failed


def test_parse_search_dedupes_across_zips(spider):
    payload = _load_fixture("ma_search_02301.json")
    list(spider.parse_search(search_response(payload, zip5=2301)))
    seen_after_first = len(spider.seen)
    outputs = list(spider.parse_search(search_response(payload, zip5=2301)))
    assert outputs == []
    assert len(spider.seen) == seen_after_first == 107


def test_search_errback_marks_zip_failed(spider):
    class _Failure:
        request = Request(SEARCH_URL, meta={"zip": 5555, "attempt": 1})
        value = TimeoutError("connection reset")

    spider.search_errback(_Failure())
    assert 5555 in spider.zips_failed
    assert spider.zips_done == 1


# --- parse_detail ------------------------------------------------------------ #


def test_parse_detail_large_group_golden(spider):
    records = _decode_records(_load_fixture("ma_search_02301.json"))
    record = _find_record(records, "AGAPE")
    item = spider._item_from_summary(record)
    encrypted_id = record["accRecord"]["Encrypted_Id__c"]
    payload = _load_fixture("ma_detail_large_group.json")
    result = next(spider.parse_detail(detail_response(payload, item=item, encrypted_id=encrypted_id)))

    assert isinstance(result, ProviderItem)
    assert result["provider_name"] == "AGAPE Childcare and Family Life Center, Inc."
    assert result["capacity"] == "100"
    assert norm.facility_category_from_type(result["provider_type"]) == "center"
    assert result["status"] == "Current"
    assert norm.canonical_status(result["status"]) == "active"
    assert result["license_number"] == "9196449"
    assert result["ma_program_number"] == "P-177295"
    assert result["hours"] == "Monday-Friday 06:00 AM - 11:30 PM"
    assert result["ma_cost_table"] and len(result["ma_cost_table"]) == 6
    assert result["ma_schedules"] and len(result["ma_schedules"]) == 3

    inspections = result["inspections"]
    assert len(inspections) == 18
    assert all(isinstance(i, InspectionItem) for i in inspections)
    visits = [i for i in inspections if i["type"] != "Investigation"]
    investigations = [i for i in inspections if i["type"] == "Investigation"]
    assert len(visits) == 8
    assert len(investigations) == 10
    assert all(v.get("ma_domains") for v in visits)  # D-1: full per-domain rows

    violations = [v for inv in investigations for v in inv.get("ma_violations", [])]
    assert violations  # D-2: redacted narratives kept
    assert any(v.get("statement") and v.get("corrective_action_plan") for v in violations)

    keys = [_date_sort_key(i.get("date")) for i in inspections]
    assert keys == sorted(keys, reverse=True)  # newest-first


def test_golden_item_has_no_undefined_fields(spider):
    records = _decode_records(_load_fixture("ma_search_02301.json"))
    record = _find_record(records, "AGAPE")
    item = spider._item_from_summary(record)
    encrypted_id = record["accRecord"]["Encrypted_Id__c"]
    payload = _load_fixture("ma_detail_large_group.json")
    result = next(spider.parse_detail(detail_response(payload, item=item, encrypted_id=encrypted_id)))
    assert dict(result)  # constructing/serializing raises on an undefined field


def test_parse_detail_family_home_golden(spider):
    records = _decode_records(_load_fixture("ma_search_02301.json"))
    record = _find_record(records, "Acosta")
    item = spider._item_from_summary(record)
    encrypted_id = record["accRecord"]["Encrypted_Id__c"]
    payload = _load_fixture("ma_detail_family_home.json")
    result = next(spider.parse_detail(detail_response(payload, item=item, encrypted_id=encrypted_id)))

    assert result["license_number"] == "9140144"  # search-sourced (D-4 has no fallback need here)
    assert result["ma_program_number"] == "P-176763"  # detail-sourced program number, kept alongside
    assert norm.facility_category_from_type(result["provider_type"]) == "family_home"
    assert result["license_begin_date"] == "2002-09-12"  # D-3: First Issue Date
    assert result["ma_last_issue_date"] == "2024-03-25"
    assert len(result["inspections"]) == 6


def test_parse_detail_informal_golden(spider):
    records = _decode_records(_load_fixture("ma_search_02301.json"))
    record = _find_record(records, "Hibbert")
    item = spider._item_from_summary(record)
    encrypted_id = record["accRecord"]["Encrypted_Id__c"]
    payload = _load_fixture("ma_detail_informal.json")
    result = next(spider.parse_detail(detail_response(payload, item=item, encrypted_id=encrypted_id)))

    assert norm.facility_category_from_type(result["provider_type"]) == "other"
    assert result["ma_is_informal"] is True
    # D-4: no Current_License_Number__c published -- falls back to the
    # P-###### program number so license_number is never empty.
    assert result["license_number"] == "P-260503"
    assert result["ma_program_number"] == "P-260503"
    assert "capacity" not in result
    assert "license_begin_date" not in result
    assert "ma_last_issue_date" not in result
    assert "inspections" not in result
    assert dict(result)  # still emits cleanly


def test_parse_detail_empty_payload_keeps_summary_item(spider):
    item = ProviderItem()
    item["provider_name"] = "Some Provider"
    item["license_number"] = "9999999"
    payload = {"actions": [{"id": "5;a", "state": "SUCCESS", "returnValue": {"returnValue": {}, "cacheable": False}}]}
    result = next(spider.parse_detail(detail_response(payload, item=item, encrypted_id="enc-empty")))
    assert result["license_number"] == "9999999"
    assert "capacity" not in result
    assert "inspections" not in result
    assert spider.detail_failures == 1


def test_parse_detail_error_state_keeps_summary_item(spider, caplog):
    item = ProviderItem()
    item["provider_name"] = "Error Provider"
    item["license_number"] = "8888888"
    payload = _load_fixture("ma_detail_error.json")
    with caplog.at_level(logging.WARNING):
        result = next(spider.parse_detail(detail_response(payload, item=item, encrypted_id="not-a-real-id")))
    assert result["license_number"] == "8888888"
    assert spider.detail_failures == 1
    assert spider.error_state_count == 1
    assert any("Aura state" in r.getMessage() for r in caplog.records)


def test_detail_errback_emits_summary_only_item(spider):
    item = ProviderItem()
    item["provider_name"] = "Timeout Provider"
    item["license_number"] = "7777777"

    class _Failure:
        request = Request(DETAIL_URL, meta={"item": item, "encrypted_id": "enc-timeout"})
        value = TimeoutError("connection reset")

    out = list(spider.detail_errback(_Failure()))
    assert out == [item]
    assert spider.detail_failures == 1


# --- closed() diagnostics ---------------------------------------------------- #


def test_closed_logs_unmapped_status_and_provider_type(spider, caplog):
    spider.status_values_seen.add("Some New Status")
    spider.provider_type_values_seen.add("Some New Facility Type")
    with caplog.at_level(logging.WARNING):
        spider.closed("finished")
    messages = [r.getMessage() for r in caplog.records]
    assert any("unmapped providerStatus" in m for m in messages)
    assert any("unmapped provider_type" in m for m in messages)


# --- normalization pipeline -------------------------------------------------- #


def test_massachusetts_status_mapping():
    assert norm.canonical_status("Current") == "active"


@pytest.mark.parametrize(
    "provider_type,category",
    [
        ("Family Child Care", "family_home"),
        ("Large Group", "center"),
        ("Small Group", "center"),
        ("Private School", "center"),
        ("Public School", "center"),
        ("Informal Child Care", "other"),
    ],
)
def test_massachusetts_facility_category_mapping(provider_type, category):
    assert norm.facility_category_from_type(provider_type) == category

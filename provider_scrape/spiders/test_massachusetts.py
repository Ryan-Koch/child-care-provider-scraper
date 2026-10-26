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


def test_normalize_date_covers_ma_last_issue_date_shapes():
    """bug_13: ma_last_issue_date/ma_original_license_date now go through the
    shared norm.normalize_date instead of the deleted bespoke
    _normalize_last_issue_date -- port that helper's old coverage (ISO
    passthrough, unpadded M/D/YYYY, blank/None passthrough) onto the shared
    function to confirm no shape it handled is lost."""
    assert norm.normalize_date("2024-03-25") == "2024-03-25"  # already ISO (search)
    assert norm.normalize_date("3/25/2024") == "2024-03-25"  # M/D/YYYY (detail)
    assert norm.normalize_date("7/6/2026") == "2026-07-06"
    assert norm.normalize_date("") == ""  # blank passes through (_put then drops it)
    assert norm.normalize_date(None) is None


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
    # Only finding rows are listed; every row is counted (fixture: 348 rows,
    # 25 Non-Compliant across the 8 visits).
    assert all(v.get("ma_domain_compliance_counts") for v in visits)
    assert sum(sum(v["ma_domain_compliance_counts"].values()) for v in visits) == 348
    findings = [d for v in visits for d in v.get("ma_domains", [])]
    assert len(findings) == 25
    assert all(d["level_of_compliance"] == "Non-Compliant" for d in findings)
    assert all(d["regulations"] and d["regulations"][0]["article_text"] for d in findings)

    violations = [v for inv in investigations for v in inv.get("ma_violations", [])]
    assert violations  # D-2: redacted narratives kept
    assert any(v.get("statement") and v.get("corrective_action_plan") for v in violations)

    keys = [_date_sort_key(i.get("date")) for i in inspections]
    assert keys == sorted(keys, reverse=True)  # newest-first


def _domain_row(compliance, indicator="Required Postings"):
    return {
        "domainName": "Administration",
        "indicator": indicator,
        "description": "Call 911 reminder ...",
        "levelOfCompliance": compliance,
        "isKeyIndicator": False,
        "regulationName": "7.04(14)(a)",
        "regulations": [{"Name": "7.04(14)(a)", "ArticleText": "Call 911 reminder ..."}],
    }


def test_parse_visit_lists_only_findings_and_counts_every_row():
    visit = {
        "id": "a04x",
        "dateOfVisit": "2025-03-01",
        "visitDomainList": [
            _domain_row("Compliant"),
            _domain_row("Compliant"),
            _domain_row("Complete"),
            _domain_row("Non-Compliant", indicator="Staff ratios"),
            _domain_row("Not Complete", indicator="Training"),
            _domain_row(None, indicator="Mystery"),  # unexpected status is kept, not dropped
        ],
    }
    entry = MassachusettsSpider._parse_visit(visit)

    assert entry["ma_domain_compliance_counts"] == {
        "Compliant": 2,
        "Complete": 1,
        "Non-Compliant": 1,
        "Not Complete": 1,
        "Unknown": 1,
    }
    assert [d["indicator"] for d in entry["ma_domains"]] == ["Staff ratios", "Training", "Mystery"]
    assert entry["ma_domains"][0] == {
        "domain": "Administration",
        "indicator": "Staff ratios",
        "description": "Call 911 reminder ...",
        "level_of_compliance": "Non-Compliant",
        "is_key_indicator": False,
        "regulation_name": "7.04(14)(a)",
        "regulations": [{"name": "7.04(14)(a)", "article_text": "Call 911 reminder ..."}],
    }


def test_parse_visit_all_passing_has_counts_but_no_domains():
    entry = MassachusettsSpider._parse_visit({"id": "a04y", "visitDomainList": [_domain_row("Compliant")] * 3})
    assert entry["ma_domain_compliance_counts"] == {"Compliant": 3}
    assert "ma_domains" not in entry


def test_parse_visit_without_domain_rows_has_neither_field():
    entry = MassachusettsSpider._parse_visit({"id": "a04z", "visitDomainList": []})
    assert "ma_domains" not in entry
    assert "ma_domain_compliance_counts" not in entry


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
    # bug_13: license_begin_date now holds the CURRENT term start (Last
    # Issue Date), detail-preferred -- so it carries the detail call's raw,
    # unpadded "3/25/2024" (the pipeline, not the spider, normalizes common
    # fields to ISO; see DATE_FIELDS). ma_last_issue_date is a state field
    # the spider normalizes itself, so it already reads ISO.
    assert result["license_begin_date"] == "3/25/2024"
    assert result["ma_last_issue_date"] == "2024-03-25"
    # The original/first issuance moves to ma_original_license_date,
    # search-preferred (already ISO from the search summary; the detail
    # call's unpadded "9/12/2002" never overrides it, since it was already
    # present).
    assert result["ma_original_license_date"] == "2002-09-12"
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
    assert "ma_original_license_date" not in result
    assert "inspections" not in result
    assert dict(result)  # still emits cleanly


def _minimal_detail_payload(**fields):
    """Build a minimal SUCCESS detail payload carrying only the given
    top-level keys -- enough for parse_detail to run without raising, since
    every other lookup in parse_detail is a defensive ``.get(...)``/``or
    []`` that tolerates missing keys."""
    return {
        "actions": [
            {"id": "5;a", "state": "SUCCESS", "returnValue": {"returnValue": fields, "cacheable": False}},
        ]
    }


def test_parse_detail_license_begin_date_is_detail_preferred(spider):
    """bug_13: license_begin_date (current-term start, fed from the Last
    Issue Date) is detail-preferred -- the detail call's lastIssueDate
    always overwrites whatever the search summary already put there, even
    though it's a common field (mirrors the old ma_last_issue_date
    precedence, just swapped onto license_begin_date)."""
    item = ProviderItem()
    item["provider_name"] = "Detail Preferred Co"
    item["license_begin_date"] = "2020-01-01"  # simulated search-sourced value
    payload = _minimal_detail_payload(lastIssueDate="6/1/2023")
    result = next(spider.parse_detail(detail_response(payload, item=item, encrypted_id="enc-precedence-1")))
    assert result["license_begin_date"] == "6/1/2023"


def test_parse_detail_original_license_date_is_search_preferred(spider):
    """bug_13: ma_original_license_date (first issuance) is search-
    preferred -- the detail call's firstIssueDate only fills it in when
    the search summary didn't already carry a value (mirrors the old
    license_begin_date precedence, just renamed)."""
    # Search summary lacked First_Issue_Date__c -- detail should fill it.
    item_missing = ProviderItem()
    item_missing["provider_name"] = "Fill When Absent Co"
    payload = _minimal_detail_payload(firstIssueDate="5/2/2015")
    result_missing = next(
        spider.parse_detail(detail_response(payload, item=item_missing, encrypted_id="enc-precedence-2"))
    )
    assert result_missing["ma_original_license_date"] == "2015-05-02"

    # Search summary already carried a value -- detail must not clobber it.
    item_present = ProviderItem()
    item_present["provider_name"] = "Already Present Co"
    item_present["ma_original_license_date"] = "1999-03-04"
    result_present = next(
        spider.parse_detail(detail_response(payload, item=item_present, encrypted_id="enc-precedence-3"))
    )
    assert result_present["ma_original_license_date"] == "1999-03-04"


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


def test_parse_visit_emits_per_domain_status_counts():
    visit = {
        "id": "a04w",
        "visitDomainList": [
            _domain_row("Compliant"),
            {**_domain_row("Non-Compliant"), "domainName": "Medication"},
            _domain_row("Complete"),
            {**_domain_row(None), "domainName": "Care of Children"},
            {**_domain_row("Compliant"), "domainName": "Medication"},
            _domain_row("Not Complete"),
        ],
    }
    entry = MassachusettsSpider._parse_visit(visit)

    assert entry["ma_domain_counts"] == {
        "Administration": {"Compliant": 1, "Complete": 1, "Not Complete": 1},
        "Medication": {"Non-Compliant": 1, "Compliant": 1},
        "Care of Children": {"Unknown": 1},
    }
    assert list(entry["ma_domain_counts"]) == ["Administration", "Medication", "Care of Children"]
    # Existing fields are unchanged.
    assert entry["ma_domain_compliance_counts"] == {
        "Compliant": 2,
        "Non-Compliant": 1,
        "Complete": 1,
        "Unknown": 1,
        "Not Complete": 1,
    }
    assert [d["level_of_compliance"] for d in entry["ma_domains"]] == ["Non-Compliant", None, "Not Complete"]


def test_parse_visit_without_domain_rows_has_no_domain_counts():
    assert "ma_domain_counts" not in MassachusettsSpider._parse_visit({"id": "a04v", "visitDomainList": []})

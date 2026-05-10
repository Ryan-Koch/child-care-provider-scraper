import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from scrapy.http import HtmlResponse, Request

from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.spiders.rhode_island import (
    RhodeIslandSpider,
    SEARCH_PAGE_URL,
    _empty_to_none,
    _extract_form_field,
    _summarize_compliance,
    build_detail_message,
    build_detail_post_body,
    build_inspections,
    build_item,
    extract_detail_payload,
    extract_search_results,
    format_age_group_capacity,
    format_ages_served,
    format_availability,
    format_hours,
)


# ---- Sample search summary (real shape from providerSearch response) ----

CHILD_INC_SUMMARY = {
    "accAddress": "849 Centerville Road, Warwick, Rhode Island 02886, "
                  "United States",
    "accCity": "Warwick",
    "accEmail": "sbowry@childincri.org",
    "accLicenseDecision": "Regular",
    "accLicenseStatus": "Active",
    "accName": "Child Inc. - Centerville Road",
    "accPhone": "(401) 823-3777",
    "accType": "Child Care Center",
    "id": "62AnBeAygTrdbiatR+F5AXZfVB2Q+56Br9ncpOmHWOuw1v+9oh5ehLnvT5VqqZpJ",
    "isCCAPType": "Yes",
    "isLea": False,
    "latitude": "41.69836129999999",
    "longitude": "-71.49100690",
    "programRating": 5,
}

QUEST_SUMMARY = {
    "accAddress": "1150 Boston Neck Road, Narragansett, Rhode Island "
                  "02882, United States",
    "accEmail": "eandrews@questschool.org",
    "accLicenseDecision": "Regular",
    "accLicenseStatus": "Active",
    "accName": "Quest Montessori School",
    "accPhone": "(401) 783-3222",
    "accType": "Child Care Center",
    "id": "9/hboKnmjMIxM+R2Xoys720E703fRgIGMhts1MGvWKuDNM5fRKcWo7qFYTZpNPcz",
    "isCCAPType": "No",
    "isLea": False,
    "latitude": "41.48829900",
    "longitude": "-71.43919079999999",
    "programRating": 0,
}


# ---- Sample detail payload (real shape from getProgramDetails response) ----

CHILD_UNIVERSITY_DETAIL = {
    "scheduleOfOperationData": {
        "data": [
            {
                "abbr": "Mon", "endTimeStr": "5:00 PM",
                "isSecondShift": False, "isSelected": True,
                "label": "Monday", "name": "Monday",
                "startTimeStr": "7:30 AM",
            },
            {
                "abbr": "Tue", "endTimeStr": "5:00 PM",
                "isSecondShift": False, "isSelected": True,
                "label": "Tuesday", "name": "Tuesday",
                "startTimeStr": "7:30 AM",
            },
            {
                "abbr": "Wed", "endTimeStr": "5:00 PM",
                "isSecondShift": False, "isSelected": True,
                "label": "Wednesday", "name": "Wednesday",
                "startTimeStr": "7:30 AM",
            },
            {
                "abbr": "Thu", "endTimeStr": "5:00 PM",
                "isSecondShift": False, "isSelected": True,
                "label": "Thursday", "name": "Thursday",
                "startTimeStr": "7:30 AM",
            },
            {
                "abbr": "Fri", "endTimeStr": "5:00 PM",
                "isSecondShift": False, "isSelected": True,
                "label": "Friday", "name": "Friday",
                "startTimeStr": "7:30 AM",
            },
            {
                "abbr": "Sat", "isSecondShift": False, "isSelected": False,
                "label": "Saturday", "name": "Saturday",
            },
            {
                "abbr": "Sun", "isSecondShift": False, "isSelected": False,
                "label": "Sunday", "name": "Sunday",
            },
        ]
    },
    "ageGroupServedWrapper": [
        {
            "name": "Infant (6 weeks up to 18 months)", "isSelected": True,
            "numberOfClassRooms": 2, "totalCapacity": 13,
        },
        {
            "name": "Toddler (18 months up to 36 months)", "isSelected": True,
            "numberOfClassRooms": 1, "totalCapacity": 12,
        },
        {
            "name": "Combined Infant/Toddler Classroom (6 weeks up to 36 months)",
            "isSelected": False,
        },
        {
            "name": "Preschool (3 - 4 yrs)", "isSelected": True,
            "numberOfClassRooms": 1, "totalCapacity": 18,
        },
    ],
    "lstVisits": [
        {
            "compliance": "87/87",
            "domains": [
                {
                    "category": "Physical Facilities",
                    "items": [
                        {"isNonCompliant": False, "name": "Exits free"},
                        {"isNonCompliant": False, "name": "Lighting"},
                    ],
                },
                {
                    "category": "Diapering",
                    "items": [
                        {"isNonCompliant": True, "name": "Surface sanitized"},
                    ],
                },
            ],
            "isComplaint": False,
            "licensor": "Teresa CastilloBakr",
            "name": "Child Care Center - All Age Groups-Unannounced Visit",
            "showViewDetailButton": True,
            "visitDateFormatted": "03/05/2026",
            "visitDownloadURL": "https://example.com/report1.pdf",
        },
        {
            "compliance": "--",
            "domains": [],
            "isComplaint": False,
            "name": "Unannounced Monitoring Visit",
            "showViewDetailButton": False,
            "visitDateFormatted": "12/17/2024",
            "visitDownloadURL": "https://example.com/report2.pdf",
        },
    ],
    "programDetailWrap": {
        "accWrap": {
            "accAddress": "695 Park Avenue Cranston, Rhode Island 02910, "
                          "United States",
            "accAvailability": [
                {
                    "ageGroup": "Infant (6 weeks up to 18 months)",
                    "slotInfo": "No slots available, contact program for "
                                "waitlist information",
                },
                {
                    "ageGroup": "Pre-K (4 - 5 yrs)",
                    "slotInfo": "Slots available, contact program for "
                                "enrollment",
                },
            ],
            "accEmail": "kelli@achilduniversity.com",
            "accLicenseDecision": "Regular",
            "accLicenseStatus": "Active",
            "accName": "A Child's University",
            "accPhone": "(401) 461-1880",
            "accType": "Child Care Center",
            "capacity": 81,
            "ccapExpirationDate": "10/31/2026",
            "ccapStatus": "Active",
            "contactPerson": "Lynsey Colgan",
            "currentLicenseStartDate": "11/01/2025",
            "headStart": "No",
            "isCCAPType": "Yes",
            "languageSpoken": "English, Spanish",
            "licenseExpirationDate": "10/31/2026",
            "originalLicenseStartDate": "10/24/2002",
            "programRating": 5,
            "providerContactName": "Kelli Moniz",
            "providerEmail": "kelli@achilduniversity.com",
            "riStatePreK": "No",
            "servicesOffered": ["Full day care"],
            "website": "achildsuniversity.com",
        }
    },
}


# ---- extract_search_results ----

def test_extract_search_results_success():
    payload = {
        "actions": [{
            "id": "85;a", "state": "SUCCESS",
            "returnValue": {
                "returnValue": {
                    "responseWrap": {"isValid": True},
                    "searchResults": [CHILD_INC_SUMMARY, QUEST_SUMMARY],
                }
            },
        }]
    }
    results = extract_search_results(payload)
    assert len(results) == 2
    assert results[0]["accName"] == "Child Inc. - Centerville Road"


def test_extract_search_results_no_actions():
    assert extract_search_results({"actions": []}) == []


def test_extract_search_results_failure_state():
    payload = {"actions": [{"state": "ERROR"}]}
    assert extract_search_results(payload) == []


def test_extract_search_results_garbage_input():
    assert extract_search_results(None) == []
    assert extract_search_results("not a dict") == []


def test_extract_search_results_missing_inner():
    payload = {"actions": [{"state": "SUCCESS", "returnValue": {}}]}
    assert extract_search_results(payload) == []


# ---- extract_detail_payload ----

def test_extract_detail_payload_success():
    payload = {
        "actions": [{
            "state": "SUCCESS",
            "returnValue": {"returnValue": CHILD_UNIVERSITY_DETAIL},
        }]
    }
    detail = extract_detail_payload(payload)
    assert detail is CHILD_UNIVERSITY_DETAIL


def test_extract_detail_payload_failure_state():
    payload = {"actions": [{"state": "ERROR"}]}
    assert extract_detail_payload(payload) is None


def test_extract_detail_payload_no_actions():
    assert extract_detail_payload({"actions": []}) is None


# ---- format_hours ----

def test_format_hours_full_week():
    schedule = CHILD_UNIVERSITY_DETAIL["scheduleOfOperationData"]["data"]
    assert format_hours(schedule) == (
        "Mon 7:30 AM-5:00 PM; Tue 7:30 AM-5:00 PM; Wed 7:30 AM-5:00 PM; "
        "Thu 7:30 AM-5:00 PM; Fri 7:30 AM-5:00 PM"
    )


def test_format_hours_skips_unselected_days():
    schedule = [
        {"name": "Monday", "isSelected": True,
         "startTimeStr": "8:00 AM", "endTimeStr": "4:00 PM"},
        {"name": "Tuesday", "isSelected": False},
    ]
    assert format_hours(schedule) == "Mon 8:00 AM-4:00 PM"


def test_format_hours_returns_none_when_no_days_selected():
    schedule = [
        {"name": d, "isSelected": False}
        for d in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
    ]
    assert format_hours(schedule) is None


def test_format_hours_empty_input():
    assert format_hours([]) is None
    assert format_hours(None) is None


def test_format_hours_drops_day_missing_times():
    # Selected but no start/end → drop
    schedule = [
        {"name": "Monday", "isSelected": True,
         "startTimeStr": None, "endTimeStr": None},
        {"name": "Tuesday", "isSelected": True,
         "startTimeStr": "9:00 AM", "endTimeStr": "3:00 PM"},
    ]
    assert format_hours(schedule) == "Tue 9:00 AM-3:00 PM"


# ---- format_age_group_capacity ----

def test_format_age_group_capacity_only_selected():
    rows = format_age_group_capacity(
        CHILD_UNIVERSITY_DETAIL["ageGroupServedWrapper"]
    )
    # 3 selected entries (Infant, Toddler, Preschool); Combined is unselected
    assert len(rows) == 3
    assert rows[0] == {
        "age_group": "Infant (6 weeks up to 18 months)",
        "classrooms": 2,
        "capacity": 13,
    }
    assert rows[2] == {
        "age_group": "Preschool (3 - 4 yrs)",
        "classrooms": 1,
        "capacity": 18,
    }


def test_format_age_group_capacity_all_unselected_returns_none():
    assert format_age_group_capacity([
        {"name": "Infant", "isSelected": False},
        {"name": "Toddler", "isSelected": False},
    ]) is None


def test_format_age_group_capacity_empty_input():
    assert format_age_group_capacity([]) is None
    assert format_age_group_capacity(None) is None


# ---- format_availability ----

def test_format_availability_drops_empty_entries():
    avail = [
        {"ageGroup": "Infant", "slotInfo": "No slots"},
        {"ageGroup": None, "slotInfo": None},
        {"ageGroup": "Pre-K", "slotInfo": "Slots available"},
    ]
    rows = format_availability(avail)
    assert rows == [
        {"age_group": "Infant", "slot_info": "No slots"},
        {"age_group": "Pre-K", "slot_info": "Slots available"},
    ]


def test_format_availability_empty_input():
    assert format_availability([]) is None
    assert format_availability(None) is None


# ---- format_ages_served ----

def test_format_ages_served_returns_unique_names_in_order():
    avail = [
        {"ageGroup": "Infant (6 weeks up to 18 months)", "slotInfo": "x"},
        {"ageGroup": "Pre-K (4 - 5 yrs)", "slotInfo": None},
        {"ageGroup": "Infant (6 weeks up to 18 months)", "slotInfo": "y"},
    ]
    assert format_ages_served(avail) == [
        "Infant (6 weeks up to 18 months)",
        "Pre-K (4 - 5 yrs)",
    ]


def test_format_ages_served_drops_blank_age_groups():
    avail = [
        {"ageGroup": None, "slotInfo": "x"},
        {"ageGroup": "", "slotInfo": "y"},
        {"ageGroup": "Toddler (18 months up to 36 months)", "slotInfo": None},
    ]
    assert format_ages_served(avail) == [
        "Toddler (18 months up to 36 months)"
    ]


def test_format_ages_served_empty_input():
    assert format_ages_served([]) is None
    assert format_ages_served(None) is None
    assert format_ages_served([{"ageGroup": None}]) is None


# ---- _summarize_compliance ----

def test_summarize_compliance_mixed_items():
    domains = [
        {"items": [
            {"isNonCompliant": False},
            {"isNonCompliant": False},
            {"isNonCompliant": True},
        ]},
        {"items": [{"isNonCompliant": False}]},
    ]
    assert _summarize_compliance(domains) == "3/4"


def test_summarize_compliance_empty_or_none():
    assert _summarize_compliance(None) is None
    assert _summarize_compliance([]) is None
    assert _summarize_compliance([{"items": []}]) is None


# ---- build_inspections ----

def test_build_inspections_uses_visit_compliance_string_when_present():
    inspections = build_inspections(CHILD_UNIVERSITY_DETAIL["lstVisits"])
    assert len(inspections) == 2
    first = inspections[0]
    assert isinstance(first, InspectionItem)
    assert first["date"] == "03/05/2026"
    assert first["type"] == (
        "Child Care Center - All Age Groups-Unannounced Visit"
    )
    assert first["report_url"] == "https://example.com/report1.pdf"
    # Visit's own compliance string should win over the recomputed one
    assert first["ri_compliance"] == "87/87"
    assert first["ri_licensor"] == "Teresa CastilloBakr"


def test_build_inspections_falls_back_to_dashes_compliance_to_none():
    inspections = build_inspections(CHILD_UNIVERSITY_DETAIL["lstVisits"])
    second = inspections[1]
    # Visit reports compliance "--" with no domains → ri_compliance is None
    assert second["ri_compliance"] is None
    assert second["report_url"] == "https://example.com/report2.pdf"


def test_build_inspections_recomputes_compliance_from_domains():
    visits = [{
        "compliance": "--",
        "domains": [
            {"items": [
                {"isNonCompliant": False},
                {"isNonCompliant": True},
            ]},
        ],
        "name": "Unannounced",
        "visitDateFormatted": "01/01/2025",
        "visitDownloadURL": None,
    }]
    inspections = build_inspections(visits)
    assert inspections[0]["ri_compliance"] == "1/2"


def test_build_inspections_empty_input():
    assert build_inspections(None) == []
    assert build_inspections([]) == []


# ---- build_item ----

def test_build_item_summary_only(spider_anyway=None):
    item = build_item(CHILD_INC_SUMMARY, None)
    assert isinstance(item, ProviderItem)
    assert item["source_state"] == "Rhode Island"
    assert item["provider_name"] == "Child Inc. - Centerville Road"
    assert item["address"] == (
        "849 Centerville Road, Warwick, Rhode Island 02886, United States"
    )
    assert item["phone"] == "(401) 823-3777"
    assert item["email"] == "sbowry@childincri.org"
    assert item["provider_type"] == "Child Care Center"
    assert item["status"] == "Active"
    assert item["scholarships_accepted"] == "Yes"
    assert item["latitude"] == "41.69836129999999"
    assert item["longitude"] == "-71.49100690"
    assert item["ri_brightstars_rating"] == 5
    assert item["ri_license_decision"] == "Regular"
    assert item["ri_is_lea"] is False
    assert item["inspections"] == []
    # provider_url contains the URL-encoded pid
    assert item["provider_url"].startswith(
        "https://earlylearningprograms.dhs.ri.gov/s/program-detail"
    )
    assert "pid=62AnBeAygTrdbiatR%2BF5AXZfVB2Q" in item["provider_url"]


def test_build_item_summary_only_quest():
    item = build_item(QUEST_SUMMARY, None)
    assert item["provider_name"] == "Quest Montessori School"
    assert item["scholarships_accepted"] == "No"
    assert item["ri_brightstars_rating"] == 0
    # No detail → these stay unset
    assert "capacity" not in item or item.get("capacity") is None


def test_build_item_full_record_core_fields():
    item = build_item(CHILD_INC_SUMMARY, CHILD_UNIVERSITY_DETAIL)

    # accWrap should override the summary values
    assert item["provider_name"] == "A Child's University"
    assert item["address"] == (
        "695 Park Avenue Cranston, Rhode Island 02910, United States"
    )
    assert item["phone"] == "(401) 461-1880"
    assert item["email"] == "kelli@achilduniversity.com"
    assert item["capacity"] == 81
    assert item["languages"] == "English, Spanish"
    assert item["administrator"] == "Lynsey Colgan"
    assert item["license_begin_date"] == "10/24/2002"
    assert item["license_expiration"] == "10/31/2026"
    assert item["provider_website"] == "achildsuniversity.com"
    assert item["hours"] == (
        "Mon 7:30 AM-5:00 PM; Tue 7:30 AM-5:00 PM; Wed 7:30 AM-5:00 PM; "
        "Thu 7:30 AM-5:00 PM; Fri 7:30 AM-5:00 PM"
    )


def test_build_item_full_record_ri_fields():
    item = build_item(CHILD_INC_SUMMARY, CHILD_UNIVERSITY_DETAIL)

    assert item["ri_most_recently_renewed"] == "11/01/2025"
    assert item["ri_ccap_status"] == "Active"
    assert item["ri_ccap_expiration_date"] == "10/31/2026"
    assert item["ri_head_start"] == "No"
    assert item["ri_state_prek"] == "No"
    assert item["ri_provider_contact_name"] == "Kelli Moniz"
    assert item["ri_provider_email"] == "kelli@achilduniversity.com"
    assert item["ri_services_offered"] == ["Full day care"]
    assert item["ri_brightstars_rating"] == 5
    assert item["ri_license_decision"] == "Regular"

    # Age-group capacity rolled up from ageGroupServedWrapper
    capacity_rows = item["ri_age_group_capacity"]
    assert len(capacity_rows) == 3
    assert capacity_rows[0]["age_group"].startswith("Infant")
    assert capacity_rows[0]["capacity"] == 13

    # Availability from accWrap.accAvailability
    avail = item["ri_availability"]
    assert avail == [
        {
            "age_group": "Infant (6 weeks up to 18 months)",
            "slot_info": "No slots available, contact program for "
                         "waitlist information",
        },
        {
            "age_group": "Pre-K (4 - 5 yrs)",
            "slot_info": "Slots available, contact program for enrollment",
        },
    ]

    # ages_served is the simple cross-state list of age-group names —
    # the rich slot info lives on ri_availability.
    assert item["ages_served"] == [
        "Infant (6 weeks up to 18 months)",
        "Pre-K (4 - 5 yrs)",
    ]


def test_build_item_populates_fields_that_were_null_in_smoke_test():
    """Regression guard: a 3-provider smoke run showed `languages`,
    `provider_website`, `ri_head_start`, `ri_state_prek`,
    `ri_provider_contact_name`, `ri_provider_email`, `ri_services_offered`,
    and `ri_ccap_status`/`ri_ccap_expiration_date` all as None on every
    item. That turned out to be sparse upstream data for those specific
    providers rather than a parser bug — but we want a single test that
    proves the parser *does* surface every one of those fields when the
    source actually provides values (the A Child's University payload).

    Plus: the post-fix `--` normalization means even a present-but-dashed
    placeholder field comes through as None rather than the literal '--'.
    """
    item = build_item(CHILD_INC_SUMMARY, CHILD_UNIVERSITY_DETAIL)

    # Fields that were null on the smoke run but should populate here:
    assert item["languages"] == "English, Spanish"
    assert item["provider_website"] == "achildsuniversity.com"
    assert item["ri_head_start"] == "No"
    assert item["ri_state_prek"] == "No"
    assert item["ri_provider_contact_name"] == "Kelli Moniz"
    assert item["ri_provider_email"] == "kelli@achilduniversity.com"
    assert item["ri_services_offered"] == ["Full day care"]
    assert item["ri_ccap_status"] == "Active"
    assert item["ri_ccap_expiration_date"] == "10/31/2026"


def test_build_item_normalizes_dash_placeholder_to_none():
    """The source returns '--' literally for non-CCAP providers'
    ccapStatus/ccapExpirationDate. After the _empty_to_none fix that
    placeholder should collapse to None rather than leaking through."""
    detail = {
        "programDetailWrap": {
            "accWrap": {
                "accName": "X",
                "ccapStatus": "--",
                "ccapExpirationDate": "--",
                "headStart": "--",
            }
        },
        "lstVisits": [],
        "ageGroupServedWrapper": [],
        "scheduleOfOperationData": {"data": []},
    }
    item = build_item(CHILD_INC_SUMMARY, detail)
    assert item["ri_ccap_status"] is None
    assert item["ri_ccap_expiration_date"] is None
    assert item["ri_head_start"] is None


def test_build_item_inspections_attached():
    item = build_item(CHILD_INC_SUMMARY, CHILD_UNIVERSITY_DETAIL)
    assert len(item["inspections"]) == 2
    assert item["inspections"][0]["date"] == "03/05/2026"


def test_build_item_detail_with_blank_accwrap_falls_back_to_summary():
    """When accWrap is empty, summary values should still populate the item."""
    detail = {
        "programDetailWrap": {"accWrap": {}},
        "lstVisits": [],
        "ageGroupServedWrapper": [],
        "scheduleOfOperationData": {"data": []},
    }
    item = build_item(CHILD_INC_SUMMARY, detail)
    assert item["provider_name"] == "Child Inc. - Centerville Road"
    assert item["address"].startswith("849 Centerville Road")
    assert item["phone"] == "(401) 823-3777"
    assert item["status"] == "Active"
    assert item["capacity"] is None
    assert item["hours"] is None
    assert item["inspections"] == []


def test_build_item_pid_missing_yields_none_provider_url():
    summary = dict(CHILD_INC_SUMMARY)
    summary["id"] = None
    item = build_item(summary, None)
    assert item["provider_url"] is None


# ---- build_detail_message / build_detail_post_body ----

def test_build_detail_message_round_trip():
    msg_str = build_detail_message("ABC+/=123")
    msg = json.loads(msg_str)
    action = msg["actions"][0]
    assert action["params"]["classname"] == (
        "RICS_ViewProgramDetailsController"
    )
    assert action["params"]["method"] == "getProgramDetails"
    assert action["params"]["params"]["programId"] == "ABC+/=123"
    assert action["params"]["params"]["language"] == "English__c"


def test_build_detail_post_body_includes_all_required_fields():
    body = build_detail_post_body("ABC+/=123", "the-context-blob")

    # Pull the message back out and confirm the pid round-trips
    msg = _extract_form_field(body, "message")
    assert json.loads(msg)["actions"][0]["params"]["params"][
        "programId"
    ] == "ABC+/=123"

    assert _extract_form_field(body, "aura.context") == "the-context-blob"
    assert _extract_form_field(body, "aura.token") == "null"

    page_uri = _extract_form_field(body, "aura.pageURI")
    # pid in pageURI is URL-encoded once (so the literal `+` becomes `%2B`)
    assert "pid=ABC%2B%2F%3D123" in page_uri


# ---- _extract_form_field ----

def test_extract_form_field_basic():
    body = "a=1&b=hello%20world&c=foo"
    assert _extract_form_field(body, "a") == "1"
    assert _extract_form_field(body, "b") == "hello world"
    assert _extract_form_field(body, "c") == "foo"


def test_extract_form_field_missing_returns_none():
    assert _extract_form_field("a=1", "z") is None


def test_extract_form_field_empty_body():
    assert _extract_form_field("", "a") is None
    assert _extract_form_field(None, "a") is None


def test_extract_form_field_unique_prefix_match():
    """Don't false-positive on a longer key that starts with the same prefix."""
    body = "aura.token=null&aura.tokenfoo=other"
    assert _extract_form_field(body, "aura.token") == "null"


# ---- _empty_to_none ----

@pytest.mark.parametrize("value", [None, "", "   ", [], "--", " -- "])
def test_empty_to_none_collapses(value):
    """The source renders missing fields as '--' (e.g. ccapStatus for
    non-CCAP providers). That placeholder should normalize to None
    alongside the regular empty cases."""
    assert _empty_to_none(value) is None


@pytest.mark.parametrize("value", [0, "hi", [1], False, "---", "--ish"])
def test_empty_to_none_preserves(value):
    """Don't over-aggressively strip — only the bare '--' sentinel
    should be nulled. Strings like '---' or '--ish' must pass through."""
    assert _empty_to_none(value) == value


# ---- parse_search_page (async) ----

@pytest.fixture
def spider():
    s = RhodeIslandSpider(
        detail_delay_min=0, detail_delay_max=0, search_retries=2,
    )
    # The warm-up methods sleep for ~20s of wall time, which is fine in
    # production (they're there to satisfy reCAPTCHA v3 behavioral signals)
    # but pointless in unit tests. Each async test that wants to drive
    # parse_search_page can rely on these no-op stubs.
    s._humanize_warmup = AsyncMock()
    s._post_form_jitter = AsyncMock()
    s._reset_and_warm_up = AsyncMock()
    return s


def _make_fake_response(fake_page):
    request = Request(url=SEARCH_PAGE_URL, meta={"playwright_page": fake_page})
    return HtmlResponse(
        url=SEARCH_PAGE_URL,
        request=request,
        body=b"<html></html>",
        encoding="utf-8",
    )


async def _collect(agen):
    items = []
    async for item in agen:
        items.append(item)
    return items


@pytest.mark.asyncio
async def test_parse_search_page_full_flow(spider):
    """Two providers in search → two detail fetches → two items."""
    page = MagicMock()
    page.close = AsyncMock()

    spider._tick_age_groups = AsyncMock()
    spider._submit_search = AsyncMock(return_value=(
        [CHILD_INC_SUMMARY, QUEST_SUMMARY],
        "captured-aura-context",
    ))
    spider._fetch_detail = AsyncMock(side_effect=[
        CHILD_UNIVERSITY_DETAIL,
        None,
    ])

    response = _make_fake_response(page)
    items = await _collect(spider.parse_search_page(response))

    assert len(items) == 2
    # First item used the detail payload (renamed via accWrap)
    assert items[0]["provider_name"] == "A Child's University"
    # Second item fell back to summary-only when detail returned None
    assert items[1]["provider_name"] == "Quest Montessori School"
    assert items[1]["inspections"] == []

    spider._tick_age_groups.assert_awaited_once_with(page)
    spider._submit_search.assert_awaited_once_with(page)
    assert spider._fetch_detail.await_count == 2
    page.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_parse_search_page_empty_results_aborts(spider):
    page = MagicMock()
    page.close = AsyncMock()

    spider._tick_age_groups = AsyncMock()
    spider._submit_search = AsyncMock(return_value=([], None))
    spider._fetch_detail = AsyncMock()

    response = _make_fake_response(page)
    items = await _collect(spider.parse_search_page(response))

    assert items == []
    spider._fetch_detail.assert_not_awaited()
    page.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_parse_search_page_results_but_no_context_aborts(spider):
    page = MagicMock()
    page.close = AsyncMock()

    spider._tick_age_groups = AsyncMock()
    spider._submit_search = AsyncMock(return_value=(
        [CHILD_INC_SUMMARY], None,
    ))
    spider._fetch_detail = AsyncMock()

    response = _make_fake_response(page)
    items = await _collect(spider.parse_search_page(response))

    assert items == []
    spider._fetch_detail.assert_not_awaited()
    page.close.assert_awaited_once()


# ---- _submit_search retry loop (async) ----


@pytest.mark.asyncio
async def test_submit_search_succeeds_on_first_attempt(spider):
    page = MagicMock()
    spider._click_and_capture = AsyncMock(return_value=(
        [CHILD_INC_SUMMARY], "ctx", False,
    ))

    results, aura_context = await spider._submit_search(page)

    assert len(results) == 1
    assert aura_context == "ctx"
    assert spider._click_and_capture.await_count == 1
    spider._reset_and_warm_up.assert_not_awaited()


@pytest.mark.asyncio
async def test_submit_search_retries_on_v3_failure_then_succeeds(spider):
    page = MagicMock()
    spider._click_and_capture = AsyncMock(side_effect=[
        ([], "ctx1", True),   # v3 failed
        ([], "ctx2", True),   # v3 failed again
        ([CHILD_INC_SUMMARY], "ctx3", False),  # success
    ])

    results, aura_context = await spider._submit_search(page)

    assert len(results) == 1
    assert aura_context == "ctx3"
    # 3 click attempts; reload+warmup happens between (so twice)
    assert spider._click_and_capture.await_count == 3
    assert spider._reset_and_warm_up.await_count == 2


@pytest.mark.asyncio
async def test_submit_search_does_not_retry_on_non_captcha_failure(spider):
    """A non-captcha empty response (shape change, network issue) shouldn't
    eat retries — those are reserved for transient v3 misses."""
    page = MagicMock()
    spider._click_and_capture = AsyncMock(return_value=(
        [], "ctx", False,
    ))

    results, aura_context = await spider._submit_search(page)

    assert results == []
    assert aura_context == "ctx"
    assert spider._click_and_capture.await_count == 1
    spider._reset_and_warm_up.assert_not_awaited()


@pytest.mark.asyncio
async def test_submit_search_exhausts_retries_then_returns_empty(spider):
    page = MagicMock()
    spider._click_and_capture = AsyncMock(return_value=(
        [], "ctx", True,
    ))
    # Spider was built with search_retries=2 → 3 total attempts.

    results, aura_context = await spider._submit_search(page)

    assert results == []
    assert spider._click_and_capture.await_count == 3
    # Reload happens between attempts (so retries-many times)
    assert spider._reset_and_warm_up.await_count == 2


@pytest.mark.asyncio
async def test_submit_search_falls_through_to_manual_after_retries(spider):
    """When all v3 retries are exhausted and manual_captcha is on, the
    spider should wait for the v2 widget solve instead of giving up."""
    spider.manual_captcha = True
    page = MagicMock()
    spider._click_and_capture = AsyncMock(return_value=(
        [], "ctx", True,
    ))
    spider._wait_for_manual_search = AsyncMock(return_value=(
        [QUEST_SUMMARY], "manual-ctx",
    ))

    results, aura_context = await spider._submit_search(page)

    assert len(results) == 1
    assert aura_context == "manual-ctx"
    spider._wait_for_manual_search.assert_awaited_once_with(page)


@pytest.mark.asyncio
async def test_submit_search_retries_zero_means_one_attempt(spider):
    """search_retries=0 → exactly one click attempt, no reload."""
    spider.search_retries = 0
    page = MagicMock()
    spider._click_and_capture = AsyncMock(return_value=(
        [], "ctx", True,
    ))

    await spider._submit_search(page)

    assert spider._click_and_capture.await_count == 1
    spider._reset_and_warm_up.assert_not_awaited()


@pytest.mark.asyncio
async def test_parse_search_page_max_providers_truncates(spider):
    spider.max_providers = 1
    page = MagicMock()
    page.close = AsyncMock()

    spider._tick_age_groups = AsyncMock()
    spider._submit_search = AsyncMock(return_value=(
        [CHILD_INC_SUMMARY, QUEST_SUMMARY],
        "ctx",
    ))
    spider._fetch_detail = AsyncMock(return_value=CHILD_UNIVERSITY_DETAIL)

    response = _make_fake_response(page)
    items = await _collect(spider.parse_search_page(response))

    assert len(items) == 1
    assert spider._fetch_detail.await_count == 1

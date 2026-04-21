import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from scrapy.http import HtmlResponse, Request

from provider_scrape.items import ProviderItem
from provider_scrape.spiders.new_jersey import (
    API_PATH,
    NewJerseySpider,
    SEARCH_URL,
    _build_social_media,
    _empty_to_none,
    _trim_seconds,
    build_address,
    build_api_path,
    build_item,
    format_hours,
)


@pytest.fixture
def spider():
    return NewJerseySpider()


# ---- Sample facility records (real JSON shape from /GetProviders.aspx) ----

WOW_KIDS = {
    "UniqueProgramID": "711671",
    "ProgramName": "#Wow Kids, LLC",
    "ProgramAddressStreetNumber": "128",
    "ProgramAddressStreetName": "Harrison Street",
    "ProgramCity": "Hoboken",
    "ProgramState": "NJ",
    "ProgramZipCode": "07030",
    "ProgramCounty": "Hudson",
    "ProgramWebsiteLink": None,
    "ProgramEmail": "wowkidshoboken@gmail.com",
    "ProgramPhoneNumber": "201-216-0822",
    "ProgramPhoneNumberExtension": None,
    "ProgramLicensedCapacityTotal": 30,
    "AgesLicensedToServe": [1, 2, 3, 4],
    "ProgramFacilityType": 1,
    "NJFacilityType": 1,
    "LicenseType": 1,
    "LicenseNumber": "160800004",
    "QualityRating": None,
    "LanguagesSpokenByStaff": ["English", "Spanish"],
    "Accreditation": [],
    "YearlySchedule": 1,
    "DohID": None,
    "DailyOpeningTimeSunday": "",
    "DailyOpeningTimeMonday": "07:30:00",
    "DailyOpeningTimeTuesday": "07:30:00",
    "DailyOpeningTimeWednesday": "07:30:00",
    "DailyOpeningTimeThursday": "07:30:00",
    "DailyOpeningTimeFriday": "07:30:00",
    "DailyOpeningTimeSaturday": "",
    "DailyClosingTimeSunday": "",
    "DailyClosingTimeMonday": "18:30:00",
    "DailyClosingTimeTuesday": "18:30:00",
    "DailyClosingTimeWednesday": "18:30:00",
    "DailyClosingTimeThursday": "18:30:00",
    "DailyClosingTimeFriday": "18:30:00",
    "DailyClosingTimeSaturday": "",
    "ProgramSocialMediaLinkTwitter": "",
    "ProgramSocialMediaLinkFacebook": "",
    "ProgramSocialMediaLinkInstagram": "",
    "ProgramSocialMediaLinkYouTube": "",
    "ParticipationInLocalStateOrFederalPrograms": [3],
    "CurriculumUsed": None,
    "ChildAssessmentUsed": None,
    "EnvironmentalFeatures": [],
    "MealOptions": [],
    "TrainingAndExperienceToSupportSpecialNeeds": [],
    "TransportationOptions": [],
    "SpecialSchedules": [],
    "TuitionInfantHourly": None,
    "TuitionInfantDaily": None,
    "TuitionInfantWeekly": None,
    "TuitionInfantMonthly": None,
    "TuitionToddlerHourly": None,
    "TuitionToddlerDaily": None,
    "TuitionToddlerWeekly": None,
    "TuitionToddlerMonthly": None,
    "TuitionPreschoolHourly": None,
    "TuitionPreschoolDaily": None,
    "TuitionPreschoolWeekly": None,
    "TuitionPreschoolMonthly": None,
    "TuitionSchoolAgeHourly": None,
    "TuitionSchoolAgeDaily": None,
    "TuitionSchoolAgeWeekly": None,
    "TuitionSchoolAgeMonthly": None,
    "DiscountsAvailable": [],
    "AdditionalDepositsAndFees": [],
    "MCCYNPlusInd": "2",
    "Hidden": False,
}

TWO_FOR_CARE = {
    "UniqueProgramID": "703257",
    "ProgramName": "2 For Care Early Childhood  Learning Center, LLC",
    "ProgramAddressStreetNumber": "619",
    "ProgramAddressStreetName": "Bangs Avenue",
    "ProgramCity": "Asbury Park",
    "ProgramState": "NJ",
    "ProgramZipCode": "07712",
    "ProgramCounty": "Monmouth",
    "ProgramWebsiteLink": None,
    "ProgramEmail": "2forcare@gmail.com",
    "ProgramPhoneNumber": "732-455-5222",
    "ProgramPhoneNumberExtension": None,
    "ProgramLicensedCapacityTotal": 27,
    "AgesLicensedToServe": [1, 2, 3, 4],
    "ProgramFacilityType": 1,
    "NJFacilityType": 1,
    "LicenseType": 1,
    "LicenseNumber": "13OLA0001",
    "QualityRating": 3,
    "LanguagesSpokenByStaff": ["English", "French Creole", "Spanish"],
    "Accreditation": [],
    "YearlySchedule": 1,
    "DohID": None,
    "DailyOpeningTimeSunday": "",
    "DailyOpeningTimeMonday": "07:30:00",
    "DailyOpeningTimeTuesday": "07:30:00",
    "DailyOpeningTimeWednesday": "07:30:00",
    "DailyOpeningTimeThursday": "07:30:00",
    "DailyOpeningTimeFriday": "07:30:00",
    "DailyOpeningTimeSaturday": "",
    "DailyClosingTimeSunday": "",
    "DailyClosingTimeMonday": "17:30:00",
    "DailyClosingTimeTuesday": "17:30:00",
    "DailyClosingTimeWednesday": "17:30:00",
    "DailyClosingTimeThursday": "17:30:00",
    "DailyClosingTimeFriday": "17:30:00",
    "DailyClosingTimeSaturday": "",
    "ProgramSocialMediaLinkTwitter": "",
    "ProgramSocialMediaLinkFacebook": "",
    "ProgramSocialMediaLinkInstagram": "",
    "ProgramSocialMediaLinkYouTube": "",
    "ParticipationInLocalStateOrFederalPrograms": [3],
    "CurriculumUsed": "creative curriculum",
    "ChildAssessmentUsed": None,
    "EnvironmentalFeatures": [2, 5, 10, 11],
    "MealOptions": [4, 5, 1, 2],
    "TrainingAndExperienceToSupportSpecialNeeds": [],
    "TransportationOptions": [2],
    "SpecialSchedules": [],
    "TuitionInfantHourly": 0.0,
    "TuitionInfantDaily": 0.0,
    "TuitionInfantWeekly": 375.6,
    "TuitionInfantMonthly": 1626.35,
    "TuitionToddlerHourly": 0.0,
    "TuitionToddlerDaily": 0.0,
    "TuitionToddlerWeekly": 321.85,
    "TuitionToddlerMonthly": 1393.61,
    "TuitionPreschoolHourly": 0.0,
    "TuitionPreschoolDaily": 0.0,
    "TuitionPreschoolWeekly": 291.9,
    "TuitionPreschoolMonthly": 1263.93,
    "TuitionSchoolAgeHourly": 0.0,
    "TuitionSchoolAgeDaily": 0.0,
    "TuitionSchoolAgeWeekly": 265.6,
    "TuitionSchoolAgeMonthly": 1147.45,
    "DiscountsAvailable": [],
    "AdditionalDepositsAndFees": [1, 2],
    "MCCYNPlusInd": "1",
    "Hidden": False,
}

TWENTY_FIRST_CENTURY = {
    "UniqueProgramID": "709899",
    "ProgramName": "21st Century Community Learning Center - Middle Earth",
    "ProgramAddressStreetNumber": "",
    "ProgramAddressStreetName": "Smalley School 163 Cherry Ave",
    "ProgramCity": "Bound Brook",
    "ProgramState": "NJ",
    "ProgramZipCode": "08805",
    "ProgramCounty": "Somerset",
    "ProgramWebsiteLink": None,
    "ProgramEmail": "blyons@middleearthnj.org",
    "ProgramPhoneNumber": "908-725-2223",
    "ProgramPhoneNumberExtension": None,
    "ProgramLicensedCapacityTotal": 150,
    "AgesLicensedToServe": [4],
    "ProgramFacilityType": 1,
    "NJFacilityType": 1,
    "LicenseType": 1,
    "LicenseNumber": "1821S0001",
    "QualityRating": None,
    "LanguagesSpokenByStaff": ["English", "Spanish"],
    "Accreditation": [],
    "YearlySchedule": 1,
    "DohID": None,
    "DailyOpeningTimeSunday": "",
    "DailyOpeningTimeMonday": "14:30:00",
    "DailyOpeningTimeTuesday": "14:30:00",
    "DailyOpeningTimeWednesday": "14:30:00",
    "DailyOpeningTimeThursday": "14:30:00",
    "DailyOpeningTimeFriday": "14:30:00",
    "DailyOpeningTimeSaturday": "",
    "DailyClosingTimeSunday": "",
    "DailyClosingTimeMonday": "17:45:00",
    "DailyClosingTimeTuesday": "17:45:00",
    "DailyClosingTimeWednesday": "17:45:00",
    "DailyClosingTimeThursday": "17:45:00",
    "DailyClosingTimeFriday": "17:45:00",
    "DailyClosingTimeSaturday": "",
    "ProgramSocialMediaLinkTwitter": "",
    "ProgramSocialMediaLinkFacebook": "",
    "ProgramSocialMediaLinkInstagram": "",
    "ProgramSocialMediaLinkYouTube": "",
    "ParticipationInLocalStateOrFederalPrograms": [],
    "CurriculumUsed": None,
    "ChildAssessmentUsed": None,
    "EnvironmentalFeatures": [],
    "MealOptions": [],
    "TrainingAndExperienceToSupportSpecialNeeds": [],
    "TransportationOptions": [],
    "SpecialSchedules": [],
    "TuitionInfantHourly": None,
    "TuitionInfantDaily": None,
    "TuitionInfantWeekly": None,
    "TuitionInfantMonthly": None,
    "TuitionToddlerHourly": None,
    "TuitionToddlerDaily": None,
    "TuitionToddlerWeekly": None,
    "TuitionToddlerMonthly": None,
    "TuitionPreschoolHourly": None,
    "TuitionPreschoolDaily": None,
    "TuitionPreschoolWeekly": None,
    "TuitionPreschoolMonthly": None,
    "TuitionSchoolAgeHourly": None,
    "TuitionSchoolAgeDaily": None,
    "TuitionSchoolAgeWeekly": None,
    "TuitionSchoolAgeMonthly": None,
    "DiscountsAvailable": [],
    "AdditionalDepositsAndFees": [],
    "MCCYNPlusInd": "2",
    "Hidden": False,
}


# ---- build_api_path ----


def test_build_api_path_contains_all_static_params():
    path = build_api_path(page_size=10)
    for required in [
        "zipcode=",
        "county=",
        "programName=",
        "camp=false",
        "center=false",
        "home=false",
        "preschool=false",
        "rating=",
        "programTypes=",
        "subsidised=false",
        "mccynplus=false",
        "sortKey=ProgramName",
        "sortDirection=0",
        "pageSize=10",
        "currentPage=0",
    ]:
        assert required in path, f"missing {required!r} in {path!r}"


def test_build_api_path_root_is_services_endpoint():
    path = build_api_path(page_size=1)
    assert path.startswith(f"{API_PATH}?")


def test_build_api_path_page_size_large():
    path = build_api_path(page_size=5000)
    assert "pageSize=5000" in path


def test_build_api_path_current_page_non_zero():
    path = build_api_path(page_size=200, current_page=3)
    assert "pageSize=200" in path
    assert "currentPage=3" in path


# ---- _trim_seconds ----


def test_trim_seconds_basic():
    assert _trim_seconds("07:30:00") == "07:30"


def test_trim_seconds_empty_string():
    assert _trim_seconds("") == ""


def test_trim_seconds_none():
    assert _trim_seconds(None) == ""


def test_trim_seconds_already_trimmed():
    assert _trim_seconds("07:30") == "07:30"


# ---- format_hours ----


def test_format_hours_weekdays_only():
    result = format_hours(WOW_KIDS)
    assert result == (
        "Mon 07:30-18:30; Tue 07:30-18:30; Wed 07:30-18:30; "
        "Thu 07:30-18:30; Fri 07:30-18:30"
    )


def test_format_hours_all_empty_returns_none():
    blank = {
        f"DailyOpeningTime{day}": ""
        for day in [
            "Sunday", "Monday", "Tuesday", "Wednesday",
            "Thursday", "Friday", "Saturday",
        ]
    }
    blank.update({
        f"DailyClosingTime{day}": ""
        for day in [
            "Sunday", "Monday", "Tuesday", "Wednesday",
            "Thursday", "Friday", "Saturday",
        ]
    })
    assert format_hours(blank) is None


def test_format_hours_only_emits_days_with_both_open_and_close():
    facility = dict(WOW_KIDS)
    # Zero out Monday close — that day should drop from the output
    facility["DailyClosingTimeMonday"] = ""
    result = format_hours(facility)
    assert "Mon " not in result
    assert "Tue 07:30-18:30" in result


# ---- build_address ----


def test_build_address_normal():
    assert build_address(WOW_KIDS) == "128 Harrison Street, Hoboken, NJ 07030"


def test_build_address_street_number_empty_uses_street_name_only():
    assert (
        build_address(TWENTY_FIRST_CENTURY)
        == "Smalley School 163 Cherry Ave, Bound Brook, NJ 08805"
    )


def test_build_address_all_empty_returns_none():
    assert build_address({}) is None


def test_build_address_only_street():
    assert build_address({
        "ProgramAddressStreetNumber": "1",
        "ProgramAddressStreetName": "Main St",
        "ProgramCity": "",
        "ProgramState": "",
        "ProgramZipCode": "",
    }) == "1 Main St"


def test_build_address_missing_zip():
    assert build_address({
        "ProgramAddressStreetNumber": "1",
        "ProgramAddressStreetName": "Main St",
        "ProgramCity": "Newark",
        "ProgramState": "NJ",
        "ProgramZipCode": "",
    }) == "1 Main St, Newark, NJ"


# ---- _empty_to_none ----


@pytest.mark.parametrize(
    "value",
    [None, "", "   ", []],
)
def test_empty_to_none_collapses_empties(value):
    assert _empty_to_none(value) is None


@pytest.mark.parametrize(
    "value",
    [0, "hi", [1], False],
)
def test_empty_to_none_preserves_real_values(value):
    assert _empty_to_none(value) == value


# ---- _build_social_media ----


def test_build_social_media_all_blank_returns_none():
    assert _build_social_media(WOW_KIDS) is None


def test_build_social_media_one_set():
    facility = dict(WOW_KIDS)
    facility["ProgramSocialMediaLinkFacebook"] = "https://fb.example/x"
    result = _build_social_media(facility)
    assert result == {
        "twitter": None,
        "facebook": "https://fb.example/x",
        "instagram": None,
        "youtube": None,
    }


# ---- build_item: full rich record ----


def test_build_item_full_record_core_fields():
    item = build_item(TWO_FOR_CARE)

    assert isinstance(item, ProviderItem)
    assert item["source_state"] == "New Jersey"
    assert item["provider_url"] is None
    assert item["provider_name"] == "2 For Care Early Childhood  Learning Center, LLC"
    assert item["address"] == "619 Bangs Avenue, Asbury Park, NJ 07712"
    assert item["county"] == "Monmouth"
    assert item["email"] == "2forcare@gmail.com"
    assert item["phone"] == "732-455-5222"
    assert item["capacity"] == 27
    assert item["license_number"] == "13OLA0001"
    assert item["languages"] == ["English", "French Creole", "Spanish"]
    assert item["ages_served"] == [1, 2, 3, 4]
    assert item["hours"] == (
        "Mon 07:30-17:30; Tue 07:30-17:30; Wed 07:30-17:30; "
        "Thu 07:30-17:30; Fri 07:30-17:30"
    )


def test_build_item_full_record_nj_fields():
    item = build_item(TWO_FOR_CARE)

    assert item["nj_unique_program_id"] == "703257"
    assert item["nj_program_facility_type"] == 1
    assert item["nj_facility_type"] == 1
    assert item["nj_license_type"] == 1
    assert item["nj_quality_rating"] == 3
    assert item["nj_accreditation"] is None  # empty list
    assert item["nj_yearly_schedule"] == 1
    assert item["nj_doh_id"] is None
    assert item["nj_participation_programs"] == [3]
    assert item["nj_curriculum"] == "creative curriculum"
    assert item["nj_environmental_features"] == [2, 5, 10, 11]
    assert item["nj_meal_options"] == [4, 5, 1, 2]
    assert item["nj_transportation"] == [2]
    assert item["nj_fees"] == [1, 2]
    assert item["nj_mccyn_plus"] == "1"
    assert item["nj_social_media"] is None


def test_build_item_full_record_tuition_grid():
    item = build_item(TWO_FOR_CARE)

    assert item["nj_tuition_infant_hourly"] == 0.0
    assert item["nj_tuition_infant_weekly"] == 375.6
    assert item["nj_tuition_infant_monthly"] == 1626.35
    assert item["nj_tuition_toddler_weekly"] == 321.85
    assert item["nj_tuition_preschool_monthly"] == 1263.93
    assert item["nj_tuition_school_age_weekly"] == 265.6
    assert item["nj_tuition_school_age_monthly"] == 1147.45


def test_build_item_all_sixteen_tuition_fields_populated_or_none():
    item = build_item(TWO_FOR_CARE)
    expected = [
        f"nj_tuition_{age}_{cadence}"
        for age in ("infant", "toddler", "preschool", "school_age")
        for cadence in ("hourly", "daily", "weekly", "monthly")
    ]
    for key in expected:
        # Either a number or None (no KeyError / unset)
        assert key in item, f"missing tuition field {key}"


# ---- build_item: sparse record ----


def test_build_item_sparse_record():
    item = build_item(WOW_KIDS)

    assert item["provider_name"] == "#Wow Kids, LLC"
    assert item["address"] == "128 Harrison Street, Hoboken, NJ 07030"
    assert item["county"] == "Hudson"
    assert item["email"] == "wowkidshoboken@gmail.com"
    assert item["nj_quality_rating"] is None
    assert item["provider_website"] is None
    assert item["nj_doh_id"] is None

    # Empty lists collapse to None
    assert item["nj_accreditation"] is None
    assert item["nj_environmental_features"] is None
    assert item["nj_meal_options"] is None
    assert item["nj_discounts"] is None
    assert item["nj_fees"] is None

    # Every tuition field should be None
    for age in ("infant", "toddler", "preschool", "school_age"):
        for cadence in ("hourly", "daily", "weekly", "monthly"):
            assert item[f"nj_tuition_{age}_{cadence}"] is None


def test_build_item_sparse_record_hours():
    item = build_item(WOW_KIDS)
    # Weekday-only hours
    assert item["hours"] == (
        "Mon 07:30-18:30; Tue 07:30-18:30; Wed 07:30-18:30; "
        "Thu 07:30-18:30; Fri 07:30-18:30"
    )


def test_build_item_packed_street_name():
    item = build_item(TWENTY_FIRST_CENTURY)
    assert (
        item["address"]
        == "Smalley School 163 Cherry Ave, Bound Brook, NJ 08805"
    )
    assert item["ages_served"] == [4]
    assert item["nj_participation_programs"] is None


def test_build_item_phone_extension_captured_when_present():
    facility = dict(WOW_KIDS)
    facility["ProgramPhoneNumberExtension"] = "123"
    item = build_item(facility)
    assert item["phone"] == "201-216-0822"
    assert item["nj_phone_extension"] == "123"


def test_build_item_phone_extension_none_when_blank():
    item = build_item(WOW_KIDS)
    assert item["nj_phone_extension"] is None


# ---- parse_search_page (async) ----


def _make_fake_response(fake_page):
    """Build a scrapy Response whose meta carries a mock Playwright page."""
    request = Request(url=SEARCH_URL, meta={"playwright_page": fake_page})
    return HtmlResponse(
        url=SEARCH_URL,
        request=request,
        body=b"<html></html>",
        encoding="utf-8",
    )


def _make_fake_page(responses):
    """Return a mock with an async `evaluate` that returns each response in order."""
    page = MagicMock()
    page.evaluate = AsyncMock(side_effect=list(responses))
    page.close = AsyncMock()
    return page


def _payload(facility_count, result_count=None, template=None):
    """Build an API payload that contains `facility_count` facilities."""
    template = template or WOW_KIDS
    facilities = []
    for i in range(facility_count):
        rec = dict(template)
        rec["UniqueProgramID"] = str(700000 + i)
        rec["ProgramName"] = f"Provider {i}"
        facilities.append(rec)
    return json.dumps({
        "Facilities": facilities,
        "ResultCount": result_count if result_count is not None else facility_count,
    })


async def _collect(agen):
    items = []
    async for item in agen:
        items.append(item)
    return items


@pytest.mark.asyncio
async def test_parse_search_page_single_shot_success(spider):
    probe_body = _payload(facility_count=1, result_count=3)
    full_body = _payload(facility_count=3, result_count=3)
    page = _make_fake_page([
        {"status": 200, "body": probe_body},
        {"status": 200, "body": full_body},
    ])
    response = _make_fake_response(page)

    items = await _collect(spider.parse_search_page(response))

    assert len(items) == 3
    for item in items:
        assert item["source_state"] == "New Jersey"

    # Two evaluate calls: probe + single-shot
    assert page.evaluate.await_count == 2
    probe_call_path = page.evaluate.call_args_list[0].args[1]
    big_call_path = page.evaluate.call_args_list[1].args[1]
    assert "pageSize=1" in probe_call_path
    # ResultCount 3 + default buffer 100 = 103
    assert "pageSize=103" in big_call_path
    page.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_parse_search_page_falls_back_to_pagination_when_capped(spider):
    spider.fallback_page_size = 2
    probe_body = _payload(facility_count=1, result_count=5)
    # Server returns only 2 facilities even though we asked for 105 — treat as capped
    capped_body = _payload(facility_count=2, result_count=5)
    page_a = _payload(facility_count=2, result_count=5)
    page_b = _payload(facility_count=2, result_count=5)
    page_c = _payload(facility_count=1, result_count=5)

    page = _make_fake_page([
        {"status": 200, "body": probe_body},
        {"status": 200, "body": capped_body},
        {"status": 200, "body": page_a},
        {"status": 200, "body": page_b},
        {"status": 200, "body": page_c},
    ])
    response = _make_fake_response(page)

    items = await _collect(spider.parse_search_page(response))

    assert len(items) == 5
    # 1 probe + 1 big-capped + 3 paginated pages
    assert page.evaluate.await_count == 5
    paginated_calls = [
        call.args[1] for call in page.evaluate.call_args_list[2:]
    ]
    assert all("pageSize=2" in path for path in paginated_calls)
    assert "currentPage=0" in paginated_calls[0]
    assert "currentPage=1" in paginated_calls[1]
    assert "currentPage=2" in paginated_calls[2]
    page.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_parse_search_page_probe_http_error_stops_early(spider):
    page = _make_fake_page([
        {"status": 403, "body": "Cloudflare blocked"},
    ])
    response = _make_fake_response(page)

    items = await _collect(spider.parse_search_page(response))

    assert items == []
    assert page.evaluate.await_count == 1
    page.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_parse_search_page_zero_result_count_exits_cleanly(spider):
    page = _make_fake_page([
        {"status": 200, "body": json.dumps({"Facilities": [], "ResultCount": 0})},
    ])
    response = _make_fake_response(page)

    items = await _collect(spider.parse_search_page(response))

    assert items == []
    # Only the probe call is made; no big fetch
    assert page.evaluate.await_count == 1
    page.close.assert_awaited_once()

import json
import os

import pytest
from scrapy.http import Request, TextResponse

from provider_scrape import normalization as norm
from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.spiders.iowa import (
    PINS_URL,
    TITAN_SEARCH_URL,
    IowaSpider,
    ages_served_flags,
    build_inspection,
    build_provider_item,
    build_report_url,
    format_hours,
    openings_by_age,
    reports_url,
    titan_iso_date,
    titan_mdy_date,
    titan_search_url,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def _load_fixture(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return json.load(fh)


def _titan_row(pins_page, provider_id):
    for row in pins_page["ComplianceList"]:
        if row["ProviderID"] == provider_id:
            return row
    raise KeyError(provider_id)


def _pin(pins_sample, license_id):
    for pin in pins_sample:
        if pin["licenseId"] == license_id:
            return pin
    raise KeyError(license_id)


@pytest.fixture
def spider():
    return IowaSpider()


@pytest.fixture
def pins_sample():
    return _load_fixture("ia_pins_sample.json")


@pytest.fixture
def titan_page():
    return _load_fixture("ia_titan_page.json")


# --- response builders ------------------------------------------------- #

def titan_response(payload, page=0):
    req = Request(TITAN_SEARCH_URL, meta={"page": page})
    return TextResponse(url=titan_search_url(page), body=json.dumps(payload).encode(),
                        encoding="utf-8", request=req)


def pins_response(payload):
    req = Request(PINS_URL, method="POST")
    return TextResponse(url=PINS_URL, body=json.dumps(payload).encode(),
                        encoding="utf-8", request=req)


def reports_response(payload, url, item, provider_id, expected_counts):
    req = Request(url, meta={"item": item, "provider_id": provider_id,
                              "expected_counts": expected_counts})
    return TextResponse(url=url, body=json.dumps(payload).encode(),
                        encoding="utf-8", request=req)


def split_requests(outputs, expected_url):
    """Partition parse_titan output into (titan_page_reqs, other_reqs)."""
    titan = [r for r in outputs if r.url.startswith(expected_url)]
    other = [r for r in outputs if not r.url.startswith(expected_url)]
    return titan, other


# --- pure helper unit tests --------------------------------------------- #

def test_titan_search_url_builds_page_query():
    url = titan_search_url(5)
    assert url.startswith(TITAN_SEARCH_URL + "?")
    assert "PageIndex=5" in url
    assert "ProviderID=" in url


def test_reports_url_builds_provider_and_type_query():
    url = reports_url(25854, 1)
    assert url == (
        "https://secureapp.dhs.state.ia.us/dhs_titan_public/ChildCare/"
        "GetProviderComplaintAndComplicanceReportList?providerID=25854&TypeOfCareID=1"
    )


# --- agesServed -> booleans (case 2) ------------------------------------ #

def test_ages_served_flags_full_vocabulary():
    ages = ("Infant(0-12), Infant(13-23), Toddler(2yo), Preschool(3yo), "
            "Preschool(4-5yo), Before&After School, School Age")
    flags = ages_served_flags(ages)
    assert flags == {"infant": True, "toddler": True, "preschool": True,
                      "school": True}


def test_ages_served_flags_partial_list():
    # 6320's real agesServed: only preschool + school tokens present.
    flags = ages_served_flags(
        "Preschool(3yo), Preschool(4-5yo), Before&After School")
    assert flags == {"preschool": True, "school": True}
    assert "infant" not in flags
    assert "toddler" not in flags


def test_ages_served_flags_unknown_token_logged(caplog):
    with caplog.at_level("WARNING"):
        flags = ages_served_flags("Infant(0-12), Martian Toddler")
    assert flags == {"infant": True}
    assert "unrecognized agesServed token" in caplog.text


def test_ages_served_flags_empty():
    assert ages_served_flags(None) == {}
    assert ages_served_flags("") == {}


# --- hours rendering (case 3) ------------------------------------------- #

def test_format_hours_uniform_week_collapses(pins_sample):
    # 45047: identical hours every day of the week.
    pin = _pin(pins_sample, 45047)
    assert format_hours(pin["formattedHoursOfOperation"]) == \
        "Monday-Sunday 12:00 AM - 11:45 PM"


def test_format_hours_null_weekend_collapses_listed_days(pins_sample):
    # 6320: Mon-Thu share one time, Fri/Sat/Sun are null (closed).
    pin = _pin(pins_sample, 6320)
    assert format_hours(pin["formattedHoursOfOperation"]) == \
        "Monday-Thursday 7:45 AM - 3:15 PM"


def test_format_hours_mixed_days_lists_each(pins_sample):
    # 45025: Wednesday closes an hour earlier than the rest of the week.
    pin = _pin(pins_sample, 45025)
    rendered = format_hours(pin["formattedHoursOfOperation"])
    assert "Monday 7:45 AM - 3:00 PM" in rendered
    assert "Wednesday 7:45 AM - 2:00 PM" in rendered
    assert "Saturday" not in rendered


def test_format_hours_all_null_is_none(pins_sample):
    # 52883: every day is null.
    pin = _pin(pins_sample, 52883)
    assert format_hours(pin["formattedHoursOfOperation"]) is None
    assert format_hours(None) is None
    assert format_hours({}) is None


# --- ia_openings_by_age (case 4) ---------------------------------------- #

def test_openings_by_age_assembled_from_flat_keys(pins_sample):
    pin = _pin(pins_sample, 45047)
    bands = openings_by_age(pin)
    assert len(bands) == 7
    infant_band = bands[0]
    assert infant_band == {"ageGroup": "Infant (0-12 mo.)", "fullTime": 1,
                            "partTime": 2}


def test_openings_by_age_all_zero(pins_sample):
    # 48370: every opening count is 0 -- still emits all 7 bands.
    pin = _pin(pins_sample, 48370)
    bands = openings_by_age(pin)
    assert len(bands) == 7
    assert all(b["fullTime"] == 0 and b["partTime"] == 0 for b in bands)


# --- build_provider_item: golden path (case 1) --------------------------- #

def test_build_provider_item_golden_joined(pins_sample, titan_page):
    c3 = _pin(pins_sample, 45047)
    titan = _titan_row(titan_page, 45047)
    item = build_provider_item(c3, titan)

    assert isinstance(item, ProviderItem)
    assert item["source_state"] == "Iowa"
    assert item["provider_url"] == "https://search.iachildcareconnect.org/Map"
    assert item["provider_name"] == "Heavenly Creations Childcare & Learning Center"
    assert item["license_number"] == "45047"
    assert item["ia_padded_license_id"] == "45047"  # not zero-padded for this id
    assert item["provider_type"] == "Registered Child Development Home C"
    assert item["status"] == "A"
    assert item["address"] == "1704 Washington Ave"
    assert item["city"] == "Des Moines"
    assert item["state"] == "IA"
    assert item["zip"] == "50314"
    assert isinstance(item["zip"], str)
    assert item["county"] == "Polk"
    assert item["latitude"] == "41.6093194"
    assert item["longitude"] == "-93.6407105"
    assert item["phone"] == "(515) 505-6655"
    assert item["email"] == "heavenlycreationschildcare@gmail.com"
    assert "provider_website" not in item  # website is null
    assert item["hours"] == "Monday-Sunday 12:00 AM - 11:45 PM"
    assert item["ages_served"].startswith("Infant(0-12)")
    assert item["infant"] and item["toddler"] and item["preschool"] and item["school"]
    assert item["languages"] == "English"
    assert item["transportation"] == "Provider Does Not Transport, On School Bus Route"
    assert item["meals"] == "Yes"
    assert item["scholarships_accepted"] is True
    assert item["ia_total_openings"] == 17
    assert item["accepting_new_children"] is True
    assert len(item["ia_openings_by_age"]) == 7
    assert item["ia_openings_as_of"] == "06/24/2026"
    assert item["ia_days_of_operation"] == "Mon, Tue, Wed, Thu, Fri, Sat, Sun"
    assert item["ia_serves_special_needs"] == "No"
    assert "ia_iq4k_level" not in item  # null for this provider
    assert item["ia_business_type"] == "house"
    assert item["ia_region"] == "Region 4"
    assert item["ia_referral_listed"] is True
    assert item["ia_compliance_report_count"] == 3
    assert item["ia_complaint_count"] == 0
    assert item["ia_regulation_checklist_count"] == 1
    assert "capacity" not in item  # never available (§4.5)


def test_golden_item_has_no_undefined_fields(pins_sample, titan_page):
    c3 = _pin(pins_sample, 45047)
    titan = _titan_row(titan_page, 45047)
    item = build_provider_item(c3, titan)
    assert dict(item)  # constructing/serializing raises on an undefined field


def test_zero_padded_license_id_join(pins_sample, titan_page):
    # 6320: paddedLicenseId "06320" != the unpadded licenseId 6320 used for
    # license_number/the join key (§5.9).
    c3 = _pin(pins_sample, 6320)
    titan = _titan_row(titan_page, 6320)
    item = build_provider_item(c3, titan)
    assert item["license_number"] == "6320"
    assert item["ia_padded_license_id"] == "06320"
    assert item["provider_type"] == "Licensed Center"
    assert item["ia_iq4k_level"] == "IQ4K Level 1"
    assert item["ia_business_type"] == "building"


# --- missing-data path (case 5) ------------------------------------------ #

def test_missing_data_path_no_bogus_fields(pins_sample, titan_page):
    # 52883: website null, iQ4KLevel null, totalOpenings 0 (present, not
    # missing) -> accepting_new_children must be an explicit False, not
    # absent and not True.
    c3 = _pin(pins_sample, 52883)
    titan = _titan_row(titan_page, 52883)
    item = build_provider_item(c3, titan)
    assert "provider_website" not in item
    assert "ia_iq4k_level" not in item
    assert item["ia_total_openings"] == 0
    assert item["accepting_new_children"] is False
    assert item.get("hours") is None


# --- C3-only / Titan-only union paths (cases 6, 7) ----------------------- #

def test_c3_only_row_omits_titan_fields(pins_sample):
    # 52674: confirmed live to have no Titan row (§11 C3-only id).
    c3 = _pin(pins_sample, 52674)
    item = build_provider_item(c3, None)
    assert item["provider_name"] == "Jana Daskalakes"
    assert item["license_number"] == "52674"
    assert "provider_type" not in item
    assert "status" not in item
    assert "ia_compliance_report_count" not in item
    # everything else C3 publishes is still populated.
    assert item["address"] == "3402 48th St"
    assert item["state"] == "IA"
    assert item["latitude"] and item["longitude"]
    assert item["accepting_new_children"] is False


def test_titan_only_row_omits_c3_fields(titan_page):
    # 49764: Titan-only Exempt-from-Licensing provider in a non-Iowa county
    # (Douglas, NE -- §5.10).
    titan = _titan_row(titan_page, 49764)
    item = build_provider_item(None, titan)
    assert item["provider_name"] == "24/2 Child Care Center"
    assert item["license_number"] == "49764"
    assert item["provider_type"] == "Exempt from Licensing"
    assert item["status"] == "A"
    assert "address" not in item
    assert "latitude" not in item
    assert "longitude" not in item
    assert "phone" not in item
    assert "state" not in item  # left None -- not all Titan-only rows are IA
    assert item["county"] == "Douglas"  # Titan's bare spelling, unchanged


def test_titan_only_in_home_row(titan_page):
    titan = _titan_row(titan_page, 53158)
    item = build_provider_item(None, titan)
    assert item["provider_type"] == "In-Home"
    assert norm.facility_category_from_type(item["provider_type"]) == "other"


# --- county suffix stripping (case 8) ------------------------------------ #

def test_county_suffix_stripped_from_c3(pins_sample, titan_page):
    c3 = _pin(pins_sample, 45047)
    assert c3["county"] == "Polk County"
    item = build_provider_item(c3, None)
    assert item["county"] == "Polk"


def test_titan_bare_county_unchanged(titan_page):
    titan = _titan_row(titan_page, 49764)
    assert titan["County"] == "Douglas"  # already bare -- no " County" suffix
    item = build_provider_item(None, titan)
    assert item["county"] == "Douglas"


# --- zipCode int -> string (case 9) -------------------------------------- #

def test_zip_code_int_to_string(pins_sample):
    c3 = _pin(pins_sample, 45047)
    assert isinstance(c3["zipCode"], int)
    item = build_provider_item(c3, None)
    assert item["zip"] == "50314"
    assert isinstance(item["zip"], str)


# --- Titan pagination chain (case 10) ------------------------------------ #

def test_parse_titan_first_page_schedules_next_page(spider, titan_page):
    outputs = list(spider.parse_titan(titan_response(titan_page, page=0)))
    titan_reqs, other_reqs = split_requests(outputs, TITAN_SEARCH_URL)
    assert len(titan_reqs) == 1
    assert titan_reqs[0].meta["page"] == 1
    assert other_reqs == []  # does NOT fire the pins POST yet
    assert len(spider.licensing) == 6


def test_parse_titan_last_page_schedules_pins_post(spider, titan_page):
    # totalPage is 68 -> the last page index is 67.
    outputs = list(spider.parse_titan(titan_response(titan_page, page=67)))
    titan_reqs, other_reqs = split_requests(outputs, TITAN_SEARCH_URL)
    assert titan_reqs == []  # no further Titan page
    assert len(other_reqs) == 1
    assert other_reqs[0].url == PINS_URL
    assert other_reqs[0].method == "POST"


# --- short-harvest warning (case 11) -------------------------------------- #

def test_parse_titan_short_harvest_logs_warning(spider, titan_page, caplog):
    # Only 6 rows are ever fed in (this fixture), but absoluteTotal is the
    # real 3,370 -- feeding it as the LAST page must trip the short-harvest
    # warning (§5.1).
    with caplog.at_level("WARNING"):
        list(spider.parse_titan(titan_response(titan_page, page=67)))
    assert "SHORT HARVEST" in caplog.text
    assert "3370" in caplog.text


def test_parse_titan_exact_harvest_no_warning(spider, titan_page, caplog):
    exact = dict(titan_page)
    exact["absoluteTotal"] = len(exact["ComplianceList"])
    exact["totalPage"] = 1
    with caplog.at_level("WARNING"):
        list(spider.parse_titan(titan_response(exact, page=0)))
    assert "SHORT HARVEST" not in caplog.text


# --- facility_category via the pipeline (case 12) ------------------------- #

@pytest.mark.parametrize("provider_type,category", [
    ("Licensed Center", "center"),
    ("Registered Child Development Home A", "family_home"),
    ("Registered Child Development Home B", "family_home"),
    ("Registered Child Development Home C", "family_home"),
    ("Registered Child Development Home C1", "family_home"),
    ("Non-Registered Child Care Home", "family_home"),
    ("Exempt from Licensing", "exempt"),
    ("In-Home", "other"),
])
def test_iowa_facility_category_mapping(provider_type, category, caplog):
    with caplog.at_level("WARNING"):
        result = norm.facility_category_from_type(provider_type)
    assert result == category
    assert result != "group_home"
    assert "unmapped provider_type" not in caplog.text


def test_iowa_status_mapping():
    assert norm.canonical_status("A") == "active"


def test_iowa_center_facility_category_via_pipeline(pins_sample, titan_page):
    c3 = _pin(pins_sample, 45025)
    titan = _titan_row(titan_page, 45025)
    item = build_provider_item(c3, titan)
    norm.normalize_item(dict(item), "iowa")  # smoke: no raise
    assert norm.facility_category_from_type(item["provider_type"]) == "center"


# --- inspections (case 14) ------------------------------------------------ #

def test_parse_reports_golden_path(spider):
    reports = _load_fixture("ia_reports.json")
    c3_less_item = ProviderItem()
    c3_less_item["provider_name"] = "Heavenly Creations Childcare & Learning Center"
    url = "https://secureapp.dhs.state.ia.us/dhs_titan_public/ChildCare/" \
          "GetProviderComplaintAndComplicanceReportList?providerID=45047&TypeOfCareID=4"
    response = reports_response(reports, url, c3_less_item, 45047, (3, 0, 1))

    item = next(spider.parse_reports(response))
    assert len(item["inspections"]) == 4
    assert all(isinstance(i, InspectionItem) for i in item["inspections"])
    # FormID:13 row sits in RegulationCheckListReportList but self-describes
    # via FileTypeDescription as "Compliance Report" -- that must win (§5.13).
    checklist_derived = [i for i in item["inspections"]
                         if i["report_url"].endswith("zJhZ0DNMZvk%3d"
                                                       "&formID=pTlS6i6%2f1Nc%3d"
                                                       "&createdDate=08/01/2025")]
    assert checklist_derived and checklist_derived[0]["type"] == "Compliance Report"
    assert spider.item_count == 1


def test_parse_reports_report_url_not_double_encoded(spider):
    reports = _load_fixture("ia_reports.json")
    item = ProviderItem()
    url = "https://secureapp.dhs.state.ia.us/dhs_titan_public/ChildCare/" \
          "GetProviderComplaintAndComplicanceReportList?providerID=45047&TypeOfCareID=4"
    response = reports_response(reports, url, item, 45047, (3, 0, 1))
    result = next(spider.parse_reports(response))
    urls = [i["report_url"] for i in result["inspections"]]
    # CrypticID/CrypticFormID percent-escapes survive verbatim -- never
    # re-encoded to %253d / %252f (§5.14).
    assert any("Id=NpRb8oszlic%3d&formID=KFFrMwn64FQ%3d" in u for u in urls)
    assert not any("%25" in u for u in urls)


def test_parse_reports_date_conversion(spider):
    reports = _load_fixture("ia_reports.json")
    item = ProviderItem()
    url = "https://secureapp.dhs.state.ia.us/dhs_titan_public/ChildCare/" \
          "GetProviderComplaintAndComplicanceReportList?providerID=45047&TypeOfCareID=4"
    response = reports_response(reports, url, item, 45047, (3, 0, 1))
    result = next(spider.parse_reports(response))
    dates = sorted(i["date"] for i in result["inspections"])
    # /Date(1754077778100)/ -> 2025-08-01 in America/Chicago.
    assert "2025-08-01" in dates


def test_late_evening_timestamp_lands_on_central_day_not_utc_day():
    # 1784259000000 ms == 2026-07-17T03:30:00Z, which is still 2026-07-16
    # 22:30 in America/Chicago (CDT, UTC-5) -- the report date must follow
    # the Central calendar day, not UTC's (§5.12).
    assert titan_iso_date("/Date(1784259000000)/") == "2026-07-16"
    assert titan_mdy_date("/Date(1784259000000)/") == "07/16/2026"


def test_titan_iso_date_bad_format_logged(caplog):
    with caplog.at_level("WARNING"):
        result = titan_iso_date("not-a-date")
    assert result is None
    assert "unrecognized Titan date format" in caplog.text


def test_parse_reports_no_request_for_zero_counts(spider, pins_sample, titan_page):
    # 52883 has all three Titan counts at 0 -- parse_pins must finalize it
    # without ever issuing a report-list request.
    for row in titan_page["ComplianceList"]:
        spider.licensing[row["ProviderID"]] = row
    outputs = list(spider.parse_pins(pins_response(pins_sample)))
    report_reqs = [r for r in outputs if isinstance(r, Request)
                   and "GetProviderComplaintAndComplicanceReportList" in r.url]
    zero_count_reqs = [r for r in report_reqs
                       if r.meta.get("provider_id") == 52883]
    assert zero_count_reqs == []
    # 52883 is still emitted as a finished item (no inspections).
    zero_count_items = [it for it in outputs if isinstance(it, ProviderItem)
                        and it.get("license_number") == "52883"]
    assert len(zero_count_items) == 1
    assert "inspections" not in zero_count_items[0]


def test_parse_reports_count_mismatch_logged(spider, caplog):
    reports = _load_fixture("ia_reports.json")
    item = ProviderItem()
    url = "https://secureapp.dhs.state.ia.us/dhs_titan_public/ChildCare/" \
          "GetProviderComplaintAndComplicanceReportList?providerID=45047&TypeOfCareID=4"
    # search row claimed 5 compliance reports; the fetched list only has 3.
    response = reports_response(reports, url, item, 45047, (5, 0, 1))
    with caplog.at_level("WARNING"):
        list(spider.parse_reports(response))
    assert "report count mismatch" in caplog.text
    assert "45047" in caplog.text


# --- IsG360Report / IsLegacy branches (never seen live) ------------------- #

def test_build_report_url_g360_branch_logs_warning(caplog):
    rec = {"IsG360Report": True, "IsLegacy": False, "FileID": 999,
           "CrypticID": "x", "CrypticFormID": "y", "CreatedDate": None}
    with caplog.at_level("WARNING"):
        url = build_report_url(rec)
    assert url == (
        "https://secureapp.dhs.state.ia.us/dhs_titan_public/DocumentRepository"
        "/ViewComplianceDocument/?CrypticFileID=999"
    )
    assert "IsG360Report" in caplog.text


def test_build_report_url_legacy_branch_uses_version_time(caplog):
    # The legacy shape keys off VersionTime, not CreatedDate (§6.5).
    rec = {"IsG360Report": False, "IsLegacy": True,
           "CrypticID": "abc%3d", "VersionTime": "/Date(1784259000000)/",
           "CreatedDate": "/Date(1)/"}
    with caplog.at_level("WARNING"):
        url = build_report_url(rec)
    assert url == (
        "https://secureapp.dhs.state.ia.us/dhs_titan_public/DocumentRepository"
        "/ProviderLegacyDocumentReport/?providerID=abc%3d&createdDate=07/16/2026"
    )
    assert "IsLegacy" in caplog.text


def test_build_report_url_capricadocument_default_branch():
    rec = {"IsG360Report": False, "IsLegacy": False,
           "CrypticID": "jzxFNdvCTp4%3d", "CrypticFormID": "0%2fUKhC%2fQNWY%3d",
           "CreatedDate": "/Date(1784259000000)/"}
    url = build_report_url(rec)
    assert url == (
        "https://secureapp.dhs.state.ia.us/dhs_titan_public/DocumentRepository"
        "/ViewCAPRICADocument/?Id=jzxFNdvCTp4%3d&formID=0%2fUKhC%2fQNWY%3d"
        "&createdDate=07/16/2026"
    )


def test_build_inspection_type_from_list_name_fallback():
    rec = {"FileTypeDescription": None, "CreatedDate": "/Date(1784259000000)/",
           "IsG360Report": False, "IsLegacy": False,
           "CrypticID": "a", "CrypticFormID": "b"}
    entry = build_inspection(rec, "ComplaintReportList")
    assert entry["type"] == "Complaint Report"


# --- pagination / crawl-level logging (closed()) --------------------------- #

def test_closed_logs_short_item_count_warning(spider, caplog):
    spider.both_count = 10
    spider.c3_only_count = 1
    spider.titan_only_count = 2
    spider.item_count = 5  # short of the 13 the join produced
    with caplog.at_level("WARNING"):
        spider.closed("finished")
    assert "possible dropped items" in caplog.text

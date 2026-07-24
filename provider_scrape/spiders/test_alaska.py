"""Alaska (AKCCIS) spider tests.

Loads the committed fixtures under ``provider_scrape/spiders/fixtures/`` --
``ak_search_sample.json`` (16 curated roster records) and
``ak_inspection_sample.json`` (4 real visits from facility 10035). See
``docs/alaska_field_mapping.md`` for what each fixture record was included
to exercise.
"""
import json
import os
from unittest.mock import MagicMock

import pytest
from scrapy.http import Request, TextResponse

from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.spiders.alaska import (
    AlaskaSpider,
    _age_flags,
    _build_address,
    _expand_compliance,
    _iso_date,
    _months_to_age,
    _stringify_coordinate,
    _yesno,
)


FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def _load_fixture(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return json.load(fh)


def _by_id(roster):
    return {r["facilityGenId"]: r for r in roster}


# --------------------------------------------------------------------------- #
# pytest fixtures
# --------------------------------------------------------------------------- #

@pytest.fixture
def spider():
    return AlaskaSpider()


@pytest.fixture
def roster():
    """The 16-record curated roster fixture, indexed by facilityGenId."""
    return _by_id(_load_fixture("ak_search_sample.json"))


@pytest.fixture
def inspections():
    """The 4-visit inspection fixture for facilityGenId=10035."""
    return _load_fixture("ak_inspection_sample.json")


def _search_response(body):
    """Build a TextResponse mimicking the POST /Facility/Search reply."""
    req = Request(AlaskaSpider.SEARCH_URL, method="POST")
    return TextResponse(url=AlaskaSpider.SEARCH_URL,
                        body=json.dumps(body).encode("utf-8"),
                        encoding="utf-8", request=req)


# --------------------------------------------------------------------------- #
# _iso_date
# --------------------------------------------------------------------------- #

def test_iso_date_dotnet_iso_input():
    assert _iso_date("2025-06-01T08:00:00Z") == "2025-06-01"
    # Sub-second precision variants also parse (defensive).
    assert _iso_date("2025-10-01T06:00:00.000Z") == "2025-10-01"


def test_iso_date_dotnet_default_sentinel_is_none():
    """0001-01-01T00:00:00Z is the .NET "no date" placeholder."""
    assert _iso_date("0001-01-01T00:00:00Z") is None


def test_iso_date_us_format_from_inspection_endpoint():
    assert _iso_date("6/23/2026 1:00 PM") == "2026-06-23"
    assert _iso_date("12/1/2024 9:15 AM") == "2024-12-01"


def test_iso_date_empty_and_none():
    assert _iso_date("") is None
    assert _iso_date(None) is None
    assert _iso_date("   ") is None


def test_iso_date_garbage_returns_none():
    """Unparseable strings become None (defensive; not observed in practice)."""
    assert _iso_date("garbage") is None
    assert _iso_date("13/40/2026") is None  # invalid month/day
    assert _iso_date("2026-13-40") is None


# --------------------------------------------------------------------------- #
# _months_to_age
# --------------------------------------------------------------------------- #

def test_months_to_age_full_licensed_range():
    """155 months = 12y 11m -- the state's maximum authorized upper bound."""
    assert _months_to_age(0.0, 155.0) == "0 Months - 12 Years, 11 Months"


def test_months_to_age_uses_singular_for_one():
    """1 Year / 1 Month -- not "1 Years", "1 Months"."""
    assert _months_to_age(0.0, 12.0) == "0 Months - 1 Year"
    assert _months_to_age(1.0, 11.0) == "1 Month - 11 Months"


def test_months_to_age_drops_zero_months():
    """36 months = 3 Years exactly -- no trailing ", 0 Months"."""
    assert _months_to_age(0.0, 36.0) == "0 Months - 3 Years"


def test_months_to_age_missing_or_inverted():
    assert _months_to_age(None, 12.0) is None
    assert _months_to_age(12.0, None) is None
    assert _months_to_age(None, None) is None
    # Inverted range (defensive; not observed in practice).
    assert _months_to_age(50.0, 20.0) is None


# --------------------------------------------------------------------------- #
# _age_flags
# --------------------------------------------------------------------------- #

def test_age_flags_full_range_all_true():
    assert _age_flags(0.0, 155.0) == {
        "infant": True, "toddler": True, "preschool": True, "school": True}


def test_age_flags_infant_only():
    assert _age_flags(0.0, 11.0) == {
        "infant": True, "toddler": False, "preschool": False, "school": False}


def test_age_flags_preschool_only():
    assert _age_flags(36.0, 59.0) == {
        "infant": False, "toddler": False, "preschool": True, "school": False}


def test_age_flags_school_only():
    assert _age_flags(60.0, 155.0) == {
        "infant": False, "toddler": False, "preschool": False, "school": True}


def test_age_flags_boundary_at_12_months():
    """A 11-13 month range hits both infant and toddler."""
    assert _age_flags(11.0, 13.0) == {
        "infant": True, "toddler": True, "preschool": False, "school": False}


def test_age_flags_missing_returns_empty():
    """Empty dict signals "don't set the fields" to the caller."""
    assert _age_flags(None, 12.0) == {}
    assert _age_flags(0.0, None) == {}
    assert _age_flags(None, None) == {}


# --------------------------------------------------------------------------- #
# _expand_compliance
# --------------------------------------------------------------------------- #

def test_expand_compliance_known_codes():
    assert _expand_compliance("C") == "In Compliance"
    assert _expand_compliance("NC") == "Non-Compliance"


def test_expand_compliance_passes_unknown_through():
    """Any future code we don't recognize is still surfaced verbatim."""
    assert _expand_compliance("PC") == "PC"


def test_expand_compliance_empty_is_none():
    assert _expand_compliance("") is None
    assert _expand_compliance(None) is None
    assert _expand_compliance("   ") is None


# --------------------------------------------------------------------------- #
# _build_address
# --------------------------------------------------------------------------- #

def test_build_address_empty_address2_no_double_space():
    """AKCCIS often ships address2 as an empty string -- must not collapse
    into a double space."""
    assert _build_address(
        "35095 Huntington Drive", "", "Soldotna", "AK", "99669"
    ) == "35095 Huntington Drive Soldotna, AK 99669"


def test_build_address_with_address2():
    assert _build_address(
        "100 Main St", "Suite 4", "Juneau", "AK", "99801"
    ) == "100 Main St Suite 4 Juneau, AK 99801"


def test_build_address_all_none():
    assert _build_address(None, None, None, None, None) is None


def test_build_address_missing_city():
    """Partial addresses still assemble what they can."""
    assert _build_address("1 Main St", "", None, "AK", None) == "1 Main St AK"


# --------------------------------------------------------------------------- #
# Small stringifier helpers
# --------------------------------------------------------------------------- #

def test_yesno():
    assert _yesno(True) == "Yes"
    assert _yesno(False) == "No"
    assert _yesno(None) is None


def test_stringify_coordinate_preserves_precision():
    """Never cast coords to float -- keep every digit as a string."""
    assert _stringify_coordinate(60.488985199709) == "60.488985199709"
    assert _stringify_coordinate("-149.93613879659") == "-149.93613879659"
    assert _stringify_coordinate(None) is None
    assert _stringify_coordinate("") is None


# --------------------------------------------------------------------------- #
# build_item -- golden path (facility 10000, Little Peoples Learning World)
# --------------------------------------------------------------------------- #

def test_build_item_golden_path_common_fields(spider, roster):
    item = spider.build_item(roster["10000"], [])
    assert isinstance(item, ProviderItem)
    assert item["source_state"] == "Alaska"
    assert item["provider_name"] == "Little Peoples Learning World"
    # licenseNumber is int on the wire -- must be stringified, not int()-cast.
    assert item["license_number"] == "970002"
    assert item["provider_url"] == (
        "https://akccis.com/client/map?facilityGenId=10000")
    assert item["phone"] == "(907)262-4113"
    assert item["capacity"] == 60
    assert item["administrator"] == "Rachel Jenkins"
    # doingBusinessAs -> license_holder (differs from facilityName).
    assert item["license_holder"] == "Jeff Baker"
    assert item["provider_type"] == "Licensed Center"
    assert item["status"] == "Active/Open"
    assert item["status_date"] == "1998-07-02"
    assert item["license_begin_date"] == "2025-06-01"
    assert item["license_expiration"] == "2027-02-28"
    assert item["scholarships_accepted"] == "Yes"


def test_build_item_golden_path_address(spider, roster):
    item = spider.build_item(roster["10000"], [])
    assert item["address"] == "35095 Huntington Drive Soldotna, AK 99669"
    assert item["city"] == "Soldotna"
    assert item["state"] == "AK"
    assert item["zip"] == "99669"
    assert item["county"] == "Kenai Peninsula Borough"


def test_build_item_golden_path_coordinates_are_strings(spider, roster):
    """The normalization pipeline expects coords as strings; float would
    silently truncate precision."""
    item = spider.build_item(roster["10000"], [])
    assert isinstance(item["latitude"], str)
    assert isinstance(item["longitude"], str)
    assert item["latitude"] == "60.488985199709"
    assert item["longitude"] == "-151.146877804374"


def test_build_item_golden_path_ages_and_flags(spider, roster):
    item = spider.build_item(roster["10000"], [])
    assert item["ages_served"] == "0 Months - 12 Years, 11 Months"
    assert item["infant"] is True
    assert item["toddler"] is True
    assert item["preschool"] is True
    assert item["school"] is True


def test_build_item_golden_path_ak_fields(spider, roster):
    item = spider.build_item(roster["10000"], [])
    assert item["ak_facility_gen_id"] == "10000"
    assert item["ak_facility_number"] == "1000000"
    assert item["ak_legacy_license_number"] == "970002"
    assert item["ak_vendor_id"] == "LPL98252"
    assert item["ak_license_type"] == "Biennial"
    assert item["ak_licensing_specialist"] == "Venus Siemens"


# --------------------------------------------------------------------------- #
# build_item -- edge / sparse records
# --------------------------------------------------------------------------- #

def test_build_item_license_exempt_no_license_object(spider, roster):
    """License Exempt records lack the `license` sub-object entirely and
    typically have no capacity or licensed ages."""
    item = spider.build_item(roster["10652"], [])
    assert item["provider_type"] == "License Exempt"
    assert item["capacity"] is None
    assert item["license_begin_date"] is None
    assert item["license_expiration"] is None
    assert item["provider_name"] == "Paula Sieghart"
    # facilityTypeSubTypeDescription is populated for exempt records only.
    assert item["ak_facility_subtype"] == (
        "License Exempt - Home (less than 4 unrelated) – MOA")


def test_build_item_illegally_unlicensed_flows_through(spider, roster):
    """Illegally-flagged operations are emitted so downstream can bucket
    them (facility_category=other via Task 4's mapping)."""
    item = spider.build_item(roster["10737"], [])
    assert item["provider_type"] == "Illegally Unlicensed"
    assert item["provider_name"]  # any real name is fine


def test_build_item_ccap_accredited(spider, roster):
    item = spider.build_item(roster["10330"], [])
    assert item["provider_type"] == "CCAP Certified/Accredited"


def test_build_item_closed_provider_still_emits_status_date(spider, roster):
    """A Closed facility retains providerStatusEffectiveDate -- keep it."""
    item = spider.build_item(roster["10005"], [])
    assert item["status"] == "Closed"
    assert item["status_date"] is not None
    # The current license is gone but the record still surfaces.
    assert item["provider_name"] == "Ray's Child Care & Learning Center"


def test_build_item_prefixed_license_number_preserved_verbatim(spider, roster):
    """Facility 10700 uses licenseNumber "MOA-H-1000700" -- the prefix must
    survive. int()-casting would explode; str.strip() would silently work."""
    item = spider.build_item(roster["10700"], [])
    assert item["license_number"] == "MOA-H-1000700"


def test_build_item_null_provider_status_becomes_none(spider, roster):
    """13 records have providerStatus=null. Emit None; the pipeline logs
    'unknown' and moves on -- not a bug."""
    item = spider.build_item(roster["10652"], [])  # exempt record, status=null
    assert item["status"] is None


def test_build_item_dba_matching_name_is_not_emitted(spider):
    """The ~62 records where doingBusinessAs == facilityName add no signal
    -- don't populate license_holder in that case."""
    record = {
        "facilityGenId": "1", "facilityName": "Same Name Preschool",
        "doingBusinessAs": "Same Name Preschool",
    }
    item = AlaskaSpider().build_item(record, [])
    # ItemAdapter treats a missing key differently from None; we didn't set it.
    assert item.get("license_holder") is None


# --------------------------------------------------------------------------- #
# build_inspections
# --------------------------------------------------------------------------- #

def test_build_inspections_golden_path(inspections):
    result = AlaskaSpider.build_inspections(inspections)
    # 4 real visits in the fixture; no dedup collapses in the wild.
    assert len(result) == 4
    for insp in result:
        assert isinstance(insp, InspectionItem)
        assert insp["date"] is not None
        assert insp["type"] is not None
        assert insp["original_status"] is not None
        assert insp["ak_visit_type"] in ("Announced", "Unannounced")


def test_build_inspections_expands_compliance_codes(inspections):
    result = AlaskaSpider.build_inspections(inspections)
    statuses = [i["original_status"] for i in result]
    assert "In Compliance" in statuses
    assert "Non-Compliance" in statuses


def test_build_inspections_dedups_identical_rows():
    """Two identical visit rows collapse to one InspectionItem."""
    event = {
        "visitDate": "5/5/2025 9:15 AM",
        "purposeOfVisit": "Renewal",
        "visitType": "Announced",
        "compliance": "C",
        "licensingSpecialist": "Jane Doe",
    }
    result = AlaskaSpider.build_inspections([event, dict(event), dict(event)])
    assert len(result) == 1


def test_build_inspections_empty_input_returns_empty():
    """Facilities with no visits are the norm -- must round-trip."""
    assert AlaskaSpider.build_inspections([]) == []
    assert AlaskaSpider.build_inspections(None) == []


def test_build_inspections_future_dated_visits_are_emitted():
    """AKCCIS occasionally publishes scheduled/upcoming visits with a future
    visitDate. Emit them as-is; downstream may filter to date<=today."""
    future = [{
        "visitDate": "6/23/2099 1:00 PM",
        "purposeOfVisit": "Annual",
        "visitType": "Announced",
        "compliance": "C",
        "licensingSpecialist": "Future Person",
    }]
    result = AlaskaSpider.build_inspections(future)
    assert len(result) == 1
    assert result[0]["date"] == "2099-06-23"


def test_build_inspections_garbage_date_still_emits_visit():
    """A malformed visitDate becomes None on the item; the visit still
    surfaces so downstream can see it."""
    events = [{
        "visitDate": "not a date",
        "purposeOfVisit": "Annual",
        "compliance": "C",
    }]
    result = AlaskaSpider.build_inspections(events)
    assert len(result) == 1
    assert result[0]["date"] is None
    assert result[0]["type"] == "Annual"


def test_build_inspections_optional_fields_only_set_when_present():
    """Missing visitType / licensingSpecialist -> unset (not None) on the
    item, so downstream sees a clean absence."""
    events = [{
        "visitDate": "5/5/2025 9:15 AM",
        "purposeOfVisit": "Annual",
        "compliance": "C",
    }]
    result = AlaskaSpider.build_inspections(events)
    assert len(result) == 1
    assert result[0].get("ak_visit_type") is None
    assert result[0].get("ak_licensing_specialist") is None


# --------------------------------------------------------------------------- #
# parse_search -- request fan-out
# --------------------------------------------------------------------------- #

def test_parse_search_yields_one_inspection_request_per_provider(spider):
    body = [
        {"facilityGenId": "10000", "facilityName": "A"},
        {"facilityGenId": "10001", "facilityName": "B"},
        {"facilityName": "no id -- must be skipped"},
    ]
    requests = list(spider.parse_search(_search_response(body)))
    assert len(requests) == 2
    assert requests[0].url == (
        "https://akccis.com/server/api/Inspection/"
        "GetFacilityInspectionTasksPublicView?facilityGenId=10000")
    assert requests[1].url.endswith("facilityGenId=10001")


def test_parse_search_carries_roster_row_through_cb_kwargs(spider):
    """The roster row is preserved so the errback can still emit the
    provider if the inspection fetch fails."""
    body = [{"facilityGenId": "10000", "facilityName": "Little Peoples"}]
    requests = list(spider.parse_search(_search_response(body)))
    assert requests[0].cb_kwargs["roster"]["facilityGenId"] == "10000"
    assert requests[0].cb_kwargs["roster"]["facilityName"] == "Little Peoples"


def test_parse_search_wires_callback_and_errback(spider):
    body = [{"facilityGenId": "10000", "facilityName": "A"}]
    requests = list(spider.parse_search(_search_response(body)))
    assert requests[0].callback == spider.parse_inspection
    assert requests[0].errback == spider.errback_inspection


# --------------------------------------------------------------------------- #
# errback_inspection -- provider still emitted on inspection-fetch failure
# --------------------------------------------------------------------------- #

def test_errback_inspection_emits_provider_without_inspections(spider):
    roster_row = {
        "facilityGenId": "10000",
        "facilityName": "Little Peoples Learning World",
        "licenseNumber": 970002,
    }
    failure = MagicMock()
    failure.value = RuntimeError("simulated 500")
    failure.request.cb_kwargs = {"roster": roster_row}

    outputs = list(spider.errback_inspection(failure))
    assert len(outputs) == 1
    item = outputs[0]
    assert isinstance(item, ProviderItem)
    assert item["inspections"] == []
    assert item["provider_name"] == "Little Peoples Learning World"
    assert item["license_number"] == "970002"


def test_errback_inspection_handles_missing_cb_kwargs(spider):
    """Defensive: even if cb_kwargs is missing/empty, we shouldn't crash --
    the item just comes out mostly-empty."""
    failure = MagicMock()
    failure.value = RuntimeError("simulated")
    failure.request.cb_kwargs = {}
    outputs = list(spider.errback_inspection(failure))
    assert len(outputs) == 1
    assert outputs[0]["source_state"] == "Alaska"
    assert outputs[0]["inspections"] == []

import json

import pytest
from scrapy.http import HtmlResponse, Request, TextResponse

from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.spiders.michigan import (
    AURA_DESCRIPTOR,
    APEX_CLASSNAME,
    MICHIGAN_COUNTIES,
    MichiganSpider,
    clean_address,
    format_hours,
    parse_inspection_doc,
)


@pytest.fixture
def spider():
    return MichiganSpider()


# ---- fwuid extraction tests ----


def test_extract_fwuid_from_json_pattern(spider):
    """Test fwuid extraction from JSON embedded in the page."""
    html = """
    <html><head>
    <script>
    var config = {"fwuid":"abc123XYZ_test-fwuid","mode":"PROD"};
    </script>
    </head><body></body></html>
    """
    response = HtmlResponse(
        url="https://cclb.michigan.gov/s/statewide-facility-search",
        body=html,
        encoding="utf-8",
    )
    assert spider._extract_fwuid(response) == "abc123XYZ_test-fwuid"


def test_extract_fwuid_missing(spider):
    """Test that missing fwuid returns None."""
    html = "<html><body>No fwuid here</body></html>"
    response = HtmlResponse(
        url="https://cclb.michigan.gov/s/statewide-facility-search",
        body=html,
        encoding="utf-8",
    )
    assert spider._extract_fwuid(response) is None


def test_parse_initial_page_yields_county_searches(spider):
    """Test that parse_initial_page yields one search request per county."""
    html = '<html><script>{"fwuid":"test-fwuid-123"}</script></html>'
    response = HtmlResponse(
        url="https://cclb.michigan.gov/s/statewide-facility-search",
        body=html,
        encoding="utf-8",
    )
    results = list(spider.parse_initial_page(response))
    from provider_scrape.spiders.michigan import MICHIGAN_COUNTIES

    assert len(results) == len(MICHIGAN_COUNTIES)
    req = results[0]
    assert req.method == "POST"
    assert "aura" in req.url
    assert req.meta["fwuid"] == "test-fwuid-123"
    assert req.meta["page_number"] == 1
    assert req.meta["county"] == MICHIGAN_COUNTIES[0]


def test_parse_initial_page_no_fwuid(spider):
    """Test that parse_initial_page yields nothing when fwuid is missing."""
    html = "<html><body>Nothing useful</body></html>"
    response = HtmlResponse(
        url="https://cclb.michigan.gov/s/statewide-facility-search",
        body=html,
        encoding="utf-8",
    )
    results = list(spider.parse_initial_page(response))
    assert len(results) == 0


# ---- Search result parsing tests ----


def _make_search_response(
    search_result_payload, page_number=1, fwuid="test-fwuid", county="TestCounty"
):
    """Helper to create a mock search API response.

    Mimics the double-encoded format: the outer returnValue contains a
    "returnValue" key whose value is a JSON string of the actual payload.
    """
    aura_response = {
        "actions": [
            {
                "id": "1;a",
                "state": "SUCCESS",
                "returnValue": {
                    "returnValue": json.dumps(search_result_payload),
                },
            }
        ]
    }
    request = Request(
        url="https://cclb.michigan.gov/s/sfsites/aura?r=1&aura.ApexAction.execute=1",
        meta={"fwuid": fwuid, "page_number": page_number, "county": county},
    )
    return TextResponse(
        url=request.url,
        request=request,
        body=json.dumps(aura_response),
        encoding="utf-8",
    )


def test_parse_search_yields_detail_requests(spider):
    """Test that search results yield one detail request per provider."""
    payload = {
        "totalRecords": 2,
        "recordStart": 1,
        "recordEnd": 2,
        "results": [
            {
                "id": "001ABC",
                "Name": "Happy Kids Daycare",
                "LicenseNumber": "DC123",
                "LicenseType": "Child Care Center",
                "Status": "Active",
                "Address": "123 Main St, Lansing, MI 48901",
                "BillingCountry": "Ingham",
            },
            {
                "id": "002DEF",
                "Name": "Little Stars Academy",
                "LicenseNumber": "DC456",
                "LicenseType": "Group Home",
                "Status": "Active",
                "Address": "456 Oak Ave, Detroit, MI 48201",
                "BillingCountry": "Wayne",
            },
        ],
    }
    response = _make_search_response(payload)
    results = list(spider.parse_search(response))

    # 2 detail requests, no pagination request (recordEnd == totalRecords)
    assert len(results) == 2
    for req in results:
        assert req.method == "POST"
        assert "aura" in req.url
        assert "provider_id" in req.meta
        assert "search_data" in req.meta


def test_parse_search_pagination(spider):
    """Test that search yields a next-page request when there are more records."""
    payload = {
        "totalRecords": 500,
        "recordStart": 1,
        "recordEnd": 200,
        "results": [
            {"id": f"id_{i}", "Name": f"Provider {i}"} for i in range(200)
        ],
    }
    response = _make_search_response(payload, page_number=1)
    results = list(spider.parse_search(response))

    # 200 detail requests + 1 pagination request
    assert len(results) == 201
    pagination_req = results[-1]
    assert pagination_req.meta["page_number"] == 2


def test_parse_search_no_pagination_on_last_page(spider):
    """Test that no pagination request is yielded on the last page."""
    payload = {
        "totalRecords": 50,
        "recordStart": 1,
        "recordEnd": 50,
        "results": [
            {"id": f"id_{i}", "Name": f"Provider {i}"} for i in range(50)
        ],
    }
    response = _make_search_response(payload, page_number=1)
    results = list(spider.parse_search(response))

    # 50 detail requests, no pagination
    assert len(results) == 50


def test_parse_search_empty_results(spider):
    """Test handling of empty search results."""
    payload = {
        "totalRecords": 0,
        "recordStart": 0,
        "recordEnd": 0,
        "results": [],
    }
    response = _make_search_response(payload)
    results = list(spider.parse_search(response))
    assert len(results) == 0


def test_parse_search_invalid_json(spider):
    """Test handling of invalid JSON in search response."""
    request = Request(
        url="https://cclb.michigan.gov/s/sfsites/aura?r=1",
        meta={"fwuid": "test", "page_number": 1, "county": "TestCounty"},
    )
    response = TextResponse(
        url=request.url,
        request=request,
        body="not json",
        encoding="utf-8",
    )
    results = list(spider.parse_search(response))
    assert len(results) == 0


# ---- Detail response parsing tests ----


def _make_detail_response(
    detail_info=None,
    operational_info=None,
    service_info=None,
    docs_info=None,
    provider_id="001ABC",
    search_data=None,
):
    """Helper to create a mock batched detail API response.

    Mimics the double-encoded format where each returnValue contains a
    "returnValue" key with a JSON string. getDetailInfo returns an array.
    """
    if search_data is None:
        search_data = {"Name": "Fallback Name", "LicenseNumber": "DC000"}

    def _action(index, return_value):
        """Build an action response with double-encoded returnValue."""
        return {
            "id": f"{index + 1};a",
            "state": "SUCCESS",
            "returnValue": {
                "returnValue": json.dumps(return_value if return_value is not None else {}),
            },
        }

    # Real API format:
    # - getDetailInfo: array of objects [{}]
    # - getOperationalDetailInfo: array of day entries [{Day,OpenTime,CloseTime},...]
    # - getServiceDetailInfo: array of objects [{ServicesProvided,FullDay}]
    # - getGenertatedDocs: object {documents:[...], documentsWithoutViolation:[...]}
    aura_response = {
        "actions": [
            _action(0, [detail_info] if detail_info else []),
            _action(1, operational_info if operational_info is not None else []),
            _action(2, [service_info] if service_info else []),
            _action(3, docs_info),
        ]
    }
    request = Request(
        url="https://cclb.michigan.gov/s/sfsites/aura?r=4&aura.ApexAction.execute=1",
        meta={
            "fwuid": "test-fwuid",
            "provider_id": provider_id,
            "search_data": search_data,
        },
    )
    return TextResponse(
        url=request.url,
        request=request,
        body=json.dumps(aura_response),
        encoding="utf-8",
    )


def test_parse_detail_full(spider):
    """Test full detail parsing with all 4 API methods returning data."""
    detail_info = {
        "Name": "Happy Kids Daycare",
        "LicenseNumber": "DC730012345",
        "Type": "Child Care Center",
        "Status": "Active",
        "LicenseStatus": "Regular",
        "Address": "123 Main St, Lansing, MI 48901 null",
        "Phone": "(517) 555-1234",
        "Country": "Ingham",
        "Capacity": "75",
        "LicenseName": "Jane Smith",
        "LicenseeAddress": "456 Office Rd, Lansing, MI 48901 null",
        "EffectiveDate": "01/15/2023",
        "ExpirationDate": "01/14/2025",
    }
    # Operational info is returned as a flat array of day/time entries
    operational_info = [
        {"Day": "Mon", "OpenTime": "7:00 AM", "CloseTime": "6:00 PM"},
        {"Day": "Tue", "OpenTime": "7:00 AM", "CloseTime": "6:00 PM"},
        {"Day": "Wed", "OpenTime": "7:00 AM", "CloseTime": "6:00 PM"},
    ]
    # Service info is returned as an array with one object
    service_info = {
        "ServicesProvided": "Infant;Toddler;Preschool",
        "FullDay": "YES",
    }
    docs_info = {
        "documents": [
            {
                "Title": "DC730012345_INSP_20240512",
                "CreatedDate": "2024-05-12",
                "docurl": "https://example.com/doc1.pdf",
            }
        ],
        "documentsWithoutViolation": [
            {
                "Title": "DC730012345_RNWL_20230115.pdf",
                "CreatedDate": "2023-01-15",
                "docurl": "https://example.com/doc2.pdf",
            }
        ],
    }

    response = _make_detail_response(
        detail_info=detail_info,
        operational_info=operational_info,
        service_info=service_info,
        docs_info=docs_info,
    )
    results = list(spider.parse_detail(response))
    assert len(results) == 1

    item = results[0]
    assert isinstance(item, ProviderItem)
    assert item["source_state"] == "Michigan"
    assert item["provider_name"] == "Happy Kids Daycare"
    assert item["license_number"] == "DC730012345"
    assert item["provider_type"] == "Child Care Center"
    assert item["status"] == "Active"
    assert item["mi_license_status"] == "Regular"
    # Address should have trailing "null" stripped
    assert item["address"] == "123 Main St, Lansing, MI 48901"
    assert item["phone"] == "(517) 555-1234"
    assert item["county"] == "Ingham"
    assert item["capacity"] == "75"
    assert item["license_holder"] == "Jane Smith"
    assert item["mi_licensee_address"] == "456 Office Rd, Lansing, MI 48901"
    assert item["license_begin_date"] == "01/15/2023"
    assert item["license_expiration"] == "01/14/2025"

    # Hours
    assert item["hours"] == "Mon: 7:00 AM-6:00 PM; Tue: 7:00 AM-6:00 PM; Wed: 7:00 AM-6:00 PM"

    # Services
    assert item["mi_services_provided"] == "Infant;Toddler;Preschool"
    assert item["mi_full_day"] == "YES"

    # Inspections - 2 merged from documents + documentsWithoutViolation
    assert len(item["inspections"]) == 2
    insp1 = item["inspections"][0]
    assert isinstance(insp1, InspectionItem)
    assert insp1["type"] == "Inspection"
    assert insp1["date"] == "05/12/2024"
    assert insp1["report_url"] == "https://example.com/doc1.pdf"

    insp2 = item["inspections"][1]
    assert insp2["type"] == "Renewal"
    assert insp2["date"] == "01/15/2023"


def test_parse_detail_empty_responses(spider):
    """Test detail parsing when all API methods return empty data."""
    search_data = {
        "Name": "Fallback Provider",
        "LicenseNumber": "DC999",
        "LicenseType": "Group Home",
        "Status": "Active",
        "Address": "789 Elm St null",
        "BillingCountry": "Wayne",
    }
    response = _make_detail_response(search_data=search_data)
    results = list(spider.parse_detail(response))
    assert len(results) == 1

    item = results[0]
    # Should fall back to search data where available
    assert item["provider_name"] == "Fallback Provider"
    assert item["license_number"] == "DC999"
    assert item["provider_type"] == "Group Home"
    assert item["status"] == "Active"
    assert item["county"] == "Wayne"
    assert item["hours"] is None
    assert item["mi_services_provided"] is None
    assert item["mi_full_day"] is None
    assert item["inspections"] == []


def test_parse_detail_invalid_json(spider):
    """Test handling of invalid JSON in detail response."""
    request = Request(
        url="https://cclb.michigan.gov/s/sfsites/aura?r=4",
        meta={
            "fwuid": "test",
            "provider_id": "001ABC",
            "search_data": {"Name": "Test"},
        },
    )
    response = TextResponse(
        url=request.url,
        request=request,
        body="not json",
        encoding="utf-8",
    )
    results = list(spider.parse_detail(response))
    assert len(results) == 0


def test_parse_detail_failed_action(spider):
    """Test handling when an action in the batch fails."""
    aura_response = {
        "actions": [
            {"id": "1;a", "state": "ERROR", "error": [{"message": "fail"}]},
            {"id": "2;a", "state": "SUCCESS", "returnValue": {"returnValue": "{}"}},
            {"id": "3;a", "state": "SUCCESS", "returnValue": {"returnValue": "{}"}},
            {"id": "4;a", "state": "SUCCESS", "returnValue": {"returnValue": "{}"}},
        ]
    }
    request = Request(
        url="https://cclb.michigan.gov/s/sfsites/aura?r=4",
        meta={
            "fwuid": "test",
            "provider_id": "001ABC",
            "search_data": {"Name": "Test Provider", "LicenseNumber": "DC111"},
        },
    )
    response = TextResponse(
        url=request.url,
        request=request,
        body=json.dumps(aura_response),
        encoding="utf-8",
    )
    results = list(spider.parse_detail(response))
    assert len(results) == 1
    item = results[0]
    # Detail info action failed, should use search_data fallback
    assert item["provider_name"] == "Test Provider"
    assert item["license_number"] == "DC111"


# ---- Unit tests for helper functions ----


class TestCleanAddress:
    def test_strips_trailing_null(self):
        assert clean_address("123 Main St null") == "123 Main St"

    def test_strips_trailing_null_case_insensitive(self):
        assert clean_address("123 Main St NULL") == "123 Main St"

    def test_strips_trailing_null_with_extra_spaces(self):
        assert clean_address("123 Main St  null ") == "123 Main St"

    def test_no_null(self):
        assert clean_address("123 Main St, Lansing, MI") == "123 Main St, Lansing, MI"

    def test_none_input(self):
        assert clean_address(None) is None

    def test_empty_string(self):
        assert clean_address("") is None

    def test_only_null(self):
        assert clean_address("null") is None


class TestFormatHours:
    def test_formats_correctly(self):
        details = [
            {"Day": "Mon", "OpenTime": "7:00 AM", "CloseTime": "6:00 PM"},
            {"Day": "Tue", "OpenTime": "8:00 AM", "CloseTime": "5:00 PM"},
        ]
        assert format_hours(details) == "Mon: 7:00 AM-6:00 PM; Tue: 8:00 AM-5:00 PM"

    def test_empty_list(self):
        assert format_hours([]) is None

    def test_none_input(self):
        assert format_hours(None) is None

    def test_missing_fields_skipped(self):
        details = [
            {"Day": "Mon", "OpenTime": "7:00 AM", "CloseTime": "6:00 PM"},
            {"Day": "Tue"},  # Missing times
        ]
        assert format_hours(details) == "Mon: 7:00 AM-6:00 PM"


class TestParseInspectionDoc:
    def test_standard_format(self):
        doc = {
            "Title": "DC730012345_INSP_20240512",
            "CreatedDate": "2024-05-12",
            "docurl": "https://example.com/report.pdf",
        }
        insp = parse_inspection_doc(doc)
        assert insp["type"] == "Inspection"
        assert insp["date"] == "05/12/2024"
        assert insp["report_url"] == "https://example.com/report.pdf"

    def test_format_with_pdf_extension(self):
        doc = {
            "Title": "DC730012345_RNWL_20230115.pdf",
            "CreatedDate": "2023-01-15",
            "docurl": "https://example.com/report.pdf",
        }
        insp = parse_inspection_doc(doc)
        assert insp["type"] == "Renewal"
        assert insp["date"] == "01/15/2023"

    def test_exit_inspection_type(self):
        doc = {"Title": "DC123_EXTINSP_20240320", "docurl": None}
        insp = parse_inspection_doc(doc)
        assert insp["type"] == "Exit Inspection"
        assert insp["date"] == "03/20/2024"

    def test_sir_type(self):
        doc = {"Title": "DC123_SIR_20231105", "docurl": "http://url"}
        insp = parse_inspection_doc(doc)
        assert insp["type"] == "Special Investigation Report"

    def test_unknown_type_code(self):
        doc = {"Title": "DC123_NEWTYPE_20240101", "docurl": None}
        insp = parse_inspection_doc(doc)
        # Unknown codes are returned as-is
        assert insp["type"] == "NEWTYPE"

    def test_title_with_suffix(self):
        doc = {"Title": "DC460016557_EXTRNWL_20250121 (2)", "docurl": None}
        insp = parse_inspection_doc(doc)
        assert insp["type"] == "External Renewal"
        assert insp["date"] == "01/21/2025"

    def test_title_with_rm_suffix(self):
        doc = {"Title": "DC630023376_EXTINSP_20250310_RM", "docurl": None}
        insp = parse_inspection_doc(doc)
        assert insp["type"] == "Exit Inspection"
        assert insp["date"] == "03/10/2025"

    def test_extsir_type(self):
        doc = {"Title": "DC123_EXTSIR_20250304", "docurl": None}
        insp = parse_inspection_doc(doc)
        assert insp["type"] == "External Special Investigation Report"

    def test_unstructured_title_fallback(self):
        doc = {
            "Title": "Some Random Document",
            "CreatedDate": "2024-01-01",
            "docurl": None,
        }
        insp = parse_inspection_doc(doc)
        assert insp["type"] == "Some Random Document"
        assert insp["date"] == "2024-01-01"

    def test_empty_title(self):
        doc = {"Title": "", "CreatedDate": "2024-01-01", "docurl": None}
        insp = parse_inspection_doc(doc)
        assert insp["date"] == "2024-01-01"

    def test_missing_fields(self):
        doc = {}
        insp = parse_inspection_doc(doc)
        assert insp.get("report_url") is None


# ---- Request builder tests ----


def test_build_search_request_structure(spider):
    """Test that search request has correct structure and parameters."""
    req = spider._build_search_request("test-fwuid", page_number=3, county="Wayne")
    assert req.method == "POST"
    assert "aura.ApexAction.execute=1" in req.url
    assert req.meta["fwuid"] == "test-fwuid"
    assert req.meta["page_number"] == 3
    assert req.meta["county"] == "Wayne"

    # Verify the body contains the expected message structure
    from urllib.parse import unquote_plus

    body_str = req.body.decode("utf-8") if isinstance(req.body, bytes) else req.body
    body_params = dict(
        pair.split("=", 1) for pair in body_str.split("&") if "=" in pair
    )
    message = json.loads(unquote_plus(body_params["message"]))
    assert len(message["actions"]) == 1
    action = message["actions"][0]
    assert action["descriptor"] == AURA_DESCRIPTOR
    assert action["params"]["classname"] == APEX_CLASSNAME
    assert action["params"]["method"] == "getFacility"
    assert action["params"]["params"]["pageSize"] == 200
    assert action["params"]["params"]["pageNumber"] == 3
    assert action["params"]["params"]["country"] == "Wayne"


def test_build_detail_request_batches_four_actions(spider):
    """Test that detail request batches all 4 API methods."""
    req = spider._build_detail_request(
        "test-fwuid", "001ABC", {"Name": "Test"}
    )
    assert req.method == "POST"

    from urllib.parse import unquote_plus

    body_str = req.body.decode("utf-8") if isinstance(req.body, bytes) else req.body
    body_params = dict(
        pair.split("=", 1) for pair in body_str.split("&") if "=" in pair
    )
    message = json.loads(unquote_plus(body_params["message"]))
    actions = message["actions"]
    assert len(actions) == 4

    # All actions use the same Aura descriptor
    for action in actions:
        assert action["descriptor"] == AURA_DESCRIPTOR
        assert action["params"]["classname"] == APEX_CLASSNAME

    # Verify each action's method name
    methods = [a["params"]["method"] for a in actions]
    assert "getDetailInfo" in methods
    assert "getOperationalDetailInfo" in methods
    assert "getServiceDetailInfo" in methods
    assert "getGenertatedDocs" in methods

    # All should reference the same provider ID in nested params
    for action in actions:
        assert action["params"]["params"]["accountId"] == "001ABC"

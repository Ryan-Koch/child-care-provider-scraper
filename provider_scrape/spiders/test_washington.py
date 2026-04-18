import json

import pytest
from scrapy.http import HtmlResponse, Request, TextResponse

from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.spiders.washington import (
    WashingtonSpider,
    extract_field,
)


@pytest.fixture
def spider():
    return WashingtonSpider()


SEARCH_PAGE_HTML = """
<html><head>
<script>
Visualforce.remoting.Manager.add(new $VFRM.RemotingProviderImpl({"vf":{"vid":"066t0000000Cg99","xhr":false},"actions":{"PSS_SearchController":{"ms":[{"name":"getKeys","len":8,"ns":"","ver":39.0,"csrf":"mock_csrf_keys","authorization":"mock_auth_keys"},{"name":"getSOSLKeys","len":8,"ns":"","ver":39.0,"csrf":"mock_csrf_sosl","authorization":"mock_auth_sosl"},{"name":"queryProviders","len":1,"ns":"","ver":39.0,"csrf":"mock_csrf_query","authorization":"mock_auth_query"}],"prm":1}},"service":"apexremote"}));
</script>
</head><body></body></html>
"""

PROVIDER_IDS_RESPONSE = json.dumps([{
    "statusCode": 200,
    "result": [
        "001t000000HmMBqAAN",
        "001t000000CjPPeAAN",
        "001t000000E93HAAAZ",
    ],
}])

DETAIL_HTML = """
<html>
<body>
<div class="panel provider-detail-panel">
    <div class="panel-heading">
        <div class="row">
            <div class="col-xs-12 col-md-8">
                <h1>1-2-3 Bambinos (1-2-3 bambinos)</h1>
            </div>
            <div class="col-xs-12 col-md-4 text-right">
                <p>Participating</p>
            </div>
        </div>
    </div>
    <div class="panel-body">
        <div class="row">
            <div class="col-xs-4">
                <p style="display:block">701 Browne Ave Yakima, WA 98902</p>
                <p style="display:block">(509) 961-3207</p>
            </div>
            <div class="col-xs-4">
                <div class="form-group">
                    <label>Provider Status</label>
                    <div>
                        Open
                    </div>
                </div>
                <div class="form-group">
                    <label>Hours of Operation</label>
                    <ul class="list-unstyled">
                        <li><div class="hoursOfOperationLabel">Sun\u00a0</div></li>
                        <li><div class="hoursOfOperationLabel">Mon\u00a0</div>7:00 AM - 6:00 PM</li>
                        <li><div class="hoursOfOperationLabel">Tue\u00a0</div>7:00 AM - 6:00 PM</li>
                        <li><div class="hoursOfOperationLabel">Wed\u00a0</div>7:00 AM - 6:00 PM</li>
                        <li><div class="hoursOfOperationLabel">Thu\u00a0</div>7:00 AM - 6:00 PM</li>
                        <li><div class="hoursOfOperationLabel">Fri\u00a0</div>7:00 AM - 6:00 PM</li>
                        <li><div class="hoursOfOperationLabel">Sat\u00a0</div></li>
                    </ul>
                </div>
            </div>
        </div>
    </div>
</div>

<div id="pageContentSource" style="display:none;">
    <div class="form-horizontal provider-detail-form">
        <div class="row">
            <div class="col-xs-12 col-md-6">
                <div class="form-group">
                    <label class="col-xs-12 col-sm-4 control-label">Website:</label>
                    <div class="col-xs-12 col-sm-8" style="display:block">
                        <p class="form-control-static">
                            <a href="https://www.example.com">https://www.example.com</a>
                        </p>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-xs-12 col-sm-4 control-label">Email:</label>
                    <div class="col-xs-12 col-sm-8" style="display:block">
                        <p class="form-control-static">test@example.com</p>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-xs-12 col-sm-4 control-label">Primary Contact:</label>
                    <div class="col-xs-12 col-sm-8">
                        <p class="form-control-static">Victoria Martinez</p>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-xs-12 col-sm-4 control-label">Head Start Funding:</label>
                    <div class="col-xs-12 col-sm-8">
                        <p class="form-control-static">No</p>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-xs-12 col-sm-4 control-label">Early Head Start Funding:</label>
                    <div class="col-xs-12 col-sm-8">
                        <p class="form-control-static">No</p>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-xs-12 col-sm-4 control-label">ECEAP Funding:</label>
                    <div class="col-xs-12 col-sm-8">
                        <p class="form-control-static">No</p>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-xs-12 col-sm-4 control-label">Total Available Slots:</label>
                    <div class="col-xs-12 col-sm-8" style="display:block">
                        <p class="form-control-static">3</p>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-xs-12 col-sm-4 control-label">Age Groups of Available Slots:</label>
                    <div class="col-xs-12 col-sm-8" style="display:block">
                        <p class="form-control-static">Slots Available;Toddler;School Age</p>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-xs-12 col-sm-4 control-label">Languages Spoken:</label>
                    <div class="col-xs-12 col-sm-8">
                        <p class="form-control-static">English</p>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-xs-12 col-sm-4 control-label">Languages of Instruction:</label>
                    <div class="col-xs-12 col-sm-8">
                        <p class="form-control-static">English; Spanish</p>
                    </div>
                </div>
            </div>
            <div class="col-xs-12 col-md-6">
                <div class="form-group">
                    <label class="col-xs-12 col-sm-4 control-label">License Name:</label>
                    <div class="col-xs-12 col-sm-8">
                        <p class="form-control-static">1-2-3 Bambinos</p>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-xs-12 col-sm-4 control-label">License Number:</label>
                    <div class="col-xs-12 col-sm-8">
                        <p class="form-control-static">PL-80322</p>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-xs-12 col-sm-4 control-label">Provider ID:</label>
                    <div class="col-xs-12 col-sm-8">
                        <p class="form-control-static">1585273</p>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-xs-12 col-sm-4 control-label">Facility Type:</label>
                    <div class="col-xs-12 col-sm-8">
                        <p class="form-control-static">Child Care Center</p>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-xs-12 col-sm-4 control-label">Ages:</label>
                    <div class="col-xs-12 col-sm-8">
                        <p class="form-control-static">12 months - 13 years 0 months</p>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-xs-12 col-sm-4 control-label">Initial License Date:</label>
                    <div class="col-xs-12 col-sm-8">
                        <p class="form-control-static">06/12/2019</p>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-xs-12 col-sm-4 control-label">License Status:</label>
                    <div class="col-xs-12 col-sm-8">
                        <p class="form-control-static">Open</p>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-xs-12 col-sm-4 control-label">License Type:</label>
                    <div class="col-xs-12 col-sm-8">
                        <p class="form-control-static">Full</p>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-xs-12 col-sm-4 control-label">Licensed Capacity:</label>
                    <div class="col-xs-12 col-sm-8">
                        <p class="form-control-static">29</p>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-xs-12 col-sm-4 control-label">School District:</label>
                    <div class="col-xs-12 col-sm-8">
                        <p class="form-control-static">Yakima School District</p>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-xs-12 col-sm-4 control-label">Food Program Participation:</label>
                    <div class="col-xs-12 col-sm-8">
                        <p class="form-control-static">Yes</p>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-xs-12 col-sm-4 control-label">Subsidy Participation:</label>
                    <div class="col-xs-12 col-sm-8">
                        <p class="form-control-static"></p>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <div>
        <h2>Provider Contacts</h2>
        <table id="ProviderContactsTable" class="table table-striped table-hover">
            <thead>
                <tr>
                    <th>Full Name</th><th>Role</th><th>Email</th><th>Phone</th><th>Start Date</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>nathalia medina</td>
                    <td>Secondary Contact</td>
                    <td>test@example.com</td>
                    <td>(509) 961-3207</td>
                    <td>06/07/2023</td>
                </tr>
                <tr>
                    <td>Victoria Martinez</td>
                    <td>Primary Contact</td>
                    <td>test@example.com</td>
                    <td>(509) 961-3207</td>
                    <td>06/07/2023</td>
                </tr>
                <tr>
                    <td>Jose Diaz</td>
                    <td>Primary Licensor</td>
                    <td>jose.diaz@dcyf.wa.gov</td>
                    <td>(509) 506-1571</td>
                    <td>02/03/2022</td>
                </tr>
            </tbody>
        </table>
    </div>

    <div style="margin-top: 30px;">
        <ul class="nav nav-tabs" role="tablist">
            <li class="active" role="presentation">
                <a href="#early-achievers" role="tab">Early Achievers</a>
            </li>
            <li role="presentation">
                <a href="#complaints" role="tab">Complaints</a>
            </li>
            <li role="presentation">
                <a href="#inspections" role="tab">Inspections</a>
            </li>
            <li role="presentation">
                <a href="#license_history" role="tab">License History</a>
            </li>
        </ul>

        <div class="tab-content">
            <div class="tab-pane active" id="early-achievers" role="tabpanel">
                <p><strong>Status: Participating</strong></p>
            </div>

            <div class="tab-pane" id="complaints" role="tabpanel">
                <div class="alert alert-danger">
                    <p>No Provider Cases available</p>
                </div>
            </div>

            <div class="tab-pane" id="inspections" role="tabpanel">
                <table class="table table-striped">
                    <thead>
                        <tr>
                            <th>Inspections Date</th>
                            <th>Inspection Type</th>
                            <th>Checklist Type</th>
                            <th>Document</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>11/07/2025</td>
                            <td>Physical</td>
                            <td>Licensing CCC Inspection Report</td>
                            <td><a href="https://wa-del.my.salesforce.com/sfc/p/test1">View</a></td>
                        </tr>
                        <tr>
                            <td>09/05/2024</td>
                            <td>Physical</td>
                            <td>ChildCareInspectionChecklist.pdf</td>
                            <td><a href="https://wa-del.my.salesforce.com/sfc/p/test2">View</a></td>
                        </tr>
                    </tbody>
                </table>
            </div>

            <div class="tab-pane" id="license_history" role="tabpanel">
                <table class="table table-striped table-hover">
                    <thead>
                        <tr>
                            <th>License ID</th>
                            <th>Regulation Type</th>
                            <th>Regulation Authority</th>
                            <th>Facility Type</th>
                            <th>License Type</th>
                            <th>License Status</th>
                            <th>License Issue Date</th>
                            <th>License Closure Date</th>
                            <th>License Status Reason</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>PL-80322</td>
                            <td>DCYF Licensed</td>
                            <td>DCYF</td>
                            <td>Child Care Center</td>
                            <td>Non-Expiring</td>
                            <td>Open</td>
                            <td>07/22/2021</td>
                            <td></td>
                            <td></td>
                        </tr>
                        <tr>
                            <td>PL-80000</td>
                            <td>DCYF Licensed</td>
                            <td>DCYF</td>
                            <td>Child Care Center</td>
                            <td>Initial (1st)</td>
                            <td>Closed</td>
                            <td>09/13/2019</td>
                            <td>12/10/2019</td>
                            <td>Withdrawn</td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>
</div>

<script>
    var aId = '001t000000HmMBqAAN';
    function onGoogleApiLoad() {
        if(!false){
            var lat = 46.6008387 + 0;
            var lng = -120.51960889999998 + 0;
        }
    }
</script>
</body>
</html>
"""

DETAIL_HTML_MINIMAL = """
<html>
<body>
<div class="panel provider-detail-panel">
    <div class="panel-heading">
        <div class="row">
            <div class="col-xs-12 col-md-8">
                <h1>Simple Provider</h1>
            </div>
        </div>
    </div>
    <div class="panel-body">
        <div class="row">
            <div class="col-xs-4">
                <p style="display:block">123 Main St Seattle, WA 98101</p>
                <p style="display:block">(206) 555-1234</p>
            </div>
        </div>
    </div>
</div>
<div id="pageContentSource" style="display:none;">
    <div class="form-horizontal provider-detail-form">
        <div class="row">
            <div class="col-xs-12 col-md-6">
                <div class="form-group">
                    <label class="col-xs-12 col-sm-4 control-label">License Number:</label>
                    <div class="col-xs-12 col-sm-8">
                        <p class="form-control-static">PL-99999</p>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-xs-12 col-sm-4 control-label">License Status:</label>
                    <div class="col-xs-12 col-sm-8">
                        <p class="form-control-static">Open</p>
                    </div>
                </div>
            </div>
        </div>
    </div>
</div>
</body>
</html>
"""


def test_extract_field():
    """Test the extract_field helper function."""
    request = Request(url="https://www.findchildcarewa.org/PSS_Provider?id=test")
    response = HtmlResponse(
        url=request.url, body=DETAIL_HTML, encoding="utf-8", request=request
    )

    assert extract_field(response, "License Number:") == "PL-80322"
    assert extract_field(response, "Provider ID:") == "1585273"
    assert extract_field(response, "Facility Type:") == "Child Care Center"
    assert extract_field(response, "Licensed Capacity:") == "29"
    assert extract_field(response, "Subsidy Participation:") is None  # empty


def test_parse_search_page(spider):
    """Test that parse_search_page extracts tokens and yields API request."""
    request = Request(url="https://www.findchildcarewa.org/PSS_Search?p=DEL%20Licensed")
    response = HtmlResponse(
        url=request.url, body=SEARCH_PAGE_HTML, encoding="utf-8", request=request
    )

    results = list(spider.parse_search_page(response))
    assert len(results) == 1

    api_request = results[0]
    assert api_request.url == "https://www.findchildcarewa.org/apexremote"
    assert api_request.method == "POST"

    body = json.loads(api_request.body)
    assert body["action"] == "PSS_SearchController"
    assert body["method"] == "getSOSLKeys"
    assert body["ctx"]["csrf"] == "mock_csrf_sosl"
    assert body["ctx"]["authorization"] == "mock_auth_sosl"
    assert body["ctx"]["vid"] == "066t0000000Cg99"
    assert body["ctx"]["ver"] == 39


def test_parse_provider_ids(spider):
    """Test that parse_provider_ids yields detail requests for each ID."""
    request = Request(url="https://www.findchildcarewa.org/apexremote")
    response = TextResponse(
        url=request.url,
        body=PROVIDER_IDS_RESPONSE.encode("utf-8"),
        encoding="utf-8",
        request=request,
    )

    results = list(spider.parse_provider_ids(response))
    assert len(results) == 3

    assert "001t000000HmMBqAAN" in results[0].url
    assert "001t000000CjPPeAAN" in results[1].url
    assert "001t000000E93HAAAZ" in results[2].url


def test_parse_detail(spider):
    """Test full detail page parsing."""
    request = Request(
        url="https://www.findchildcarewa.org/PSS_Provider?id=001t000000HmMBqAAN"
    )
    response = HtmlResponse(
        url=request.url, body=DETAIL_HTML, encoding="utf-8", request=request
    )

    results = list(spider.parse_detail(response))
    assert len(results) == 1

    item = results[0]
    assert isinstance(item, ProviderItem)

    # Core fields
    assert item["source_state"] == "Washington"
    assert item["provider_name"] == "1-2-3 Bambinos (1-2-3 bambinos)"
    assert item["address"] == "701 Browne Ave Yakima, WA 98902"
    assert item["phone"] == "(509) 961-3207"
    assert item["wa_provider_status"] == "Open"
    assert item["email"] == "test@example.com"
    assert item["administrator"] == "Victoria Martinez"

    # License info
    assert item["license_number"] == "PL-80322"
    assert item["wa_provider_id"] == "1585273"
    assert item["wa_license_name"] == "1-2-3 Bambinos"
    assert item["wa_license_type"] == "Full"
    assert item["status"] == "Open"
    assert item["license_begin_date"] == "06/12/2019"
    assert item["capacity"] == "29"
    assert item["provider_type"] == "Child Care Center"

    # Ages and slots
    assert item["ages_served"] == "12 months - 13 years 0 months"
    assert item["wa_available_slots"] == "3"
    assert "Toddler" in item["wa_slot_age_groups"]

    # Funding
    assert item["wa_head_start"] == "No"
    assert item["wa_early_head_start"] == "No"
    assert item["wa_eceap"] == "No"

    # School/language/program
    assert item["wa_school_district"] == "Yakima School District"
    assert item["languages"] == "English"
    assert item["wa_languages_of_instruction"] == "English; Spanish"
    assert item["wa_food_program"] == "Yes"
    assert item["wa_subsidy"] is None  # empty on page

    # Early Achievers
    assert item["wa_early_achievers_status"] == "Participating"

    # Lat/Lng
    assert item["latitude"] == "46.6008387"
    assert item["longitude"] == "-120.51960889999998"

    # Website
    assert item["provider_website"] == "https://www.example.com"

    # Hours
    assert "Mon 7:00 AM - 6:00 PM" in item["hours"]
    assert "Fri 7:00 AM - 6:00 PM" in item["hours"]

    # Provider URL
    assert "001t000000HmMBqAAN" in item["provider_url"]


def test_parse_contacts(spider):
    """Test provider contacts table parsing."""
    request = Request(
        url="https://www.findchildcarewa.org/PSS_Provider?id=test"
    )
    response = HtmlResponse(
        url=request.url, body=DETAIL_HTML, encoding="utf-8", request=request
    )

    results = list(spider.parse_detail(response))
    item = results[0]

    assert len(item["wa_contacts"]) == 3
    assert item["wa_contacts"][0]["name"] == "nathalia medina"
    assert item["wa_contacts"][0]["role"] == "Secondary Contact"
    assert item["wa_contacts"][1]["name"] == "Victoria Martinez"
    assert item["wa_contacts"][1]["role"] == "Primary Contact"
    assert item["wa_contacts"][2]["name"] == "Jose Diaz"
    assert item["wa_contacts"][2]["role"] == "Primary Licensor"
    assert item["wa_contacts"][2]["email"] == "jose.diaz@dcyf.wa.gov"


def test_parse_inspections(spider):
    """Test inspections table parsing."""
    request = Request(
        url="https://www.findchildcarewa.org/PSS_Provider?id=test"
    )
    response = HtmlResponse(
        url=request.url, body=DETAIL_HTML, encoding="utf-8", request=request
    )

    results = list(spider.parse_detail(response))
    item = results[0]

    assert len(item["inspections"]) == 2
    assert item["inspections"][0]["date"] == "11/07/2025"
    assert item["inspections"][0]["type"] == "Physical"
    assert item["inspections"][0]["original_status"] == "Licensing CCC Inspection Report"
    assert "test1" in item["inspections"][0]["report_url"]
    assert item["inspections"][1]["date"] == "09/05/2024"
    assert "test2" in item["inspections"][1]["report_url"]


def test_parse_license_history(spider):
    """Test license history table parsing."""
    request = Request(
        url="https://www.findchildcarewa.org/PSS_Provider?id=test"
    )
    response = HtmlResponse(
        url=request.url, body=DETAIL_HTML, encoding="utf-8", request=request
    )

    results = list(spider.parse_detail(response))
    item = results[0]

    assert len(item["wa_license_history"]) == 2
    assert item["wa_license_history"][0]["license_id"] == "PL-80322"
    assert item["wa_license_history"][0]["license_type"] == "Non-Expiring"
    assert item["wa_license_history"][0]["license_status"] == "Open"
    assert item["wa_license_history"][0]["issue_date"] == "07/22/2021"
    assert item["wa_license_history"][0]["closure_date"] == ""
    assert item["wa_license_history"][0]["status_reason"] == ""
    assert item["wa_license_history"][1]["license_id"] == "PL-80000"
    assert item["wa_license_history"][1]["license_type"] == "Initial (1st)"
    assert item["wa_license_history"][1]["license_status"] == "Closed"
    assert item["wa_license_history"][1]["closure_date"] == "12/10/2019"
    assert item["wa_license_history"][1]["status_reason"] == "Withdrawn"


def test_parse_detail_minimal(spider):
    """Test parsing a minimal detail page with few fields."""
    request = Request(
        url="https://www.findchildcarewa.org/PSS_Provider?id=test"
    )
    response = HtmlResponse(
        url=request.url,
        body=DETAIL_HTML_MINIMAL,
        encoding="utf-8",
        request=request,
    )

    results = list(spider.parse_detail(response))
    assert len(results) == 1

    item = results[0]
    assert item["provider_name"] == "Simple Provider"
    assert item["address"] == "123 Main St Seattle, WA 98101"
    assert item["phone"] == "(206) 555-1234"
    assert item["license_number"] == "PL-99999"
    assert item["status"] == "Open"
    assert item["source_state"] == "Washington"
    assert item["inspections"] == []
    assert item["wa_contacts"] == []
    assert item["wa_license_history"] == []

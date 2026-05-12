import pytest
from scrapy.http import HtmlResponse, Request

from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.spiders.west_virginia import (
    SEARCH_URL,
    WestVirginiaSpider,
)


def make_response(body, url="https://www.wvdhhr.org/bcf/ece/cccenters/get_detailsWVI.asp?q=12345"):
    request = Request(url=url)
    return HtmlResponse(
        url=url, body=body.encode("utf-8"), encoding="utf-8", request=request
    )


RESULTS_HTML = """
<html><body>
<table class='sortable' id='center_table'>
    <thead><tr>
        <th>Agency</th><th>City</th><th>Postal</th>
        <th>County</th><th>Phone</th><th>Contact</th>
    </tr></thead>
    <tbody id='center_data'><tr>
        <td><a href='get_detailsWVI.asp?q=30179048'>
            <img src='images/details.gif' alt='View Center Details'>
            &nbsp;10 Fingers 10 Toes
        </a></td>
        <td>Charles Town</td><td>25414</td><td>Jefferson</td>
        <td>(304) 724-8800</td><td>Default</td>
    </tr></tbody>
    <tbody id='center_data'><tr>
        <td><a href='get_detailsWVI.asp?q=30192173'>
            <img src='images/details.gif' alt='View Center Details'>
            &nbsp;A Child's Place
        </a></td>
        <td>Evans</td><td>25241</td><td>Jackson</td>
        <td>(304) 372 3934</td><td>Default</td>
    </tr></tbody>
</table>
</body></html>
"""


GOLDEN_DETAIL_HTML = """
<html><body>
<h1>Provider Details</h1>
<table id='centertablea'>
    <tr><td><strong>Agency Name</strong></td><td>10 Fingers 10 Toes at the Yellow Schoolhouse 6/13/2026</td></tr>
    <tr><td><strong>Address 1</strong></td><td>8368 Summit Point Rd</td></tr>
    <tr><td><strong>Address 2</strong></td><td>Suite 102</td></tr>
    <tr><td><strong>City</strong></td><td>Charles Town</td></tr>
    <tr><td><strong>Zip Code</strong></td><td>25414</td></tr>
    <tr><td><strong>County</strong></td><td>Jefferson</td></tr>
    <tr><td><strong>Phone</strong></td><td>(304) 724-8800</td></tr>
    <tr><td><strong>DHHR Licensing Specialist</strong></td><td>WILLIAM RIGGLEMAN</td></tr>
    <tr><td><strong>License Type</strong></td><td>Regular</td></tr>
    <tr><td><strong>License Expires</strong></td><td>06/13/2026</td></tr>
    <tr><td><strong>Contact</strong></td><td> Default</td></tr>
    <tr><td><strong>Title</strong></td><td>Owner</td></tr>
</table>
<h1>Capacity Details</h1>
<table id='centertableb'>
    <tr><td><strong>Capacity</strong></td><td>24        </td></tr>
    <tr><td><strong>Age From</strong></td><td>2 Years 0 Months</td></tr>
    <tr><td><strong>Age To</strong></td><td>6 Years 0 Months</td></tr>
</table>
<h1>Corrective Action Plans</h1>
<table id='centertablec'>
    <tr><td bgcolor='#000080'> </td><td bgcolor='#000080'> </td></tr>
    <tr><td><strong>Corrective Action Plan Start</strong></td><td>6/6/2024</td></tr>
    <tr><td><strong>Corrective Action Plan End</strong></td><td>3/18/2025</td></tr>
    <tr><td><strong>Non Compliance Code</strong></td><td>8.4.d. Authorization release on file.</td></tr>
    <tr><td><strong>Outcome Code</strong></td><td>Achieved</td></tr>
    <tr><td><strong>Issue Completed Date</strong></td><td>7/6/2024</td></tr>
    <tr><td bgcolor='#000080'> </td><td bgcolor='#000080'> </td></tr>
    <tr><td><strong>Corrective Action Plan Start</strong></td><td>9/16/2025</td></tr>
    <tr><td><strong>Corrective Action Plan End</strong></td><td>10/8/2025</td></tr>
    <tr><td><strong>Non Compliance Code</strong></td><td>First aid kit must be equipped.</td></tr>
    <tr><td><strong>Outcome Code</strong></td><td></td></tr>
    <tr><td><strong>Issue Completed Date</strong></td><td>1/1/1753</td></tr>
</table>
</body></html>
"""


SPARSE_DETAIL_HTML = """
<html><body>
<h1>Provider Details</h1>
<table id='centertablea'>
    <tr><td><strong>Agency Name</strong></td><td>A Child's Place</td></tr>
    <tr><td><strong>Address 1</strong></td><td>142 King Dr</td></tr>
    <tr><td><strong>Address 2</strong></td><td></td></tr>
    <tr><td><strong>City</strong></td><td>Evans</td></tr>
    <tr><td><strong>Zip Code</strong></td><td>25241</td></tr>
    <tr><td><strong>County</strong></td><td>Jackson</td></tr>
    <tr><td><strong>Phone</strong></td><td>(304) 372 3934</td></tr>
    <tr><td><strong>License Type</strong></td><td>Regular</td></tr>
    <tr><td><strong>License Expires</strong></td><td>12/31/2026</td></tr>
</table>
<h1>Capacity Details</h1>
<table id='centertableb'></table>
<h1>Corrective Action Plans</h1>
<table id='centertablec'></table>
</body></html>
"""


def test_parse_results_yields_request_per_provider():
    spider = WestVirginiaSpider()
    response = make_response(RESULTS_HTML, url=SEARCH_URL)

    requests = list(spider.parse_results(response))

    assert len(requests) == 2
    assert requests[0].url == (
        "https://www.wvdhhr.org/bcf/ece/cccenters/get_detailsWVI.asp?q=30179048"
    )
    assert requests[1].url == (
        "https://www.wvdhhr.org/bcf/ece/cccenters/get_detailsWVI.asp?q=30192173"
    )
    for req in requests:
        assert req.callback == spider.parse_details


def test_parse_details_golden_path():
    spider = WestVirginiaSpider()
    response = make_response(GOLDEN_DETAIL_HTML)

    items = list(spider.parse_details(response))
    assert len(items) == 1
    provider = items[0]

    assert isinstance(provider, ProviderItem)
    assert provider["source_state"] == "WV"
    assert provider["provider_url"] == response.url
    assert provider["provider_name"] == (
        "10 Fingers 10 Toes at the Yellow Schoolhouse 6/13/2026"
    )
    assert provider["address"] == (
        "8368 Summit Point Rd Suite 102, Charles Town 25414"
    )
    assert provider["county"] == "Jefferson"
    assert provider["phone"] == "(304) 724-8800"
    assert provider["wv_licensing_specialist"] == "WILLIAM RIGGLEMAN"
    assert provider["wv_license_type"] == "Regular"
    assert provider["license_expiration"] == "06/13/2026"
    assert provider["wv_contact"] == "Default"
    assert provider["wv_contact_title"] == "Owner"

    assert provider["capacity"] == "24"
    assert provider["wv_age_from"] == "2 Years 0 Months"
    assert provider["wv_age_to"] == "6 Years 0 Months"

    inspections = provider["inspections"]
    assert len(inspections) == 2
    assert all(isinstance(i, InspectionItem) for i in inspections)

    first, second = inspections
    assert first["wv_corrective_action_plan_start"] == "6/6/2024"
    assert first["wv_corrective_action_plan_end"] == "3/18/2025"
    assert first["wv_non_compliance_code"] == "8.4.d. Authorization release on file."
    assert first["wv_outcome_code"] == "Achieved"
    assert first["wv_issue_completed_date"] == "7/6/2024"

    assert second["wv_corrective_action_plan_start"] == "9/16/2025"
    assert second["wv_outcome_code"] == ""
    assert second["wv_issue_completed_date"] == "1/1/1753"


def test_parse_details_handles_missing_capacity_and_inspections():
    spider = WestVirginiaSpider()
    response = make_response(SPARSE_DETAIL_HTML)

    provider = next(spider.parse_details(response))

    assert provider["provider_name"] == "A Child's Place"
    assert provider["address"] == "142 King Dr, Evans 25241"
    assert provider["wv_license_type"] == "Regular"
    assert provider["license_expiration"] == "12/31/2026"

    # Capacity, age, and inspector fields should not be set when source data is absent.
    for field in ("capacity", "wv_age_from", "wv_age_to", "wv_licensing_specialist"):
        assert field not in provider

    assert provider["inspections"] == []


def test_start_requests_posts_view_all_form():
    spider = WestVirginiaSpider()
    requests = list(spider.start_requests())

    assert len(requests) == 1
    request = requests[0]
    assert request.url == SEARCH_URL
    assert request.method == "POST"
    assert b"view_all=View+All+Providers" in request.body

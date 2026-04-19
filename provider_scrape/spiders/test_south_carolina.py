from scrapy.http import HtmlResponse, Request
from scrapy import Selector

import pytest

from provider_scrape.items import ProviderItem
from provider_scrape.spiders.south_carolina import (
    SouthCarolinaSpider,
    extract_attribute,
    parse_abc_rating,
    parse_fac_coords,
)


@pytest.fixture
def spider():
    return SouthCarolinaSpider()


LISTING_HTML = """
<html><body>
<section class="search-results">
    <div class="results-details">
        <span class="number">2433</span>
    </div>

    <div class="row result">
        <a href="/provider/24680/bright-horizons/">
            <h2>Bright Horizons</h2>
        </a>
    </div>
    <div class="row result">
        <a href="/provider/CC045778/sunshine-daycare/">
            <h2>Sunshine Daycare</h2>
        </a>
    </div>
    <div class="row result">
        <a href="/provider/99999/untracked-provider/">
            <h2>Untracked Provider</h2>
        </a>
    </div>

    <ul class="pagination">
        <li><a class="page-link" aria-label="Next" href="/provider-search/?all=1&amp;page=2">Next</a></li>
    </ul>
</section>

<script>
var facCoords = {
    '24680':{'latlng':new google.maps.LatLng(34.008744,-81.040186),'name':'Bright Horizons'},
    'CC045778':{'latlng':new google.maps.LatLng(32.776475,-79.931053),'name':'Sunshine Daycare'}
};
</script>
</body></html>
"""


LISTING_HTML_LAST_PAGE = """
<html><body>
<section class="search-results">
    <div class="results-details">
        <span class="number">2433</span>
    </div>

    <div class="row result">
        <a href="/provider/11111/final-provider/">
            <h2>Final Provider</h2>
        </a>
    </div>

    <ul class="pagination">
        <li><a class="page-link" href="/provider-search/?all=1&amp;page=304">Previous</a></li>
    </ul>
</section>

<script>
var facCoords = {
    '11111':{'latlng':new google.maps.LatLng(34.000000,-81.000000),'name':'Final Provider'}
};
</script>
</body></html>
"""


DETAIL_HTML = """
<html><body>
<section class="location-info">
    <div class="row">
        <div class="col-lg-8">
            <h1>Bright Horizons at Columbia Federal Child Development Center</h1>
            <div class="row">
                <div class="col-md-6">
                    <div class="icon-detail">
                        <p>Child Care Center</p>
                    </div>
                </div>
            </div>

            <div class="row grey-bg">
                <div class="col-md-6">
                    <p>ABC Quality Rating</p>
                </div>
                <div class="col-md-6">
                    <img src="/img/abc-a-plus.png" alt="A+ Rating">
                </div>
            </div>

            <div class="row">
                <div class="col-md-6">
                    <p class="attribute-title">Operator:</p>
                    <p>Bright Horizons Family Solutions</p>
                </div>
                <div class="col-md-6">
                    <p class="attribute-title">Capacity:</p>
                    <p>120</p>
                </div>
            </div>

            <div class="provider-tags">
                <span class="tag"><img alt="ABC Quality"></span>
                <span class="tag"><img alt="Accepts Vouchers"></span>
            </div>

            <div class="facility-hours">
                <table class="data-table">
                    <tbody>
                        <tr>
                            <th>Monday</th>
                            <td><ul><li>6:30 AM - 6:30 PM</li></ul></td>
                        </tr>
                        <tr>
                            <th>Tuesday</th>
                            <td><ul><li>6:30 AM - 6:30 PM</li></ul></td>
                        </tr>
                        <tr>
                            <th>Friday</th>
                            <td><ul><li>6:30 AM - 6:30 PM</li></ul></td>
                        </tr>
                    </tbody>
                </table>
            </div>

            <div class="row">
                <div class="col-md-6">
                    <p class="attribute-title">Licensing Type &amp; Number:</p>
                    <p>License#: 30725</p>
                </div>
            </div>

            <div class="row">
                <div class="col-md-6">
                    <p class="attribute-title">Issue Date:</p>
                    <p>04/01/2024</p>
                </div>
                <div class="col-md-6">
                    <p class="attribute-title">Expiration Date:</p>
                    <p>03/31/2026</p>
                </div>
            </div>

            <div class="specialists">
                <div class="specialist">
                    <p class="attribute-title">DSS Licensing Specialist</p>
                    <p>Jane Doe</p>
                    <a href="tel:+18035551234">(803) 555-1234</a>
                </div>
            </div>
        </div>

        <div class="col-lg-4">
            <div class="location-contact">
                <div class="icon-detail">
                    <p>1601 Main Street</p>
                    <p>Columbia, SC 29201</p>
                    <p>Richland County</p>
                </div>
                <div class="icon-detail">
                    <a href="tel:+18035557890">(803) 555-7890</a>
                </div>
            </div>
        </div>
    </div>
</section>

<section class="location-inspections">
    <div class="inspection-row">
        <div class="date"><p>10/15/2025</p></div>
        <div class="type"><p>Routine</p></div>
        <div class="download"><a href="/inspection-reports/30725-2025-10-15.pdf">PDF</a></div>
        <div class="alerts">
            <p>2 Alert(s)</p>
            <p>1 Resolved</p>
        </div>
        <div class="alert-dropdown">
            <div class="alert-slide">
                <a class="severity">High</a>
                <div class="alert-info">
                    <span class="label">Severity Level:</span>
                    <p>High</p>
                </div>
                <div class="alert-info">
                    <span class="label">Regulation:</span>
                    <p>114-502.A</p>
                </div>
                <div class="alert-info">
                    <span class="label">Finding:</span>
                    <p>Staff-to-child ratio exceeded in the toddler room.</p>
                </div>
                <div class="alert-info">
                    <span class="label">Status:</span>
                    <p>Resolved</p>
                </div>
            </div>
            <div class="alert-slide">
                <a class="severity">Medium</a>
                <div class="alert-info">
                    <span class="label">Regulation:</span>
                    <p>114-503.B</p>
                </div>
                <div class="alert-info">
                    <span class="label">Finding:</span>
                    <p>Minor cleanliness issue in kitchen.</p>
                </div>
            </div>
        </div>
    </div>
    <div class="inspection-row">
        <div class="date"><p>05/12/2024</p></div>
        <div class="type"><p>Complaint</p></div>
        <div class="download"><a href="/inspection-reports/30725-2024-05-12.pdf">PDF</a></div>
    </div>

    <table class="data-table deficiencies">
        <tbody>
            <tr>
                <td data-title="Date">10/2025</td>
                <td data-title="Rating"><img src="/img/abc-a-plus.png" alt="A+"></td>
            </tr>
            <tr>
                <td data-title="Date">06/2023</td>
                <td data-title="Rating"><img src="/img/abc-a.png" alt="A"></td>
            </tr>
        </tbody>
    </table>
</section>
</body></html>
"""


DETAIL_HTML_EXEMPT = """
<html><body>
<section class="location-info">
    <div class="row">
        <div class="col-lg-8">
            <h1>Grandma's House Family Care</h1>
            <div class="row">
                <div class="col-md-6">
                    <div class="icon-detail">
                        <p>Family Child Care Home</p>
                    </div>
                </div>
            </div>

            <div class="row">
                <div class="col-md-6">
                    <p class="attribute-title">Licensing Type &amp; Number:</p>
                    <p>Not Licensed (Exempt)</p>
                </div>
            </div>
        </div>

        <div class="col-lg-4">
            <div class="location-contact">
                <div class="icon-detail">
                    <p>45 Oak Lane</p>
                    <p>Greenville, SC 29601</p>
                </div>
            </div>
        </div>
    </div>
</section>
</body></html>
"""


def test_parse_abc_rating():
    assert parse_abc_rating("/img/abc-a-plus.png") == "A+"
    assert parse_abc_rating("/img/abc-a.png") == "A"
    assert parse_abc_rating("/img/abc-b-plus.png") == "B+"
    assert parse_abc_rating("/img/abc-b.png") == "B"
    assert parse_abc_rating("/img/abc-c.png") == "C"
    assert parse_abc_rating("/img/abc-p.png") == "P"
    assert parse_abc_rating("https://example.com/img/abc-a.png?v=1") == "A"
    assert parse_abc_rating(None) is None
    assert parse_abc_rating("/img/logo.png") is None


def test_parse_fac_coords():
    js = """
    var facCoords = {
        '24680':{'latlng':new google.maps.LatLng(34.008744,-81.040186),'n':'x'},
        'CC045778':{'latlng':new google.maps.LatLng(32.776475,-79.931053),'n':'y'}
    };
    """
    coords = parse_fac_coords(js)
    assert coords["24680"] == ("34.008744", "-81.040186")
    assert coords["CC045778"] == ("32.776475", "-79.931053")
    assert len(coords) == 2


def test_parse_fac_coords_empty():
    assert parse_fac_coords("") == {}
    assert parse_fac_coords("no coords here") == {}


def test_extract_attribute():
    html = """
    <div>
        <p class="attribute-title">Operator:</p>
        <p>Bright Horizons Family Solutions</p>
        <p class="attribute-title">Capacity:</p>
        <p>120</p>
        <p class="attribute-title">Empty Field:</p>
        <p></p>
    </div>
    """
    sel = Selector(text=html)
    assert extract_attribute(sel, "Operator:") == "Bright Horizons Family Solutions"
    assert extract_attribute(sel, "Capacity:") == "120"
    assert extract_attribute(sel, "Empty Field:") is None
    assert extract_attribute(sel, "Missing:") is None


def test_parse_search_page(spider):
    request = Request(url="https://www.scchildcare.org/provider-search/?all=1")
    response = HtmlResponse(
        url=request.url,
        body=LISTING_HTML,
        encoding="utf-8",
        request=request,
    )
    response.meta["page"] = 1

    results = list(spider.parse_search_page(response))

    detail_requests = [
        r for r in results if "/provider/" in r.url and "?" not in r.url
    ]
    assert len(detail_requests) == 3

    # Provider 24680 should have coords carried in meta
    req_24680 = next(
        r for r in detail_requests if "/provider/24680/" in r.url
    )
    assert req_24680.meta["sc_provider_id"] == "24680"
    assert req_24680.meta["latitude"] == "34.008744"
    assert req_24680.meta["longitude"] == "-81.040186"

    # Alphanumeric ID also captured
    req_cc = next(
        r for r in detail_requests if "/provider/CC045778/" in r.url
    )
    assert req_cc.meta["sc_provider_id"] == "CC045778"
    assert req_cc.meta["latitude"] == "32.776475"

    # Provider 99999 has no coords but should still be requested
    req_99999 = next(
        r for r in detail_requests if "/provider/99999/" in r.url
    )
    assert req_99999.meta["sc_provider_id"] == "99999"
    assert req_99999.meta["latitude"] is None
    assert req_99999.meta["longitude"] is None

    # Pagination: "Next" link should yield another search request
    pagination_requests = [
        r for r in results if "provider-search" in r.url
    ]
    assert len(pagination_requests) == 1
    assert "page=2" in pagination_requests[0].url
    assert pagination_requests[0].meta["page"] == 2


def test_parse_search_page_last_page(spider):
    """The last page has no Next link, so no pagination request is yielded."""
    request = Request(url="https://www.scchildcare.org/provider-search/?all=1&page=305")
    response = HtmlResponse(
        url=request.url,
        body=LISTING_HTML_LAST_PAGE,
        encoding="utf-8",
        request=request,
    )
    response.meta["page"] = 305

    results = list(spider.parse_search_page(response))

    pagination_requests = [r for r in results if "provider-search" in r.url]
    assert len(pagination_requests) == 0

    detail_requests = [r for r in results if "/provider/" in r.url]
    assert len(detail_requests) == 1
    assert detail_requests[0].meta["sc_provider_id"] == "11111"


def test_parse_detail_golden(spider):
    request = Request(
        url="https://www.scchildcare.org/provider/24680/bright-horizons/"
    )
    request.meta["sc_provider_id"] = "24680"
    request.meta["latitude"] = "34.008744"
    request.meta["longitude"] = "-81.040186"
    response = HtmlResponse(
        url=request.url,
        body=DETAIL_HTML,
        encoding="utf-8",
        request=request,
    )

    results = list(spider.parse_detail(response))
    assert len(results) == 1
    item = results[0]
    assert isinstance(item, ProviderItem)

    assert item["source_state"] == "South Carolina"
    assert item["provider_url"].endswith("/provider/24680/bright-horizons/")
    assert item["sc_provider_id"] == "24680"
    assert item["latitude"] == "34.008744"
    assert item["longitude"] == "-81.040186"

    assert (
        item["provider_name"]
        == "Bright Horizons at Columbia Federal Child Development Center"
    )
    assert item["provider_type"] == "Child Care Center"
    assert item["sc_abc_quality_rating"] == "A+"
    assert item["administrator"] == "Bright Horizons Family Solutions"
    assert item["capacity"] == "120"
    assert item["sc_program_participation"] == ["ABC Quality", "Accepts Vouchers"]

    assert "Monday 6:30 AM - 6:30 PM" in item["hours"]
    assert "Friday 6:30 AM - 6:30 PM" in item["hours"]

    assert item["sc_license_category"] == "License"
    assert item["license_number"] == "30725"
    assert item["license_begin_date"] == "04/01/2024"
    assert item["license_expiration"] == "03/31/2026"

    assert item["sc_licensing_specialist_name"] == "Jane Doe"
    assert item["sc_licensing_specialist_phone"] == "(803) 555-1234"

    assert item["address"] == "1601 Main Street, Columbia, SC 29201"
    assert item["county"] == "Richland"
    assert item["phone"] == "(803) 555-7890"

    assert len(item["inspections"]) == 2
    first_insp = item["inspections"][0]
    assert first_insp["date"] == "10/15/2025"
    assert first_insp["type"] == "Routine"
    assert first_insp["report_url"].endswith(
        "/inspection-reports/30725-2025-10-15.pdf"
    )
    assert first_insp["sc_alert_count"] == 2
    assert first_insp["sc_alert_resolved_count"] == 1

    deficiencies = first_insp["sc_deficiencies"]
    assert len(deficiencies) == 2
    assert deficiencies[0]["severity"] == "High"
    assert deficiencies[0]["regulation"] == "114-502.A"
    assert deficiencies[0]["finding"].startswith("Staff-to-child ratio")
    assert deficiencies[0]["status"] == "Resolved"
    # Severity Level from alert-info must not duplicate into its own key
    assert "severity_level" not in deficiencies[0]
    assert deficiencies[1]["severity"] == "Medium"

    second_insp = item["inspections"][1]
    assert second_insp["date"] == "05/12/2024"
    assert second_insp["type"] == "Complaint"
    assert "sc_alert_count" not in second_insp

    assert item["sc_abc_rating_history"] == [
        {"date": "10/2025", "rating": "A+"},
        {"date": "06/2023", "rating": "A"},
    ]


def test_parse_detail_exempt(spider):
    request = Request(
        url="https://www.scchildcare.org/provider/EX001/grandmas-house/"
    )
    request.meta["sc_provider_id"] = "EX001"
    request.meta["latitude"] = None
    request.meta["longitude"] = None
    response = HtmlResponse(
        url=request.url,
        body=DETAIL_HTML_EXEMPT,
        encoding="utf-8",
        request=request,
    )

    results = list(spider.parse_detail(response))
    assert len(results) == 1
    item = results[0]

    assert item["source_state"] == "South Carolina"
    assert item["provider_name"] == "Grandma's House Family Care"
    assert item["provider_type"] == "Family Child Care Home"
    assert item["sc_license_category"] == "Exempt"
    assert item["status"] == "Not Licensed (Exempt)"
    assert "license_number" not in item
    assert item["address"] == "45 Oak Lane, Greenville, SC 29601"
    assert "county" not in item
    assert item["inspections"] == []
    assert "sc_abc_quality_rating" not in item
    assert "sc_abc_rating_history" not in item
    assert "administrator" not in item
    assert "capacity" not in item

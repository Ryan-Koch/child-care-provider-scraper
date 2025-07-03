
import pytest
from scrapy.http import HtmlResponse, Request
from provider_scrape.spiders.ohdcy import OhdcySpider
from provider_scrape.items import ProviderItem, InspectionItem

# Full HTML content for a provider page
PROVIDER_HTML = """
<!DOCTYPE html>
<html>
<body>
    <div class="detailGroupContainer">
        <div class="detailGroup">
            <div class="detailRow">
                <span class="detailLabel">Program Status:</span>
                <span class="detailInfo"><span>Active</span></span>
            </div>
            <div class="detailRow">
                <span class="detailLabel">SUTQ Rating:</span>
                <span class="detailInfo"><span>Star 5</span></span>
            </div>
            <div class="detailRow">
                <span class="detailLabel">Number:</span>
                <span class="detailInfo">12345</span>
            </div>
            <div class="detailRow">
                <span class="detailLabel">County:</span>
                <span class="detailInfo">Franklin</span>
            </div>
            <div class="detailRow">
                <span class="detailLabel">License Begin Date:</span>
                <span class="detailInfo">01/01/2020</span>
            </div>
            <div class="detailRow">
                <span class="detailLabel">License Expiration Date:</span>
                <span class="detailInfo">12/31/2025</span>
            </div>
            <div class="detailRow">
                <span class="detailLabel">Administrator(s):</span>
                <span class="detailInfo">John Doe</span>
            </div>
            <div class="detailRow">
                <span class="detailLabel">Email:</span>
                <span class="detailInfo"><a href="mailto:test@example.com">test@example.com</a></span>
            </div>
            <div class="detailRow">
                <span class="detailLabel">Phone:</span>
                <span class="detailInfo"><a href="tel:614-555-1212">614-555-1212</a></span>
            </div>
            <div class="detailRow">
                <span class="detailLabel">Current Inspections:</span>
                <span class="detailInfo"><a href="/inspections/12345">View Inspections</a></span>
            </div>
        </div>
    </div>
</body>
</html>
"""

# HTML content for a provider page with missing fields
PROVIDER_HTML_MISSING = """
<!DOCTYPE html>
<html>
<body>
    <div class="detailGroupContainer">
        <div class="detailGroup">
            <div class="detailRow">
                <span class="detailLabel">Program Status:</span>
                <span class="detailInfo"><span>Active</span></span>
            </div>
            <div class="detailRow">
                <span class="detailLabel">Number:</span>
                <span class="detailInfo">54321</span>
            </div>
            <div class="detailRow">
                <span class="detailLabel">Current Inspections:</span>
                <span class="detailInfo"><a href="/inspections/54321">View Inspections</a></span>
            </div>
        </div>
    </div>
</body>
</html>
"""

# Full HTML content for an inspections page
INSPECTIONS_HTML = """
<!DOCTYPE html>
<html>
<body>
    <div class="resultsListRow">
        <div class="resultsListColumn"><span>Date:</span> 06/01/2023</div>
        <div class="resultsListColumn"><span>Type:</span> Annual</div>
        <div class="resultsListColumn"><span></span>Non-Compliant</div>
        <div class="resultsListColumn"><span id="statusDescription">Corrected</span></div>
        <div class="resultsListColumn"><span>Status Updated:</span> 06/15/2023</div>
        <div class="resultsListColumn"><a href="/report/1">View Report</a></div>
    </div>
    <div class="resultsListRow">
        <div class="resultsListColumn"><span>Date:</span> 03/01/2023</div>
        <div class="resultsListColumn"><span>Type:</span> Complaint</div>
        <div class="resultsListColumn"><span></span>Compliant</div>
        <div class="resultsListColumn"><span id="statusDescription"></span></div>
        <div class="resultsListColumn"><span>Status Updated:</span> 03/01/2023</div>
        <div class="resultsListColumn"><a href="/report/2">View Report</a></div>
    </div>
</body>
</html>
"""

@pytest.fixture
def spider():
    return OhdcySpider()

def test_parse_provider_page_happy_path(spider):
    response = HtmlResponse(url="http://example.com/provider/12345", body=PROVIDER_HTML, encoding='utf-8')
    
    # The spider yields a Request for inspections, so we need to capture that
    requests = list(spider.parse_provider_page(response))
    
    # There should be one request for inspections and one provider item
    assert len(requests) == 2
    
    provider_item_request = next(r for r in requests if isinstance(r, ProviderItem))
    inspection_request = next(r for r in requests if not isinstance(r, ProviderItem))

    provider = inspection_request.meta['provider']
    
    assert provider['status'] == 'Active'
    assert provider['sutq_rating'] == 'Star 5'
    assert provider['license_number'] == '12345'
    assert provider['county'] == 'Franklin'
    assert provider['license_begin_date'] == '01/01/2020'
    assert provider['license_expiration'] == '12/31/2025'
    assert provider['administrator'] == 'John Doe'
    assert provider['email'] == 'test@example.com'
    assert provider['phone'] == 'View Inspections'
    assert inspection_request.url == "https://childcaresearch.ohio.gov/inspections/12345"

def test_parse_provider_page_missing_fields(spider):
    response = HtmlResponse(url="http://example.com/provider/54321", body=PROVIDER_HTML_MISSING, encoding='utf-8')
    
    requests = list(spider.parse_provider_page(response))
    
    assert len(requests) == 2

    provider_item_request = next(r for r in requests if isinstance(r, ProviderItem))
    inspection_request = next(r for r in requests if not isinstance(r, ProviderItem))

    provider = inspection_request.meta['provider']
    
    assert provider['status'] == 'Active'
    assert provider.get('sutq_rating') is None
    assert provider['license_number'] == '54321'
    assert provider.get('county') is None
    assert provider.get('license_begin_date') is None
    assert provider.get('license_expiration') is None
    assert provider.get('administrator') is None
    assert provider.get('email') is None
    assert provider.get('phone') == 'View Inspections'
    assert inspection_request.url == "https://childcaresearch.ohio.gov/inspections/54321"

def test_parse_inspections(spider):
    provider = ProviderItem(license_number='12345')
    request = Request(url="http://example.com/inspections/12345", meta={'provider': provider})
    response = HtmlResponse(
        url="http://example.com/inspections/12345",
        body=INSPECTIONS_HTML,
        encoding='utf-8',
        request=request
    )
    
    # The spider yields a single item, which is the provider with inspections
    result_provider = next(spider.parse_inspections(response))
    
    assert 'inspections' in result_provider
    inspections = result_provider['inspections']
    
    assert len(inspections) == 2
    
    assert inspections[0]['date'] == '06/01/2023'
    assert inspections[0]['type'] == 'Annual'
    assert inspections[0]['original_status'] == ''
    assert inspections[0]['corrective_status'] == 'Corrected'
    assert inspections[0]['status_updated'] == '06/15/2023'
    assert inspections[0]['report_url'] == '/report/1'
    
    assert inspections[1]['date'] == '03/01/2023'
    assert inspections[1]['type'] == 'Complaint'
    assert inspections[1]['original_status'] == ''
    assert inspections[1]['corrective_status'] == ''
    assert inspections[1]['status_updated'] == '03/01/2023'
    assert inspections[1]['report_url'] == '/report/2'

import pytest
import os
import asyncio
from scrapy.http import HtmlResponse, Request
from provider_scrape.spiders.montana import MontanaSpider

@pytest.fixture
def spider():
    return MontanaSpider()

def test_parse_detail_page_golden_path(spider):
    # Mock the necessary HTML structure
    html_content = """
    <html>
    <body>
        <div>
            <span>Provider Name</span>
            <div>2 Grandma's House</div>
        </div>
        <div>
            <span>Provider Number</span>
            <div>PV109736</div>
        </div>
        <div>
            <span>Capacity</span>
            <div>85</div>
        </div>
        <div>
            <span>License Status</span>
            <div>Active</div>
        </div>
        <div>
            <span>Provider Type</span>
            <div>Child Care Center</div>
        </div>
        <div>
            <span>Min Age to Max Age</span>
            <div>0 to 12</div>
        </div>
        <table class="slds-table">
            <tr>
                <th>Date</th>
                <th>Type</th>
                <th>Inspector</th>
                <th>File</th>
            </tr>
            <tr>
                <td>6/24/2025</td>
                <td>Complaint</td>
                <td></td>
                <td><a href="/report">View File</a></td>
            </tr>
        </table>
    </body>
    </html>
    """

    request = Request(
        url="https://mtdphhs.my.site.com/MAQCSChildCareLicensing/s/provider-detail?language=en_US&pid=123", 
        meta={"latitude": "46.6288", "longitude": "-111.9196"}
    )
    response = HtmlResponse(url=request.url, body=html_content, encoding='utf-8', request=request)

    async def get_items():
        return [item async for item in spider.parse_detail_page(response)]
        
    items = asyncio.run(get_items())
    assert len(items) == 1
    
    item = items[0]
    assert item['source_state'] == 'Montana'
    assert item['provider_url'] == request.url
    assert item['latitude'] == "46.6288"
    assert item['longitude'] == "-111.9196"
    assert item['provider_name'] == "2 Grandma's House"
    assert item['license_number'] == "PV109736"
    assert item['capacity'] == "85"
    assert item['status'] == "Active"
    assert item['mt_license_type'] == "Child Care Center"
    assert item['provider_type'] == "Child Care Center"
    assert item['ages_served'] == "0 to 12"
    
    # Check inspections
    assert 'inspections' in item
    assert len(item['inspections']) > 0
    
    first_inspection = item['inspections'][0]
    assert first_inspection['date'] == "6/24/2025"
    assert first_inspection['type'] == "Complaint"
    assert first_inspection['mt_inspector_name'] == ""

def test_parse_detail_page_missing_data(spider):
    html_content = "<html><body><div>No data here</div></body></html>"
    request = Request(url="https://mtdphhs.my.site.com/test")
    response = HtmlResponse(url=request.url, body=html_content, encoding='utf-8', request=request)
    
    async def get_items():
        return [item async for item in spider.parse_detail_page(response)]
        
    items = asyncio.run(get_items())
    assert len(items) == 1
    
    item = items[0]
    assert item['source_state'] == 'Montana'
    assert item['provider_name'] is None
    assert item['license_number'] is None
    assert item['capacity'] is None
    assert len(item['inspections']) == 0

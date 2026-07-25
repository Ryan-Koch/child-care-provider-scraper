import pytest
import os
import asyncio
import scrapy
from scrapy.http import HtmlResponse, Request
from provider_scrape.spiders.montana import MontanaSpider

@pytest.fixture
def spider():
    return MontanaSpider()


# Real card markup as served by the site (current plain-<button> layout).
CURRENT_CARD_HTML = """
<article class="provider-card">
  <div class="slds-card__body slds-card__body_inner">
    <div class="card-header slds-m-bottom_small">
      <h3 class="provider-name slds-text-heading_small slds-truncate"
          title="Ashley McCray / Little Squirts Childcare">
        Ashley McCray / Little Squirts Childcare - PV108880
      </h3>
    </div>
    <span class="provider-address">Gallatin, Belgrade, 59714</span>
    <div class="card-actions slds-m-top_small">
      <button class="slds-button slds-button_outline-brand slds-button_stretch"
              data-lat="45.763770000000000" data-lon="-111.169227000000000"
              data-address="Gallatin, Belgrade, 59714">Get Directions</button>
      <button class="slds-button slds-button_brand slds-button_stretch"
              data-pid="aef28ce5e199805a4211bc44a1b368ddb5556d953a5085ad1c1fcc3200c637c0f72bbf16b91af94d2ab1b432d9316ead">View Details</button>
    </div>
  </div>
</article>
"""

# Legacy card markup (single <lightning-button> carrying all three data attrs).
LEGACY_CARD_HTML = """
<article class="provider-card">
  <lightning-button data-pid="legacypid123" data-lat="46.6288" data-lon="-111.9196">
    View Details
  </lightning-button>
</article>
"""


def test_extract_card_fields_current_markup():
    card = scrapy.Selector(text=CURRENT_CARD_HTML).css("article.provider-card")[0]
    pid, lat, lon = MontanaSpider.extract_card_fields(card)
    assert pid == "aef28ce5e199805a4211bc44a1b368ddb5556d953a5085ad1c1fcc3200c637c0f72bbf16b91af94d2ab1b432d9316ead"
    assert lat == "45.763770000000000"
    assert lon == "-111.169227000000000"


def test_extract_card_fields_legacy_markup():
    card = scrapy.Selector(text=LEGACY_CARD_HTML).css("article.provider-card")[0]
    pid, lat, lon = MontanaSpider.extract_card_fields(card)
    assert pid == "legacypid123"
    assert lat == "46.6288"
    assert lon == "-111.9196"


# Real inspection-table markup as served by the detail page. The "View File"
# control is a <lightning-button data-documentitemid> wrapping a plain <button>
# -- no <a href> -- so report_url is expected to stay unset while the three
# core fields still extract cleanly.
INSPECTION_DETAIL_HTML = """
<html><body>
  <div><span>Provider Name</span><div>Little Squirts Childcare</div></div>
  <table class="slds-table slds-no-row-hover table-with-accordion slds-table_bordered">
    <thead><tr class="table-heading-row">
      <th><div class="slds-truncate" title="Inspection Date">Inspection Date</div></th>
      <th><div class="slds-truncate" title="Inspection Type">Inspection Type</div></th>
      <th><div class="slds-truncate" title="Inspector Name">Inspector Name</div></th>
      <th><div class="slds-truncate" title="Action">Action</div></th>
    </tr></thead>
    <tbody>
      <tr class="table-body-row">
        <td data-label="Inspection Date"><div class="slds-truncate" title="9/16/2025"><span>9/16/2025</span></div></td>
        <td data-label="Inspection Type"><div class="slds-truncate" title="Renewal Inspection"><span>Renewal Inspection</span></div></td>
        <td data-label="Inspector Name"><div class="slds-truncate" title="Teri Whitesitt"><span>Teri Whitesitt</span></div></td>
        <td data-label="Action"><lightning-button class="base-table-btn" data-documentitemid="a0acr00000XcenTAAR"><button class="slds-button" title="View File">View File</button></lightning-button></td>
      </tr>
      <tr class="table-body-row">
        <td data-label="Inspection Date"><div class="slds-truncate" title="9/9/2024"><span>9/9/2024</span></div></td>
        <td data-label="Inspection Type"><div class="slds-truncate" title="Renewal Inspection"><span>Renewal Inspection</span></div></td>
        <td data-label="Inspector Name"><div class="slds-truncate" title="Teri Whitesitt"><span>Teri Whitesitt</span></div></td>
        <td data-label="Action"><lightning-button class="base-table-btn" data-documentitemid="a0acr00000XcdisAAB"><button class="slds-button" title="View File">View File</button></lightning-button></td>
      </tr>
    </tbody>
  </table>
</body></html>
"""


def test_parse_detail_page_inspection_table(spider):
    request = Request(url="https://mtdphhs.my.site.com/MAQCSChildCareLicensing/s/provider-detail?language=en_US&pid=abc")
    response = HtmlResponse(url=request.url, body=INSPECTION_DETAIL_HTML, encoding="utf-8", request=request)

    async def get_items():
        return [item async for item in spider.parse_detail_page(response)]

    item = asyncio.run(get_items())[0]
    inspections = item["inspections"]
    assert len(inspections) == 2

    first = inspections[0]
    assert first["date"] == "9/16/2025"
    assert first["type"] == "Renewal Inspection"
    assert first["mt_inspector_name"] == "Teri Whitesitt"
    # No <a href> anymore -> report_url is left unset, not a broken value.
    assert first.get("report_url") is None

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

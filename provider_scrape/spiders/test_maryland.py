import pytest
import scrapy
from scrapy.http import HtmlResponse, Request, TextResponse
from provider_scrape.spiders.maryland import MarylandSpider, extract_address_from_pdf
from provider_scrape.items import ProviderItem
from unittest.mock import patch


@pytest.fixture
def spider():
    return MarylandSpider()


DETAIL_HTML = """
<html>
<body>
<div class="programdetail">
    <ul class="detailBox">
        <li class="detailRow">
            <label for="MainContent_txtProviderName" id="MainContent_lblProviderName" class="labelForm">Provider Name:</label>
            <span id="MainContent_txtProviderName" class="detailText">7 Day Kiddie Kare Inc.</span>
        </li>
        <li class="detailRow">
            <label for="MainContent_txtLicense" id="MainContent_lblLicense" class="labelForm">License #:</label>
            <span id="MainContent_txtLicense" class="detailText">150301</span>
        </li>
        <li class="detailRow">
            <label class="labelForm" for="txtCounty">County:</label>
            <span id="MainContent_txtCounty" class="detailText">Baltimore City</span>
        </li>
        <li class="detailRow">
            <label class="labelForm" for="txtProviderStatus">Provider Status:</label>
            <span id="MainContent_txtProviderStatus" class="detailText">Continuing - Full</span>
        </li>
        <li class="detailRow">
            <label class="labelForm" for="txtCapacity">Capacity:</label>
            <span id="MainContent_txtCapacity" class="detailText">53<br>6 weeks through 17 months<br>18 months through 23 months<br>2 years<br></span>
        </li>
        <li class="detailRow">
            <label class="labelForm" for="txtApprovedEducationProgram">Approved Education Program:</label>
            <span id="MainContent_txtApprovedEducationProgram" class="detailText">No</span>
        </li>
        <li class="detailRow">
            <label class="labelForm" for="txtAccreditation">Accreditation:</label>
            <span id="MainContent_txtAccreditation" class="detailText">NA</span>
        </li>
    </ul>
    <ul class="detailBox">
        <li class="detailRow">
            <label class="labelForm" for="txtPhone">Phone:</label>
            <span id="MainContent_txtPhone" class="detailText">(410) 539-7329</span>
        </li>
        <li class="detailRow">
            <label class="labelForm" for="txtEmail">E-mail:</label>
            <span id="MainContent_txtEmail" class="detailText">Dbearlylearning@gmail.com</span>
        </li>
        <li class="detailRow">
            <label class="labelForm" for="txtHours">Approved For:</label>
            <span id="MainContent_txtHours" class="detailText">Sunday - Saturday<br>6:00 AM - 12:20 AM<br>January - December<br></span>
        </li>
        <li class="detailRow">
            <label for="MainContent_txtFatalities" id="MainContent_lblFatalities" class="labelForm">Fatalities:</label>
            <span id="MainContent_txtFatalities" class="detailText">0</span>
        </li>
        <li class="detailRow">
            <label for="MainContent_txtInjuries" id="MainContent_lblInjuries" class="labelForm">Serious Injuries:</label>
            <span id="MainContent_txtInjuries" class="detailText">0</span>
        </li>
        <li class="detailRow">
            <label class="labelForm" for="txtEXCELSLevel">Level:</label>
            <span id="MainContent_txtEXCELSLevel" class="detailLevelText">4</span>
        </li>
    </ul>
</div>

<table class="listViewGrid" cellspacing="0" cellpadding="4" rules="all" border="1" id="MainContent_grdInspection" style="width:100%;border-collapse:collapse;">
    <tr>
        <th scope="col">Inspection Report</th>
        <th scope="col">Summary Of Correction</th>
        <th scope="col">Date</th>
        <th scope="col">Inspection Type</th>
        <th scope="col">Regulations</th>
        <th scope="col">Finding</th>
        <th scope="col">Summary of Findings Status</th>
    </tr>
    <tr class="rowStyle">
        <td><a href="PublicReports/PrintTask.aspx?t=526&amp;d=3384">Inspection Report</a></td>
        <td><a href="PublicReports/PrintTask.aspx?t=543&amp;d=3384">Summary Of Correction</a></td>
        <td align="center">12/01/2025</td>
        <td align="center">Mandatory Review</td>
        <td>&nbsp;</td>
        <td>No Noncompliances Found</td>
        <td align="center">&nbsp;</td>
    </tr>
    <tr class="rowStyle" style="background-color:White;">
        <td><a href="PublicReports/PrintTask.aspx?t=345&amp;d=3384">Inspection Report</a></td>
        <td><a href="PublicReports/PrintTask.aspx?t=350&amp;d=3384">Summary Of Correction</a></td>
        <td align="center">04/04/2025</td>
        <td align="center">Complaint</td>
        <td>13A.16.07.03A(1)</td>
        <td>A child was picked up abruptly and placed down on the floor.</td>
        <td align="center">Corrected</td>
    </tr>
    <tr class="rowStyle">
        <td><a href="PublicReports/PrintTask.aspx?t=270&amp;d=3384">Inspection Report</a></td>
        <td><a href="PublicReports/PrintTask.aspx?t=279&amp;d=3384">Summary Of Correction</a></td>
        <td align="center">12/12/2024</td>
        <td align="center">Full</td>
        <td>&nbsp;</td>
        <td>No Noncompliances Found</td>
        <td align="center">&nbsp;</td>
    </tr>
</table>
</body>
</html>
"""

RESULTS_HTML = """
<html>
<body>
<form id="ctl01" action="/" method="post">
<input type="hidden" name="__VIEWSTATE" value="abc123" />
<input type="hidden" name="__VIEWSTATEGENERATOR" value="ABCD" />
<input type="hidden" name="__EVENTVALIDATION" value="xyz789" />
<span id="MainContent_lblTotalRows">Providers = 8476</span>
<table class="listViewGrid" cellspacing="0" cellpadding="4" rules="cols" border="1" id="grdResults" style="width:100%;border-collapse:collapse;">
    <tr>
        <th scope="col">Provider Name</th>
        <th scope="col">Facility Name</th>
        <th scope="col">Address</th>
        <th scope="col">County</th>
        <th scope="col">School Name</th>
        <th scope="col">Program Type</th>
        <th scope="col">License Status</th>
    </tr>
    <tr class="rowStyle">
        <td><a href="FacilityDetail.aspx?ft=&amp;fn=&amp;sn=&amp;z=&amp;c=&amp;co=&amp;lc=&amp;fi=463466"> Caliday Before and Aftercare of Franklin </a></td>
        <td>&nbsp;</td>
        <td> Cockeys Mill Road, Reisterstown, MD 21136</td>
        <td><span id="lblItem">Baltimore County</span></td>
        <td>&nbsp;</td>
        <td><span id="lblItem">CTR</span></td>
        <td><span id="lblItem" title="An active provider.">Open</span></td>
    </tr>
    <tr class="rowStyle" style="background-color:White;">
        <td><a href="FacilityDetail.aspx?ft=&amp;fn=&amp;sn=&amp;z=&amp;c=&amp;co=&amp;lc=&amp;fi=134978">7 Day Kiddie Kare Inc.</a></td>
        <td>&nbsp;</td>
        <td> N Howard Street, Baltimore, MD 21201</td>
        <td><span id="lblItem">Baltimore City</span></td>
        <td><span id="lblItem">Lincoln Elementary</span></td>
        <td><span id="lblItem">CTR</span></td>
        <td><span id="lblItem" title="An active provider.">Open</span></td>
    </tr>
    <tr class="dataPager" align="center" style="text-decoration:none;width:100%;">
        <td colspan="7"><table><tr>
            <td><span>1</span></td>
            <td><a href="javascript:__doPostBack('ctl00$MainContent$grdResults','Page$2')">2</a></td>
            <td><a href="javascript:__doPostBack('ctl00$MainContent$grdResults','Page$3')">3</a></td>
        </tr></table></td>
    </tr>
</table>
</form>
</body>
</html>
"""


def test_parse_detail_page_chains_to_pdf(spider):
    """Test that parse_detail yields a PDF request when inspections exist."""
    request = Request(
        url="https://www.checkccmd.org/FacilityDetail.aspx?ft=&fn=&sn=&z=&c=&co=&lc=&fi=134978",
    )
    response = HtmlResponse(
        url=request.url, body=DETAIL_HTML, encoding="utf-8", request=request
    )

    results = list(spider.parse_detail(
        response,
        address="N Howard Street, Baltimore, MD 21201",
        school_name="Lincoln Elementary",
        program_type="CTR",
    ))

    # Should yield a Request for the first inspection report PDF
    assert len(results) == 1
    pdf_request = results[0]
    assert isinstance(pdf_request, scrapy.Request)
    assert "PrintTask.aspx?t=526&d=3384" in pdf_request.url

    # The item should be passed through cb_kwargs
    item = pdf_request.cb_kwargs["item"]
    assert item["provider_name"] == "7 Day Kiddie Kare Inc."
    assert item["license_number"] == "150301"
    assert item["county"] == "Baltimore City"
    assert item["status"] == "Continuing - Full"
    assert item["phone"] == "(410) 539-7329"
    assert item["email"] == "Dbearlylearning@gmail.com"
    assert item["capacity"] == "53"
    assert "6 weeks through 17 months" in item["ages_served"]
    assert "18 months through 23 months" in item["ages_served"]
    assert item["md_approved_education"] == "No"
    assert item["md_accreditation"] == "NA"
    assert item["md_fatalities"] == "0"
    assert item["md_serious_injuries"] == "0"
    assert item["md_excels_level"] == "4"
    assert "Sunday - Saturday" in item["hours"]

    # Results-page fields should be set (will be overwritten by OCR if successful)
    assert item["address"] == "N Howard Street, Baltimore, MD 21201"
    assert item["md_school_name"] == "Lincoln Elementary"
    assert item["provider_type"] == "CTR"


def test_parse_detail_page_inspections(spider):
    """Test that inspection data is correctly extracted and passed in the item."""
    request = Request(
        url="https://www.checkccmd.org/FacilityDetail.aspx?fi=134978",
    )
    response = HtmlResponse(
        url=request.url, body=DETAIL_HTML, encoding="utf-8", request=request
    )

    results = list(spider.parse_detail(response))
    # With inspections, parse_detail yields a Request (for PDF), not an item
    pdf_request = results[0]
    item = pdf_request.cb_kwargs["item"]

    assert len(item["inspections"]) == 3

    insp1 = item["inspections"][0]
    assert insp1["date"] == "12/01/2025"
    assert insp1["type"] == "Mandatory Review"
    assert insp1["report_url"] == "https://www.checkccmd.org/PublicReports/PrintTask.aspx?t=526&d=3384"
    assert insp1["md_finding"] == "No Noncompliances Found"
    assert insp1["md_regulation"] is None
    assert insp1["md_inspection_status"] is None

    insp2 = item["inspections"][1]
    assert insp2["date"] == "04/04/2025"
    assert insp2["type"] == "Complaint"
    assert insp2["md_regulation"] == "13A.16.07.03A(1)"
    assert "picked up abruptly" in insp2["md_finding"]
    assert insp2["md_inspection_status"] == "Corrected"

    insp3 = item["inspections"][2]
    assert insp3["date"] == "12/12/2024"
    assert insp3["type"] == "Full"


def test_parse_results_page(spider):
    """Test extracting provider links, row data, and pagination from the results page."""
    request = Request(url="https://www.checkccmd.org/SearchResults.aspx")
    response = HtmlResponse(
        url=request.url, body=RESULTS_HTML, encoding="utf-8", request=request
    )

    spider.requested_pages_by_county["TestCounty"] = set()
    results = list(spider.parse_results(response, county_key="TestCounty"))

    detail_requests = [r for r in results if not isinstance(r, scrapy.FormRequest)]
    form_requests = [r for r in results if isinstance(r, scrapy.FormRequest)]

    assert len(detail_requests) == 2
    assert "fi=463466" in detail_requests[0].url
    assert "fi=134978" in detail_requests[1].url

    kwargs0 = detail_requests[0].cb_kwargs
    assert kwargs0["address"] == "Cockeys Mill Road, Reisterstown, MD 21136"
    assert kwargs0["school_name"] is None
    assert kwargs0["program_type"] == "CTR"

    kwargs1 = detail_requests[1].cb_kwargs
    assert kwargs1["address"] == "N Howard Street, Baltimore, MD 21201"
    assert kwargs1["school_name"] == "Lincoln Elementary"
    assert kwargs1["program_type"] == "CTR"

    assert len(form_requests) == 1


def test_parse_results_deduplicates_on_reprocess(spider):
    """Test that reprocessing the same results page skips duplicate detail/pagination."""
    request = Request(url="https://www.checkccmd.org/SearchResults.aspx")
    response = HtmlResponse(
        url=request.url, body=RESULTS_HTML, encoding="utf-8", request=request
    )

    spider.requested_pages_by_county["TestCounty"] = set()

    # First call processes normally
    results1 = list(spider.parse_results(response, county_key="TestCounty"))
    assert len(results1) == 3  # 2 detail + 1 pagination

    # Second call: detail requests are skipped (seen fi), pagination is skipped (requested)
    results2 = list(spider.parse_results(response, county_key="TestCounty"))
    assert len(results2) == 0


def test_parse_detail_page_missing_data(spider):
    """Test handling a detail page with no provider data yields item directly."""
    html_content = "<html><body><div>No data here</div></body></html>"
    request = Request(url="https://www.checkccmd.org/FacilityDetail.aspx?fi=999999")
    response = HtmlResponse(
        url=request.url, body=html_content, encoding="utf-8", request=request
    )

    results = list(spider.parse_detail(response))
    # No inspections, so yields item directly (not a Request)
    assert len(results) == 1
    item = results[0]
    assert isinstance(item, ProviderItem)

    assert item["source_state"] == "Maryland"
    assert item["provider_name"] is None
    assert item["license_number"] is None
    assert item["county"] is None
    assert item["status"] is None
    assert len(item["inspections"]) == 0


def test_parse_detail_page_no_inspections(spider):
    """Test detail page with provider info but no inspection table yields item directly."""
    html_content = """
    <html><body>
    <span id="MainContent_txtProviderName" class="detailText">Test Provider</span>
    <span id="MainContent_txtLicense" class="detailText">999999</span>
    <span id="MainContent_txtCounty" class="detailText">Montgomery County</span>
    <span id="MainContent_txtProviderStatus" class="detailText">Continuing - Full</span>
    <span id="MainContent_txtPhone" class="detailText">(301) 555-1234</span>
    <span id="MainContent_txtEmail" class="detailText">test@example.com</span>
    <span id="MainContent_txtCapacity" class="detailText">25</span>
    <span id="MainContent_txtApprovedEducationProgram" class="detailText">Yes</span>
    <span id="MainContent_txtAccreditation" class="detailText">NAEYC</span>
    <span id="MainContent_txtHours" class="detailText">Monday - Friday<br>7:00 AM - 6:00 PM<br></span>
    <span id="MainContent_txtFatalities" class="detailText">0</span>
    <span id="MainContent_txtInjuries" class="detailText">1</span>
    <span id="MainContent_txtEXCELSLevel" class="detailLevelText">5</span>
    </body></html>
    """
    request = Request(url="https://www.checkccmd.org/FacilityDetail.aspx?fi=999999")
    response = HtmlResponse(
        url=request.url, body=html_content, encoding="utf-8", request=request
    )

    results = list(spider.parse_detail(response))
    assert len(results) == 1
    item = results[0]
    assert isinstance(item, ProviderItem)

    assert item["provider_name"] == "Test Provider"
    assert item["license_number"] == "999999"
    assert item["county"] == "Montgomery County"
    assert item["capacity"] == "25"
    assert item["md_approved_education"] == "Yes"
    assert item["md_accreditation"] == "NAEYC"
    assert item["md_serious_injuries"] == "1"
    assert item["md_excels_level"] == "5"
    assert "Monday - Friday" in item["hours"]
    assert len(item["inspections"]) == 0


NON_OPERATING_HTML = """
<html>
<body>
<div id="MainContent_PnlNonOperating">
    <span id="MainContent_txtProviderNameOp">Adrian McMillan</span>
    <span id="MainContent_txtFacilityNameOp"></span>
    <span id="MainContent_txtLicenseOp">570530</span>
    <span id="MainContent_txtProviderStatusOp">Suspended - Emergency</span>
    <span id="MainContent_txtApprovedEducationProgramOp">No</span>
    <span id="MainContent_txtAccreditationOp">NA</span>
    <span id="MainContent_txtEXCELSLevelOp"></span>
</div>
<table id="MainContent_grdInspection">
    <tr><th>Inspection Report</th><th>Summary</th><th>Date</th><th>Type</th><th>Reg</th><th>Finding</th><th>Status</th></tr>
</table>
</body>
</html>
"""


def test_parse_detail_non_operating(spider):
    """Test that closed/suspended providers using the non-operating panel are parsed."""
    request = Request(
        url="https://www.checkccmd.org/FacilityDetail.aspx?fi=570530",
    )
    response = HtmlResponse(
        url=request.url, body=NON_OPERATING_HTML, encoding="utf-8", request=request
    )

    results = list(spider.parse_detail(
        response,
        address="Harmans Road, Hanover, MD 21076",
        program_type="FCCH",
    ))

    assert len(results) == 1
    item = results[0]
    assert isinstance(item, ProviderItem)
    assert item["provider_name"] == "Adrian McMillan"
    assert item["license_number"] == "570530"
    assert item["status"] == "Suspended - Emergency"
    assert item["md_approved_education"] == "No"
    assert item["md_accreditation"] == "NA"
    assert item["address"] == "Harmans Road, Hanover, MD 21076"
    assert item["provider_type"] == "FCCH"
    assert item["source_state"] == "Maryland"
    assert len(item["inspections"]) == 0


def test_parse_detail_skips_non_detail_url(spider):
    """Test that parse_detail skips responses that aren't detail pages."""
    html_content = "<html><body>Search form</body></html>"
    request = Request(url="https://www.checkccmd.org/")
    response = HtmlResponse(
        url=request.url, body=html_content, encoding="utf-8", request=request
    )

    items = list(spider.parse_detail(response))
    assert len(items) == 0


@pytest.mark.asyncio
async def test_parse_inspection_pdf_updates_address(spider):
    """Test that parse_inspection_pdf updates the item address from OCR."""
    item = ProviderItem()
    item["provider_name"] = "Test Provider"
    item["address"] = "Howard Street, Baltimore, MD 21201"

    with patch(
        "provider_scrape.spiders.maryland.extract_address_from_pdf",
        return_value="325 N Howard Street, Baltimore, MD 21201",
    ):
        request = Request(url="https://www.checkccmd.org/PublicReports/PrintTask.aspx?t=526&d=3384")
        response = HtmlResponse(
            url=request.url, body=b"fake pdf bytes", encoding="utf-8", request=request
        )
        results = [item async for item in spider.parse_inspection_pdf(response, item=item)]

    assert len(results) == 1
    assert results[0]["address"] == "325 N Howard Street, Baltimore, MD 21201"


@pytest.mark.asyncio
async def test_parse_inspection_pdf_keeps_fallback_on_ocr_failure(spider):
    """Test that the results-page address is preserved if OCR fails."""
    item = ProviderItem()
    item["provider_name"] = "Test Provider"
    item["address"] = "Howard Street, Baltimore, MD 21201"

    with patch(
        "provider_scrape.spiders.maryland.extract_address_from_pdf",
        return_value=None,
    ):
        request = Request(url="https://www.checkccmd.org/PublicReports/PrintTask.aspx?t=526&d=3384")
        response = HtmlResponse(
            url=request.url, body=b"bad pdf", encoding="utf-8", request=request
        )
        results = [item async for item in spider.parse_inspection_pdf(response, item=item)]

    assert len(results) == 1
    assert results[0]["address"] == "Howard Street, Baltimore, MD 21201"

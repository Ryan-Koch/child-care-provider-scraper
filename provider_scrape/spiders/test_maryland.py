import json

import pytest
import scrapy
from scrapy.http import HtmlResponse, Request, TextResponse
from provider_scrape.spiders.maryland import (
    MarylandSpider,
    extract_address_from_pdf,
    MAX_NAV_ATTEMPTS,
    MAX_CHAIN_RESTARTS,
    MAX_DETAIL_REPRIMES,
    STALL_ALERT_WINDOWS,
    RESULTS_PRIORITY,
    DETAIL_DOWNLOAD_TIMEOUT,
    EXCELS_DOWNLOAD_TIMEOUT,
    DETAIL_COOKIEJAR,
    SEARCH_RESULTS_REFERER,
)
from types import SimpleNamespace
from twisted.python.failure import Failure
from provider_scrape.items import ProviderItem
from unittest.mock import patch


@pytest.fixture
def spider():
    # proxies="off" pins single-IP mode so tests are deterministic regardless of
    # whether a webshare.env happens to exist at the repo root.
    return MarylandSpider(proxies="off")


def _excels_response(data, license_number="150301"):
    """Build a TextResponse mimicking the EXCELS search?license= endpoint."""
    request = Request(
        url=f"https://findaprogram.marylandexcels.org/api/fap/search?license={license_number}"
    )
    body = json.dumps({"statusCode": 200, "data": data})
    return TextResponse(
        url=request.url, body=body, encoding="utf-8", request=request
    )


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


def test_parse_detail_page_chains_to_excels(spider):
    """parse_detail yields an EXCELS enrichment request keyed by license."""
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

    # Should yield a Request to the EXCELS API for the provider's license
    assert len(results) == 1
    excels_request = results[0]
    assert isinstance(excels_request, scrapy.Request)
    assert "findaprogram.marylandexcels.org" in excels_request.url
    assert "license=150301" in excels_request.url
    assert excels_request.callback == spider.parse_excels
    # EXCELS is a lightweight JSON API off the critical path — fail fast, don't
    # inherit the 180s pagination default.
    assert excels_request.meta["download_timeout"] == EXCELS_DOWNLOAD_TIMEOUT
    # The first inspection report URL is carried for the PDF fallback path.
    assert "PrintTask.aspx?t=526&d=3384" in excels_request.cb_kwargs["first_report_url"]

    # The item should be passed through cb_kwargs
    item = excels_request.cb_kwargs["item"]
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
    # parse_detail now yields an EXCELS enrichment Request; the item (with its
    # parsed inspections) rides along in cb_kwargs.
    enrich_request = results[0]
    item = enrich_request.cb_kwargs["item"]

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

    # Detail requests are idempotent and not chain-critical, so they fail fast on
    # a shorter per-request timeout (not the patient 180s default) and retry.
    for dr in detail_requests:
        assert dr.meta["download_timeout"] == DETAIL_DOWNLOAD_TIMEOUT

    # One pagination request to page 2, carrying its expected page and a high
    # priority so it isn't starved behind the detail-request backlog.
    assert len(form_requests) == 1
    assert form_requests[0].cb_kwargs["expected_page"] == 2
    assert form_requests[0].priority == RESULTS_PRIORITY
    assert form_requests[0].errback == spider._pagination_errback
    assert "Page%242" in form_requests[0].body.decode() or "Page$2" in form_requests[0].body.decode()
    # Pagination postbacks are chain-critical: they must NOT get the fail-fast
    # timeout — they keep the patient default so a stale-ViewState truncation
    # can't be reintroduced.
    assert "download_timeout" not in form_requests[0].meta


def test_parse_results_deduplicates_on_reprocess(spider):
    """Test that reprocessing the same results page skips duplicate detail/pagination."""
    request = Request(url="https://www.checkccmd.org/SearchResults.aspx")
    response = HtmlResponse(
        url=request.url, body=RESULTS_HTML, encoding="utf-8", request=request
    )

    # First call processes normally (page 1)
    results1 = list(spider.parse_results(response, county_key="TestCounty"))
    assert len(results1) == 3  # 2 detail + 1 pagination

    # Second delivery of the same page: rows already parsed, so nothing is
    # re-extracted and the chain isn't re-driven from it.
    results2 = list(spider.parse_results(response, county_key="TestCounty"))
    assert len(results2) == 0


def test_parse_results_self_heals_stale_postback(spider):
    """A response for the wrong page re-issues navigation toward the wanted page.

    Simulates a paginated postback that timed out and came back rendering page 1
    when page 2 was requested. The spider should NOT extract page 1's rows again
    and should re-navigate to page 2 instead of silently dropping the chain.
    """
    request = Request(url="https://www.checkccmd.org/SearchResults.aspx")
    response = HtmlResponse(  # RESULTS_HTML renders as page 1 (pager span = "1")
        url=request.url, body=RESULTS_HTML, encoding="utf-8", request=request
    )

    results = list(
        spider.parse_results(response, county_key="TestCounty", expected_page=2)
    )

    # Exactly one re-navigation request, no detail items, page 1 not parsed.
    assert len(results) == 1
    renav = results[0]
    assert isinstance(renav, scrapy.FormRequest)
    assert renav.cb_kwargs["expected_page"] == 2
    assert renav.priority == RESULTS_PRIORITY
    assert 1 not in spider.parsed_pages_by_county.get("TestCounty", set())


def test_parse_results_tracks_declared_and_found_counts(spider):
    """parse_results records the declared total and tallies rows found per county."""
    request = Request(url="https://www.checkccmd.org/SearchResults.aspx")
    response = HtmlResponse(
        url=request.url, body=RESULTS_HTML, encoding="utf-8", request=request
    )

    list(spider.parse_results(response, county_key="TestCounty"))

    # RESULTS_HTML declares "Providers = 8476" and renders 2 provider rows.
    assert spider.declared_total_by_county["TestCounty"] == 8476
    assert spider.found_count_by_county["TestCounty"] == 2


def test_closed_reports_incomplete_county(spider, caplog):
    """A county short of its declared total is reported loudly at close."""
    spider.declared_total_by_county = {"Baltimore County": 1481, "Kent County": 35}
    spider.found_count_by_county = {"Baltimore County": 70, "Kent County": 35}

    with caplog.at_level("INFO"):
        spider.closed("finished")

    msgs = "\n".join(r.message for r in caplog.records)
    assert "Crawl INCOMPLETE" in msgs
    assert "[Baltimore County] 70/1481" in msgs
    assert "1411 missing" in msgs
    # The complete county is not flagged.
    assert "[Kent County]" not in msgs


def test_closed_reports_complete_crawl(spider, caplog):
    """When every county meets its declared total, close logs completion, not error."""
    spider.declared_total_by_county = {"Kent County": 35, "Garrett County": 44}
    spider.found_count_by_county = {"Kent County": 35, "Garrett County": 44}

    with caplog.at_level("INFO"):
        spider.closed("finished")

    msgs = "\n".join(r.message for r in caplog.records)
    assert "Crawl complete" in msgs
    assert "INCOMPLETE" not in msgs


def test_closed_reports_item_shortfall(spider, caplog):
    """Full pagination but too few scraped items is reported loudly at close.

    Guards the 2026-07 failure mode: every county paginated to its declared
    total, but the detail backlog stalled and most rows never became items. The
    paginated-row check alone would have called this a clean run.
    """
    spider.declared_total_by_county = {"Kent County": 35, "Garrett County": 44}
    spider.found_count_by_county = {"Kent County": 35, "Garrett County": 44}
    # Only 10 of the 79 declared providers actually became items.
    spider.crawler = SimpleNamespace(
        stats=SimpleNamespace(get_value=lambda key, default=None: 10)
    )

    with caplog.at_level("INFO"):
        spider.closed("finished")

    msgs = "\n".join(r.message for r in caplog.records)
    # Pagination looks complete, but the item-level check screams.
    assert "Crawl complete" in msgs
    assert "INCOMPLETE" in msgs
    assert "scraped 10 items" in msgs
    assert "69 missing" in msgs


def test_closed_no_item_shortfall_when_items_meet_declared(spider, caplog):
    """When scraped items meet the declared total, no item-shortfall error fires."""
    spider.declared_total_by_county = {"Kent County": 35, "Garrett County": 44}
    spider.found_count_by_county = {"Kent County": 35, "Garrett County": 44}
    spider.crawler = SimpleNamespace(
        stats=SimpleNamespace(get_value=lambda key, default=None: 80)
    )

    with caplog.at_level("INFO"):
        spider.closed("finished")

    msgs = "\n".join(r.message for r in caplog.records)
    assert "Crawl complete" in msgs
    assert "INCOMPLETE" not in msgs


def _stats_from(values):
    """A minimal stats stand-in returning fixed counter values."""
    return SimpleNamespace(get_value=lambda key, default=None: values.get(key, default))


def test_check_stall_fires_after_consecutive_no_progress_windows(spider, caplog):
    """A persistent stall (enough consecutive no-progress windows) is reported."""
    spider._pages_parsed = 50
    spider._stall_last_responses = 8000
    spider._stall_last_progress = 50 + 3351  # pages + items from the prior tick
    # One window short of the alert threshold; this call reaches it.
    spider._stall_windows = STALL_ALERT_WINDOWS - 1
    spider.crawler = SimpleNamespace(
        stats=_stats_from({"response_received_count": 8020, "item_scraped_count": 3351})
    )

    with caplog.at_level("ERROR"):
        spider._check_stall()

    assert "STALL" in "\n".join(r.message for r in caplog.records)


def test_check_stall_single_window_does_not_fire(spider, caplog):
    """A single no-progress window (e.g. the search ramp-up) must not cry wolf."""
    spider._pages_parsed = 0
    spider._stall_last_responses = 0
    spider._stall_last_progress = 0
    spider._stall_windows = 0
    spider.crawler = SimpleNamespace(
        stats=_stats_from({"response_received_count": 18, "item_scraped_count": 0})
    )

    with caplog.at_level("ERROR"):
        spider._check_stall()

    assert spider._stall_windows == 1
    assert "STALL" not in "\n".join(r.message for r in caplog.records)


def test_check_stall_resets_counter_on_progress(spider, caplog):
    """Any progress clears accumulated no-progress windows, so the alarm re-arms."""
    spider._pages_parsed = 60  # advanced since the prior tick
    spider._stall_last_responses = 8000
    spider._stall_last_progress = 50 + 3351
    spider._stall_windows = STALL_ALERT_WINDOWS - 1  # was nearly at threshold
    spider.crawler = SimpleNamespace(
        stats=_stats_from({"response_received_count": 8020, "item_scraped_count": 3351})
    )

    with caplog.at_level("ERROR"):
        spider._check_stall()

    assert spider._stall_windows == 0
    assert "STALL" not in "\n".join(r.message for r in caplog.records)


def test_check_stall_quiet_when_items_advance(spider, caplog):
    """Items advancing (detail-drain phase) is progress — no false stall alarm."""
    spider._pages_parsed = 50
    spider._stall_last_responses = 8000
    spider._stall_last_progress = 50 + 3351
    spider._stall_windows = STALL_ALERT_WINDOWS - 1
    spider.crawler = SimpleNamespace(
        stats=_stats_from({"response_received_count": 8020, "item_scraped_count": 3400})
    )

    with caplog.at_level("ERROR"):
        spider._check_stall()

    assert spider._stall_windows == 0
    assert "STALL" not in "\n".join(r.message for r in caplog.records)


def test_single_ip_mode_by_default_when_proxies_off():
    """`-a proxies=off` (and no env) means no pool — single-IP behavior."""
    s = MarylandSpider(proxies="off")
    assert s.proxy_pool is None
    assert s.proxy_pool_domains == ["checkccmd.org"]


def test_pool_built_from_inline_endpoints():
    """`-a proxies="h:p,h:p"` builds a pool without needing an env file."""
    s = MarylandSpider(
        proxies="1.1.1.1:80,2.2.2.2:81", proxy_env="/nonexistent.env"
    )
    assert s.proxy_pool is not None
    assert len(s.proxy_pool) == 2


def test_detail_requests_carry_no_proxy_affinity(spider):
    """Detail GETs rotate across the pool, so they must not pin to a proxy."""
    request = Request(url="https://www.checkccmd.org/SearchResults.aspx")
    response = HtmlResponse(
        url=request.url, body=RESULTS_HTML, encoding="utf-8", request=request
    )
    results = list(spider.parse_results(response, county_key="TestCounty"))
    detail_requests = [r for r in results if not isinstance(r, scrapy.FormRequest)]
    assert detail_requests
    for dr in detail_requests:
        assert "proxy_affinity" not in dr.meta


def test_pagination_request_pins_proxy_affinity_to_county(spider):
    """A county's pagination postback sticks to that county's proxy."""
    request = Request(url="https://www.checkccmd.org/SearchResults.aspx")
    response = HtmlResponse(  # renders as page 1; expected_page=2 -> re-navigation
        url=request.url, body=RESULTS_HTML, encoding="utf-8", request=request
    )
    results = list(
        spider.parse_results(response, county_key="TestCounty", expected_page=2)
    )
    renav = results[0]
    assert isinstance(renav, scrapy.FormRequest)
    assert renav.meta["proxy_affinity"] == "TestCounty"


def test_pagination_gives_up_after_max_nav_attempts(spider, caplog):
    """Repeated stale postbacks for one page are capped, not looped forever."""
    request = Request(url="https://www.checkccmd.org/SearchResults.aspx")
    response = HtmlResponse(
        url=request.url, body=RESULTS_HTML, encoding="utf-8", request=request
    )

    # Pretend page 2 has already been attempted the maximum number of times.
    spider.nav_attempts_by_county["TestCounty"] = {2: MAX_NAV_ATTEMPTS}

    results = list(
        spider.parse_results(response, county_key="TestCounty", expected_page=2)
    )

    # No further navigation is issued; the give-up is surfaced at ERROR.
    assert results == []
    assert any(
        "gave up navigating to page 2" in r.message for r in caplog.records
    )


def _failed_pagination_failure(county="Baltimore County", page=58, retry_times=10):
    """A Failure wrapping a timed-out pagination postback, as errback receives it."""
    request = scrapy.FormRequest(
        "https://www.checkccmd.org/SearchResults.aspx",
        cb_kwargs={"county_key": county, "expected_page": page},
        meta={"cookiejar": county, "retry_times": retry_times},
    )
    try:
        raise TimeoutError("took longer than 180.0 seconds")
    except TimeoutError:
        failure = Failure()
    failure.request = request
    return failure


def test_pagination_errback_reissues_request(spider):
    """A terminal pagination failure re-issues the postback with a fresh retry budget."""
    failure = _failed_pagination_failure()

    out = spider._pagination_errback(failure)

    assert isinstance(out, scrapy.Request)
    assert out.cb_kwargs["expected_page"] == 58
    assert out.cb_kwargs["county_key"] == "Baltimore County"
    assert out.errback == spider._pagination_errback  # further failures recurse
    assert out.dont_filter is True
    # The exhausted retry counter is reset so the re-issue gets a full budget.
    assert "retry_times" not in out.meta
    assert spider.chain_restarts_by_county["Baltimore County"] == 1


def test_pagination_errback_gives_up_after_max_restarts(spider, caplog):
    """After MAX_CHAIN_RESTARTS, the errback stops re-issuing and logs loudly."""
    spider.chain_restarts_by_county["Baltimore County"] = MAX_CHAIN_RESTARTS
    failure = _failed_pagination_failure()

    out = spider._pagination_errback(failure)

    assert out is None
    assert any(
        "exhausted" in r.message and "truncated" in r.message
        for r in caplog.records
    )


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
    # Numeric license -> parse_detail chains to the EXCELS request; the fully
    # built item (incl. fields) is carried in cb_kwargs.
    assert len(results) == 1
    enrich_request = results[0]
    assert isinstance(enrich_request, scrapy.Request)
    assert "findaprogram.marylandexcels.org" in enrich_request.url
    assert enrich_request.cb_kwargs["first_report_url"] is None
    item = enrich_request.cb_kwargs["item"]
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

    # Non-operating provider still has a numeric license -> EXCELS request.
    assert len(results) == 1
    enrich_request = results[0]
    assert isinstance(enrich_request, scrapy.Request)
    assert "license=570530" in enrich_request.url
    item = enrich_request.cb_kwargs["item"]
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


def test_parse_detail_drops_loudly_when_unrecoverable(spider, caplog):
    """A non-detail response with no recoverable detail URL is dropped at ERROR.

    Without a ``redirect_urls`` trail (so no original ``FacilityDetail`` URL to
    re-issue) the provider can't be recovered — it must be surfaced loudly, never
    silently swallowed the way the old code did.
    """
    html_content = "<html><body>Search form</body></html>"
    request = Request(url="https://www.checkccmd.org/")
    response = HtmlResponse(
        url=request.url, body=html_content, encoding="utf-8", request=request
    )

    with caplog.at_level("ERROR"):
        results = list(spider.parse_detail(response))

    assert len(results) == 0
    assert "Detail dropped" in "\n".join(r.message for r in caplog.records)


def test_parse_detail_reissues_on_session_bounce(spider, caplog):
    """A detail that bounced to the search page is re-issued, not dropped.

    Simulates the cold-session redirect: the response is the search page, but the
    original detail URL is preserved in ``redirect_urls``. The spider re-issues
    that fi on the shared detail jar with the SearchResults referer restored.
    """
    detail_url = (
        "https://www.checkccmd.org/FacilityDetail.aspx?ft=&fn=&sn=&z=&c=&co=&lc=&fi=463466"
    )
    request = Request(
        url="https://www.checkccmd.org/default.aspx",
        meta={"redirect_urls": [detail_url], "cookiejar": DETAIL_COOKIEJAR},
    )
    response = HtmlResponse(
        url=request.url, body=b"<html>search</html>", encoding="utf-8", request=request
    )

    with caplog.at_level("WARNING"):
        results = list(
            spider.parse_detail(response, address="A St", program_type="CTR")
        )

    assert len(results) == 1
    retry = results[0]
    assert retry.url == detail_url
    assert retry.callback == spider.parse_detail
    assert retry.meta["cookiejar"] == DETAIL_COOKIEJAR
    assert retry.meta["detail_reprimes"] == 1
    assert retry.meta["download_timeout"] == DETAIL_DOWNLOAD_TIMEOUT
    assert retry.headers.get("Referer").decode() == SEARCH_RESULTS_REFERER
    # cb_kwargs from the results page ride along so the retry parses identically.
    assert retry.cb_kwargs["address"] == "A St"
    assert retry.cb_kwargs["program_type"] == "CTR"
    assert "session bounce" in "\n".join(r.message for r in caplog.records)


def test_parse_detail_gives_up_after_max_reprimes(spider, caplog):
    """Re-priming is bounded: a fi that keeps bouncing is dropped loudly."""
    detail_url = "https://www.checkccmd.org/FacilityDetail.aspx?fi=463466"
    request = Request(
        url="https://www.checkccmd.org/default.aspx",
        meta={
            "redirect_urls": [detail_url],
            "cookiejar": DETAIL_COOKIEJAR,
            "detail_reprimes": MAX_DETAIL_REPRIMES,
        },
    )
    response = HtmlResponse(
        url=request.url, body=b"<html>search</html>", encoding="utf-8", request=request
    )

    with caplog.at_level("ERROR"):
        results = list(spider.parse_detail(response))

    assert len(results) == 0
    assert "Detail dropped" in "\n".join(r.message for r in caplog.records)


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


EXCELS_RECORD = {
    "name": "7 Day Kiddie Kare Inc.",
    "license": "150301",
    "type": "Child Care Center",
    "streetAddress": "325 N Howard Street ",
    "city": "Baltimore",
    "state": "Maryland",
    "zipcode": "21201",
    "county": "Baltimore City",
    "lat": 39.29512,
    "long": -76.61968,
}


def test_parse_excels_center_hit_sets_address_and_coords(spider):
    """A center (house-numbered EXCELS address) adopts it; no PDF needed."""
    item = ProviderItem()
    item["provider_name"] = "7 Day Kiddie Kare Inc."
    item["license_number"] = "150301"
    item["address"] = "N Howard Street, Baltimore, MD 21201"  # street-name only

    response = _excels_response([EXCELS_RECORD])
    results = list(
        spider.parse_excels(
            response, item=item, first_report_url="https://www.checkccmd.org/x.pdf"
        )
    )

    # No PDF request — the house-numbered EXCELS address satisfied the need.
    assert len(results) == 1
    out = results[0]
    assert isinstance(out, ProviderItem)
    assert out["address"] == "325 N Howard Street, Baltimore, MD 21201"
    assert out["latitude"] == "39.29512"
    assert out["longitude"] == "-76.61968"
    assert out["city"] == "Baltimore"
    assert out["state"] == "MD"
    assert out["zip"] == "21201"


def test_parse_excels_family_home_keeps_coords_no_pdf(spider):
    """A family home (street-only EXCELS address) keeps rooftop coords, no PDF.

    EXCELS withholds the house number for homes but its lat/long is
    rooftop-accurate, so the precise location is captured and no PDF is fetched
    even though a report URL is available.
    """
    item = ProviderItem()
    item["license_number"] = "150301"
    item["address"] = "Roundhill Road, Ellicott City, MD 21043"

    # Street-name-only address, but coordinates ARE present (as EXCELS returns
    # for family homes).
    record = dict(
        EXCELS_RECORD,
        streetAddress="Roundhill Road ",
        city="Ellicott City",
        zipcode="21043",
        lat=39.2427582,
        long=-76.78768113,
    )
    response = _excels_response([record])
    results = list(
        spider.parse_excels(
            response,
            item=item,
            first_report_url="https://www.checkccmd.org/PublicReports/PrintTask.aspx?t=1&d=2",
        )
    )

    # The item is yielded directly — no PDF — with coordinates captured.
    assert len(results) == 1
    assert results[0] is item
    assert item["latitude"] == "39.2427582"
    assert item["longitude"] == "-76.78768113"
    assert item["city"] == "Ellicott City"
    assert item["zip"] == "21043"
    # Address keeps the results-page street-name value (no house number).
    assert item["address"] == "Roundhill Road, Ellicott City, MD 21043"


def test_parse_excels_family_home_no_report_keeps_coords(spider):
    """A street-only family home with no report keeps its coords + address."""
    item = ProviderItem()
    item["license_number"] = "150301"
    item["address"] = "Roundhill Road, Ellicott City, MD 21043"

    record = dict(EXCELS_RECORD, streetAddress="Roundhill Road",
                  lat=39.24, long=-76.78)
    response = _excels_response([record])
    results = list(
        spider.parse_excels(response, item=item, first_report_url=None)
    )

    assert len(results) == 1
    assert results[0] is item
    assert item["latitude"] == "39.24"
    assert item["address"] == "Roundhill Road, Ellicott City, MD 21043"


def test_parse_excels_miss_falls_back_to_pdf(spider):
    """An EXCELS miss with a report URL yields a PDF request on the host slot."""
    item = ProviderItem()
    item["license_number"] = "999999"
    item["address"] = "Some Street, Baltimore, MD 21201"

    response = _excels_response([], license_number="999999")
    results = list(
        spider.parse_excels(
            response,
            item=item,
            first_report_url="https://www.checkccmd.org/PublicReports/PrintTask.aspx?t=1&d=2",
        )
    )

    assert len(results) == 1
    pdf_request = results[0]
    assert isinstance(pdf_request, scrapy.Request)
    assert "PrintTask.aspx?t=1&d=2" in pdf_request.url
    assert pdf_request.callback == spider.parse_inspection_pdf
    # The PDF is on the same checkccmd host and must share the single-flight
    # host slot (per-IP rate limit) — NOT a separate download slot.
    assert "download_slot" not in pdf_request.meta


def test_parse_excels_miss_without_report_yields_item(spider):
    """An EXCELS miss with no report keeps the results-page address."""
    item = ProviderItem()
    item["license_number"] = "999999"
    item["address"] = "Some Street, Baltimore, MD 21201"

    response = _excels_response([], license_number="999999")
    results = list(
        spider.parse_excels(response, item=item, first_report_url=None)
    )

    assert len(results) == 1
    assert results[0] is item
    assert results[0]["address"] == "Some Street, Baltimore, MD 21201"


def test_parse_excels_miss_with_ocr_disabled_yields_item():
    """With ocr_fallback off, an EXCELS miss never produces a PDF request."""
    spider = MarylandSpider(ocr_fallback="false", proxies="off")
    assert spider.ocr_fallback is False

    item = ProviderItem()
    item["license_number"] = "999999"
    item["address"] = "Some Street, Baltimore, MD 21201"

    response = _excels_response([], license_number="999999")
    results = list(
        spider.parse_excels(
            response,
            item=item,
            first_report_url="https://www.checkccmd.org/PublicReports/PrintTask.aspx?t=1&d=2",
        )
    )

    assert len(results) == 1
    assert results[0] is item


def test_parse_excels_record_without_street_keeps_coords_no_pdf(spider):
    """A record with coords but a blank street still yields (no PDF)."""
    item = ProviderItem()
    item["license_number"] = "150301"
    item["address"] = "N Howard Street, Baltimore, MD 21201"

    record = dict(EXCELS_RECORD, streetAddress="  ")
    response = _excels_response([record])
    results = list(
        spider.parse_excels(
            response,
            item=item,
            first_report_url="https://www.checkccmd.org/PublicReports/PrintTask.aspx?t=1&d=2",
        )
    )

    # Record present (with coords) -> no PDF; coordinates captured.
    assert len(results) == 1
    assert results[0] is item
    assert item["latitude"] == "39.29512"
    # Address unchanged (no house-numbered street to adopt).
    assert item["address"] == "N Howard Street, Baltimore, MD 21201"


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

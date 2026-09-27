import logging
import os
import urllib.parse

import pytest
from scrapy.http import HtmlResponse, Request
from twisted.python.failure import Failure

from provider_scrape import normalization as norm
from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.spiders.mississippi import (
    SEARCH_URL,
    MississippiSpider,
    _parse_record_count,
    split_city,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def _response(name, meta=None, url=SEARCH_URL):
    with open(os.path.join(FIXTURES, name), "rb") as fh:
        body = fh.read()
    req = Request(url, meta=meta or {})
    return HtmlResponse(url=url, body=body, encoding="utf-8", request=req)


def _html_response(html, meta=None, url=SEARCH_URL):
    """A response built from a hand-written snippet (fallback-path cases)."""
    req = Request(url, meta=meta or {})
    return HtmlResponse(url=url, body=html.encode(), encoding="utf-8", request=req)


def _fixture_text(name):
    """A fixture's raw HTML, for the tests that mutate it before parsing."""
    with open(os.path.join(FIXTURES, name)) as fh:
        return fh.read()


def _formdata(request):
    """Decode a FormRequest's urlencoded body into a flat dict."""
    body = request.body.decode() if isinstance(request.body, bytes) else request.body
    parsed = urllib.parse.parse_qs(body, keep_blank_values=True)
    return {k: v[0] for k, v in parsed.items()}


def split_requests(outputs):
    """Partition parse_results output into (items, detail requests, pager requests).

    With the 2026-09 redesign a results row no longer carries its own detail
    data (module docstring item 2), so parse_results normally yields a detail
    postback per provider and the provider itself is emitted at the end of
    that chain. Only ``-a details=off`` yields items straight out of here.
    """
    items = [o for o in outputs if isinstance(o, ProviderItem)]
    details = [o for o in outputs if not isinstance(o, ProviderItem) and "item" in o.meta]
    pager = [o for o in outputs if not isinstance(o, ProviderItem) and "item" not in o.meta]
    return items, details, pager


@pytest.fixture
def spider():
    return MississippiSpider()


@pytest.fixture
def ms():
    """A spider that's already been through parse_search_page.

    Gives real access to the site's own 488-city ddlCity dictionary, which the
    address fallback split (split_city) depends on.
    """
    s = MississippiSpider()
    list(s.parse_search_page(_response("ms_search_page.html")))
    return s


def _hinds_page_one(spider_obj, county="HINDS", county_value="25", page=1):
    return list(
        spider_obj.parse_results(
            _response("ms_results_page.html", meta={"page": page, "county": county, "county_value": county_value})
        )
    )


def _clarke_last_page(spider_obj, county="CLARKE", county_value="12"):
    return list(
        spider_obj.parse_results(
            _response("ms_results_last_page.html", meta={"page": 1, "county": county, "county_value": county_value})
        )
    )


def _detail_by_id(details, facility_id):
    return next(r for r in details if r.meta["item"]["ms_facility_id"] == facility_id)


def _advance(spider_obj, request, fixture=None, html=None):
    """Feed a detail fixture to the pending request and return parse_detail's output."""
    meta = dict(request.meta)
    response = _response(fixture, meta=meta) if fixture else _html_response(html, meta=meta)
    return list(spider_obj.parse_detail(response))


def _walk_chain(spider_obj, request, fixtures):
    """Walk a provider's whole detail chain, returning the finished item."""
    output = None
    for fixture in fixtures:
        output = _advance(spider_obj, request, fixture)
        request = output[0]
    return output[0]


# --------------------------------------------------------------------------- #
# 1. parse_search_page
# --------------------------------------------------------------------------- #


def test_parse_search_page_harvests_dropdowns_and_posts_one_search_per_county(spider):
    # One search per county, not one statewide search: the county fan-out is
    # both the v1 poison-page workaround and the only source of the `county`
    # field (module docstring item 6).
    response = _response("ms_search_page.html")
    requests = list(spider.parse_search_page(response))

    assert len(spider.known_cities) == 488
    assert "YAZOO CITY" in spider.known_cities
    assert "STARKVILLE" in spider.known_cities

    assert len(spider.counties) == 82
    assert spider.counties["25"] == "HINDS"
    assert spider.counties["17"] == "DESOTO"

    assert len(requests) == 82
    counties_seen = {r.meta["county"] for r in requests}
    assert counties_seen == set(spider.counties.values())

    # Every county's own, distinct cookiejar: concurrent counties sharing one
    # session corrupted each other's server-side pagination state on the v1
    # site -- mandatory, not a nicety, exactly like Kansas Sec 5.1.
    assert {r.meta["cookiejar"] for r in requests} == counties_seen

    sample = next(r for r in requests if r.meta["county"] == "HINDS")
    assert sample.method == "POST"
    assert sample.meta["page"] == 1
    assert sample.meta["county_value"] == "25"
    formdata = _formdata(sample)
    assert formdata["btnFind"] == "Search"
    assert formdata["__EVENTTARGET"] == ""
    assert formdata["ddlCounty"] == "25"
    # The full hidden-field set has to be echoed back -- a partial one bounces
    # to the empty search page or 500s (module docstring item 3).
    assert formdata["hdnFocusControl"] == ""
    assert "__VIEWSTATE" in formdata and "__EVENTVALIDATION" in formdata
    # No OTHER filter fields sent -- county is the only scoping applied.
    assert "ddlCity" not in formdata
    assert "ddlProviderType" not in formdata


def test_counties_argument_restricts_the_fan_out(caplog):
    s = MississippiSpider(counties="hinds, clarke, nowhere")
    requests = list(s.parse_search_page(_response("ms_search_page.html")))
    assert sorted(r.meta["county"] for r in requests) == ["CLARKE", "HINDS"]
    assert any("NOWHERE" in r.message for r in caplog.records)


@pytest.mark.parametrize(
    "mode,expected_tabs",
    [("all", 4), ("license", 1), ("off", 0)],
)
def test_details_argument_selects_the_tabs_walked(mode, expected_tabs):
    assert len(MississippiSpider(details=mode).detail_tabs) == expected_tabs


def test_details_argument_rejects_an_unknown_mode():
    with pytest.raises(ValueError):
        MississippiSpider(details="everything")


# --------------------------------------------------------------------------- #
# 2. parse_results -- provider rows (the selector the redesign broke)
# --------------------------------------------------------------------------- #


def test_parse_results_finds_every_provider_row(ms):
    # The regression this rebuild fixes: the redesign renamed the row wrapper,
    # so the v1 selector matched nothing and every county logged "0 providers"
    # (module docstring item 1).
    _items, details, _pager = split_requests(_hinds_page_one(ms))

    assert len(details) == 20  # the redesign puts 20 rows on a page (was 25)
    ids = [r.meta["item"]["ms_facility_id"] for r in details]
    # The div id gained a "div_" prefix; stripping it keeps ms_facility_id
    # comparable with the v1 corpus.
    assert ids[:3] == ["20012896", "20006102", "20014742"]
    assert all(not i.startswith("div_") for i in ids)


def test_results_row_fields(ms):
    _items, details, _pager = split_requests(_hinds_page_one(ms))
    item = _detail_by_id(details, "20012896").meta["item"]

    assert item["provider_name"] == "A PLACE TO GROW"
    assert item["source_state"] == "Mississippi"
    assert item["state"] == "MS"
    assert item["county"] == "Hinds"
    assert item["provider_url"] == SEARCH_URL
    assert item["phone"] == "601-981-3133"
    assert item["email"] == "shunwhi@yahoo.com"
    assert item["provider_type"] == "Center based Child Care Facility"
    # The row's own subsidy banner (the License page repeats the fact).
    assert item["ms_subsidy"] is True
    assert item["scholarships_accepted"] is True
    # Coordinates are no longer published anywhere on the site.
    assert "latitude" not in item
    assert "longitude" not in item
    assert "geocode_source" not in item


def test_address_is_split_on_the_citystatezip_line(ms):
    _items, details, _pager = split_requests(_hinds_page_one(ms))
    item = _detail_by_id(details, "20012896").meta["item"]

    # "2607 MEDGAR EVERS BLVD" / "JACKSON, MS 39213-7271" -- the redesign
    # delimits the two (module docstring item 7), and zip/address keep v1's
    # 5-digit form.
    assert item["address"] == "2607 MEDGAR EVERS BLVD, Jackson, MS 39213"
    assert item["city"] == "Jackson"
    assert item["zip"] == "39213"


def test_address_falls_back_to_the_ddlcity_split_without_a_zip_line(ms):
    # A row shaped like the v1 site (no city/state/zip line) still parses via
    # the ddlCity longest-suffix dictionary rather than being dropped.
    html = """
    <div id="div_999" class="row border border-1 rounded-3">
      <dl class="row">
        <dt class="col-4">Provider Name:</dt><dd class="col-8"><strong>LEGACY SHAPE</strong></dd>
        <dt class="col-4">Address:</dt><dd class="col-8">875 E FIFTEENTH ST YAZOO CITY, MS</dd>
      </dl>
    </div>
    """
    response = _html_response(html, meta={"page": 1, "county": "YAZOO", "county_value": "82"})
    item = ms._build_item(response.css('div.row.border[id^="div_"]')[0], response, "YAZOO")
    assert item["city"] == "Yazoo City"
    assert item["address"] == "875 E FIFTEENTH ST, Yazoo City, MS"
    assert "zip" not in item


def test_address_without_a_state_line_warns_and_keeps_the_raw_text(ms, caplog):
    html = """
    <div id="div_998" class="row border border-1 rounded-3">
      <dl class="row">
        <dt class="col-4">Provider Name:</dt><dd class="col-8"><strong>NO STATE</strong></dd>
        <dt class="col-4">Address:</dt><dd class="col-8">123 MAIN ST</dd>
      </dl>
    </div>
    """
    response = _html_response(html)
    item = ms._build_item(response.css('div.row.border[id^="div_"]')[0], response, "HINDS")
    assert item["address"] == "123 MAIN ST"
    assert "city" not in item
    assert any("no ', MS' line" in r.message for r in caplog.records)


def test_row_without_id_or_name_is_skipped_with_an_error(ms, caplog):
    html = '<div id="div_" class="row border border-1 rounded-3"><dl class="row"></dl></div>'
    response = _html_response(html)
    assert ms._build_item(response.css('div.row.border[id^="div_"]')[0], response, "HINDS") is None
    assert any("missing its id/name" in r.message for r in caplog.records)


def test_golden_items_have_no_undefined_fields(ms):
    _items, details, _pager = split_requests(_hinds_page_one(ms))
    for request in details:
        assert dict(request.meta["item"])  # raises on an undefined field


# --------------------------------------------------------------------------- #
# 3. The record-count label + pagination
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "label,expected",
    [
        ("201 records found - Page 1 of 11", (201, 1, 11)),
        ("6 records found - Page 1 of 1", (6, 1, 1)),
        ("1,204 records found - Page 3 of 61", (1204, 3, 61)),
        ("1 record found - Page 1 of 1", (1, 1, 1)),
        ("", None),
        ("Something else entirely", None),
        (None, None),
    ],
)
def test_parse_record_count(label, expected):
    assert _parse_record_count(label) == expected


def test_pagination_mid_run_chains_the_next_page(ms):
    _items, _details, pager = split_requests(_hinds_page_one(ms))
    assert len(pager) == 1
    request = pager[0]
    assert request.meta["page"] == 2
    assert request.meta["county"] == "HINDS"
    assert request.meta["cookiejar"] == "HINDS"
    formdata = _formdata(request)
    assert formdata["__EVENTTARGET"] == "dtPgProviders$ctl01$lnkNextPage"
    # No btnFind on a page turn, and ddlCounty re-sent anyway (item 6).
    assert "btnFind" not in formdata
    assert formdata["ddlCounty"] == "25"
    # The county's expected total is recorded off the label for the closed()
    # reconciliation.
    assert ms.county_expected_total["HINDS"] == 201


def test_pagination_stops_on_the_last_page(ms):
    # Clarke: "6 records found - Page 1 of 1", and the redesign renders no
    # pager markup at all on a final page (module docstring item 5).
    _items, details, pager = split_requests(_clarke_last_page(ms))
    assert len(details) == 6
    assert pager == []
    assert "CLARKE" not in ms.failed_counties


def test_page_number_mismatch_fails_just_that_county(ms, caplog):
    # We asked for page 3, the site answered "Page 1 of 11": our place in the
    # chain is lost, so stop this county rather than double-count.
    outputs = _hinds_page_one(ms, page=3)
    assert outputs == []
    assert "page mismatch" in ms.failed_counties["HINDS"]
    assert any("asked for page 3" in r.message for r in caplog.records)

    # Another county is entirely unaffected -- no shared state beyond dedupe.
    _items, details, _pager = split_requests(_clarke_last_page(ms))
    assert len(details) == 6
    assert "CLARKE" not in ms.failed_counties


def test_missing_label_falls_back_to_the_next_page_link(ms, caplog):
    html = _fixture_text("ms_results_page.html").replace("lblRecordCount", "lblGone")
    outputs = list(ms.parse_results(_html_response(html, meta={"page": 1, "county": "HINDS", "county_value": "25"})))
    _items, details, pager = split_requests(outputs)
    assert len(details) == 20
    assert len(pager) == 1  # the Next Page link alone still drives the chain
    assert any("no parseable record-count label" in r.message for r in caplog.records)


def test_truncation_is_reported_when_the_pager_link_disappears_early(ms, caplog):
    # Label says 11 pages, but page 1 has no Next Page link: the county is
    # short and that must be loud, not a silent clean stop.
    html = _fixture_text("ms_results_page.html").replace("lnkNextPage", "lnkGone")
    outputs = list(ms.parse_results(_html_response(html, meta={"page": 1, "county": "HINDS", "county_value": "25"})))
    _items, _details, pager = split_requests(outputs)
    assert pager == []
    assert "no next-page link on page 1 of 11" in ms.failed_counties["HINDS"]
    assert any("TRUNCATED" in r.message for r in caplog.records)


def test_empty_county_on_page_one_is_a_warning_not_a_failure(ms, caplog):
    # The bare search page stands in for a county whose search returns nothing:
    # no rows, no pager. On page 1 that reads as "possibly a genuinely tiny
    # county", not a failure.
    outputs = list(
        ms.parse_results(
            _response("ms_search_page.html", meta={"page": 1, "county": "ISSAQUENA", "county_value": "28"})
        )
    )
    assert outputs == []
    assert "ISSAQUENA" not in ms.failed_counties
    assert any("may genuinely have none" in r.message for r in caplog.records)


def test_zero_rows_with_a_live_pager_fails_that_county(ms, caplog):
    # 0 rows while the pager still offers a next page is the "we lost the
    # result set" symptom -- fail this county, keep the other 81.
    html = (
        _fixture_text("ms_results_page.html")
        .replace('class="row border border-1 rounded-3', 'class="row renamed')
        .replace("Page 1 of 11", "Page 2 of 11")
    )
    outputs = list(ms.parse_results(_html_response(html, meta={"page": 2, "county": "HINDS", "county_value": "25"})))
    assert outputs == []
    assert "0 providers with a live next-page link" in ms.failed_counties["HINDS"]
    assert any("unexplained 0-provider page" in r.message for r in caplog.records)


def test_cross_county_duplicate_facility_id_is_skipped(ms):
    # Defensive guard: every provider belongs to exactly one county, so a
    # repeated ms_facility_id across two county responses must not
    # double-count or double-emit.
    _items, details, _pager = split_requests(_clarke_last_page(ms))
    assert len(details) == 6
    assert ms.duplicate_facility_ids == 0

    _items2, details2, pager2 = split_requests(_clarke_last_page(ms, county="LAUDERDALE", county_value="38"))
    assert details2 == []
    assert ms.duplicate_facility_ids == 6
    # Dedupe affects which providers are scheduled, never the stop rule.
    assert pager2 == []


# --------------------------------------------------------------------------- #
# 4. The detail chain (License -> Site Visits -> Investigations -> Penalties)
# --------------------------------------------------------------------------- #


def test_detail_chain_walks_all_four_tabs_in_order(ms):
    _items, details, _pager = split_requests(_hinds_page_one(ms))
    request = _detail_by_id(details, "20012896")

    assert request.meta["tab_label"] == "License"
    formdata = _formdata(request)
    # The postback target is read off the row's own link, and the body echoes
    # the results page's hidden fields (items 3/4).
    assert formdata["__EVENTTARGET"] == "lstProviders$ctrl0$lbkBtnViewLicense"
    assert "__VIEWSTATE" in formdata and "hdnFocusControl" in formdata
    assert "btnFind" not in formdata
    # Details outrank page turns so a page's chains drain before the pager
    # advances -- otherwise the scheduler holds one ~600 KB viewstate body per
    # provider in the state at once.
    assert request.priority == 1

    after_license = _advance(ms, request, "ms_license.html")
    assert after_license[0].meta["tab_label"] == "Site Visits"
    after_visits = _advance(ms, after_license[0], "ms_site_visits.html")
    assert after_visits[0].meta["tab_label"] == "Investigations"
    after_investigations = _advance(ms, after_visits[0], "ms_investigations_empty.html")
    assert after_investigations[0].meta["tab_label"] == "Monetary Penalties"
    # Reusing the empty-investigations page for the last tab: no penalty grid
    # on it either, so the chain ends and the provider is emitted.
    final = _advance(ms, after_investigations[0], "ms_investigations_empty.html")
    assert isinstance(final[0], ProviderItem)
    assert final[0]["ms_facility_id"] == "20012896"


def test_license_page_fields(ms):
    _items, details, _pager = split_requests(_hinds_page_one(ms))
    request = _detail_by_id(details, "20012896")
    item = _advance(ms, request, "ms_license.html")[0].meta["item"]

    assert item["license_number"] == "25CCPFWA-7507"
    assert item["capacity"] == 44
    assert item["status"] == "ACTIVE"
    assert item["license_begin_date"] == "12/01/2025"
    assert item["license_expiration"] == "11/30/2026"
    # "Accepts MDHS Subsidy Children" is listed as a service now; it is lifted
    # out into the boolean rather than left in ms_services.
    assert item["ms_services"] == ["School Age After School", "Full Day", "Special Needs"]
    assert item["ms_subsidy"] is True
    assert item["head_start"] is False
    assert item["ms_early_head_start"] is False
    # Ages Served is a <ul> now; the ids drive the coarse flags.
    assert (item["infant"], item["toddler"], item["preschool"], item["school"]) == (True, True, True, True)
    assert item["ages_served"].startswith("Infant Care, 1 year old")
    assert item["ages_served"].endswith("10 to 12 year old")
    # Months come back spelled out and are mapped to v1's short form.
    assert item["ms_months_of_operation"] == [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]
    assert item["hours"] == "Mon-Fri 06:30 AM-11:00 PM"


def test_license_page_handles_a_partial_card(ms, caplog):
    item = ProviderItem()
    html = """
    <span id="ucProviderInfo_lblProviderInfo">License and Service Details of <br/>X</span>
    <span id="ucProviderInfo_lblCapacityLabel">not a number</span>
    <span id="ucProviderInfo_lblServicesLabel">Full Day, Head Start, Early Head Start</span>
    """
    ms._apply_license(item, _html_response(html), "999")
    assert "capacity" not in item
    assert any("non-integer capacity" in r.message for r in caplog.records)
    # Fallback for a comma-joined services string (the v1 shape).
    assert item["ms_services"] == ["Full Day", "Head Start", "Early Head Start"]
    assert item["head_start"] is True
    assert item["ms_early_head_start"] is True


def test_site_visits_include_nested_follow_up_inspections(ms):
    _items, details, _pager = split_requests(_hinds_page_one(ms))
    request = _detail_by_id(details, "20012896")
    after_license = _advance(ms, request, "ms_license.html")
    after_visits = _advance(ms, after_license[0], "ms_site_visits.html")
    inspections = after_visits[0].meta["inspections"]

    # 9 site visits + 4 follow-ups nested under them.
    assert len(inspections) == 13
    assert sum(1 for i in inspections if i.get("ms_exam_type") == "Follow Up") == 4

    first = inspections[0]
    assert first["type"] == "Inspection"
    assert first["date"] == "9/17/2026"
    assert first["ms_end_date"] == "9/17/2026"
    assert first["original_status"] == "Pass"
    # The redesign stopped publishing the exam type for a regular visit.
    assert "ms_exam_type" not in first
    # The token is percent-encoded on the way out: the markup now holds it raw,
    # and a bare "+" would reach the server as a space.
    assert first["report_url"].startswith(
        "https://www.mdhs.provider.webapps.ms.gov/PublicViewInspectionDocument.aspx?pdf="
    )
    assert "%2B" in first["report_url"]

    followup = next(i for i in inspections if i.get("ms_exam_type") == "Follow Up")
    # A follow-up's own dates, not its parent visit's (the lblFollowup* ids are
    # what keep the nested block apart).
    assert followup["date"] == "6/13/2025"
    assert followup["original_status"] == "Pass Pending"


def test_investigation_and_penalty_grids(ms):
    _items, details, _pager = split_requests(_clarke_last_page(ms))
    request = _detail_by_id(details, "20010336")  # THE SHEPHERD'S STAFF
    # Jump the chain to the Investigations tab -- the fixtures for this
    # provider are the two grid pages.
    meta = dict(request.meta)
    meta.update({"tab_index": 2, "tab_label": "Investigations", "tab_handler": "_parse_investigations"})
    after_investigations = list(ms.parse_detail(_response("ms_investigations.html", meta=meta)))
    investigations = after_investigations[0].meta["inspections"]
    assert len(investigations) == 1
    assert investigations[0]["type"] == "Investigation"
    assert investigations[0]["date"] == "3/13/2023"
    assert investigations[0]["ms_description"] == "Investigation – Complaint"
    assert "PublicViewInspectionDocument.aspx?pdf=" in investigations[0]["report_url"]

    final = _advance(ms, after_investigations[0], "ms_monetary_penalties.html")
    item = final[0]
    assert isinstance(item, ProviderItem)
    penalties = [i for i in item["inspections"] if i["type"] == "Monetary Penalty"]
    assert len(penalties) == 1
    assert penalties[0]["date"] == "4/7/2026"
    assert penalties[0]["ms_description"] == "Monetary Penalty Letter"


def test_absent_grid_yields_no_records(ms):
    # The common case: most providers have no investigation or penalty at all,
    # and the whole table is simply absent. Not a failure.
    assert ms._parse_investigations(_response("ms_investigations_empty.html"), "20012896") == []
    assert ms._parse_monetary_penalties(_response("ms_investigations_empty.html"), "20012896") == []
    assert ms.detail_failures == {}


def test_detail_page_for_the_wrong_provider_is_dropped_not_merged(ms, caplog):
    # The postback is positional (module docstring item 4): if the server
    # resolves an index against a different page, merging would corrupt the
    # item, so the tab's data is dropped instead.
    _items, details, _pager = split_requests(_clarke_last_page(ms))
    request = _detail_by_id(details, "20008682")  # FIRST UNITED METHODIST PRESCHOOL
    after = _advance(ms, request, "ms_license.html")  # A PLACE TO GROW's page

    assert after[0].meta["tab_label"] == "Site Visits"  # chain continues
    item = after[0].meta["item"]
    assert "license_number" not in item
    assert "capacity" not in item
    assert ms.detail_failures == {"License": 1}
    assert any("tab dropped rather than merged" in r.message for r in caplog.records)


def test_bounced_postback_is_reported_and_the_chain_continues(ms, caplog):
    # A body the server rejects lands back on the bare search page, which has
    # no provider header at all.
    _items, details, _pager = split_requests(_clarke_last_page(ms))
    request = _detail_by_id(details, "20008682")
    after = _advance(ms, request, "ms_search_page.html")

    assert after[0].meta["tab_label"] == "Site Visits"
    assert ms.detail_failures == {"License": 1}
    assert any("did not return a detail page" in r.message for r in caplog.records)


def test_failed_detail_request_still_emits_the_partial_provider(ms, caplog):
    _items, details, _pager = split_requests(_clarke_last_page(ms))
    request = _detail_by_id(details, "20008682")
    try:
        raise ConnectionRefusedError("connection lost")
    except ConnectionRefusedError:
        failure = Failure()
    failure.request = request

    emitted = ms.detail_errback(failure)
    assert len(emitted) == 1
    item = emitted[0]
    assert isinstance(item, ProviderItem)
    # Everything the row gave is still there; only the tab's data is missing.
    assert item["provider_name"] == "FIRST UNITED METHODIST PRESCHOOL"
    assert "license_number" not in item
    assert ms.detail_failures == {"License": 1}
    assert any("emitting the provider without it" in r.message for r in caplog.records)


def test_details_off_emits_rows_without_any_postback():
    s = MississippiSpider(details="off")
    list(s.parse_search_page(_response("ms_search_page.html")))
    items, details, pager = split_requests(_clarke_last_page(s))
    assert len(items) == 6
    assert details == [] and pager == []
    assert all("provider_name" in i for i in items)


def test_details_license_stops_after_the_license_tab():
    s = MississippiSpider(details="license")
    list(s.parse_search_page(_response("ms_search_page.html")))
    _items, details, _pager = split_requests(_hinds_page_one(s))
    request = _detail_by_id(details, "20012896")
    assert request.meta["tab_label"] == "License"
    final = _advance(s, request, "ms_license.html")
    assert isinstance(final[0], ProviderItem)  # no Site Visits postback
    assert "inspections" not in final[0]


def test_row_missing_its_detail_links_still_emits(ms, caplog):
    html = """
    <div id="div_997" class="row border border-1 rounded-3">
      <dl class="row">
        <dt class="col-4">Provider Name:</dt><dd class="col-8"><strong>NO LINKS</strong></dd>
      </dl>
    </div>
    """
    response = _html_response(html, meta={"page": 1, "county": "HINDS", "county_value": "25"})
    container = response.css('div.row.border[id^="div_"]')[0]
    item = ms._build_item(container, response, "HINDS")
    outputs = list(ms._start_detail_chain(item, container, {"__VIEWSTATE": "x"}, "HINDS"))
    assert outputs == [item]
    assert any("has no postback link" in r.message for r in caplog.records)


# --------------------------------------------------------------------------- #
# 5. facility_category mapping for the 3 Mississippi provider types
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "provider_type,category",
    [
        ("Center based Child Care Facility", "center"),
        ("Home based Child Care Facility", "family_home"),
        ("Youth Camp", "other"),
    ],
)
def test_mississippi_facility_category_mapping(provider_type, category):
    assert norm.facility_category_from_type(provider_type) == category


# --------------------------------------------------------------------------- #
# 6. status extraction + canonical mapping
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "raw_word,canonical",
    [
        ("ACTIVE", "active"),
        ("PENDING", "pending"),
        ("PENDING-INSPECTION", "pending"),
        ("PENDING-DOCS-INSPECT", "pending"),
        ("PENDING-DOCUMENTS", "pending"),
        ("TEMPORARY", "provisional"),
        ("RESTRICTED", "enforcement"),
    ],
)
def test_mississippi_statuses_are_mapped(raw_word, canonical):
    assert norm.canonical_status(raw_word) == canonical


def test_apply_status_parses_word_and_date_range(spider):
    item = ProviderItem()
    spider._apply_status(item, " PENDING-INSPECTION (10/01/2026 - 09/30/2027)", "12345")
    assert item["status"] == "PENDING-INSPECTION"
    assert item["license_begin_date"] == "10/01/2026"
    assert item["license_expiration"] == "09/30/2027"


def test_apply_status_fallback_on_unparsed_text(spider, caplog):
    item = ProviderItem()
    spider._apply_status(item, "Some Unexpected Text", "12345")
    assert item["status"] == "Some Unexpected Text"
    assert "license_begin_date" not in item
    assert any("unparsed status" in r.message for r in caplog.records)


# --------------------------------------------------------------------------- #
# 7. closed() reconciliation against the site's own counter
# --------------------------------------------------------------------------- #


def test_closed_flags_a_county_short_of_the_sites_own_count(ms, caplog):
    caplog.set_level(logging.INFO)
    _hinds_page_one(ms)  # 20 of the 201 the label promises, and we stop there
    ms.closed("finished")
    assert any("captured 20 providers but the site's own counter said 201" in r.message for r in caplog.records)
    # And it says out loud that coordinates now come from geocode_enrich.
    assert any("geocode_enrich" in r.message for r in caplog.records)


def test_closed_is_quiet_when_a_county_matches_its_counter(ms, caplog):
    _clarke_last_page(ms)  # 6 rows, label says 6
    ms.closed("finished")
    assert not any("but the site's own counter said" in r.message for r in caplog.records)


# --------------------------------------------------------------------------- #
# 8. split_city (the address fallback path)
# --------------------------------------------------------------------------- #


KNOWN_CITIES = {"KOSCIUSKO", "STARKVILLE", "YAZOO CITY", "BAY ST LOUIS", "OCEAN SPRINGS"}


@pytest.mark.parametrize(
    "addr_head,expected_street,expected_city",
    [
        # plain single-word city
        ("1129 N NATCHEZ ST KOSCIUSKO", "1129 N NATCHEZ ST", "Kosciusko"),
        # multi-word city -- a naive last-token split would fail here
        ("875 E FIFTEENTH ST YAZOO CITY", "875 E FIFTEENTH ST", "Yazoo City"),
        # another multi-word city, three tokens
        ("100 OAK ST BAY ST LOUIS", "100 OAK ST", "Bay St Louis"),
        # city name that is itself a suffix-collision risk ("Springs" alone is
        # not a known city, so this must match the full "OCEAN SPRINGS").
        ("42 GULF AVE OCEAN SPRINGS", "42 GULF AVE", "Ocean Springs"),
    ],
)
def test_split_city_success_cases(addr_head, expected_street, expected_city):
    street, city = split_city(addr_head, KNOWN_CITIES)
    assert street == expected_street
    assert city == expected_city


def test_split_city_fallback_when_no_known_city_matches():
    street, city = split_city("123 MAIN ST SOMEWHERE", KNOWN_CITIES)
    assert street == "123 MAIN ST SOMEWHERE"
    assert city is None


def test_split_city_word_boundary_guard():
    # "KOSCIUSKO" must not match inside a longer word with no space before it.
    street, city = split_city("100 MAIN STNKOSCIUSKO", KNOWN_CITIES)
    assert city is None
    assert street == "100 MAIN STNKOSCIUSKO"


# --------------------------------------------------------------------------- #
# 9. No undefined item fields (guards ms_* typos)
# --------------------------------------------------------------------------- #


def test_provider_and_inspection_items_reject_unknown_fields():
    item = ProviderItem()
    with pytest.raises(KeyError):
        item["ms_totally_made_up_field"] = True

    insp = InspectionItem()
    with pytest.raises(KeyError):
        insp["ms_also_made_up"] = True

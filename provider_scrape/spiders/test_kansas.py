import os
import urllib.parse

import pytest
from scrapy.http import HtmlResponse, Request

from provider_scrape import normalization as norm
from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.spiders.kansas import (
    COUNTY_CLIENTSTATE_FIELD,
    PROGRAM_TYPE_CLIENTSTATE_FIELD,
    SEARCH_URL,
    KansasSpider,
    _zip5,
    split_address,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def _response(name, meta=None, url=SEARCH_URL):
    with open(os.path.join(FIXTURES, name), "rb") as fh:
        body = fh.read()
    req = Request(url, meta=meta or {})
    return HtmlResponse(url=url, body=body, encoding="utf-8", request=req)


def _formdata(request):
    """Decode a FormRequest's urlencoded body into a flat dict."""
    body = request.body.decode() if isinstance(request.body, bytes) else request.body
    parsed = urllib.parse.parse_qs(body, keep_blank_values=True)
    return {k: v[0] for k, v in parsed.items()}


def split_requests(outputs):
    """Partition parse_results output into (mint requests, next-page requests).

    Mint requests always carry the listing ``row`` in meta; the chained
    ``Next`` postback never does.
    """
    mints = [r for r in outputs if "row" in r.meta]
    nexts = [r for r in outputs if "row" not in r.meta]
    return mints, nexts


@pytest.fixture
def spider():
    return KansasSpider()


# --------------------------------------------------------------------------- #
# 1. parse_counties
# --------------------------------------------------------------------------- #


def test_parse_counties_extracts_all_and_posts_one_search_per_county(spider):
    response = _response("ks_search_page.html")
    requests = list(spider.parse_counties(response))

    assert len(requests) == 106
    counties = {r.meta["county"] for r in requests}
    assert "Out of state" in counties
    assert "Crawford" in counties
    assert len(counties) == 106  # every cookiejar is distinct

    cookiejars = {r.meta["cookiejar"] for r in requests}
    assert cookiejars == counties  # cookiejar == county, 1:1

    sample = next(r for r in requests if r.meta["county"] == "Crawford")
    formdata = _formdata(sample)
    assert formdata["ctl00$ContentPlaceHolder1$searchBtn"] == "Search"
    assert '"value": "Crawford"' in formdata[COUNTY_CLIENTSTATE_FIELD]
    assert formdata[PROGRAM_TYPE_CLIENTSTATE_FIELD].count(",") > 0
    assert '"selectedIndices": [1, 2, 3, 4, 5, 6, 7]' in formdata[PROGRAM_TYPE_CLIENTSTATE_FIELD]


# --------------------------------------------------------------------------- #
# 2-4. parse_results pagination
# --------------------------------------------------------------------------- #


def test_parse_results_full_page_mints_and_chains_next(spider):
    response = _response("ks_results_page.html", meta={"county": "Crawford", "page": 1})
    outputs = list(spider.parse_results(response))
    mints, nexts = split_requests(outputs)

    assert len(mints) == 10
    assert len(nexts) == 1
    next_req = nexts[0]
    assert next_req.meta["cookiejar"] == "Crawford"
    assert next_req.meta["page"] == 2
    formdata = _formdata(next_req)
    assert formdata["ctl00$ContentPlaceHolder1$nextBtn"] == "Next"


def test_parse_results_short_page_stops(spider):
    response = _response("ks_results_last_page.html", meta={"county": "Crawford", "page": 6})
    outputs = list(spider.parse_results(response))
    mints, nexts = split_requests(outputs)

    assert len(mints) == 8
    assert len(nexts) == 0  # the Sec 5.2 regression guard: never trust the
    # button's own disabled/enabled state


def test_parse_results_empty_page_yields_nothing(spider):
    # ks_search_page.html has no SearchLink rows at all -- a convenient
    # stand-in for a genuinely empty results page (e.g. Commanche/"Out of
    # state").
    response = _response("ks_search_page.html", meta={"county": "Commanche", "page": 1})
    outputs = list(spider.parse_results(response))
    mints, nexts = split_requests(outputs)
    assert mints == []
    assert nexts == []


# --------------------------------------------------------------------------- #
# 5. Dedupe vs. the raw-count stop rule
# --------------------------------------------------------------------------- #


def test_dedupe_does_not_affect_the_pagesize_stop_rule(spider):
    response = _response("ks_results_page.html", meta={"county": "Crawford", "page": 1})
    first_mints, first_nexts = split_requests(list(spider.parse_results(response)))
    assert len(first_mints) == 10
    assert len(first_nexts) == 1

    # Feed the SAME 10 rows again as if they were "page 2" (every token
    # already seen). No new mints -- but the stop rule looks at the RAW row
    # count (10), not the deduped count (0), so it must still chain a next
    # request.
    response2 = _response("ks_results_page.html", meta={"county": "Crawford", "page": 2})
    second_mints, second_nexts = split_requests(list(spider.parse_results(response2)))
    assert second_mints == []
    assert len(second_nexts) == 1
    assert spider.duplicate_rows == 10


# --------------------------------------------------------------------------- #
# 6. Mint request shape
# --------------------------------------------------------------------------- #


def test_mint_request_shape(spider):
    response = _response("ks_results_page.html", meta={"county": "Crawford", "page": 1})
    row = spider._parse_rows(response)[0]
    request = spider._mint_request(response, row)

    formdata = _formdata(request)
    assert formdata["__EVENTTARGET"] == f"ctl00$ContentPlaceHolder1${row['token']}"
    assert formdata["__EVENTARGUMENT"] == ""
    assert request.meta["dont_merge_cookies"] is True
    assert request.meta["row"] == row


# --------------------------------------------------------------------------- #
# 7. parse_detail golden path (data-rich facility)
# --------------------------------------------------------------------------- #


def test_parse_detail_golden(spider):
    row = {
        "token": "SearchLink.abc==",
        "city": "Dodge City",
        "zip": "67801",
        "county": "Ford",
        "program_type": "School Age Program",
    }
    response = _response("ks_detail_center.html", meta={"row": row})
    item = next(spider.parse_detail(response))

    assert isinstance(item, ProviderItem)
    assert item["source_state"] == "Kansas"
    assert item["provider_name"] == "Dodge City Family YMCA Camp Dodge"
    assert item["license_number"] == "0081148"
    assert item["license_holder"] == "Dodge City Family YMCA Inc"
    assert item["provider_type"] == "School Age Program"
    # NOTE: this fixture's real provider_type is "School Age Program" (not
    # "Child Care Center" as the plan's Sec 8 case-7 wording implies) --
    # asserting the actual live-captured value rather than the plan's
    # example. facility_category_from_type("School Age Program") ==
    # "school_age" (an existing mapping, unaffected by the 4 new Kansas
    # entries added in normalization.py).
    assert norm.facility_category_from_type(item["provider_type"]) == "school_age"
    assert item["status"] == "Open"
    assert item["address"] == "240 San Jose Dr, Dodge City, KS 67801"
    assert item["city"] == "Dodge City"
    assert item["state"] == "KS"
    assert item["zip"] == "67801"
    assert item["county"] == "Ford"
    assert item["phone"] == "620-225-8157"
    assert item["capacity"] == 80
    assert item["license_begin_date"] == "07/01/2026"
    assert item["license_expiration"] == "06/30/2027"
    assert item["ks_facility_token"] == "SearchLink.abc=="
    assert item["ks_address_suppressed"] is False
    assert "ks_listing_program_type" not in item  # listing matches detail

    inspections = item["inspections"]
    assert inspections and all(isinstance(i, InspectionItem) for i in inspections)
    assert any(i.get("ks_survey_number") for i in inspections)
    assert any(i.get("ks_findings_count") is not None for i in inspections)
    assert any(i.get("report_url") for i in inspections)
    assert any(i.get("date") for i in inspections)
    assert any(i["type"] == "Licensing Survey" for i in inspections)
    assert any(i["type"] == "Administrative Order" for i in inspections)


def test_golden_item_has_no_undefined_fields(spider):
    row = {
        "token": "SearchLink.abc==",
        "city": "Dodge City",
        "zip": "67801",
        "county": "Ford",
        "program_type": "School Age Program",
    }
    response = _response("ks_detail_center.html", meta={"row": row})
    item = next(spider.parse_detail(response))
    assert dict(item)  # constructing/serializing raises on an undefined field


# --------------------------------------------------------------------------- #
# 8. parse_detail suppressed
# --------------------------------------------------------------------------- #


def test_parse_detail_suppressed(spider):
    row = {
        "token": "SearchLink.sup==",
        "city": "Topeka",
        "zip": "66611 - 0000",
        "county": "Shawnee",
        "program_type": "Family Child Care Home",
    }
    response = _response("ks_detail_suppressed.html", meta={"row": row})
    item = next(spider.parse_detail(response))

    assert "address" not in item
    assert "phone" not in item
    assert item["ks_address_suppressed"] is True
    # city/zip/county still come from the LISTING row (Sec 5.4 guard).
    assert item["city"] == "Topeka"
    assert item["zip"] == "66611"
    assert item["county"] == "Shawnee"


# --------------------------------------------------------------------------- #
# 9. Program-type disagreement (listing vs. detail)
# --------------------------------------------------------------------------- #


def test_program_type_disagreement_prefers_detail(spider):
    row = {
        "token": "SearchLink.comp==",
        "city": "Wichita",
        "zip": "67205",
        "county": "Sedgwick",
        "program_type": "Preschool",
    }
    response = _response("ks_detail_complaints.html", meta={"row": row})
    item = next(spider.parse_detail(response))

    assert item["provider_type"] == "Child Care Center"
    assert item["ks_listing_program_type"] == "Preschool"


# --------------------------------------------------------------------------- #
# 10. Empty grids render no phantom InspectionItems
# --------------------------------------------------------------------------- #


def test_empty_grid_yields_no_items(spider):
    # ks_detail_center.html's complaint grid is genuinely empty ("No
    # Complaint Survey Records Found."); ks_detail_complaints.html's
    # administrative-order grid is genuinely empty ("No Administrative
    # Orders Found."). Neither placeholder row may become an InspectionItem.
    row = {
        "token": "SearchLink.abc==",
        "city": "Dodge City",
        "zip": "67801",
        "county": "Ford",
        "program_type": "School Age Program",
    }
    center_item = next(spider.parse_detail(_response("ks_detail_center.html", meta={"row": row})))
    assert not any(i["type"] == "Complaint Survey" for i in center_item["inspections"])

    row2 = {
        "token": "SearchLink.comp==",
        "city": "Wichita",
        "zip": "67205",
        "county": "Sedgwick",
        "program_type": "Child Care Center",
    }
    complaints_item = next(spider.parse_detail(_response("ks_detail_complaints.html", meta={"row": row2})))
    assert not any(i["type"] == "Administrative Order" for i in complaints_item["inspections"])


# --------------------------------------------------------------------------- #
# 11. Multi-NOSF survey -> two InspectionItems
# --------------------------------------------------------------------------- #


def test_multi_nosf_survey_yields_two_items(spider):
    row = {
        "token": "SearchLink.abc==",
        "city": "Dodge City",
        "zip": "67801",
        "county": "Ford",
        "program_type": "School Age Program",
    }
    item = next(spider.parse_detail(_response("ks_detail_center.html", meta={"row": row})))

    shared_survey = [i for i in item["inspections"] if i.get("ks_survey_number") == "26-004836"]
    assert len(shared_survey) == 2
    nosf_ids = {i["ks_nosf_id"] for i in shared_survey}
    assert nosf_ids == {"168634", "168820"}
    findings_counts = {i["ks_nosf_id"]: i["ks_findings_count"] for i in shared_survey}
    assert findings_counts == {"168634": 15, "168820": 5}
    # each row keeps its own report_url.
    report_urls = {i["report_url"] for i in shared_survey}
    assert len(report_urls) == 2


# --------------------------------------------------------------------------- #
# 12. New status values
# --------------------------------------------------------------------------- #


def test_new_kansas_statuses_are_mapped():
    assert norm.canonical_status("License Expired") == "closed"
    assert norm.canonical_status("License Suspended") == "enforcement"
    assert norm.canonical_status("Open") == "active"


# --------------------------------------------------------------------------- #
# 13. split_address
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "raw,city,expected_address,expected_street",
    [
        # plain glued street+city
        (
            "240 San Jose Dr Dodge City, KS 67801",
            "Dodge City",
            "240 San Jose Dr, Dodge City, KS 67801",
            "240 San Jose Dr",
        ),
        # unit letter glued directly to the city with no space at all
        (
            "601 S Edwards St Unit ALewis, KS 67552",
            "Lewis",
            "601 S Edwards St Unit A, Lewis, KS 67552",
            "601 S Edwards St Unit A",
        ),
        # -0000 filler dropped
        ("1831 E 21st St Andover, KS 67002-0000", "Andover", "1831 E 21st St, Andover, KS 67002", "1831 E 21st St"),
        # city name equals the county name -- must anchor on the ZIP tail, not
        # search for the city string anywhere in the address.
        (
            "612 N Commercial Ave Sedgwick, KS 67135",
            "Sedgwick",
            "612 N Commercial Ave, Sedgwick, KS 67135",
            "612 N Commercial Ave",
        ),
        # real ZIP+4 preserved
        (
            "7026 W 21st St N Wichita, KS 67205-1760",
            "Wichita",
            "7026 W 21st St N, Wichita, KS 67205-1760",
            "7026 W 21st St N",
        ),
    ],
)
def test_split_address_success_cases(raw, city, expected_address, expected_street):
    address, street = split_address(raw, city)
    assert address == expected_address
    assert street == expected_street


@pytest.mark.parametrize(
    "raw,city",
    [
        # the head doesn't end with the given city -> never guess.
        ("123 Main St Somewhere, KS 12345", "Nowhere"),
        # no ", XX 12345" tail at all -> never guess.
        ("this address has no zip tail", "Wichita"),
    ],
)
def test_split_address_fallback_cases(raw, city):
    address, street = split_address(raw, city)
    assert address == raw
    assert street is None


def test_split_address_missing_inputs():
    assert split_address(None, "Wichita") == (None, None)
    raw = "123 Main St, KS 12345"
    assert split_address(raw, "") == (raw, None)
    assert split_address(raw, None) == (raw, None)


# --------------------------------------------------------------------------- #
# 14. Missing detail (guard against a blank/expired-token page)
# --------------------------------------------------------------------------- #


def test_parse_detail_missing_name_emits_nothing(spider, caplog):
    # ks_search_page.html has no faciltyNameValue span at all.
    response = _response("ks_search_page.html", meta={"row": {"token": "x"}})
    items = list(spider.parse_detail(response))
    assert items == []
    assert any("parse_detail missing faciltyNameValue" in record.message for record in caplog.records)


# --------------------------------------------------------------------------- #
# 15. Zip normalization
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("66743 - 0000", "66743"),
        ("66218 - 9758", "66218"),
        ("66762", "66762"),
        (None, None),
        ("", None),
    ],
)
def test_zip5(raw, expected):
    assert _zip5(raw) == expected


# --------------------------------------------------------------------------- #
# 17. facility_category mapping for all 7 Kansas provider types
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "provider_type,category",
    [
        ("Family Child Care Home", "family_home"),
        ("Child Care Center", "center"),
        ("School Age Program", "school_age"),
        ("Preschool", "center"),
        ("Head Start Child Care Center", "center"),
        ("Youth Development Program", "school_age"),
        ("Outdoor Summer Camp", "other"),
    ],
)
def test_kansas_facility_category_mapping(provider_type, category):
    assert norm.facility_category_from_type(provider_type) == category

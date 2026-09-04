import os
import urllib.parse

import pytest
from scrapy.http import HtmlResponse, Request

from provider_scrape import normalization as norm
from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.spiders.mississippi import (
    SEARCH_URL,
    MississippiSpider,
    _parse_pager,
    split_city,
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
    """Partition parse_results output into (items, next-page requests)."""
    items = [o for o in outputs if isinstance(o, ProviderItem)]
    requests = [o for o in outputs if not isinstance(o, ProviderItem)]
    return items, requests


@pytest.fixture
def spider():
    return MississippiSpider()


@pytest.fixture
def spider_with_cities():
    """A spider that's already been through parse_search_page.

    Gives real access to the site's own 487-city ddlCity dictionary, which
    parse_results' address split (split_city, Sec 5.4) depends on.
    """
    s = MississippiSpider()
    list(s.parse_search_page(_response("ms_search_page.html")))
    return s


def _item_by_id(items, facility_id):
    return next(i for i in items if i["ms_facility_id"] == facility_id)


# --------------------------------------------------------------------------- #
# 1. parse_search_page
# --------------------------------------------------------------------------- #


def test_parse_search_page_harvests_cities_and_posts_one_search_per_county(spider):
    # Replaces the single statewide empty search (module docstring item 7:
    # that result set has an unrenderable "poison" offset around row
    # 901-925) with one search per county, so no individual query is ever
    # deep enough to reach it.
    response = _response("ms_search_page.html")
    requests = list(spider.parse_search_page(response))

    assert len(spider.known_cities) == 487
    assert "YAZOO CITY" in spider.known_cities
    assert "STARKVILLE" in spider.known_cities

    assert len(spider.counties) == 82
    assert spider.counties["25"] == "HINDS"
    assert spider.counties["17"] == "DESOTO"

    assert len(requests) == 82
    counties_seen = {r.meta["county"] for r in requests}
    assert counties_seen == set(spider.counties.values())
    assert len(counties_seen) == 82  # every county gets exactly one search

    # Every county's own, distinct cookiejar (module docstring item 8):
    # concurrent counties sharing one session corrupted each other's
    # server-side pagination state (live-verified 2026-09-05) -- mandatory,
    # not a nicety, exactly like Kansas Sec 5.1.
    cookiejars = {r.meta["cookiejar"] for r in requests}
    assert cookiejars == counties_seen

    sample = next(r for r in requests if r.meta["county"] == "HINDS")
    assert sample.method == "POST"
    assert sample.meta["page"] == 1
    assert sample.meta["cookiejar"] == "HINDS"
    formdata = _formdata(sample)
    assert formdata["btnFind"] == "Search"
    assert formdata["__EVENTTARGET"] == ""
    assert formdata["__EVENTARGUMENT"] == ""
    assert formdata["ddlCounty"] == "25"
    # No OTHER filter fields sent -- county is the only scoping applied.
    assert "txtProviderName" not in formdata
    assert "btnReset" not in formdata


# --------------------------------------------------------------------------- #
# 2. parse_results -- provider extraction (the single-quoted-id selector)
# --------------------------------------------------------------------------- #


def test_parse_results_extracts_all_providers(spider_with_cities):
    response = _response("ms_results_page.html", meta={"page": 1, "county": "Attala", "county_value": "04"})
    items, _requests = split_requests(list(spider_with_cities.parse_results(response)))

    # The fixture is trimmed to the first 4 of a real 25-provider page (Sec
    # 8) -- the single-quoted div id (Sec 5.1) must still find all 4.
    assert len(items) == 4
    assert {i["ms_facility_id"] for i in items} == {"20006183", "20012910", "20005047", "7001416"}

    item = _item_by_id(items, "20006183")
    assert isinstance(item, ProviderItem)
    assert item["source_state"] == "Mississippi"
    assert item["provider_url"] == SEARCH_URL
    assert item["provider_name"] == "3 STEP DAYCARE"
    assert item["license_number"] == "04CBPFA-6901"
    assert item["provider_type"] == "Center based Child Care Facility"
    assert item["status"] == "ACTIVE"
    assert item["license_begin_date"] == "09/01/2026"
    assert item["license_expiration"] == "08/31/2027"
    assert item["capacity"] == 30
    assert item["phone"] == "662-792-4180"
    assert item["email"] == "brendajzollicoffer@gmail.com"
    assert item["zip"] == "39090"
    assert item["state"] == "MS"
    assert item["city"] == "Kosciusko"
    assert item["address"] == "1129 N NATCHEZ ST, Kosciusko, MS 39090"
    # Populated because the county filter value maps 1:1 to a known county
    # name -- the statewide search this replaced had no per-record county.
    assert item["county"] == "Attala"


def test_golden_item_has_no_undefined_fields(spider_with_cities):
    response = _response("ms_results_page.html", meta={"page": 1, "county": "Attala", "county_value": "04"})
    items, _requests = split_requests(list(spider_with_cities.parse_results(response)))
    for item in items:
        assert dict(item)  # constructing/serializing raises on an undefined field


# --------------------------------------------------------------------------- #
# 3. Coordinates from htJson
# --------------------------------------------------------------------------- #


def test_coordinates_from_htjson_by_id(spider_with_cities):
    response = _response("ms_results_page.html", meta={"page": 1, "county": "Attala", "county_value": "04"})
    items, _requests = split_requests(list(spider_with_cities.parse_results(response)))

    item = _item_by_id(items, "20006183")
    # htJson coordinates are [lng, lat] -- swapped into our lat/lon (Sec 5.7).
    assert item["latitude"] == pytest.approx(33.074193)
    assert item["longitude"] == pytest.approx(-89.58441)
    assert item["geocode_source"] == "state"

    item2 = _item_by_id(items, "7001416")
    assert item2["latitude"] == pytest.approx(33.17639)
    assert item2["longitude"] == pytest.approx(-90.488952)


def test_provider_missing_from_htjson_gets_no_coordinates(spider_with_cities, caplog):
    response = _response("ms_results_missing_coords.html", meta={"page": 1, "county": "Attala", "county_value": "04"})
    items, _requests = split_requests(list(spider_with_cities.parse_results(response)))

    assert len(items) == 1
    item = items[0]
    assert item["ms_facility_id"] == "20005047"
    assert "latitude" not in item
    assert "longitude" not in item
    assert "geocode_source" not in item
    assert any("no htJson coordinates" in r.message for r in caplog.records)
    # the rest of the item still built fine -- a missing coordinate never
    # crashes or blanks out the whole row.
    assert item["license_number"] == "53CBPFWA-6827"


# --------------------------------------------------------------------------- #
# 4. facility_category mapping for the 3 Mississippi provider types
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
# 5. status extraction + canonical mapping
# --------------------------------------------------------------------------- #


def test_status_extraction_from_results_page(spider_with_cities):
    response = _response("ms_results_page.html", meta={"page": 1, "county": "Attala", "county_value": "04"})
    items, _requests = split_requests(list(spider_with_cities.parse_results(response)))
    for item in items:
        # every sampled provider on this page is ACTIVE
        assert item["status"] == "ACTIVE"
        assert "/" in item["license_begin_date"]
        assert "/" in item["license_expiration"]


@pytest.mark.parametrize(
    "raw_word,canonical",
    [
        ("ACTIVE", "active"),
        ("PENDING", "pending"),
        ("PENDING-INSPECTION", "pending"),
        # Surfaced on the 2026-09-04 full live run -- not in the original
        # recon sample, but the vocab was flagged open (plan §4.3).
        ("PENDING-DOCS-INSPECT", "pending"),
        # The PENDING-* family keeps growing across live runs -- each new
        # variant surfaced so far has belonged in `pending` (see the
        # STATUS_BUCKETS comment).
        ("PENDING-DOCUMENTS", "pending"),
        ("TEMPORARY", "provisional"),
        # Surfaced on the 2026-09-05 full live run -- a single occurrence,
        # not in the original recon sample; treated as a regulatory
        # limitation (cf. Probation/Suspended), to confirm with Ryan later.
        ("RESTRICTED", "enforcement"),
    ],
)
def test_new_mississippi_statuses_are_mapped(raw_word, canonical):
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
# 6. Age flags / ages_served / hours
# --------------------------------------------------------------------------- #


def test_age_flags_and_hours(spider_with_cities):
    response = _response("ms_results_page.html", meta={"page": 1, "county": "Attala", "county_value": "04"})
    items, _requests = split_requests(list(spider_with_cities.parse_results(response)))
    item = _item_by_id(items, "20006183")

    assert item["infant"] is True
    assert item["toddler"] is True
    assert item["preschool"] is True
    assert item["school"] is True
    assert item["ages_served"] == "Infant Care, 1 yr old, 2 yr old, 3 yr old, 4 yr old, 5 yr old Pre-Sch, 5-9 yr old"
    assert item["hours"] == "Mon-Fri 06:00 AM-06:00 PM; Sat 08:00 AM-03:00 PM; Sun 08:30 AM-03:00 PM"
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


# --------------------------------------------------------------------------- #
# 7. Services / subsidy / head start
# --------------------------------------------------------------------------- #


def test_services_subsidy_and_head_start(spider_with_cities):
    response = _response("ms_results_page.html", meta={"page": 1, "county": "Attala", "county_value": "04"})
    items, _requests = split_requests(list(spider_with_cities.parse_results(response)))

    item = _item_by_id(items, "20006183")
    assert item["ms_services"] == ["School Age After School", "Full Day"]
    assert item["scholarships_accepted"] is True
    assert item["ms_subsidy"] is True
    assert item["head_start"] is False
    assert item["ms_early_head_start"] is False

    # A Bright Start's services list includes "Special Needs" -- confirms the
    # split doesn't drop entries beyond the first two.
    item2 = _item_by_id(items, "20005047")
    assert "Special Needs" in item2["ms_services"]


# --------------------------------------------------------------------------- #
# 8. Inspections / Investigations / Monetary Penalties
# --------------------------------------------------------------------------- #


def test_all_three_inspection_types_on_one_provider(spider_with_cities):
    # 20012910 has all three tables populated (Sec 8).
    response = _response("ms_results_page.html", meta={"page": 1, "county": "Attala", "county_value": "04"})
    items, _requests = split_requests(list(spider_with_cities.parse_results(response)))
    item = _item_by_id(items, "20012910")

    inspections = item["inspections"]
    assert inspections and all(isinstance(i, InspectionItem) for i in inspections)

    by_type = {}
    for i in inspections:
        by_type.setdefault(i["type"], []).append(i)

    assert len(by_type["Inspection"]) == 14
    assert len(by_type["Investigation"]) == 3
    assert len(by_type["Monetary Penalty"]) == 3

    insp = by_type["Inspection"][0]
    assert insp["date"]
    assert insp["ms_end_date"]
    assert insp["ms_exam_type"]
    assert insp["original_status"]
    assert insp["report_url"].startswith(
        "https://www.mdhs.provider.webapps.ms.gov/PublicViewInspectionDocument.aspx?pdf="
    )

    inv = by_type["Investigation"][0]
    assert inv["date"]
    assert inv["ms_description"]
    assert inv["report_url"]

    mp = by_type["Monetary Penalty"][0]
    assert mp["date"]
    assert mp["ms_description"] == "Monetary Penalty Letter"
    assert mp["report_url"]


def test_empty_tables_yield_no_items(spider_with_cities):
    # 20005047 has inspections but NO investigations and NO monetary
    # penalties -- the "No ... were found." placeholder must not become
    # phantom InspectionItems.
    response = _response("ms_results_page.html", meta={"page": 1, "county": "Attala", "county_value": "04"})
    items, _requests = split_requests(list(spider_with_cities.parse_results(response)))
    item = _item_by_id(items, "20005047")

    inspections = item["inspections"]
    assert len(inspections) == 5
    assert all(i["type"] == "Inspection" for i in inspections)
    assert not any(i["type"] == "Investigation" for i in inspections)
    assert not any(i["type"] == "Monetary Penalty" for i in inspections)


# --------------------------------------------------------------------------- #
# 9-11. Pagination
# --------------------------------------------------------------------------- #


def test_pagination_mid_run_chains_next_page(spider_with_cities):
    # ms_results_page.html is trimmed to 4 of the real page's 25 providers,
    # but the stop rule is entirely pager-driven (never a row-count check --
    # unlike Kansas), so this still exercises the real continuation logic.
    response = _response("ms_results_page.html", meta={"page": 1, "county": "Attala", "county_value": "04"})
    items, requests = split_requests(list(spider_with_cities.parse_results(response)))

    assert len(items) == 4
    assert len(requests) == 1
    next_request = requests[0]
    assert next_request.meta["page"] == 2
    assert next_request.meta["cookiejar"] == "Attala"  # same jar as this county's every other request
    assert next_request.meta["county_value"] == "04"
    formdata = _formdata(next_request)
    assert formdata["__EVENTTARGET"] == "lvProvider$lvDataPager$ctl01$ctl01"
    assert formdata["__EVENTARGUMENT"] == ""
    # ddlCounty MUST be re-sent on every pager postback -- live-verified
    # 2026-09-05 that omitting it drops the county filter from page 2 on,
    # reverting the server to the full statewide (poison-page-prone) set.
    assert formdata["ddlCounty"] == "04"
    assert "btnFind" not in formdata  # never sent on a pager postback


def test_pagination_last_page_stops(spider_with_cities):
    # ms_pager_last.html is just the page-59 pager (window 56 57 58 [59],
    # Last disabled) -- no provider containers at all, so parse_results must
    # recognize this as a CLEAN stop (the pager itself confirms Last is
    # disabled with no next target) rather than flagging it as the
    # poison-page failure symptom, even though the row count is 0.
    response = _response("ms_pager_last.html", meta={"page": 59, "county": "Attala", "county_value": "04"})
    outputs = list(spider_with_cities.parse_results(response))
    assert outputs == []
    assert "Attala" not in spider_with_cities.failed_counties


def test_pager_stops_cleanly_when_last_disabled():
    # Exercises _parse_pager directly against the real page-59 pager markup
    # (Sec 5.2 regression guard): current==59, no current+1 link, Last
    # disabled -> no next target.
    from scrapy.http import HtmlResponse as _HtmlResponse

    body = open(os.path.join(FIXTURES, "ms_pager_last.html"), "rb").read()
    response = _HtmlResponse(url=SEARCH_URL, body=body, encoding="utf-8", request=Request(SEARCH_URL))
    pager_html = response.css("#lvProvider_lvDataPager").get()
    current, next_target, last_disabled = _parse_pager(pager_html)
    assert current == 59
    assert next_target is None
    assert last_disabled is True


def test_pager_first_page_targets_page_two():
    body = open(os.path.join(FIXTURES, "ms_results_page.html"), "rb").read()
    response = HtmlResponse(url=SEARCH_URL, body=body, encoding="utf-8", request=Request(SEARCH_URL))
    pager_html = response.css("#lvProvider_lvDataPager").get()
    current, next_target, last_disabled = _parse_pager(pager_html)
    assert current == 1
    assert next_target == "lvProvider$lvDataPager$ctl01$ctl01"
    assert last_disabled is False


def test_pager_falls_back_to_ellipsis_at_window_edge():
    # A synthesized mid-crawl window "... 5 6 7 8 [9] ..." -- current+1 (10)
    # isn't a direct numeric link in this window, so the trailing "..."
    # (next group jump) must be chosen instead (Sec 2.3 case 11).
    pager_html = (
        '<span id="lvProvider_lvDataPager">'
        '<input type="submit" name="lvProvider$lvDataPager$ctl00$ctl00" value="First" class="btn btn-default">'
        "\xa0"
        "<a href=\"javascript:__doPostBack('lvProvider$lvDataPager$ctl01$ctl00','')\">...</a>"
        "\xa0"
        "<a href=\"javascript:__doPostBack('lvProvider$lvDataPager$ctl01$ctl01','')\">5</a>"
        "\xa0"
        "<a href=\"javascript:__doPostBack('lvProvider$lvDataPager$ctl01$ctl02','')\">6</a>"
        "\xa0"
        "<a href=\"javascript:__doPostBack('lvProvider$lvDataPager$ctl01$ctl03','')\">7</a>"
        "\xa0"
        "<a href=\"javascript:__doPostBack('lvProvider$lvDataPager$ctl01$ctl04','')\">8</a>"
        "\xa0<span>9</span>\xa0"
        "<a href=\"javascript:__doPostBack('lvProvider$lvDataPager$ctl01$ctl05','')\">...</a>"
        "\xa0"
        '<input type="submit" name="lvProvider$lvDataPager$ctl02$ctl00" value="Last" class="btn btn-default">'
        "\xa0</span>"
    )
    current, next_target, last_disabled = _parse_pager(pager_html)
    assert current == 9
    assert next_target == "lvProvider$lvDataPager$ctl01$ctl05"
    assert last_disabled is False


# --------------------------------------------------------------------------- #
# 12. split_city
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
        # city name that is itself a suffix-collision risk ("Springs" alone
        # is not a known city, so this must match the full "OCEAN SPRINGS").
        ("42 GULF AVE OCEAN SPRINGS", "42 GULF AVE", "Ocean Springs"),
    ],
)
def test_split_city_success_cases(addr_head, expected_street, expected_city):
    street, city = split_city(addr_head, KNOWN_CITIES)
    assert street == expected_street
    assert city == expected_city


def test_split_city_fallback_when_no_known_city_matches():
    # the head doesn't end with any known city -> never guess.
    street, city = split_city("123 MAIN ST SOMEWHERE", KNOWN_CITIES)
    assert street == "123 MAIN ST SOMEWHERE"
    assert city is None


def test_split_city_word_boundary_guard():
    # "KOSCIUSKO" must not match inside a longer word with no space before it
    # (e.g. a hypothetical "...NKOSCIUSKO" glued run) -- the boundary check
    # requires the character before the match to be a space or the start.
    street, city = split_city("100 MAIN STNKOSCIUSKO", KNOWN_CITIES)
    assert city is None
    assert street == "100 MAIN STNKOSCIUSKO"


# --------------------------------------------------------------------------- #
# 13. Nested pager guard
# --------------------------------------------------------------------------- #


def test_nested_pager_with_page_two_link_warns_but_still_emits_first_page(spider_with_cities, caplog):
    response = _response("ms_results_nested_pager.html", meta={"page": 1, "county": "Attala", "county_value": "04"})
    items, _requests = split_requests(list(spider_with_cities.parse_results(response)))

    assert len(items) == 1
    item = items[0]
    assert item["ms_facility_id"] == "20005047"
    # v1 still emits the first (only fetched) page's rows.
    assert len(item["inspections"]) == 5

    assert any("20005047 Inspections table has more than one page" in r.message for r in caplog.records)


# --------------------------------------------------------------------------- #
# 14. Per-county robustness: dedupe + poison-page failure isolation
# --------------------------------------------------------------------------- #


def test_cross_county_duplicate_facility_id_is_skipped(spider_with_cities):
    # Defensive guard: every provider should belong to exactly one county,
    # so a repeated ms_facility_id across two county responses must not
    # double-count or double-yield.
    first = _response("ms_results_page.html", meta={"page": 1, "county": "Attala", "county_value": "04"})
    first_items, _ = split_requests(list(spider_with_cities.parse_results(first)))
    assert len(first_items) == 4
    assert spider_with_cities.duplicate_facility_ids == 0

    # Same fixture again, "in" a different county -- every id was already seen.
    second = _response("ms_results_page.html", meta={"page": 1, "county": "Oktibbeha", "county_value": "53"})
    second_items, second_requests = split_requests(list(spider_with_cities.parse_results(second)))
    assert second_items == []
    assert spider_with_cities.duplicate_facility_ids == 4
    # The (empty) county still gets a next-page request if its own pager says
    # so -- the dedupe only affects which items are yielded, not pagination.
    assert len(second_requests) == 1


def test_poison_page_symptom_fails_just_that_county_and_continues(spider_with_cities, caplog):
    # ms_search_page.html stands in for what Scrapy's RedirectMiddleware
    # hands parse_results after the poison-page 302 (module docstring item
    # 7): it auto-follows to the bare search page, which the live
    # reproduction confirmed has neither a results pager nor any provider
    # containers. Landing here on page > 1 (i.e. after at least one earlier
    # page succeeded) must fail ONLY this county, not raise or otherwise
    # disrupt the spider.
    poisoned = _response("ms_search_page.html", meta={"page": 3, "county": "Hinds", "county_value": "25"})
    outputs = list(spider_with_cities.parse_results(poisoned))
    assert outputs == []
    assert "Hinds" in spider_with_cities.failed_counties
    assert "page 3" in spider_with_cities.failed_counties["Hinds"]
    assert any("Hinds" in r.message and "poison-page" in r.message for r in caplog.records)

    # A different county's chain is completely unaffected -- no shared state
    # between counties beyond the dedupe set.
    healthy = _response("ms_results_page.html", meta={"page": 1, "county": "DeSoto", "county_value": "17"})
    items, _requests = split_requests(list(spider_with_cities.parse_results(healthy)))
    assert len(items) == 4
    assert "DeSoto" not in spider_with_cities.failed_counties


def test_empty_county_on_page_one_is_a_warning_not_a_failure(spider_with_cities, caplog):
    # ms_pager_last.html has a pager (First/Last both disabled -- a
    # legitimate single/last-page shape) but 0 provider containers. On page
    # 1 this must read as "possibly a genuinely tiny/empty county", not the
    # poison-page failure path.
    response = _response("ms_pager_last.html", meta={"page": 1, "county": "Issaquena", "county_value": "28"})
    outputs = list(spider_with_cities.parse_results(response))
    assert outputs == []
    assert "Issaquena" not in spider_with_cities.failed_counties
    assert any("Issaquena" in r.message and "may genuinely have none" in r.message for r in caplog.records)


# --------------------------------------------------------------------------- #
# 15. No undefined item fields (guards ms_* typos)
# --------------------------------------------------------------------------- #


def test_provider_and_inspection_items_reject_unknown_fields():
    item = ProviderItem()
    with pytest.raises(KeyError):
        item["ms_totally_made_up_field"] = True

    insp = InspectionItem()
    with pytest.raises(KeyError):
        insp["ms_also_made_up"] = True

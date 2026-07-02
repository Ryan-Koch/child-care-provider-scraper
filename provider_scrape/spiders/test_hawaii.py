import json
import os

import pytest
from scrapy.http import HtmlResponse, Request, TextResponse

from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.spiders.hawaii import (
    HawaiiSpider,
    build_area_index,
    code_table_map,
    convert_military_time,
    count_requirements_not_met,
    extract_braced_json,
    extract_embedded_json,
    extract_endpoint_urls,
    extract_inspection_details,
    extract_inspection_list_url,
    format_address,
    format_age_range,
    format_hours,
    format_phone,
    fully_qualified,
    subtree_codes,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def _load_text(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return fh.read()


def _load_json(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return json.load(fh)


@pytest.fixture
def spider():
    return HawaiiSpider()


@pytest.fixture
def area_spider(spider):
    """A spider with the area tree pre-loaded from the areas fixture."""
    rows = _load_json("hawaii_areas.json")["hanaResponse"]["codeTableRows"]
    spider.parent_of, spider.children = build_area_index(rows)
    return spider


def _text_response(url, body, meta=None):
    request = Request(url=url, meta=meta or {})
    return TextResponse(url=url, body=body.encode(), encoding="utf-8", request=request)


# ---- §6 SAS-URL extraction + fallback ----


def test_extract_endpoint_urls_finds_both():
    areas_url, search_url = extract_endpoint_urls(_load_text("hawaii_landing.html"))
    assert "5a3c6892c14442138e4b600e03411aa2" in areas_url
    assert "179f51f14f6a4837b49e82a3099bc3c3" in search_url
    assert areas_url.startswith("https://prod-28.usgovtexas.logic.azure.us")


def test_extract_endpoint_urls_returns_none_when_absent():
    areas_url, search_url = extract_endpoint_urls("<html><body>nothing here</body></html>")
    assert areas_url is None and search_url is None


def test_parse_landing_falls_back_to_hardcoded_urls(spider):
    from provider_scrape.spiders.hawaii import AREAS_URL, SEARCH_URL

    resp = _text_response("https://childcareprovidersearch.dhs.hawaii.gov/", "<html></html>")
    requests = list(spider.parse_landing(resp))
    # Still proceeds to the area-table fetch using the hardcoded fallback URL.
    assert len(requests) == 1
    assert requests[0].url == AREAS_URL
    assert spider.search_url == SEARCH_URL


# ---- §8.1 fully_qualified area-code builder ----


def test_fully_qualified_resolves_deep_leaf(area_spider):
    # JO under AH under AB -> ABAHJO.
    assert fully_qualified("JO", area_spider.parent_of) == "ABAHJO"


def test_fully_qualified_top_level_island_is_bare(area_spider):
    assert fully_qualified("AB", area_spider.parent_of) == "AB"


def test_fully_qualified_never_emits_root(area_spider):
    # The synthetic root AA is excluded from every qualified code.
    assert "AA" not in fully_qualified("BW", area_spider.parent_of)
    assert fully_qualified("BW", area_spider.parent_of) == "ABAHBW"


def test_subtree_codes_collects_descendants(area_spider):
    codes = set(subtree_codes("AD", area_spider.children))
    assert codes == {"AD", "AO", "AP"}


def test_build_area_index_islands_are_children_of_root(area_spider):
    assert area_spider.children["AA"] == ["AB", "AC", "AD", "AE", "AF", "AG"]
    assert area_spider.parent_of["AB"] == "AA"


# ---- §8.2 + §8.4 search: one-provider-many-services, dedupe ----


def _search_response(area_spider, fixture="hawaii_search_lanai.json", area_code="AD"):
    return _text_response(
        area_spider.search_url,
        json.dumps(_load_json(fixture)),
        meta={"area_code": area_code, "island_name": "Lanai", "single": False},
    )


def test_parse_search_emits_one_request_per_service(area_spider):
    requests = list(area_spider.parse_search(_search_response(area_spider)))
    # 3 providers but 4 services (XPLOR has 2) -> 4 detail requests.
    assert len(requests) == 4
    service_ids = {r.meta["partial_item"]["hi_service_id"] for r in requests}
    assert service_ids == {91187, 91655, 91700, 91800}
    for req in requests:
        item = req.meta["partial_item"]
        assert item["provider_url"] == req.url
        assert item["source_state"] == "Hawaii"
        assert item["county"] == "Lanai"


def test_parse_search_dedupes_across_queries(area_spider):
    list(area_spider.parse_search(_search_response(area_spider)))
    # A second query surfacing the same serviceIds yields nothing new.
    requests = list(area_spider.parse_search(_search_response(area_spider)))
    assert requests == []


def test_partial_item_carries_search_fields(area_spider):
    requests = list(area_spider.parse_search(_search_response(area_spider)))
    xplor = next(r for r in requests if r.meta["partial_item"]["hi_service_id"] == 91187)
    item = xplor.meta["partial_item"]
    assert item["provider_name"] == "XPLOR EDUCATION PRESCHOOL"
    assert item["license_holder"] == "XPLOR EDUCATION INC"
    assert item["hi_provider_id"] == 40001
    assert item["hi_service_type_code"] == "03"
    assert item["hi_area_code"] == "ADAO"
    assert item["hi_provider_kind"] == "OR"


# ---- §8.3 cap detection + recursion ----


def test_parse_search_subdivides_on_cap(spider):
    spider.parent_of = {"AG": "AA", "AG1": "AG", "AG2": "AG"}
    spider.children = {"AA": ["AG"], "AG": ["AG1", "AG2"], "AG1": [], "AG2": []}
    capped = {
        "hanaResponse": {
            "results": [
                {"providerId": i, "providerType": "OR", "name": f"P{i}",
                 "services": [{"serviceId": i, "serviceType": "03",
                               "serviceName": f"S{i}", "area": "AG1"}]}
                for i in range(100)
            ]
        }
    }
    resp = _text_response(
        spider.search_url,
        json.dumps(capped),
        meta={"area_code": "AG", "island_name": "Oahu", "single": False},
    )
    requests = list(spider.parse_search(resp))
    # Capped: re-query each child subtree plus the node alone; no detail fetches.
    assert len(requests) == 3
    assert all(r.url == spider.search_url for r in requests)
    single_flags = sorted(r.meta["single"] for r in requests)
    assert single_flags == [False, False, True]


def test_parse_search_single_query_is_not_subdivided(spider):
    spider.parent_of = {"AG": "AA"}
    spider.children = {"AG": []}
    capped = {
        "hanaResponse": {
            "results": [
                {"providerId": i, "providerType": "OR", "name": f"P{i}",
                 "services": [{"serviceId": i, "serviceType": "03",
                               "serviceName": f"S{i}", "area": "AG"}]}
                for i in range(100)
            ]
        }
    }
    resp = _text_response(
        spider.search_url,
        json.dumps(capped),
        meta={"area_code": "AG", "island_name": "Oahu", "single": True},
    )
    requests = list(spider.parse_search(resp))
    # A single-code query can't subdivide further; take what we got.
    assert all("details" in r.url for r in requests)
    assert len(requests) == 100


# ---- §8.5 OR vs CG address handling ----


def test_format_address_or_includes_street():
    addr = {"street1": "16-120 OPUKAHA IA STREET", "street2": None, "building": None,
            "city": "KEAAU", "state": "HI", "zipCode": 96749}
    assert format_address(addr, "OR") == "16-120 OPUKAHA IA STREET, KEAAU, HI 96749"


def test_format_address_cg_suppresses_street():
    addr = {"street1": "123 SECRET LN", "street2": None, "building": None,
            "city": "HILO", "state": "HI", "zipCode": 96720}
    result = format_address(addr, "CG")
    assert "SECRET" not in result
    assert result == "HILO, HI 96720"


def test_format_address_none_when_empty():
    assert format_address(None, "OR") is None


# ---- §8.6 embedded-JSON extraction + invalid service id ----


def test_extract_embedded_json_pulls_bundle_from_real_html():
    bundle = extract_embedded_json(_load_text("hawaii_detail.html"), "response")
    assert bundle["summary"]["hanaResponse"]["serviceId"] == 92021
    assert bundle["details"]["hanaResponse"]["meals"] == ["AS", "BR", "LU"]


def test_extract_embedded_json_missing_returns_none():
    assert extract_embedded_json("<html>no const here</html>", "response") is None


def test_parse_detail_invalid_service_id_yields_no_item(spider):
    html = (
        "<html><body><script>const response = `"
        '{"summary":{"hanaResponseStatus":{"responseCode":400},"hanaResponse":{}},'
        '"details":{"hanaResponse":{}},"history":{"hanaResponse":{}}}`'
        "</script></body></html>"
    )
    item = ProviderItem()
    item["hi_service_id"] = 99999
    resp = HtmlResponse(
        url="https://childcareprovidersearch.dhs.hawaii.gov/details/?serviceId=99999",
        body=html.encode(),
        encoding="utf-8",
        request=Request(url="https://x", meta={"partial_item": item}),
    )
    resp.meta["partial_item"] = item
    assert list(spider.parse_detail(resp)) == []


# ---- Detail golden path against the real captured page ----


def _detail_response(spider, service_id=92021, provider_kind="OR"):
    item = ProviderItem()
    item["source_state"] = "Hawaii"
    item["hi_service_id"] = service_id
    item["hi_provider_kind"] = provider_kind
    item["inspections"] = []
    body = _load_text("hawaii_detail.html").encode()
    resp = HtmlResponse(
        url=f"https://childcareprovidersearch.dhs.hawaii.gov/details/?serviceId={service_id}",
        body=body,
        encoding="utf-8",
        request=Request(url="https://x", meta={"partial_item": item}),
    )
    resp.meta["partial_item"] = item
    return resp


def test_parse_detail_golden_path(spider):
    # parse_detail now chains to an inspections fetch; the fully populated item
    # rides along in that request's meta.
    out = list(spider.parse_detail(_detail_response(spider)))
    assert len(out) == 1
    request = out[0]
    assert "inspections/?serviceId=92021" in request.url
    item = request.meta["partial_item"]
    assert item["provider_name"] == "PUNANA LEO O HILO INFANT TODDLER"
    assert item["license_holder"] == "AHA PUNANA LEO INC"
    assert item["provider_type"] == "Infant and Toddler Center"
    assert item["license_number"] == 17993
    assert item["license_begin_date"] == "2025-09-08"
    assert item["license_expiration"] == "2026-09-07"
    assert item["capacity"] == 9
    assert item["hi_license_type"] == "Regular"
    assert item["ages_served"] == "over 9 months - under 3 years"
    # OR provider: full street address.
    assert item["address"] == "16-120 OPUKAHA IA STREET, KEAAU, HI 96749"
    assert item["hi_mailing_address"] == "96 PUUHONU PL, HILO, HI 96720"
    # Phone normalized from the "NA"-prefixed value.
    assert item["phone"] == "(808) 935-4304"
    # Meals codes mapped to descriptions.
    assert item["hi_meals"] == ["Afternoon snack", "Breakfast", "Lunch"]
    assert item["hi_accreditations"] == []
    # History: last entry is the current status.
    assert item["status"] == "Active"
    assert item["status_date"] == "2023-09-22"
    assert len(item["hi_license_history"]) == 4
    # Hours: Mon-Fri 7:30 AM - 3:30 PM, emitted in week order.
    assert item["hours"] == (
        "Mon 7:30 AM - 3:30 PM; Tue 7:30 AM - 3:30 PM; Wed 7:30 AM - 3:30 PM;"
        " Thu 7:30 AM - 3:30 PM; Fri 7:30 AM - 3:30 PM"
    )


def test_parse_detail_loads_code_tables(spider):
    list(spider.parse_detail(_detail_response(spider)))
    assert spider.service_type_map["05"] == "Infant and Toddler Center"
    assert spider.meals_map["BR"] == "Breakfast"
    # Languages and accreditations tables are present too.
    assert len(spider.languages_map) > 0


def test_detail_errback_emits_partial_item(spider):
    item = ProviderItem()
    item["hi_service_id"] = 555
    item["provider_name"] = "PARTIAL"

    class _Failure:
        request = Request(url="https://x", meta={"partial_item": item})

    out = list(spider.detail_errback(_Failure()))
    assert out == [item]


# ---- §8.7 age translation ----


def test_format_age_range_weeks_and_months():
    assert format_age_range("W", 6, "M", 36) == "over 6 weeks - under 3 years"
    assert format_age_range("M", 9, "M", 36) == "over 9 months - under 3 years"


def test_format_age_range_handles_nulls():
    assert format_age_range("M", None, "M", None) is None
    assert format_age_range("M", 9, "M", None) == "over 9 months"
    assert format_age_range(None, None, "W", 6) == "under 6 weeks"


# ---- §8.8 contact modes ----


def test_format_phone_normalizes_and_strips_prefix():
    assert format_phone("NA8089354304") == "(808) 935-4304"
    assert format_phone("IN44123") == "44123"
    assert format_phone(None) is None
    assert format_phone("") is None


def test_fill_contacts_handles_null_and_website(spider):
    item = ProviderItem()
    # None contact modes -> no crash, nothing set.
    spider._fill_contacts(item, None)
    assert item.get("phone") is None
    assert item.get("email") is None

    item2 = ProviderItem()
    spider._fill_contacts(item2, [
        {"mode": "WW", "value": "https://example.com"},
        {"mode": "EM", "value": "info@example.com"},
        {"mode": "PH", "value": "NA8089354304"},
    ])
    assert item2["provider_website"] == "https://example.com"
    assert item2["email"] == "info@example.com"
    assert item2["phone"] == "(808) 935-4304"


# ---- §8.9 hours conversion ----


def test_convert_military_time():
    assert convert_military_time(730) == "7:30 AM"
    assert convert_military_time(1530) == "3:30 PM"
    assert convert_military_time(0) == "12:00 AM"
    assert convert_military_time(1200) == "12:00 PM"
    assert convert_military_time(None) is None


def test_format_hours_orders_days_and_handles_empty():
    shifts = [{"shiftNumber": 1, "hours": [
        {"weekdayNumber": 6, "startTime": 730, "endTime": 1530},
        {"weekdayNumber": 2, "startTime": 730, "endTime": 1530},
    ]}]
    assert format_hours(shifts) == "Mon 7:30 AM - 3:30 PM; Fri 7:30 AM - 3:30 PM"
    assert format_hours([]) is None
    assert format_hours(None) is None


# ---- §8.10 meals / accreditations code mapping ----


def test_code_table_map_falls_back_to_code():
    table = {"hanaResponse": {"codeTableRows": [
        {"code": "BR", "description": "Breakfast"},
        {"code": "XX", "description": None},
    ]}}
    mapping = code_table_map(table)
    assert mapping["BR"] == "Breakfast"
    # Null description falls back to the raw code.
    assert mapping["XX"] == "XX"


def test_fill_details_unknown_meal_code_does_not_raise(spider):
    spider.meals_map = {"BR": "Breakfast"}
    spider.accreditations_map = {}
    item = ProviderItem()
    item["hi_provider_kind"] = "OR"
    details = {
        "locationAddress": {"city": "HILO", "state": "HI", "zipCode": 96720},
        "meals": ["BR", "ZZ"],
        "accreditations": [],
        "shifts": [],
    }
    spider.fill_details(item, details)
    # Known code maps; unknown falls back to the raw code rather than raising.
    assert item["hi_meals"] == ["Breakfast", "ZZ"]
    assert item["hi_accreditations"] == []


# ---- Inspections ----


def _inspections_response(spider, service_id=41747, item=None):
    if item is None:
        item = ProviderItem()
        item["hi_service_id"] = service_id
        item["inspections"] = []
    body = _load_text("hawaii_inspections.html").encode()
    resp = HtmlResponse(
        url=f"https://childcareprovidersearch.dhs.hawaii.gov/inspections/?serviceId={service_id}",
        body=body,
        encoding="utf-8",
        request=Request(url="https://x", meta={"partial_item": item}),
    )
    resp.meta["partial_item"] = item
    return resp


def test_extract_braced_json_reads_cached_list():
    cached = extract_braced_json(_load_text("hawaii_inspections.html"), "const cachedList = ")
    summaries = cached["hanaResponse"]["visitSummaries"]
    assert len(summaries) == 2
    assert summaries[0]["visitId"] == 14341


def test_extract_inspection_details_keys_by_visit_id():
    details = extract_inspection_details(_load_text("hawaii_inspections.html"))
    assert set(details) == {18408}
    assert details[18408]["hanaResponse"]["visitType"] == "LR"


def test_count_requirements_not_met_counts_n_flags():
    details = extract_inspection_details(_load_text("hawaii_inspections.html"))
    # One visitDetails item has itemReqMet == "N".
    assert count_requirements_not_met(details[18408]) == 1
    assert count_requirements_not_met(None) is None


def _visit_detail_payload(not_met, code=200):
    """A per-visit detail response with `not_met` requirements flagged 'N'."""
    items = [{"itemReqMet": "N"} for _ in range(not_met)]
    items += [{"itemReqMet": "Y"}, {"itemReqMet": "X"}, {"itemReqMet": None}]
    return {
        "hanaResponseStatus": {"responseCode": code},
        "hanaResponse": {"visitDetails": items},
    }


def test_parse_inspections_warm_cache_counts_embedded_and_fetches_rest(spider):
    out = list(spider.parse_inspections(_inspections_response(spider)))
    # 18408's detail is embedded (counted in place); 14341 needs a per-visit fetch,
    # so parse_inspections yields that POST rather than the item.
    assert len(out) == 1
    request = out[0]
    assert request.method == "POST"
    assert "85cc14072d62409fbfc0c2a7ea3da2bd" in request.url
    assert json.loads(request.body) == {"visitId": "14341"}

    state = request.meta["state"]
    item = state["item"]
    inspections = {i["hi_visit_id"]: i for i in item["inspections"]}
    assert len(inspections) == 2
    assert all(isinstance(i, InspectionItem) for i in inspections.values())
    assert inspections[18408]["type"] == "Annual/Biennial"
    assert inspections[18408]["hi_requirements_not_met"] == 1  # embedded
    assert inspections[14341]["type"] == "Off-year"
    assert inspections[14341].get("hi_requirements_not_met") is None
    assert state["pending"] == 1

    # Resolving the outstanding visit-detail fetch sets the count and emits.
    resp = _text_response(
        request.url, json.dumps(_visit_detail_payload(2)), meta=request.meta
    )
    out2 = list(spider.parse_visit_detail(resp))
    assert out2 == [item]
    assert inspections[14341]["hi_requirements_not_met"] == 2


def test_extract_inspection_list_url_finds_cold_cache_endpoint():
    url = extract_inspection_list_url(_load_text("hawaii_inspections_nocache.html"))
    assert url is not None
    assert "e17345f72a8f424597023d79e2608fb2" in url


def test_parse_inspections_cold_cache_falls_back_to_list_endpoint(spider):
    item = ProviderItem()
    item["hi_service_id"] = 34
    item["inspections"] = []
    body = _load_text("hawaii_inspections_nocache.html")
    resp = _text_response(
        "https://childcareprovidersearch.dhs.hawaii.gov/inspections/?serviceId=34",
        body,
        meta={"partial_item": item},
    )
    out = list(spider.parse_inspections(resp))
    # No embedded cachedList -> a POST to the live list endpoint, not an item yet.
    assert len(out) == 1
    request = out[0]
    assert request.method == "POST"
    assert "e17345f72a8f424597023d79e2608fb2" in request.url
    assert json.loads(request.body) == {"serviceId": "34"}
    assert request.meta["partial_item"] is item


def test_parse_inspections_cold_cache_uses_fallback_url_when_absent(spider):
    from provider_scrape.spiders.hawaii import INSPECTION_LIST_URL

    item = ProviderItem()
    item["hi_service_id"] = 34
    item["inspections"] = []
    resp = _text_response(
        "https://childcareprovidersearch.dhs.hawaii.gov/inspections/?serviceId=34",
        "<html><body>no cached list, no embedded url here</body></html>",
        meta={"partial_item": item},
    )
    out = list(spider.parse_inspections(resp))
    assert out[0].url == INSPECTION_LIST_URL


def _list_payload(*visits):
    return {
        "hanaResponseStatus": {"responseCode": 200},
        "hanaResponse": {"serviceId": 34, "visitSummaries": list(visits)},
    }


def test_parse_inspection_list_dispatches_a_visit_detail_fetch_per_visit(spider):
    item = ProviderItem()
    item["hi_service_id"] = 34
    item["inspections"] = []
    payload = _list_payload(
        {"visitId": 15516, "visitType": "LR", "visitDate": "2024-10-15",
         "licensingPeriodStart": "2024-11-20", "licensingPeriodEnd": "2026-10-31"},
        {"visitId": 20542, "visitType": "LO", "visitDate": "2025-10-13",
         "licensingPeriodStart": "2024-11-01", "licensingPeriodEnd": "2026-10-31"},
    )
    resp = _text_response(
        "https://prod-26.usgovtexas.logic.azure.us/x", json.dumps(payload),
        meta={"partial_item": item},
    )
    out = list(spider.parse_inspection_list(resp))
    # No embedded detail on this path: one visit-detail POST per visit, no item yet.
    assert len(out) == 2
    assert all(r.method == "POST" for r in out)
    assert all("85cc14072d62409fbfc0c2a7ea3da2bd" in r.url for r in out)
    assert [json.loads(r.body)["visitId"] for r in out] == ["15516", "20542"]
    state = out[0].meta["state"]
    assert state["pending"] == 2

    # First fetch resolves (count 0) -> still pending, no emit.
    resp1 = _text_response(out[0].url, json.dumps(_visit_detail_payload(0)), meta=out[0].meta)
    assert list(spider.parse_visit_detail(resp1)) == []
    # Last fetch resolves (count 3) -> item emitted with both counts populated.
    resp2 = _text_response(out[1].url, json.dumps(_visit_detail_payload(3)), meta=out[1].meta)
    final = list(spider.parse_visit_detail(resp2))
    assert final == [state["item"]]
    counts = {i["hi_visit_id"]: i["hi_requirements_not_met"] for i in state["item"]["inspections"]}
    assert counts == {15516: 0, 20542: 3}


def test_parse_visit_detail_error_response_leaves_count_unset_but_emits(spider):
    item = ProviderItem()
    item["hi_service_id"] = 34
    item["inspections"] = [InspectionItem()]
    inspection = item["inspections"][0]
    inspection["hi_visit_id"] = 15516
    state = {"item": item, "pending": 1}
    resp = _text_response(
        "https://prod-05.usgovtexas.logic.azure.us/x",
        json.dumps(_visit_detail_payload(0, code=500)),
        meta={"state": state, "inspection": inspection},
    )
    out = list(spider.parse_visit_detail(resp))
    assert out == [item]
    assert inspection.get("hi_requirements_not_met") is None


def test_visit_detail_errback_emits_item_when_last(spider):
    item = ProviderItem()
    item["hi_service_id"] = 34
    item["inspections"] = [InspectionItem()]
    inspection = item["inspections"][0]
    inspection["hi_visit_id"] = 15516
    state = {"item": item, "pending": 1}

    class _Failure:
        request = Request(
            url="https://x", meta={"state": state, "inspection": inspection}
        )

    out = list(spider.visit_detail_errback(_Failure()))
    assert out == [item]
    assert inspection.get("hi_requirements_not_met") is None


def test_parse_inspection_list_handles_error_response(spider):
    item = ProviderItem()
    item["hi_service_id"] = 34
    item["inspections"] = []
    payload = {"hanaResponseStatus": {"responseCode": 500}, "hanaResponse": {}}
    resp = _text_response(
        "https://prod-26.usgovtexas.logic.azure.us/x", json.dumps(payload),
        meta={"partial_item": item},
    )
    out = list(spider.parse_inspection_list(resp))
    assert out[0]["inspections"] == []


def test_inspections_errback_emits_item(spider):
    item = ProviderItem()
    item["hi_service_id"] = 555
    item["inspections"] = []

    class _Failure:
        request = Request(url="https://x", meta={"partial_item": item})

    out = list(spider.inspections_errback(_Failure()))
    assert out == [item]


def test_fill_details_maps_populated_accreditations(spider):
    spider.meals_map = {}
    spider.accreditations_map = {"02": "NECPA"}
    item = ProviderItem()
    item["hi_provider_kind"] = "OR"
    details = {
        "locationAddress": {"city": "HILO", "state": "HI", "zipCode": 96720},
        "meals": [],
        # Accreditations are dicts keyed by accreditationType, with dates.
        "accreditations": [
            {"accreditationType": "02", "effectiveDate": "2021-10-31",
             "expirationDate": "2027-10-31"},
            {"accreditationType": "99", "effectiveDate": None, "expirationDate": None},
        ],
        "shifts": [],
    }
    spider.fill_details(item, details)
    assert item["hi_accreditations"] == [
        {"name": "NECPA", "effective_date": "2021-10-31", "expiration_date": "2027-10-31"},
        # Unknown code falls back to the raw code rather than raising.
        {"name": "99", "effective_date": None, "expiration_date": None},
    ]

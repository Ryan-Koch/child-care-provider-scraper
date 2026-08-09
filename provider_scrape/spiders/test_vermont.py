import os

import pytest
from scrapy.http import HtmlResponse, Request

from provider_scrape.normalization import (
    facility_category_from_type,
    normalize_item,
)
from provider_scrape.spiders.vermont import (
    VermontSpider,
    _content_after,
    _hidden,
    _num,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")
RESULTS_URL = "https://www.brightfutures.dcf.state.vt.us/vtcc/process.do?results"
DETAIL_URL = "https://www.brightfutures.dcf.state.vt.us/vtcc/process.do?detail"


def _read_bytes(name):
    with open(os.path.join(FIXTURES, name), "rb") as fh:
        return fh.read()


def _detail_text():
    return _read_bytes("vermont_detail.html").decode("latin-1")


def _response(name, url):
    return HtmlResponse(
        url=url, body=_read_bytes(name), encoding="latin-1",
        request=Request(url),
    )


@pytest.fixture
def spider():
    return VermontSpider()


# --------------------------------------------------------------------------- #
# Pure helpers
# --------------------------------------------------------------------------- #

def test_hidden_reads_input_values():
    text = _detail_text()
    assert _hidden(text, "field_name2") == "Wareing, Pennie"
    assert _hidden(text, "field_pid2") == "3053"
    assert _hidden(text, "field_does_not_exist") is None


def test_content_after_reads_labelled_value():
    text = _detail_text()
    assert _content_after(text, "field_ltype3") == "Registered Family Child Care Home"
    assert _content_after(text, "field_pnum3") == "(802)468-2993"
    # An empty content1 span reads as missing, not "".
    assert _content_after(text, "field_web3") is None


def test_content_after_joins_multiline_address():
    # <br>-separated lines become newline-separated text.
    addr = _content_after(_detail_text(), "field_addr3")
    assert addr.split("\n") == ["1434 Route 30 North", "Castleton, VT 05732",
                                "City: Bomoseen"]


def test_num_coerces_only_clean_integers():
    assert _num("10") == 10
    assert _num("4                 ".strip()) == 4
    assert _num("") == ""      # caller strips; empty passes through
    assert _num(None) is None
    assert _num("6-12") == "6-12"


# --------------------------------------------------------------------------- #
# parse_detail
# --------------------------------------------------------------------------- #

@pytest.fixture
def detail_item(spider):
    resp = _response("vermont_detail.html", DETAIL_URL)
    items = list(spider.parse_detail(resp))
    assert len(items) == 1
    return items[0]


def test_parse_detail_core_fields(detail_item):
    assert detail_item["source_state"] == "Vermont"
    assert detail_item["provider_name"] == "Wareing, Pennie"
    assert detail_item["vt_provider_id"] == "3053"
    # No published license number -> Provider ID stands in.
    assert detail_item["license_number"] == "3053"
    assert detail_item["provider_type"] == "Registered Family Child Care Home"
    assert detail_item["status"] == "Registered"


def test_parse_detail_address_drops_postal_city(detail_item):
    # The "City: <postal city>" line is dropped; street + municipal line kept.
    assert detail_item["address"] == "1434 Route 30 North, Castleton, VT 05732"


def test_parse_detail_contact_and_dates(detail_item):
    assert detail_item["phone"] == "(802)468-2993"
    assert detail_item["email"] == "penniehartwareing@gmail.com"
    assert detail_item["provider_website"] is None
    assert detail_item["license_begin_date"] == "07/01/2005"


def test_parse_detail_capacity_and_vacancies(detail_item):
    assert detail_item["infant"] == 2
    assert detail_item["toddler"] == 2
    assert detail_item["preschool"] == 2
    assert detail_item["school"] == 4
    assert detail_item["capacity"] == 10
    assert detail_item["vt_current_vacancy"] == 0
    assert detail_item["vt_vacancy_as_of"] == "06/24/2026"


def test_parse_detail_program_fields(detail_item):
    assert detail_item["ages_served"] == "Infant, Toddler, Preschool, School Age"
    assert detail_item["languages"] == "English"
    assert detail_item["hours"] == "07:30:00 to 16:30:00"
    assert detail_item["transportation"] == "School Bus Route"
    assert detail_item["school_district"] == "Castleton-Hubbardton USD #42"
    assert detail_item["meals"] == "Allergy Awareness, Special Diets"
    assert detail_item["scholarships_accepted"] == "Yes"
    assert detail_item["vt_star_level"] == "1 Star"
    assert detail_item["vt_building_type"] == "House"
    assert detail_item["vt_area_description"] == "Fenced Yard"


def test_parse_detail_site_visits_become_inspections(detail_item):
    insp = detail_item["inspections"]
    # The fixture's Site Visits section lists six dated visits.
    assert len(insp) == 6
    assert insp[0]["date"] == "12/09/2025"
    assert insp[0]["type"] == "Site Visit"
    assert all(i["type"] == "Site Visit" for i in insp)


# --------------------------------------------------------------------------- #
# parse_results (link harvesting + pagination)
# --------------------------------------------------------------------------- #

@pytest.fixture
def results_response():
    # The captured search fixture is a subtable fragment; wrap it so xpath sees
    # a normal document, mirroring how the live results page serves it.
    frag = _read_bytes("vermont_search.html").decode("latin-1")
    html = f"<html><body><table>{frag}</table></body></html>"
    return HtmlResponse(url=RESULTS_URL, body=html.encode("latin-1"),
                        encoding="latin-1", request=Request(RESULTS_URL))


def test_parse_results_harvests_details_and_next(spider, results_response):
    reqs = list(spider.parse_results(results_response))
    details = [r for r in reqs if r.callback == spider.parse_detail]
    pages = [r for r in reqs if r.callback == spider.parse_results]
    # 20 providers per results page, and exactly one "next" page request.
    assert len(details) == 20
    assert len(pages) == 1
    # Detail requests are isolated from the crawl's session cookie.
    assert all(r.meta.get("dont_merge_cookies") for r in details)


def test_parse_results_bounce_retries_the_search(spider):
    # A bounce re-renders the search form (has eventSubmit_doSearch, no Details).
    form = (
        "<html><body><form name='main' method='post' "
        "action='https://www.brightfutures.dcf.state.vt.us/vtcc/process.do?x'>"
        "<input type='hidden' name='action' value='MaximusFormProcessAction'>"
        "<input type='submit' name='eventSubmit_doSearch' value='Search'>"
        "</form></body></html>"
    )
    resp = HtmlResponse(url=RESULTS_URL, body=form.encode("latin-1"),
                        encoding="latin-1", request=Request(RESULTS_URL))
    resp.meta["search_attempt"] = 1
    reqs = list(spider.parse_results(resp))
    # Exactly one retry, carrying an incremented attempt counter.
    assert len(reqs) == 1
    assert reqs[0].meta["search_attempt"] == 2


def test_parse_results_bounce_gives_up_at_limit(spider):
    form = (
        "<html><body><form name='main' method='post' "
        "action='https://www.brightfutures.dcf.state.vt.us/vtcc/process.do?x'>"
        "<input type='submit' name='eventSubmit_doSearch' value='Search'>"
        "</form></body></html>"
    )
    resp = HtmlResponse(url=RESULTS_URL, body=form.encode("latin-1"),
                        encoding="latin-1", request=Request(RESULTS_URL))
    resp.meta["search_attempt"] = VermontSpider.MAX_SEARCH_ATTEMPTS
    assert list(spider.parse_results(resp)) == []


# --------------------------------------------------------------------------- #
# Normalization integration (facility_category from VT license types)
# --------------------------------------------------------------------------- #

def test_facility_category_for_vt_license_types():
    assert facility_category_from_type(
        "Registered Family Child Care Home") == "family_home"
    assert facility_category_from_type(
        "Center Based Child Care and Preschool Program") == "center"
    assert facility_category_from_type(
        "Afterschool Child Care Program") == "school_age"
    assert facility_category_from_type(
        "Licensed Family Child Care Home") == "family_home"
    assert facility_category_from_type(
        "Center Based Child Care and Preschool Program - Non-Recurring") == "center"


def test_normalize_item_sets_family_home_category(spider):
    resp = _response("vermont_detail.html", DETAIL_URL)
    item = next(iter(spider.parse_detail(resp)))
    data = normalize_item(dict(item), spider.name)
    assert data["facility_category"] == "family_home"
    # provider_type is preserved (additive, D2).
    assert data["provider_type"] == "Registered Family Child Care Home"
    # status normalizes into the canonical "active" bucket.
    assert data["status"] == "active"

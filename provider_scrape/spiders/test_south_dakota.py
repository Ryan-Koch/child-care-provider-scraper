import os

import pytest
from scrapy.http import HtmlResponse, Request

from provider_scrape.spiders.south_dakota import (
    DETAIL,
    SouthDakotaSpider,
    _badges,
    _capacity,
    _clean_phone,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures", "south_dakota")

LIST_PENDING_URL = (
    "https://olapublic.sd.gov/child-care-provider-search/?search=true&providerType=Child+Care&status=Pending"
)


def _response(name, url):
    with open(os.path.join(FIXTURES, name), "rb") as fh:
        body = fh.read()
    return HtmlResponse(url=url, body=body, encoding="utf-8", request=Request(url))


@pytest.fixture
def spider():
    return SouthDakotaSpider()


# --------------------------------------------------------------------------- #
# Pure helpers
# --------------------------------------------------------------------------- #


def test_badges_reads_script_arg_not_spans():
    with open(os.path.join(FIXTURES, "detail_center.html")) as f:
        text = f.read()
    assert _badges(text, "servicesForm") == ["After School"]
    assert _badges(text, "monthsOfOperationForm") == ["12 Months"]
    ages = _badges(text, "agesChildrenForm")
    # HTML entity in the raw script arg ("&#x2B;") must be decoded.
    assert "School age (5+ years of age)" in ages


def test_badges_empty_script_arg_is_empty_list():
    with open(os.path.join(FIXTURES, "detail_informal.html")) as f:
        text = f.read()
    assert _badges(text, "servicesForm") == []
    assert _badges(text, "agesChildrenForm") == []
    assert _badges(text, "monthsOfOperationForm") == []


def test_capacity_blank_is_none_not_zero():
    assert _capacity("") is None
    assert _capacity(None) is None
    assert _capacity("360") == 360
    assert isinstance(_capacity("360"), int)


def test_clean_phone_drops_digitless_placeholder():
    # Live site quirk: providers with no phone on file still render a
    # formatted-but-empty template ("+ () -"), driven by a ?phone=00000000000
    # placeholder param -- not a real number.
    assert _clean_phone("+ () -") is None
    assert _clean_phone("() -") is None
    assert _clean_phone("") is None
    assert _clean_phone(None) is None
    assert _clean_phone("(605) 747-4441") == "(605) 747-4441"


# --------------------------------------------------------------------------- #
# Case 1: List parse
# --------------------------------------------------------------------------- #


def test_parse_results_yields_detail_requests_with_facet_status(spider):
    resp = _response("list_pending.html", LIST_PENDING_URL)
    resp.meta["raw_status"] = "Pending"

    # Simulate the other 3 facets already having been collected (empty),
    # so this call is the one that reaches the dispatch threshold.
    spider.facet_rows = {"Operational": [], "Closed": [], None: []}

    reqs = list(spider.parse_results(resp))

    # 39 Pending rows on the live fixture; every request must carry the
    # Pending status and a clean (no ?phone=) detail URL.
    assert len(reqs) == 39
    for req in reqs:
        assert req.callback == spider.parse_detail
        assert req.meta["raw_status"] == "Pending"
        assert "?phone=" not in req.url
        assert req.url.startswith("https://olapublic.sd.gov/child-care-program-profile/")
        assert req.url.rsplit("/", 1)[1].isdigit()


def test_parse_results_waits_for_all_four_facets(spider):
    resp = _response("list_pending.html", LIST_PENDING_URL)
    resp.meta["raw_status"] = "Pending"
    # Only 1 of 4 facets collected -- dispatch must not fire yet.
    spider.facet_rows = {}
    reqs = list(spider.parse_results(resp))
    assert reqs == []
    assert "Pending" in spider.facet_rows


# --------------------------------------------------------------------------- #
# Case 2: Detail golden path
# --------------------------------------------------------------------------- #


@pytest.fixture
def center_item(spider):
    url = DETAIL.format(id="79779")
    resp = _response("detail_center.html", url)
    resp.meta["raw_status"] = "Operational"
    resp.meta["list_phone"] = "(605) 622-7000"
    items = list(spider.parse_detail(resp))
    assert len(items) == 1
    return items[0]


def test_parse_detail_core_fields(center_item):
    assert center_item["source_state"] == "South Dakota"
    assert center_item["provider_name"] == "YMCA YOUTH DEVELOPMENT CENTER"
    assert center_item["license_number"] == "011008567"
    assert isinstance(center_item["license_number"], str)
    assert center_item["provider_type"] == "Licensed Center"
    assert center_item["address"] == "6 S State St, Aberdeen, SD 57401, USA"
    assert center_item["capacity"] == 360
    assert isinstance(center_item["capacity"], int)


def test_parse_detail_badges_and_derived_fields(center_item):
    assert center_item["sd_services_offered"] == ["After School"]
    assert center_item["sd_months_of_operation"] == ["12 Months"]
    assert center_item["ages_served"] == (
        "Infants (Birth-one year of age), Toddlers (1-3 years of age), "
        "Preschool age (3-4 years of age), School age (5+ years of age), "
        "School age only (school age programs)"
    )
    # "Transportation" isn't in this provider's active services.
    assert center_item["transportation"] is False


def test_parse_detail_openings_and_accreditation(center_item):
    assert center_item["accepting_new_children"] == "Yes"
    # Nationally Accredited is "No" on this fixture -> None (no accreditor).
    assert center_item["accreditation"] is None


def test_parse_detail_inspections_non_empty_with_working_urls(center_item):
    inspections = center_item["inspections"]
    assert len(inspections) > 0
    for insp in inspections:
        assert insp["date"]
        assert insp["type"]
        assert insp["report_url"].startswith("https://olapublic.sd.gov/api/mcase/attachments/")
    # The Program Certificate entry: bare-date subtitle -> synthetic type.
    certs = [i for i in inspections if i["type"] == "Program Certificate"]
    assert len(certs) == 1
    assert certs[0]["date"] == "02/05/2025"
    # A regular inspection entry: "<Type> - MM/DD/YYYY" subtitle split.
    inspection_rows = [i for i in inspections if i["type"] == "Inspection"]
    assert any(i["date"] == "08/22/2024" for i in inspection_rows)


# --------------------------------------------------------------------------- #
# Case 3: Badges come from the showBadges script, not the visible spans
# --------------------------------------------------------------------------- #


def test_inactive_badge_absent_despite_span_present(center_item):
    # "Drop-in" has a visible <span> badge on this fixture (all options are
    # always rendered), but it is NOT part of the showBadges(...) active
    # list -- only "After School" is. If the spider parsed the spans instead
    # of the script, this would wrongly include "Drop-in".
    with open(os.path.join(FIXTURES, "detail_center.html")) as f:
        text = f.read()
    assert 'value="Drop-in"' in text  # sanity: the span really is present
    assert "Drop-in" not in center_item["sd_services_offered"]


# --------------------------------------------------------------------------- #
# Case 4: Status comes from the facet meta, never the detail page
# --------------------------------------------------------------------------- #


def test_status_from_meta_not_page_text(spider):
    # detail_center.html's own "Status" field says "Operational" (see the
    # fixture), but we drive parse_detail with a Closed facet in meta -- the
    # emitted status must be "Closed", proving the page text is never read.
    url = DETAIL.format(id="79779")
    resp = _response("detail_center.html", url)
    resp.meta["raw_status"] = "Closed"
    resp.meta["list_phone"] = None
    item = next(iter(spider.parse_detail(resp)))
    assert item["status"] == "Closed"

    text = resp.text
    assert "Operational" in text  # sanity: the page itself claims Operational


def test_status_straggler_is_none(spider):
    url = DETAIL.format(id="79779")
    resp = _response("detail_center.html", url)
    resp.meta["raw_status"] = None
    resp.meta["list_phone"] = None
    item = next(iter(spider.parse_detail(resp)))
    assert item["status"] is None


# --------------------------------------------------------------------------- #
# Case 5: Missing / sparse fields
# --------------------------------------------------------------------------- #


@pytest.fixture
def informal_item(spider):
    url = DETAIL.format(id="103234")
    resp = _response("detail_informal.html", url)
    resp.meta["raw_status"] = "Closed"
    resp.meta["list_phone"] = None
    items = list(spider.parse_detail(resp))
    assert len(items) == 1
    return items[0]


def test_sparse_detail_yields_valid_item_with_none_fields(informal_item):
    assert informal_item["capacity"] is None
    assert informal_item["provider_website"] is None  # was "N/A"
    assert informal_item["accepting_new_children"] is None  # was "N/A"
    assert informal_item["sd_services_offered"] == []
    assert informal_item["sd_months_of_operation"] == []
    assert informal_item["ages_served"] is None
    assert informal_item["inspections"] == []
    # Still a fully-formed, valid item despite the sparse data.
    assert informal_item["provider_name"] == "Matayah Hughes"
    assert informal_item["license_number"] == "499143513"
    assert informal_item["provider_type"] == "In-Home"

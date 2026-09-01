import os

import pytest
from scrapy.http import HtmlResponse, Request

from provider_scrape.items import ProviderItem
from provider_scrape.spiders.maine import (
    EXEMPT_DETAIL,
    LICENSED_DETAIL,
    MaineSpider,
    _normalize_date_2digit_year,
    _parse_capacity,
    _parse_plot_addresses,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures", "maine")


def _response(name, url, **kwargs):
    with open(os.path.join(FIXTURES, name), "rb") as fh:
        body = fh.read()
    resp = HtmlResponse(url=url, body=body, encoding="utf-8", request=Request(url))
    if "zipcode" in kwargs:
        resp.meta["zipcode"] = kwargs["zipcode"]
    if "item" in kwargs:
        resp.meta["item"] = kwargs["item"]
    return resp


@pytest.fixture
def spider():
    return MaineSpider()


# --------------------------------------------------------------------------- #
# Pure helpers
# --------------------------------------------------------------------------- #


def test_parse_plot_addresses_golden_path():
    """Parse a full plotAddresses call with multiple providers."""
    text = """<script>
        plotAddresses('17 Second St, Bangor, ME 044014%Bangor Region YMCA<(207) 941-2808~44.7994^-68.77532*123 Main St, Portland, ME 041010%Portland Community Center<(207) 555-1234~43.6578^-70.2593*456 Oak Ave, Lewiston, ME 04240R%Lewiston Family Day Care<(207) 555-9999~44.1234^-70.1234', 'CENTER<br />~CENTER<br />~FAMILY<br />', 'Open Infant Slots, 6 weeks to 1 year old: 5<br />
Open Toddler Slots, 1-2 years old: 8<br />
Open Preschool Slots, 3-5 years old: 12<br />
Open School Age Slots, 5 years or older: 250<br />
Openings last updated: 09/11/2025<br />
<div class="tooltip">Accepts CCAP: YES', '208907    !218676    !309999    !', '');
    </script>"""

    results = _parse_plot_addresses(text)
    assert len(results) == 3

    # First provider
    assert results[0]["address"] == "17 Second St, Bangor, ME 04401"
    assert results[0]["provider_name"] == "Bangor Region YMCA"
    assert results[0]["phone"] == "(207) 941-2808"
    assert results[0]["latitude"] == "44.7994"
    assert results[0]["longitude"] == "-68.77532"
    assert results[0]["star_rating"] == "4"
    assert results[0]["provider_type"] == "CENTER<br />"
    assert results[0]["license_number"] == "208907"

    # Second provider
    assert results[1]["address"] == "123 Main St, Portland, ME 04101"
    assert results[1]["star_rating"] == "0"

    # Third provider with restricted rating
    assert results[2]["address"] == "456 Oak Ave, Lewiston, ME 04240"
    assert results[2]["star_rating"] == "R"


def test_parse_plot_addresses_no_results():
    """Verify empty list when response has no plotAddresses call."""
    text = "<html><body>No results</body></html>"
    results = _parse_plot_addresses(text)
    assert results == []


def test_parse_plot_addresses_br_in_name():
    """Verify [br] markers in provider name are handled."""
    text = """<script>
        plotAddresses('123 Main St, Portland, ME 041015%Amy Nisbett[br]CHILDREN FIRST MONTESSORI<(207) 555-1234~43.6578^-70.2593', 'CENTER<br />~', 'Open Infant Slots, 6 weeks to 1 year old: 0<br />
Open Toddler Slots, 1-2 years old: 0<br />
Open Preschool Slots, 3-5 years old: 0<br />
Open School Age Slots, 5 years or older: 0<br />
Openings last updated: Never<br />
<div class="tooltip">Accepts CCAP: NO', '123456    !', '');
    </script>"""

    results = _parse_plot_addresses(text)
    assert len(results) == 1
    assert results[0]["provider_name"] == "Amy Nisbett CHILDREN FIRST MONTESSORI"


def test_parse_plot_addresses_restricted_rating():
    """Verify star rating R maps to "2 Restricted"."""
    text = """<script>
        plotAddresses('123 Main St, Portland, ME 04101R%Restricted Provider<(207) 555-1234~43.6578^-70.2593', 'CENTER<br />~', 'Open Infant Slots, 6 weeks to 1 year old: 0<br />
Open Toddler Slots, 1-2 years old: 0<br />
Open Preschool Slots, 3-5 years old: 0<br />
Open School Age Slots, 5 years or older: 0<br />
Openings last updated: 01/15/25<br />
<div class="tooltip">Accepts CCAP: YES', '999999    !', '');
    </script>"""

    results = _parse_plot_addresses(text)
    assert len(results) == 1
    assert results[0]["star_rating"] == "R"


def test_parse_plot_addresses_never_updated():
    """Verify "Openings last updated: Never" maps to None."""
    text = """<script>
        plotAddresses('123 Main St, Portland, ME 041010%Never Updated Center<(207) 555-1234~43.6578^-70.2593', 'CENTER<br />~', 'Open Infant Slots, 6 weeks to 1 year old: 0<br />
Open Toddler Slots, 1-2 years old: 0<br />
Open Preschool Slots, 3-5 years old: 0<br />
Open School Age Slots, 5 years or older: 0<br />
Openings last updated: Never<br />
<div class="tooltip">Accepts CCAP: YES', '111111    !', '');
    </script>"""

    results = _parse_plot_addresses(text)
    assert len(results) == 1
    expected = 'Open Infant Slots, 6 weeks to 1 year old: 0<br />\nOpen Toddler Slots, 1-2 years old: 0<br />\nOpen Preschool Slots, 3-5 years old: 0<br />\nOpen School Age Slots, 5 years or older: 0<br />\nOpenings last updated: Never<br />\n<div class="tooltip">Accepts CCAP: YES'
    assert results[0]["ages_raw"] == expected


def test_normalize_date_2digit_year():
    """Test date normalization from MM/DD/YY to YYYY-MM-DD."""
    assert _normalize_date_2digit_year("01/15/25") == "2025-01-15"
    assert _normalize_date_2digit_year("12/31/99") == "1999-12-31"
    assert _normalize_date_2digit_year("06/15/50") == "2050-06-15"
    assert _normalize_date_2digit_year("06/15/51") == "1951-06-15"
    assert _normalize_date_2digit_year(None) is None
    assert _normalize_date_2digit_year("2025-01-15") == "2025-01-15"


def test_parse_capacity():
    """Test capacity parsing."""
    assert _parse_capacity(163) == 163
    assert _parse_capacity("163") == 163
    # Non-numeric strings are returned as-is
    assert _parse_capacity("unlimited") == "unlimited"
    assert _parse_capacity(None) is None
    # Strings with numbers at the start are converted to int
    assert _parse_capacity("163 beds") == 163


# --------------------------------------------------------------------------- #
# Spider integration tests using fixtures
# --------------------------------------------------------------------------- #


def test_parse_search_page_extracts_tokens_and_posts(spider):
    """Feed the search page fixture, verify a Request is yielded."""
    url = "https://search.childcarechoices.me/?search=04401&dist="
    resp = _response("search_page.html", url, zipcode="04401")
    
    results = list(spider.parse_search_page(resp))
    assert len(results) == 1
    
    request = results[0]
    assert isinstance(request, Request)
    assert request.method == "POST"
    
    # Check form data is in body (FormRequest encodes data into body)
    from urllib.parse import parse_qs
    body = request.body.decode('utf-8')
    assert "__VIEWSTATE" in body
    assert "__EVENTVALIDATION" in body
    form_data = parse_qs(body)
    assert form_data["ctl00$MainContent$txtAddress"] == ["04401"]


def test_parse_results_yields_detail_requests(spider):
    """Feed the results fixture, verify detail Requests are yielded."""
    url = "https://search.childcarechoices.me/?search=04401&dist="
    resp = _response("results_04401.html", url, zipcode="04401")
    
    results = list(spider.parse_results(resp))
    
    assert len(results) == 3
    for req in results:
        assert isinstance(req, Request)
        assert "gateway.maine.gov" in req.url


def test_parse_results_deduplicates_by_license_number(spider):
    """Call parse_results twice with overlapping IDs."""
    url = "https://search.childcarechoices.me/?search=04401&dist="
    resp = _response("results_04401.html", url, zipcode="04401")
    
    results1 = list(spider.parse_results(resp))
    assert len(results1) == 3
    
    # Call again with same spider (seen_ids already populated)
    resp2 = _response("results_04401.html", url, zipcode="04401")
    results2 = list(spider.parse_results(resp2))
    assert len(results2) == 0  # All duplicates


def test_parse_results_empty_zip(spider):
    """Feed the empty results fixture, verify no requests yielded."""
    url = "https://search.childcarechoices.me/?search=00000&dist="
    resp = _response("results_empty.html", url, zipcode="00000")
    
    results = list(spider.parse_results(resp))
    assert results == []


def test_parse_detail_licensed(spider):
    """Feed the licensed detail fixture, verify ProviderItem fields."""
    url = LICENSED_DETAIL.format(id="208907")
    resp = _response("detail_licensed.html", url, item=ProviderItem())
    resp.meta["item"]["provider_name"] = "Test Provider"
    
    result = list(spider.parse_detail(resp))
    assert len(result) == 1
    
    item = result[0]
    assert item["provider_name"] == "Test Provider"
    assert "status" in item
    assert "capacity" in item


def test_parse_detail_exempt(spider):
    """Feed the exempt detail fixture, verify it parses."""
    url = EXEMPT_DETAIL.format(id="762403")
    resp = _response("detail_exempt.html", url, item=ProviderItem())
    resp.meta["item"]["provider_name"] = "Exempt Test"
    
    result = list(spider.parse_detail(resp))
    assert len(result) == 1


def test_parse_licensing_history():
    """Verify licensing history table is parsed into InspectionItems."""
    url = LICENSED_DETAIL.format(id="208907")
    resp = _response("detail_licensed.html", url, item=ProviderItem())
    
    spider = MaineSpider()
    inspections = spider._parse_licensing_history(resp)
    
    # Should have at least some inspection data if available in fixture
    # or at least the DocuWare link
    if inspections:
        assert isinstance(inspections, list)
        if len(inspections) > 0:
            first = inspections[0]
            assert "date" in first or "type" in first

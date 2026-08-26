import json
import os

import pytest
from scrapy.http import HtmlResponse, Request, TextResponse

from provider_scrape import normalization as norm
from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.spiders.idaho import (
    CHILD_CARE_CHECK_BASE,
    DETAIL_URL,
    EXPECTED_MIN_PROVIDERS,
    LISTING_URL,
    PAGE_SIZE,
    IdahoSpider,
    build_address,
    extract_detail_fields,
    extract_email,
    extract_static_text,
    parse_criteria,
    parse_incidents,
    parse_inspections,
    parse_listing_fields,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def _read_fixture(name, binary=False):
    mode = "rb" if binary else "r"
    with open(os.path.join(FIXTURES, name), mode) as fh:
        return fh.read()


def _load_json_fixture(name):
    return json.loads(_read_fixture(name))


@pytest.fixture
def spider():
    return IdahoSpider()


# --- response builders ------------------------------------------------- #


def bootstrap_response(token="abc123token"):
    body = f"""
    <html><body>
    <input type="hidden" name="__RequestVerificationToken" value="{token}" />
    </body></html>
    """
    req = Request("https://idahostars.org/Families")
    return HtmlResponse(url="https://idahostars.org/Families", body=body.encode(), encoding="utf-8", request=req)


def listing_response(payload, page=1, url=LISTING_URL):
    req = Request(url, meta={"page": page})
    return TextResponse(url=url, body=json.dumps(payload).encode(), encoding="utf-8", request=req)


def detail_response(item, fixture_name="idaho_detail_load.html", url=DETAIL_URL):
    body = _read_fixture(fixture_name, binary=True)
    req = Request(url, meta={"item": item})
    return HtmlResponse(url=url, body=body, encoding="utf-8", request=req)


def child_care_check_response(item, fixture_name="idaho_child_care_check.html", url=None):
    url = url or f"{CHILD_CARE_CHECK_BASE}/20607"
    body = _read_fixture(fixture_name, binary=True)
    req = Request(url, meta={"item": item})
    return HtmlResponse(url=url, body=body, encoding="utf-8", request=req)


def split_requests(outputs, listing_url=LISTING_URL, detail_url=DETAIL_URL):
    """Partition parse_listing output into (listing_reqs, detail_reqs)."""
    listing_reqs = [r for r in outputs if r.url.startswith(listing_url)]
    detail_reqs = [r for r in outputs if r.url.startswith(detail_url)]
    return listing_reqs, detail_reqs


class _FakeFailure:
    """A minimal stand-in for a Twisted Failure -- just what the errbacks read."""

    def __init__(self, item, url="https://x", value=None):
        self.request = Request(url=url, meta={"item": item})
        self.value = value or Exception("boom")


# --- parse_listing_fields / build_address ------------------------------- #


def test_parse_listing_fields():
    data = _load_json_fixture("idaho_listing_page1.json")
    result = data["results"][0]
    fields = parse_listing_fields(result)
    assert fields["BusinessName"] == "Ability Early Learning Center"
    assert fields["AddressLine"] == "3323 4th St"
    assert fields["City"] == "Lewiston"
    # ints from the API are coerced to str (idaho_plan.md Sec 5.3).
    assert fields["ZipCode"] == "83501"
    assert fields["AlternateRiseId"] == "21403"
    assert fields["Id"] == "4619"
    assert fields["OperatorName"] == "Charlene Ricard"
    assert fields["FacilityAddressPhone"] == "(208) 553-8650"
    assert fields["WebsiteURL"] == "https://abilityelc.com/"
    assert fields["ICCPStatus"] == "Certified"


def test_parse_listing_fields_missing_name_skipped():
    result = {"fields": [{"Name": None, "Value": "x"}, {"Name": "Id", "Value": 1}]}
    fields = parse_listing_fields(result)
    assert fields == {"Id": "1"}


def test_build_address():
    fields = {"AddressLine": "3323 4th St", "City": "Lewiston", "ZipCode": "83501"}
    assert build_address(fields) == "3323 4th St, Lewiston, ID 83501"


def test_build_address_no_street_is_none():
    assert build_address({"City": "Lewiston", "ZipCode": "83501"}) is None
    assert build_address({}) is None


# --- bootstrap ------------------------------------------------------------ #


def test_parse_bootstrap_extracts_token_and_yields_page1(spider):
    outputs = list(spider.parse_bootstrap(bootstrap_response(token="tok-999")))
    assert spider.verification_token == "tok-999"
    assert len(outputs) == 1
    req = outputs[0]
    assert req.url.startswith(LISTING_URL)
    assert req.meta["page"] == 1
    assert "page=1" in req.url
    assert f"pagesize={PAGE_SIZE}" in req.url


def test_parse_bootstrap_missing_token_logs_warning_but_continues(spider, caplog):
    body = "<html><body>no token here</body></html>"
    req = Request("https://idahostars.org/Families")
    resp = HtmlResponse(url="https://idahostars.org/Families", body=body.encode(), encoding="utf-8", request=req)
    with caplog.at_level("WARNING"):
        outputs = list(spider.parse_bootstrap(resp))
    assert spider.verification_token is None
    assert len(outputs) == 1  # still proceeds to page 1
    assert "no RequestVerificationToken" in caplog.text


# --- parse_listing: pagination + fan-out -------------------------------- #


def test_parse_listing_page1_fans_out_and_yields_details(spider):
    data = _load_json_fixture("idaho_listing_page1.json")  # totalResults 461, totalPages 20
    listing_reqs, detail_reqs = split_requests(list(spider.parse_listing(listing_response(data, page=1))))
    # totalPages=20 in the fixture -> pages 2..20 == 19 follow-up requests.
    assert len(listing_reqs) == 19
    assert {r.meta["page"] for r in listing_reqs} == set(range(2, 21))
    # one detail request per result on the page (3 in the trimmed fixture).
    assert len(detail_reqs) == 3
    assert spider.provider_count == 3


def test_parse_listing_later_page_only_details(spider):
    data = _load_json_fixture("idaho_listing_page1.json")
    listing_reqs, detail_reqs = split_requests(list(spider.parse_listing(listing_response(data, page=5))))
    assert listing_reqs == []  # no re-fan-out off a non-first page
    assert len(detail_reqs) == 3


def test_parse_listing_uses_real_page_and_pagesize_not_decoys(spider):
    data = _load_json_fixture("idaho_listing_page1.json")
    listing_reqs, _ = split_requests(list(spider.parse_listing(listing_response(data, page=1))))
    one = listing_reqs[0]
    # the decoy stays pinned at 1 regardless of the real page being requested.
    assert "page13735=1" in one.url
    assert f"page={one.meta['page']}" in one.url
    assert f"pagesize={PAGE_SIZE}" in one.url


def test_parse_listing_dedupes_repeated_ids(spider):
    # Observed live (2026-08-26): the listing API can repeat the same result
    # row within a single page (e.g. Id 3950 back-to-back). A duplicate Id --
    # whether within one page or across pages -- must not yield a second
    # detail request/provider.
    data = _load_json_fixture("idaho_listing_page1.json")
    duplicate_result = dict(data["results"][0])
    data["results"] = [data["results"][0], duplicate_result, *data["results"][1:]]
    _, detail_reqs = split_requests(list(spider.parse_listing(listing_response(data, page=1))))
    assert len(detail_reqs) == 3  # not 4 -- the repeat is dropped
    assert spider.provider_count == 3
    assert spider.seen_ids == {"4619", "803", "71"}


def test_parse_listing_detail_requests_carry_verification_token(spider):
    spider.verification_token = "the-token"
    data = _load_json_fixture("idaho_listing_page1.json")
    _, detail_reqs = split_requests(list(spider.parse_listing(listing_response(data, page=1))))
    for req in detail_reqs:
        assert req.headers[b"RequestVerificationToken"] == b"the-token"
        assert req.headers[b"ModuleId"] == b"16101"
        assert req.headers[b"TabId"] == b"5308"


def test_parse_listing_builds_provider_url_and_common_fields(spider):
    data = _load_json_fixture("idaho_listing_page1.json")
    _, detail_reqs = split_requests(list(spider.parse_listing(listing_response(data, page=1))))
    item = detail_reqs[0].meta["item"]
    assert item["source_state"] == "Idaho"
    assert item["provider_name"] == "Ability Early Learning Center"
    assert item["license_number"] == "4619"
    assert item["license_holder"] == "Charlene Ricard"
    assert item["status"] == "Certified"
    assert item["address"] == "3323 4th St, Lewiston, ID 83501"
    assert item["phone"] == "(208) 553-8650"
    assert item["provider_website"] == "https://abilityelc.com/"
    assert item["id_alternate_rise_id"] == "21403"
    assert item["provider_url"] == "https://idahostars.org/Provider-Detail/ID/4619"


def test_parse_listing_empty_website_not_set(spider):
    data = _load_json_fixture("idaho_listing_page1.json")
    _, detail_reqs = split_requests(list(spider.parse_listing(listing_response(data, page=1))))
    # third fixture record ("ADVANCE CHILD CARE") has WebsiteURL == "".
    advance = next(r.meta["item"] for r in detail_reqs if r.meta["item"]["license_number"] == "71")
    assert "provider_website" not in advance
    assert advance["status"] == "Certified"


# --- extract_static_text / extract_detail_fields / extract_email -------- #


def test_extract_static_text():
    raw = _read_fixture("idaho_detail_load.html")
    html_content = extract_static_text(raw)
    assert html_content is not None
    assert "<strong>Director Name:</strong>" in html_content
    assert "Briana Mahaffey" in html_content


def test_extract_static_text_no_marker_returns_none():
    assert extract_static_text("<html>nothing here</html>") is None


def test_detail_field_extraction():
    raw = _read_fixture("idaho_detail_load.html")
    html_content = extract_static_text(raw)
    fields = extract_detail_fields(html_content)
    assert fields["Director Name"] == "Briana Mahaffey"
    assert fields["Facility Type"] == "Large Child Care Center (26 or more children)"
    assert fields["License Status"] == "Pending Renewal"
    assert fields["National Accreditation"] == "NOT Nationally Accredited"
    assert fields["Quality Achiever Status"] == "Eligible"
    assert fields["Program Philosophy"] == "Other"
    assert fields["Philosophy Comment"].startswith("We use Homeschooling")
    assert fields["Philosophy Description"].startswith("We want kiddos")
    assert fields["Program Description"].startswith("Accepts ages 6 weeks")
    assert fields["Participating in USDA Food Program"] == "No"
    assert fields["Family Style Dining"] == "Yes"
    assert fields["Other Opportunities at this Center"] == "No"
    assert fields["Opportunities Comment"] == "Outdoor play is done inside"
    assert fields["Consistent Daily Schedule"] == "Yes"
    assert fields["Policy on Pets"] == "Pet Free"


def test_detail_field_extraction_strips_commented_out_fields():
    # "Are there openings available" / "Number of Openings" / "Is there a
    # waitlist" are wrapped in an HTML comment in the live template
    # (idaho_plan.md Sec 5.6 / items.py comment) -- must NOT surface.
    raw = _read_fixture("idaho_detail_load.html")
    html_content = extract_static_text(raw)
    fields = extract_detail_fields(html_content)
    assert "Are there openings available" not in fields
    assert "Number of Openings" not in fields
    assert "Is there a waitlist" not in fields


def test_detail_field_extraction_empty_html_returns_empty_dict():
    assert extract_detail_fields(None) == {}
    assert extract_detail_fields("") == {}


def test_email_extraction():
    raw = _read_fixture("idaho_detail_load.html")
    html_content = extract_static_text(raw)
    assert extract_email(html_content) == "firststepsnampa@aol.com"


def test_email_extraction_empty_mailto_returns_none():
    html_content = '<a href="mailto:"></a>'
    assert extract_email(html_content) is None
    assert extract_email(None) is None


# --- parse_detail: golden path ------------------------------------------ #


def _base_item(license_number="3657", alt_id="20607"):
    item = ProviderItem()
    item["source_state"] = "Idaho"
    item["provider_name"] = "1st steps"
    item["license_number"] = license_number
    item["status"] = "Pending Renewal"
    if alt_id is not None:
        item["id_alternate_rise_id"] = alt_id
    return item


def test_parse_detail_enriches_item_and_chains_to_child_care_check(spider):
    item = _base_item()
    outputs = list(spider.parse_detail(detail_response(item)))
    assert len(outputs) == 1
    req = outputs[0]
    assert req.url == f"{CHILD_CARE_CHECK_BASE}/20607"
    enriched = req.meta["item"]
    assert enriched is item
    assert enriched["administrator"] == "Briana Mahaffey"
    assert enriched["provider_type"] == "Large Child Care Center (26 or more children)"
    assert enriched["email"] == "firststepsnampa@aol.com"
    assert enriched["id_license_status"] == "Pending Renewal"
    assert enriched["id_national_accreditation"] == "NOT Nationally Accredited"
    assert enriched["id_quality_achiever_status"] == "Eligible"
    assert enriched["id_program_philosophy"] == "Other"
    assert enriched["id_philosophy_comment"].startswith("We use Homeschooling")
    assert enriched["id_philosophy_description"].startswith("We want kiddos")
    assert enriched["id_usda_food_program"] == "No"
    assert enriched["id_family_style_dining"] == "Yes"
    assert enriched["id_consistent_schedule"] == "Yes"
    assert enriched["id_pet_policy"] == "Pet Free"
    # commented-out fields never surface.
    for absent in ("id_openings_available", "id_number_of_openings", "id_waitlist"):
        assert absent not in enriched
    # empty-valued labels (e.g. "Quality Achievement(s):") stay unset.
    assert "id_quality_achievements" not in enriched


def test_parse_detail_no_alternate_id_yields_item_directly(spider):
    item = _base_item(alt_id=None)
    outputs = list(spider.parse_detail(detail_response(item)))
    assert len(outputs) == 1
    assert isinstance(outputs[0], ProviderItem)
    assert outputs[0] is item
    assert outputs[0]["administrator"] == "Briana Mahaffey"


def test_parse_detail_no_static_text_keeps_listing_only(spider, caplog):
    item = _base_item()
    req = Request(DETAIL_URL, meta={"item": item})
    resp = HtmlResponse(url=DETAIL_URL, body=b"<html>nothing</html>", encoding="utf-8", request=req)
    with caplog.at_level("WARNING"):
        outputs = list(spider.parse_detail(resp))
    assert len(outputs) == 1  # still chains to child care check
    assert "administrator" not in item
    assert "could not extract StaticText" in caplog.text


def test_detail_empty_template(spider, caplog):
    item = _base_item()
    with caplog.at_level("WARNING"):
        outputs = list(spider.parse_detail(detail_response(item, fixture_name="idaho_detail_load_empty.html")))
    assert len(outputs) == 1  # still chains to child care check
    enriched = outputs[0].meta["item"]
    assert "administrator" not in enriched
    assert "provider_type" not in enriched
    assert "id_license_status" not in enriched
    # listing-sourced fields survive untouched.
    assert enriched["provider_name"] == "1st steps"
    assert enriched["status"] == "Pending Renewal"
    assert "empty detail template" in caplog.text


def test_detail_errback_still_chains_to_child_care_check(spider):
    item = _base_item()
    outputs = list(spider.detail_errback(_FakeFailure(item)))
    assert len(outputs) == 1
    assert outputs[0].url == f"{CHILD_CARE_CHECK_BASE}/20607"
    assert outputs[0].meta["item"] is item


def test_detail_errback_no_alt_id_emits_item(spider):
    item = _base_item(alt_id=None)
    outputs = list(spider.detail_errback(_FakeFailure(item)))
    assert outputs == [item]


def test_detail_errback_no_item_yields_nothing(spider):
    failure = _FakeFailure(ProviderItem())
    failure.request.meta.pop("item")
    assert list(spider.detail_errback(failure)) == []


# --- parse_inspections / parse_incidents / parse_criteria --------------- #


def test_parse_inspections():
    resp = child_care_check_response(_base_item())
    report_url = resp.url
    inspections = parse_inspections(resp, report_url)
    assert len(inspections) == 2
    clean, failed = inspections
    assert isinstance(clean, InspectionItem)
    assert clean["date"] == "May 29th, 2026"
    assert clean["type"] == "Investigation"
    assert clean["original_status"] == "Passed"
    assert clean["report_url"] == report_url
    assert clean["id_investigation_resolved"] == "Resolved"

    assert failed["date"] == "March 24th, 2026"
    assert failed["type"] == "Investigation"
    assert failed["original_status"] == "Failed"
    assert failed["id_investigation_resolved"] == "Not Resolved"


def test_parse_criteria_captures_pass_fail_and_comments():
    resp = child_care_check_response(_base_item())
    articles = resp.css("article.health-inspection-report")
    clean_criteria = parse_criteria(articles[0])
    assert len(clean_criteria) == 2
    assert clean_criteria[0]["name"] == "1. Provider Age/Supervision"
    assert clean_criteria[0]["passed"] is True
    assert clean_criteria[0]["comment"] is not None

    failed_criteria = parse_criteria(articles[1])
    assert len(failed_criteria) == 3
    first = failed_criteria[0]
    assert first["name"] == "1. Provider Age/Supervision"
    assert first["passed"] is False
    assert "changing table" in first["comment"]
    # a criterion with no inspector comment -> None, not an empty string.
    no_comment = next(c for c in failed_criteria if c["name"] == "3. Child-Staff Ratio")
    assert no_comment["passed"] is True
    assert no_comment["comment"] is None


def test_parse_inspections_no_articles_returns_empty():
    resp = child_care_check_response(_base_item(), fixture_name="idaho_child_care_check_404.html")
    assert parse_inspections(resp, resp.url) == []


def test_parse_incidents():
    resp = child_care_check_response(_base_item())
    report_url = resp.url
    incidents = parse_incidents(resp, report_url)
    assert len(incidents) == 2
    first, second = incidents
    assert isinstance(first, InspectionItem)
    assert first["type"] == "Incident"
    assert first["report_url"] == report_url
    assert first["date"] == "March 20th, 2026"
    assert first["original_status"] == "Supervision Concern"
    assert first["id_incident_title"] == "Supervision Concerns"
    assert "rolled off the changing table" in first["id_incident_description"]
    assert "immediately terminated" in first["id_incident_resolution"]

    assert second["original_status"] == "Child Abuse and Neglect"
    assert second["id_incident_title"] == "Child Abuse from Staff Member"


def test_parse_incidents_no_divs_returns_empty():
    resp = child_care_check_response(_base_item(), fixture_name="idaho_child_care_check_404.html")
    assert parse_incidents(resp, resp.url) == []


# --- inspection date ordinal stripping (normalization pipeline) --------- #


def test_inspection_date_ordinal_strip():
    resp = child_care_check_response(_base_item())
    inspections = parse_inspections(resp, resp.url)
    raw_date = inspections[0]["date"]
    assert raw_date == "May 29th, 2026"
    # the spider passes the raw ordinal-suffixed string straight through;
    # the shared normalization pipeline is responsible for parsing it.
    assert norm.normalize_date(raw_date) == "2026-05-29"
    assert norm.normalize_date(inspections[1]["date"]) == "2026-03-24"


# --- parse_child_care_check / errback ------------------------------------ #


def test_parse_child_care_check_attaches_inspections_and_incidents(spider):
    item = _base_item()
    outputs = list(spider.parse_child_care_check(child_care_check_response(item)))
    assert len(outputs) == 1
    result = outputs[0]
    assert result is item
    assert len(result["inspections"]) == 4  # 2 health inspections + 2 incidents
    assert any(i["type"] == "Incident" for i in result["inspections"])
    assert any(i["type"] == "Investigation" for i in result["inspections"])


def test_child_care_check_404(spider):
    item = _base_item()
    outputs = list(spider.child_care_check_errback(_FakeFailure(item, value=Exception("404 Not Found"))))
    assert outputs == [item]
    assert "inspections" not in item


def test_child_care_check_errback_no_item_yields_nothing(spider):
    failure = _FakeFailure(ProviderItem())
    failure.request.meta.pop("item")
    assert list(spider.child_care_check_errback(failure)) == []


def test_golden_item_has_no_undefined_fields(spider):
    item = _base_item()
    outputs = list(spider.parse_detail(detail_response(item)))
    result = next(spider.parse_child_care_check(child_care_check_response(outputs[0].meta["item"])))
    assert dict(result)  # constructing/serializing raises on an undefined field


# --- test_field_mapping: full 3-phase happy path ------------------------- #


def test_field_mapping_end_to_end(spider):
    data = _load_json_fixture("idaho_listing_page1.json")
    _, detail_reqs = split_requests(list(spider.parse_listing(listing_response(data, page=1))))
    listing_item = detail_reqs[0].meta["item"]  # "Ability Early Learning Center", id 4619

    # Detail fixture is captured for a DIFFERENT provider (3657 / "1st
    # steps"); re-point id_alternate_rise_id so the chained child-care-check
    # request lines up with our child_care_check fixture's URL.
    listing_item["id_alternate_rise_id"] = "20607"
    detail_outputs = list(spider.parse_detail(detail_response(listing_item)))
    enriched = detail_outputs[0].meta["item"]

    final_outputs = list(spider.parse_child_care_check(child_care_check_response(enriched)))
    final_item = final_outputs[0]

    # listing-sourced fields
    assert final_item["source_state"] == "Idaho"
    assert final_item["provider_name"] == "Ability Early Learning Center"
    assert final_item["license_number"] == "4619"
    assert final_item["license_holder"] == "Charlene Ricard"
    assert final_item["status"] == "Certified"
    assert final_item["provider_url"] == "https://idahostars.org/Provider-Detail/ID/4619"
    # detail-sourced fields
    assert final_item["administrator"] == "Briana Mahaffey"
    assert final_item["provider_type"] == "Large Child Care Center (26 or more children)"
    assert final_item["email"] == "firststepsnampa@aol.com"
    # child-care-check-sourced fields
    assert len(final_item["inspections"]) == 4


# --- normalization: status / facility_category --------------------------- #


@pytest.mark.parametrize(
    "iccp_status,bucket",
    [
        ("Certified", "active"),
        ("Pending Renewal", "active"),
        ("Pending Facility Type Change", "active"),
        ("Closed", "closed"),
        ("Not Participating", "closed"),
    ],
)
def test_normalization_status(iccp_status, bucket):
    assert norm.canonical_status(iccp_status) == bucket


@pytest.mark.parametrize(
    "facility_type,category",
    [
        ("Large Child Care Center (26 or more children)", "center"),
        ("Group Child Care Facility (up to 12 children)", "group_home"),
        ("Family Child Care Facility (up to 6 children)", "family_home"),
    ],
)
def test_normalization_facility_category(facility_type, category):
    assert norm.facility_category_from_type(facility_type) == category


# --- closed() guardrail --------------------------------------------------- #


def test_closed_warns_below_baseline(spider, caplog):
    spider.provider_count = EXPECTED_MIN_PROVIDERS - 1
    with caplog.at_level("WARNING"):
        spider.closed("finished")
    assert "possible incomplete crawl" in caplog.text


def test_closed_no_warning_at_or_above_baseline(spider, caplog):
    spider.provider_count = EXPECTED_MIN_PROVIDERS
    with caplog.at_level("WARNING"):
        spider.closed("finished")
    assert "possible incomplete crawl" not in caplog.text

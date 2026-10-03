import logging
import os
from urllib.parse import parse_qs

import pytest
from scrapy import FormRequest, Request
from scrapy.http import HtmlResponse

from provider_scrape import normalization as norm
from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.spiders.missouri import (
    MissouriSpider,
    apply_detail,
    compose_hours,
    dedupe_rows,
    parse_inspection_report,
    parse_inspection_rows,
    parse_investigation_report,
    parse_open_violations,
    parse_search_row,
    total_deficiencies,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")
BASE = "https://healthapps.dhss.mo.gov"
DETAIL = BASE + "/childcaresearch/Facility.aspx?LID={}"
INSPECTION_URL = BASE + "/ChildCareSearch/ViewInspection.aspx?Inspection_ID={}&LID={}"
INVESTIGATION_URL = BASE + "/ChildCareSearch/ViewInvestigation.aspx?cid=116977379349"


def _response(fixture, url):
    with open(os.path.join(FIXTURES, fixture), encoding="utf-8") as fh:
        body = fh.read()
    return HtmlResponse(url=url, body=body, encoding="utf-8", request=Request(url=url))


def _html(url, body):
    return HtmlResponse(url=url, body=body, encoding="utf-8", request=Request(url=url))


@pytest.fixture
def spider():
    return MissouriSpider()


@pytest.fixture
def search_response():
    return _response("mo_search_results.html", BASE + "/childcaresearch/SearchEngine.aspx")


@pytest.fixture
def center_detail():
    return _response("mo_detail_center.html", DETAIL.format("002295994"))


@pytest.fixture
def exempt_detail():
    return _response("mo_detail_exempt.html", DETAIL.format("002829630"))


@pytest.fixture
def sparse_detail():
    return _response("mo_detail_sparse.html", DETAIL.format("003160798"))


@pytest.fixture
def compliant_report():
    return _response("mo_inspection_compliant.html", INSPECTION_URL.format("2481428150", "002295994"))


@pytest.fixture
def violations_report():
    return _response("mo_inspection_violations.html", INSPECTION_URL.format("2496346432", "002650073"))


@pytest.fixture
def investigation_report():
    return _response("mo_investigation.html", INVESTIGATION_URL)


def _search_items(search_response):
    rows = search_response.xpath('//table[@id="ctl00_ContentPlaceHolder1_dgSearchEngine"]//tr[td]')
    return [parse_search_row(row) for row in rows]


def _form_body(request):
    return {k: v[0] for k, v in parse_qs(request.body.decode(), keep_blank_values=True).items()}


# --------------------------------------------------------------------------- #
# Phase 1
# --------------------------------------------------------------------------- #


def test_search_row_golden_path(search_response):
    center = _search_items(search_response)[0]
    assert center["provider_name"] == '"TRAINING UP A CHILD" LLC'
    assert center["license_number"] == "002295994"
    assert center["address"] == "390 W SAINT ANTHONY LN, FLORISSANT, MO 63031-6857"
    assert center["phone"] == "(314) 839-7731"
    assert center["hours"] == "6:00 AM - 9:00 PM"
    assert center["ages_served"] == "6 WEEKS - 12 YEARS"
    assert center["capacity"] == "94"
    assert center["source_state"] == "Missouri"
    assert center["provider_url"] == DETAIL.format("002295994")
    assert "status" not in center


def test_search_row_sparse_has_no_phone_hours_ages_capacity(search_response):
    sparse = _search_items(search_response)[-1]
    assert sparse["license_number"] == "003160798"
    assert sparse["provider_name"] == "4Everstrong Childcare LLC"
    for field in ("phone", "hours", "ages_served", "capacity"):
        assert field not in sparse


def test_search_row_unescapes_literal_quotes(search_response):
    dup_sparse = _search_items(search_response)[2]
    assert '"B.I.R.T.H."' in dup_sparse["provider_name"]
    assert "\\" not in dup_sparse["provider_name"]


def test_dedupe_keeps_full_row_and_counts_duplicates(search_response):
    items = _search_items(search_response)
    assert len(items) == 4
    unique, duplicates = dedupe_rows(items)
    assert duplicates == 1
    assert [i["license_number"] for i in unique] == ["002295994", "002829630", "003160798"]
    exempt = unique[1]
    assert exempt["capacity"] == "33"
    # Full row (ST LOUIS), not the sparse SAINT LOUIS one.
    assert "ST LOUIS" in exempt["address"]


def test_dedupe_full_row_wins_even_when_sparse_comes_first(search_response):
    items = _search_items(search_response)
    unique, duplicates = dedupe_rows([items[2], items[1]])
    assert duplicates == 1
    assert unique[0]["capacity"] == "33"


def test_parse_search_results_dedupes_and_requests_details(spider, search_response, caplog):
    with caplog.at_level(logging.INFO):
        requests = list(spider.parse_search_results(search_response))
    assert [r.url for r in requests] == [DETAIL.format(d) for d in ("002295994", "002829630", "003160798")]
    for request in requests:
        assert request.meta["dont_merge_cookies"] is True
        assert request.errback is not None
    assert "4 raw rows, 3 unique DVNs, 1 duplicates collapsed" in caplog.text
    assert spider.total_providers == 3


def test_empty_results_table_logs_error_and_yields_nothing(spider, caplog):
    response = _html(SEARCH_URL := BASE + "/childcaresearch/SearchEngine.aspx", "<html><body>Error</body></html>")
    assert SEARCH_URL
    with caplog.at_level(logging.ERROR):
        assert list(spider.parse_search_results(response)) == []
    assert "search results table missing or empty" in caplog.text


def test_search_form_request_carries_all_hidden_inputs(spider):
    response = _response("mo_search_form.html", BASE + "/childcaresearch/SearchEngine.aspx")
    (request,) = list(spider.parse_search_form(response))
    assert isinstance(request, FormRequest)
    assert request.method == "POST"
    body = _form_body(request)
    for key in ("__VIEWSTATE", "__VIEWSTATEENCRYPTED", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"):
        assert key in body
    assert body["ctl00$ContentPlaceHolder1$btnSubmit"] == "Search"
    assert request.callback == spider.parse_search_results


# --------------------------------------------------------------------------- #
# Phase 2
# --------------------------------------------------------------------------- #


def test_detail_golden_path_center(spider, search_response, center_detail):
    item = _search_items(search_response)[0]
    spider.fetch_reports = False
    (out,) = list(spider.parse_detail(center_detail, item))
    assert out["provider_name"] == '"TRAINING UP A CHILD" LLC'
    assert out["license_number"] == "002295994"
    assert out["address"] == "390 W SAINT ANTHONY LN, FLORISSANT, MO 63031-6857"
    assert out["county"] == "ST LOUIS"
    assert out["phone"] == "(314) 839-7731"
    assert out["provider_type"] == "CHILD CARE CENTER"
    assert out["license_begin_date"] == "12/05/2022"
    assert out["mo_license_anniversary_date"] == "11/01"
    assert out["capacity"] == "94"
    assert out["ages_served"] == "6 WEEKS - 12 YEARS"
    assert out["hours"] == "6:00 AM - 9:00 PM"
    assert "email" not in out
    assert len(out["inspections"]) == 7
    assert out["inspections"][0]["date"] == "05/07/2026"
    assert out["inspections"][0]["type"] == "COMPLIANCE MONITORING"
    assert out["inspections"][5]["date"] == "01/05/2024"
    assert out["inspections"][5]["type"] == "COMPLIANCE VERIFICATION"
    assert "deficiencies" not in out  # no reports fetched


def test_anniversary_without_year_does_not_set_license_expiration(center_detail):
    item = ProviderItem()
    apply_detail(center_detail, item)
    assert item["mo_license_anniversary_date"] == "11/01"
    assert "license_expiration" not in item


def test_full_anniversary_date_sets_license_expiration(exempt_detail):
    item = ProviderItem()
    apply_detail(exempt_detail, item)
    assert item["license_begin_date"] == "11/01/2025"
    assert item["mo_license_anniversary_date"] == "10/31/2026"
    assert item["license_expiration"] == "10/31/2026"
    assert item["provider_type"] == "LICENSE EXEMPT PROGRAM"
    assert item["county"] == "ST LOUIS CITY"
    assert item["hours"] == "5:00 AM - 11:59 PM"


def test_sparse_detail_fills_phone_and_leaves_rest_missing(spider, search_response, sparse_detail):
    item = _search_items(search_response)[-1]
    spider.fetch_reports = True
    (out,) = list(spider.parse_detail(sparse_detail, item))  # no rows: no postbacks
    assert out["phone"] == "(314) 215-7474"
    assert out["provider_type"] == "6 or Fewer"
    for field in ("capacity", "hours", "ages_served", "license_begin_date", "license_expiration", "inspections"):
        assert field not in out
    assert "mo_license_anniversary_date" not in out


def test_empty_detail_value_does_not_clobber_search_value(sparse_detail):
    item = ProviderItem()
    item["capacity"] = "12"
    item["hours"] = "7:00 AM - 5:00 PM"
    item["ages_served"] = "2 YEARS - 5 YEARS"
    apply_detail(sparse_detail, item)
    assert item["capacity"] == "12"
    assert item["hours"] == "7:00 AM - 5:00 PM"
    assert item["ages_served"] == "2 YEARS - 5 YEARS"


def test_hours_need_both_ends(center_detail):
    assert compose_hours("6:00 AM", None) is None
    assert compose_hours(None, "9:00 PM") is None
    html = center_detail.text.replace('lblHoursTo"> 9:00 PM', 'lblHoursTo">')
    item = ProviderItem()
    apply_detail(_html(center_detail.url, html), item)
    assert "hours" not in item


def test_detail_errback_yields_search_item(spider, search_response):
    item = _search_items(search_response)[0]
    request = Request(DETAIL.format("002295994"), cb_kwargs={"item": item})

    class _Failure:
        value = RuntimeError("boom")

    failure = _Failure()
    failure.request = request
    (out,) = list(spider.detail_errback(failure))
    assert out is item
    assert out["capacity"] == "94"
    assert spider.detail_failures == 1


def test_normalization_maps_new_facility_types_to_exempt():
    for raw in ("LICENSE EXEMPT PROGRAM", "6 or Fewer", "Exempt Program"):
        assert norm.facility_category_from_type(raw) == "exempt"
    assert norm.facility_category_from_type("CHILD CARE CENTER") == "center"
    assert norm.facility_category_from_type("FAMILY HOME") == "family_home"
    assert norm.facility_category_from_type("GROUP HOME") == "group_home"


# --------------------------------------------------------------------------- #
# Phase 3 chain
# --------------------------------------------------------------------------- #


def test_detail_with_rows_issues_first_postback(spider, search_response, center_detail):
    item = _search_items(search_response)[0]
    (request,) = list(spider.parse_detail(center_detail, item))
    assert isinstance(request, FormRequest)
    body = _form_body(request)
    assert body["__EVENTTARGET"] == "ctl00$ContentPlaceHolder1$dgInspInv$ctl02$lbtnView"
    assert "__VIEWSTATE" in body
    assert request.meta["dont_merge_cookies"] is True
    assert request.dont_filter is True
    assert request.cb_kwargs["index"] == 0
    assert len(request.cb_kwargs["targets"]) == 7
    assert request.callback == spider.parse_report


def test_reports_disabled_emits_item_with_stubs_and_no_postbacks(search_response, center_detail):
    spider = MissouriSpider(reports=0)
    item = _search_items(search_response)[0]
    (out,) = list(spider.parse_detail(center_detail, item))
    assert isinstance(out, ProviderItem)
    assert len(out["inspections"]) == 7
    assert "report_url" not in out["inspections"][0]
    assert "deficiencies" not in out


def _chain_start(spider, search_response, center_detail):
    item = _search_items(search_response)[0]
    (request,) = list(spider.parse_detail(center_detail, item))
    return request


def test_parse_report_advances_to_next_row_then_yields_item(spider, search_response, center_detail, compliant_report):
    first = _chain_start(spider, search_response, center_detail)
    kwargs = first.cb_kwargs
    # Pretend each row's report is the compliant one; walk the whole chain.
    request = first
    for index in range(7):
        results = list(spider.parse_report(compliant_report, **request.cb_kwargs))
        assert len(results) == 1
        if index < 6:
            request = results[0]
            assert isinstance(request, FormRequest)
            assert request.cb_kwargs["index"] == index + 1
            target = f"ctl00$ContentPlaceHolder1$dgInspInv$ctl{index + 3:02d}$lbtnView"
            assert _form_body(request)["__EVENTTARGET"] == target
            assert request.meta["dont_merge_cookies"] is True
            assert request.dont_filter is True
        else:
            out = results[0]
            assert isinstance(out, ProviderItem)
    assert all(s["report_url"] == compliant_report.url for s in out["inspections"])
    assert out["deficiencies"] == 0
    assert spider.reports_fetched == 7
    assert kwargs["item"] is out


def test_inspection_report_compliant(compliant_report):
    stub = InspectionItem(date="11/04/2024", type="COMPLIANCE MONITORING")
    parse_inspection_report(compliant_report, stub)
    assert stub["mo_in_compliance"] is True
    assert stub["mo_open_violations"] == 0
    assert stub["mo_inspection_id"] == "2481428150"
    assert stub["mo_notice"] == "UNANNOUNCED"
    assert stub["mo_arrival_time"] == "1:25 PM"
    assert stub["mo_departure_time"] == "3:56 PM"
    assert stub["mo_specialist"] == "CHARMEL WILLIAMS"
    assert stub["mo_compliance_list"]
    first = stub["mo_compliance_list"][0]
    assert first["rule"].startswith("5 CSR 25-")
    assert first["result"] in ("Compliance", "Violation", "Not Observed")


def test_inspection_report_violations_ignores_boilerplate_text(violations_report):
    stub = InspectionItem()
    assert "in compliance" in violations_report.text  # the misleading lblComplience text
    parse_inspection_report(violations_report, stub)
    assert stub["mo_in_compliance"] is False
    assert stub["mo_open_violations"] == 13
    results = [row["result"] for row in stub["mo_compliance_list"]]
    assert results.count("Violation") == 5
    assert results.count("Not Observed") == 1
    assert set(results) <= {"Compliance", "Violation", "Not Observed"}


def test_inspection_report_missing_elements_does_not_raise(caplog):
    response = _html(INSPECTION_URL.format("1", "002295994"), "<html><body><span>nothing</span></body></html>")
    stub = InspectionItem(date="01/01/2024", type="ANNUAL")
    with caplog.at_level(logging.WARNING):
        parse_inspection_report(response, stub)
    assert dict(stub) == {"date": "01/01/2024", "type": "ANNUAL"}


def test_open_violation_parsing(caplog):
    assert parse_open_violations("13") == 13
    assert parse_open_violations("NA") == 0
    assert parse_open_violations(" na ") == 0
    assert parse_open_violations("many") is None
    assert parse_open_violations(None) is None


def test_non_numeric_open_violations_warns(compliant_report, caplog):
    html = compliant_report.text.replace(">NA</span>", ">lots</span>", 1)
    stub = InspectionItem()
    with caplog.at_level(logging.WARNING):
        parse_inspection_report(_html(compliant_report.url, html), stub, logger=logging.getLogger("missouri"))
    assert "mo_open_violations" not in stub
    assert "non-numeric open violation count" in caplog.text


def test_investigation_report(investigation_report):
    stub = InspectionItem(date="07/11/2025", type="COMPLAINT INVESTIGATION")
    parse_investigation_report(investigation_report, stub)
    assert stub["mo_investigation_id"] == "116977379349"
    assert stub["original_status"] == "SUBSTANTIATED"
    assert stub["mo_disposition_date"] == "7/11/2025 12:00:00 AM"
    assert stub["mo_approving_supervisor"] == "CHRISCO, MARLA L"
    assert stub["mo_specialist"] == "HUGHES, CHRISTINE D"
    assert len(stub["mo_violations"]) == 3
    assert stub["mo_violations"][0]["rule"] == "5 CSR 25-500.182(1)(C)1."
    assert stub["mo_violations"][0]["description"].startswith("The provider shall establish simple")
    assert len(stub["mo_corrective_measures"]) == 1
    measure = stub["mo_corrective_measures"][0]
    assert measure["measure"].startswith("The facility shall notify all staff")
    assert measure["completed"] == "Y"
    assert measure["completed_date"] == "7/28/2025"
    assert stub["mo_conclusion"].startswith("On May 12, 2025")


def test_investigation_report_missing_tables_does_not_raise():
    response = _html(INVESTIGATION_URL, "<html><body></body></html>")
    stub = InspectionItem()
    parse_investigation_report(response, stub)
    assert dict(stub) == {"mo_investigation_id": "116977379349"}


def test_investigation_dates_are_normalized_by_pipeline_field_list():
    assert "mo_disposition_date" in norm.INSPECTION_DATE_FIELDS
    assert norm.normalize_date("7/11/2025 12:00:00 AM") == "2025-07-11"


# --------------------------------------------------------------------------- #
# Phase 3 provider enrichment, deficiencies, errback, unknown redirects
# --------------------------------------------------------------------------- #


def _two_stub_state(spider, detail, with_email=False):
    item = ProviderItem(license_number="002295994")
    if with_email:
        item["email"] = "detail@example.com"
    item["inspections"] = [InspectionItem(type="COMPLAINT INVESTIGATION"), InspectionItem(type="ANNUAL")]
    targets = ["t0", "t1"]
    return item, targets


def test_enrichment_from_first_routine_report_even_after_investigation(
    spider, center_detail, investigation_report, compliant_report
):
    item, targets = _two_stub_state(spider, center_detail)
    list(spider.parse_report(investigation_report, item, targets, 0, center_detail))
    assert "license_holder" not in item
    list(spider.parse_report(compliant_report, item, targets, 1, center_detail))
    assert item["license_holder"] == '"TRAINING UP A CHILD" LLC'
    assert item["administrator"] == "MCAFEE, PRISCILLA ANN"
    assert item["email"] == "info@trainingupachild-stl.com"
    assert item["mo_limitations"] == "16 CHILDREN UNDER 24 MONTHS"
    assert item["mo_mailing_address"] == "727 CEDAR FIELD CT, TOWN AND COUNTRY, MO 63017"


def test_enrichment_does_not_overwrite_detail_email(spider, center_detail, compliant_report):
    item, _ = _two_stub_state(spider, center_detail, with_email=True)
    item["inspections"] = [InspectionItem(type="ANNUAL")]
    list(spider.parse_report(compliant_report, item, ["t0"], 0, center_detail))
    assert item["email"] == "detail@example.com"


def test_enrichment_skips_blank_mailing_address(spider, center_detail, violations_report):
    item = ProviderItem(license_number="002650073")
    item["inspections"] = [InspectionItem(type="ANNUAL")]
    list(spider.parse_report(violations_report, item, ["t0"], 0, center_detail))
    assert "mo_mailing_address" not in item
    assert item["license_holder"] == "CENTRAL REFORM CONGREGATION"


def test_deficiencies_sum_across_mixed_report_kinds(spider, center_detail, investigation_report, violations_report):
    item, targets = _two_stub_state(spider, center_detail)
    list(spider.parse_report(investigation_report, item, targets, 0, center_detail))
    (out,) = list(spider.parse_report(violations_report, item, targets, 1, center_detail))
    assert out["deficiencies"] == 13 + 3


def test_total_deficiencies_none_when_nothing_fetched():
    assert total_deficiencies(None) is None
    assert total_deficiencies([InspectionItem(date="01/01/2024", type="ANNUAL")]) is None


def test_report_errback_keeps_stub_and_continues_chain(spider, center_detail):
    item, targets = _two_stub_state(spider, center_detail)
    request = Request(
        center_detail.url,
        cb_kwargs={"item": item, "targets": targets, "index": 0, "detail_response": center_detail},
    )

    class _Failure:
        value = RuntimeError("boom")

    failure = _Failure()
    failure.request = request
    (next_request,) = list(spider.report_errback(failure))
    assert isinstance(next_request, FormRequest)
    assert next_request.cb_kwargs["index"] == 1
    assert dict(item["inspections"][0]) == {"type": "COMPLAINT INVESTIGATION"}
    assert spider.report_failures == 1


def test_report_errback_on_last_row_still_yields_item(spider, center_detail):
    item = ProviderItem(license_number="002295994")
    item["inspections"] = [InspectionItem(type="ANNUAL")]
    request = Request(
        center_detail.url,
        cb_kwargs={"item": item, "targets": ["t0"], "index": 0, "detail_response": center_detail},
    )

    class _Failure:
        value = RuntimeError("boom")

    failure = _Failure()
    failure.request = request
    (out,) = list(spider.report_errback(failure))
    assert out is item
    assert "deficiencies" not in out


def test_unknown_redirect_path_keeps_stub_and_continues(spider, center_detail, caplog):
    item = ProviderItem(license_number="002295994")
    item["inspections"] = [InspectionItem(type="ANNUAL")]
    odd = _html(BASE + "/ChildCareSearch/Surprise.aspx", "<html></html>")
    with caplog.at_level(logging.WARNING):
        (out,) = list(spider.parse_report(odd, item, ["t0"], 0, center_detail))
    assert out is item
    assert "unexpected path" in caplog.text
    assert dict(item["inspections"][0]) == {"type": "ANNUAL"}


def test_inspection_rows_target_extraction(center_detail):
    rows = parse_inspection_rows(center_detail)
    assert len(rows) == 7
    assert rows[0][1] == "ctl00$ContentPlaceHolder1$dgInspInv$ctl02$lbtnView"
    assert rows[6][1] == "ctl00$ContentPlaceHolder1$dgInspInv$ctl08$lbtnView"


def test_pdf_viewer_redirect_keeps_stub_without_warning(spider, center_detail, caplog):
    item = ProviderItem(license_number="001614999")
    item["inspections"] = [InspectionItem(date="09/25/2024", type="Subsidy Renewal")]
    pdf = _html(BASE + "/childcaresearch/ViewChildCarePDF.aspx?LID=001614999&cid=abc", "<html></html>")
    with caplog.at_level(logging.WARNING):
        (out,) = list(spider.parse_report(pdf, item, ["t0"], 0, center_detail))
    assert out is item
    assert caplog.text == ""
    assert spider.report_failures == 0
    assert dict(item["inspections"][0]) == {"date": "09/25/2024", "type": "Subsidy Renewal"}

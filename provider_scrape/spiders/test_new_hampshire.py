import json
import os

import pytest
from scrapy.http import HtmlResponse, Request, TextResponse

from provider_scrape import normalization as norm
from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.spiders.new_hampshire import (
    DETAIL_URL,
    NewHampshireSpider,
    build_postback_formdata,
    build_record_type_map,
    compose_address,
    coordinate,
    extract_after_label,
    extract_postback_context,
    extract_search_records,
    extract_view_state,
    inspection_stub,
    item_from_search_record,
    normalize_shipping_state,
    parse_available_slots,
    parse_header,
    parse_hours_and_rates,
    parse_license_history,
    parse_other_information,
    parse_program_info,
    parse_qris_and_endorsements,
    parse_visit_modal,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def _load(name):
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as fh:
        return fh.read()


def _load_json(name):
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as fh:
        return json.load(fh)


def _html_response(url, body):
    return HtmlResponse(url=url, body=body, encoding="utf-8", request=Request(url=url))


def _text_response(url, body, request=None):
    return TextResponse(url=url, body=body.encode("utf-8"), encoding="utf-8", request=request or Request(url=url))


@pytest.fixture
def spider():
    return NewHampshireSpider()


@pytest.fixture
def search_records():
    return extract_search_records(_load_json("nh_search_sample.json"))


@pytest.fixture
def record_type_map(search_records):
    return build_record_type_map(search_records)


def _record(records, account_id):
    for rec in records:
        if rec.get("Id") == account_id:
            return rec
    raise AssertionError(f"no record with Id {account_id!r}")


# --------------------------------------------------------------------------- #
# Phase 1 -- search API / Salesforce reference-sharing
# --------------------------------------------------------------------------- #


def test_extract_search_records_unwraps_envelope(search_records):
    assert len(search_records) == 11
    ids = {r["Id"] for r in search_records}
    assert "0018z00000BHnVSAA1" in ids


def test_build_record_type_map_covers_all_five_types(record_type_map):
    assert record_type_map == {
        "012t00000004LplAAE": "Licensed Group Child Care Program",
        "012t00000004LpkAAE": "Licensed Family Child Care Program",
        "012t00000004LpjAAE": "License Exempt Family Child Care Program",
        "012t00000004LpiAAE": "License Exempt Facility",
        "012t0000000g2cBAAQ": "Residential Child Care Program",
    }


def test_golden_path_record(search_records, record_type_map):
    """100A Middle Child Care: the expanded RecordType shape, a plain "NH"
    ShippingState, and every optional field populated."""
    record = _record(search_records, "0018z00000BHnVSAA1")
    item = item_from_search_record(record, record_type_map)

    assert isinstance(item, ProviderItem)
    assert item["source_state"] == "New Hampshire"
    assert item["nh_account_id"] == "0018z00000BHnVSAA1"
    assert item["provider_url"] == DETAIL_URL.format("0018z00000BHnVSAA1")
    assert item["provider_name"] == "100A Middle Child Care"
    assert item["provider_type"] == "Licensed Group Child Care Program"
    assert item["address"] == "180 Bridge Street, Manchester, NH 03104"
    assert item["latitude"] == "42.995799"
    assert item["longitude"] == "-71.456072"
    assert item["phone"] == "(603) 668-6868"
    assert item["email"] == "ben.noble@homeinstead.com"
    assert item["capacity"] == 5
    assert item["status"] == "Active"
    assert item["nh_qris_rating"] == "Licensed"
    assert item["nh_infant_openings"] == 2
    assert item["nh_toddler_openings"] == 3
    assert item["nh_licensed"] == "Yes"
    assert item["nh_covid_closure"] is False
    # No Age_Group__c on this record.
    assert "ages_served" not in item


def test_record_type_back_reference_resolves_via_id_map(search_records, record_type_map):
    """A record whose own RecordType is a bare `{"r": N}` back-reference
    still resolves provider_type via the RecordTypeId -> Name map -- the
    plan's central Salesforce ref-sharing trap."""
    record = _record(search_records, "001t000000UiY2dAAF")
    assert isinstance(record["RecordType"], dict)
    assert "r" in record["RecordType"]
    assert "v" not in record["RecordType"]

    item = item_from_search_record(record, record_type_map)
    assert item["provider_type"] == "Licensed Group Child Care Program"


def test_permit_issued_status_and_missing_optional_fields(search_records, record_type_map):
    """BGCCNH - Sara M. Allen ELC: Permit Issued, and missing
    Infant__c/Toddler__c/Age_Group__c entirely."""
    record = _record(search_records, "001cs00001q8LYfAAM")
    item = item_from_search_record(record, record_type_map)

    assert item["status"] == "Permit Issued"
    assert "nh_infant_openings" not in item
    assert "nh_toddler_openings" not in item
    assert "ages_served" not in item


def test_missing_phone(search_records, record_type_map):
    record = _record(search_records, "001cs00000yVKBLAA4")
    assert "Phone" not in record
    item = item_from_search_record(record, record_type_map)
    assert "phone" not in item


def test_missing_lat_lon(search_records, record_type_map):
    record = _record(search_records, "001t000000blHzVAAU")
    item = item_from_search_record(record, record_type_map)
    assert "latitude" not in item
    assert "longitude" not in item


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("NH", "NH"),
        ("New Hampshire", "NH"),
        ("NH\xa0", "NH"),
        (None, None),
        ("", None),
    ],
)
def test_normalize_shipping_state(raw, expected):
    assert normalize_shipping_state(raw) == expected


def test_dirty_state_record_address_composition(search_records, record_type_map):
    """Early Care and Education Center at PSU carries the "NH\\xa0" dirty
    ShippingState value; the composed address must still read "..., NH ####"."""
    record = _record(search_records, "001cs00001sb7UMAAY")
    assert record["ShippingState"] == "NH\xa0"
    item = item_from_search_record(record, record_type_map)
    assert item["address"].endswith(", NH 03264")


def test_compose_address_all_pieces():
    assert compose_address("123 Main St", "Concord", "NH", "03301") == "123 Main St, Concord, NH 03301"


def test_compose_address_missing_postal_code():
    assert compose_address("123 Main St", "Concord", "NH", None) == "123 Main St, Concord, NH"


def test_compose_address_missing_street_and_city():
    assert compose_address(None, None, "NH", "03301") == "NH 03301"


# --------------------------------------------------------------------------- #
# Phase 1 -- Visualforce remoting token harvesting
# --------------------------------------------------------------------------- #

SEARCH_PAGE_HTML = """
<html><head>
<script>
Visualforce.remoting.Manager.add(new $VFRM.RemotingProviderImpl({"vf":{"vid":"066t0000000bm4t"},"actions":{"NH_ChildCareSearchClass":{"ms":[{"name":"fetchAccountList","len":23,"ns":"","ver":48.0,"csrf":"mock_csrf_fetch","authorization":"mock_auth_fetch"},{"name":"fetchProviderList","len":1,"ns":"","ver":48.0,"csrf":"mock_csrf_provider","authorization":"mock_auth_provider"},{"name":"retrieveAccountRecords","len":29,"ns":"","ver":48.0,"csrf":"mock_csrf_retrieve","authorization":"mock_auth_retrieve"}],"prm":1}},"service":"apexremote"}));
</script>
</head><body></body></html>
"""


def test_parse_search_page_harvests_tokens_and_posts_all_empty_vector(spider):
    response = _html_response("https://new-hampshire.my.site.com/nhccis/NH_ChildCareSearch", SEARCH_PAGE_HTML)
    results = list(spider.parse_search_page(response))
    assert len(results) == 1

    api_request = results[0]
    assert api_request.url == "https://new-hampshire.my.site.com/nhccis/apexremote"
    assert api_request.method == "POST"

    body = json.loads(api_request.body)
    assert body["action"] == "NH_ChildCareSearchClass"
    assert body["method"] == "retrieveAccountRecords"
    assert len(body["data"]) == 29
    assert all(v in ("", False) for v in body["data"])
    assert body["ctx"]["csrf"] == "mock_csrf_retrieve"
    assert body["ctx"]["vid"] == "066t0000000bm4t"
    assert body["ctx"]["authorization"] == "mock_auth_retrieve"
    assert body["ctx"]["ver"] == 48


def test_parse_search_page_missing_config_logs_error(spider, caplog):
    response = _html_response("https://new-hampshire.my.site.com/nhccis/NH_ChildCareSearch", "<html></html>")
    results = list(spider.parse_search_page(response))
    assert results == []


def test_parse_search_results_yields_one_detail_request_per_record(spider):
    search_json = _load_json("nh_search_sample.json")
    response = _text_response("https://new-hampshire.my.site.com/nhccis/apexremote", json.dumps(search_json))
    results = list(spider.parse_search_results(response))
    assert len(results) == 11
    urls = {r.url for r in results}
    assert DETAIL_URL.format("0018z00000BHnVSAA1") in urls


# --------------------------------------------------------------------------- #
# Phase 2 -- detail page: label/value extraction (both shapes)
# --------------------------------------------------------------------------- #


def test_extract_after_label_inline_shape():
    """The rich fixture's "Main Contact:" is the inline shape -- value is a
    bare sibling text node in the same parent div."""
    response = _html_response("https://example/detail", _load("nh_detail_rich.html"))
    assert extract_after_label(response, "Main Contact:") == "Brooke Andrade, Kristen E. Biron"


def test_extract_after_label_two_column_table_shape():
    """ "Other Information" fields are the two-column <td>/<td> shape."""
    response = _html_response("https://example/detail", _load("nh_detail_rich.html"))
    assert extract_after_label(response, "TYPE OF FINANCIAL ASSISTANCE:") == (
        "PREVENTIVE AND PROTECTIVE CARE, STATE CC ASSISTANCE (SCHOLARSHIP)"
    )
    assert extract_after_label(response, "ENVIRONMENT:") == (
        "WHEELCHAIR ACCESSIBLE, NO CAT, NO TV USE, OUTDOOR PLAY EQUIPMENT, MIXED AGE (3-5), "
        "NO DOG, PUBLIC TRANSPORTATION, WADING POOL, FENCED IN YARD"
    )


def test_extract_after_label_blank_value_is_none():
    """TRANSPORTATION and MEALS are populated labels with an empty value
    cell on this provider -- must read as None, not an empty string."""
    response = _html_response("https://example/detail", _load("nh_detail_rich.html"))
    assert extract_after_label(response, "TRANSPORTATION:") is None
    assert extract_after_label(response, "MEALS:") is None


def test_extract_after_label_missing_label_is_none():
    response = _html_response("https://example/detail", _load("nh_detail_rich.html"))
    assert extract_after_label(response, "Not A Real Label:") is None


def test_extract_after_label_nested_span_value():
    """Granite Step for Quality's value is nested in a following-sibling
    <span id="...">, not a bare sibling text node."""
    response = _html_response("https://example/detail", _load("nh_detail_gsq.html"))
    assert extract_after_label(response, "Granite Step for Quality:") == "Step 4"


def test_extract_after_label_badge_image_endorsement_yields_none():
    """The GSQ fixture's Endorsements value is a badge <img> with no text --
    legitimately nothing to extract."""
    response = _html_response("https://example/detail", _load("nh_detail_gsq.html"))
    assert extract_after_label(response, "Endorsements:") is None


# --------------------------------------------------------------------------- #
# Phase 2 -- full field parsing across the three real detail fixtures
# --------------------------------------------------------------------------- #


def _parse_full_detail(fixture_name, item=None):
    response = _html_response("https://example/detail", _load(fixture_name))
    item = item if item is not None else ProviderItem()
    parse_header(response, item)
    parse_program_info(response, item)
    parse_hours_and_rates(response, item)
    parse_available_slots(response, item)
    parse_qris_and_endorsements(response, item)
    parse_other_information(response, item)
    visits = parse_license_history(response)
    item["inspections"] = [inspection_stub(v) for v in visits]
    return item, visits


def test_rich_detail_full_mapping():
    item, visits = _parse_full_detail("nh_detail_rich.html")

    assert item["accepting_new_children"] == "Accepting Children"
    assert item["provider_website"] == "https://www.childrenselc.org"
    assert item["administrator"] == "Brooke Andrade, Kristen E. Biron"
    assert item["scholarships_accepted"] == "Yes"
    assert "nh_head_start" not in item  # blank on this provider
    assert item["hours"] == (
        "Monday 07:00 AM-05:00 PM; Tuesday 07:00 AM-05:00 PM; Wednesday 07:00 AM-05:00 PM; "
        "Thursday 07:00 AM-05:00 PM; Friday 07:00 AM-05:00 PM"
    )
    assert len(item["nh_schedule"]) == 5
    assert item["nh_schedule"][0] == {"day": "Monday", "start": "07:00 AM", "end": "05:00 PM"}
    assert item["nh_rates"] == "Please contact the child care provider for the rates."
    assert item["nh_preschool_openings"] == "4"
    assert item["nh_school_age_openings"] == "0"
    assert "nh_infant_openings" not in item  # blank cell -- unknown, not 0
    assert "nh_toddler_openings" not in item
    assert item["nh_financial_assistance"] == "PREVENTIVE AND PROTECTIVE CARE, STATE CC ASSISTANCE (SCHOLARSHIP)"
    assert item["nh_environment"].startswith("WHEELCHAIR ACCESSIBLE")
    assert "transportation" not in item
    assert item["nh_schedule_options"] == "PART WEEK;FULL DAY;OPEN SCHOOL VACATION WEEK;FULL WEEK;FLEXIBLE SCHEDULE"
    assert item["nh_special_needs"].startswith("EXPERIENCE")
    assert item["languages"] == "ENGLISH"
    assert "meals" not in item
    assert "nh_special_skills" not in item

    assert len(visits) == 3
    assert visits[0]["date"] == "03/17/2026"
    assert visits[0]["type"] == "Monitoring Visit"
    assert visits[0]["nh_level_of_compliance"] == "221 / 222"
    assert visits[0]["nh_visit_id"] == "a4lcs00000m32EaAAI"
    assert visits[1]["nh_visit_id"] == "a4lcs00000HvU9GAAV"
    assert visits[2]["nh_visit_id"] == "a4l8z0000006Zh4AAE"
    assert len(item["inspections"]) == 3
    assert isinstance(item["inspections"][0], InspectionItem)


def test_sparse_detail_permit_issued_one_visit_empty_other_info():
    item, visits = _parse_full_detail("nh_detail_sparse.html")

    assert item["administrator"] == "Amanda Carroll, Robert Carter Jr"
    assert "hours" not in item
    assert "nh_schedule" not in item
    assert "nh_rates" not in item
    assert "nh_infant_openings" not in item
    assert "nh_gsq_step" not in item
    assert "nh_financial_assistance" not in item
    assert "nh_environment" not in item

    assert len(visits) == 1
    assert visits[0]["nh_visit_id"] == "a4lcs0000155GAPAA2"
    assert visits[0]["nh_level_of_compliance"] == "116 / 116"


def test_gsq_detail_step_four_and_populated_slots():
    item, _visits = _parse_full_detail("nh_detail_gsq.html")

    assert item["nh_gsq_step"] == "Step 4"
    assert item["nh_infant_openings"] == "0"
    assert item["nh_toddler_openings"] == "1"
    assert item["nh_preschool_openings"] == "1"
    assert item["administrator"] == "Kameron Estes"


def test_gsq_step_detail_overwrites_api_value_and_logs_mismatch(caplog):
    """The detail page's Granite Step wins over a (deliberately different)
    Phase 1 value, and the mismatch is logged for visibility."""
    response = _html_response("https://example/detail", _load("nh_detail_gsq.html"))
    item = ProviderItem()
    item["provider_name"] = "Beckam's Childcare"
    item["nh_gsq_step"] = "Step 1"  # pretend Phase 1 disagreed
    with caplog.at_level("DEBUG"):
        parse_qris_and_endorsements(response, item, logger=_debug_logger())
    assert item["nh_gsq_step"] == "Step 4"


class _RecordingLogger:
    def __init__(self):
        self.debug_calls = []

    def debug(self, *args, **kwargs):
        self.debug_calls.append(args)

    def warning(self, *args, **kwargs):
        pass


def _debug_logger():
    return _RecordingLogger()


def test_capacity_and_type_of_care_cross_check_never_overwrites():
    response = _html_response("https://example/detail", _load("nh_detail_rich.html"))
    item = ProviderItem()
    item["provider_type"] = "Something Totally Different"
    item["capacity"] = 999
    logger = _debug_logger()
    parse_program_info(response, item, logger=logger)
    # never overwritten, regardless of the mismatch against the detail page
    assert item["capacity"] == 999
    assert item["provider_type"] == "Something Totally Different"
    assert len(logger.debug_calls) == 2


# --------------------------------------------------------------------------- #
# Phase 2 -- licensing history edge case: no visits at all
# --------------------------------------------------------------------------- #

NO_VISITS_HTML = """
<html><body>
<div class="paddingBottom" id="InspectionMonitoring">
    <table class="ma__table ma__table--wide">
        <thead><tr><th>Review Date</th></tr></thead>
        <tbody>
        </tbody>
    </table>
</div>
</body></html>
"""


def test_parse_license_history_no_visits():
    response = _html_response("https://example/detail", NO_VISITS_HTML)
    assert parse_license_history(response) == []


# --------------------------------------------------------------------------- #
# Phase 3 -- postback context / form data assembly
# --------------------------------------------------------------------------- #


def test_extract_postback_context_from_detail_page():
    text = _load("nh_detail_rich.html")
    context = extract_postback_context(text)
    assert context["form_id"] == "j_id0:j_id7:j_id98"
    assert context["similarity_grouping_id"] == "j_id0:j_id7:j_id98:j_id343"
    assert context["view_state"] == "MOCK_INITIAL_VIEWSTATE"
    assert context["view_state_mac"] == "MOCK_INITIAL_VIEWSTATE_MAC"


def test_extract_postback_context_missing_form_is_none():
    assert extract_postback_context("<html><body>nothing here</body></html>") is None


def test_extract_view_state_rotates_between_detail_page_and_postback_response():
    initial = extract_view_state(_load("nh_detail_rich.html"))
    rotated = extract_view_state(_load("nh_visit_modal.html"))
    assert initial["view_state"] == "MOCK_INITIAL_VIEWSTATE"
    assert rotated["view_state"] == "MOCK_ROTATED_VIEWSTATE"
    assert initial["view_state"] != rotated["view_state"]


def test_build_postback_formdata_shape():
    context = {
        "view_state": "VS",
        "view_state_version": "1",
        "view_state_mac": "MAC",
        "form_id": "j_id0:formX",
        "similarity_grouping_id": "j_id0:formX:j_id99",
    }
    formdata = build_postback_formdata(context, "a4lVisit1")
    assert formdata["com.salesforce.visualforce.ViewState"] == "VS"
    assert formdata["com.salesforce.visualforce.ViewStateVersion"] == "1"
    assert formdata["com.salesforce.visualforce.ViewStateMAC"] == "MAC"
    assert formdata["j_id0:formX"] == "j_id0:formX"
    assert formdata["AJAXREQUEST"] == "j_id0:formX"
    assert formdata["j_id0:formX:j_id99"] == "j_id0:formX:j_id99"
    assert formdata["selectedVisitId"] == "a4lVisit1"


# --------------------------------------------------------------------------- #
# Phase 3 -- domain -> item join, option (a) violation-only retention
# --------------------------------------------------------------------------- #


def test_visit_modal_reconciles_to_221_of_222_with_one_violation():
    text = _load("nh_visit_modal.html")
    domains, violations, deficiencies, header_fields, warnings = parse_visit_modal(text)

    assert warnings == []
    assert len(domains) == 13
    assert deficiencies == 1
    assert len(violations) == 1

    by_domain = {d["domain"]: d["level_of_compliance"] for d in domains}
    assert by_domain["Administration"] == "23 / 23"
    assert by_domain["Care of Chidlren"] == "1 / 1"  # source's own misspelling, kept as-is
    assert by_domain["Care of Children"] == "41 / 41"
    assert by_domain["Medication"] == "19 / 20"

    # Total compliance denominator across all domains reconciles to 222; the
    # numerator sum (221) is denominator-minus-violations, since only the
    # violation is discoverable directly from the retained data.
    total_denominator = sum(int(lvl.split("/")[1]) for lvl in by_domain.values())
    assert total_denominator == 222

    assert header_fields == {
        "nh_announcement_type": "Unannounced",
        "nh_licensor": "Olya Mahoney",
        "nh_corrective_action_accepted": "04/03/2026",
    }


def test_nh_domains_entries_have_exactly_two_keys_no_items():
    text = _load("nh_visit_modal.html")
    domains, _violations, _deficiencies, _header, _warnings = parse_visit_modal(text)
    for entry in domains:
        assert set(entry.keys()) == {"domain", "level_of_compliance"}


def test_violation_shape_citation_comma_stripped_and_observations_present():
    text = _load("nh_visit_modal.html")
    _domains, violations, _deficiencies, _header, _warnings = parse_visit_modal(text)
    violation = violations[0]
    assert violation["domain"] == "Medication"
    assert violation["result"] == "Non-Compliant"
    assert violation["regulations"] == [
        {
            "citation": "He-C 4002.20(n)",  # trailing comma stripped
            "text": (
                "All medications belonging to child care staff shall be stored separate "
                "from children’s medications in a locked area, or otherwise "
                "inaccessible to children."
            ),
        }
    ]
    assert violation["observations"] == (
        "The licensing coordinator observed a staff epi-pen in the same bin as the children's medications."
    )
    assert violation["corrective_action_plan"].startswith("All medications are stored")


# Small synthetic postback fragments for cases the one real capture can't
# cover: zero violations, a multi-citation item, and a broken data-target
# join.

ZERO_VIOLATIONS_MODAL_HTML = """
<html><body>
<div class="row">
<div class="col-sm-12 col-md-7">
<div class="col-sm-12 col-md-12"><div class="boldLabel">Monitoring</div>Announced</div>
<div class="col-sm-12 col-md-3"><div><b>Licensor Assigned</b></div>Jamie Rivera</div>
</div>
</div>
<table><tbody>
<tr>
  <td data-label="Domain Category">
    <a data-toggle="collapse" data-target="#collapseA"><span class="categoryName">Health</span></a>
  </td>
  <td data-label="Level of Compliance">2 / 2</td>
</tr>
<tr><td class="nested-table"><div id="collapseA">
  <table><tbody>
    <tr>
      <td data-label="Visit Item Name"><span>Handwashing policy posted</span></td>
      <td data-label="Associated Regulations">
        <div class="ma__tooltip__inner">
          <label class="ma__tooltip__open">He-C 4002.10(a),</label>
          <div class="ma__tooltip__message"><p>Text A.</p></div>
        </div>
      </td>
      <td data-label="Result">Compliant</td>
    </tr>
    <tr>
      <td data-label="Visit Item Name"><span>First aid kit stocked</span></td>
      <td data-label="Associated Regulations">
        <div class="ma__tooltip__inner">
          <label class="ma__tooltip__open">He-C 4002.10(b),</label>
          <div class="ma__tooltip__message"><p>Text B.</p></div>
        </div>
      </td>
      <td data-label="Result">Compliant</td>
    </tr>
  </tbody></table>
</div></td></tr>
</tbody></table>
</body></html>
"""


def test_visit_with_zero_violations():
    domains, violations, deficiencies, header_fields, warnings = parse_visit_modal(ZERO_VIOLATIONS_MODAL_HTML)
    assert warnings == []
    assert domains == [{"domain": "Health", "level_of_compliance": "2 / 2"}]
    assert violations == []
    assert deficiencies == 0
    assert header_fields["nh_announcement_type"] == "Announced"
    assert header_fields["nh_licensor"] == "Jamie Rivera"


MULTI_CITATION_MODAL_HTML = """
<html><body>
<table><tbody>
<tr>
  <td data-label="Domain Category">
    <a data-toggle="collapse" data-target="#collapseB"><span class="categoryName">Records</span></a>
  </td>
  <td data-label="Level of Compliance">1 / 1</td>
</tr>
<tr><td class="nested-table"><div id="collapseB">
  <table><tbody>
    <tr class="nonComplaintColorClass">
      <td data-label="Visit Item Name"><span>Missing multiple required records</span></td>
      <td data-label="Associated Regulations">
        <div class="ma__tooltip__inner">
          <label class="ma__tooltip__open">He-C 4002.30(a),</label>
          <div class="ma__tooltip__message"><p>Text 1.</p></div>
        </div>
        <div class="ma__tooltip__inner">
          <label class="ma__tooltip__open">He-C 4002.30(b),</label>
          <div class="ma__tooltip__message"><p>Text 2.</p></div>
        </div>
        <div class="ma__tooltip__inner">
          <label class="ma__tooltip__open">He-C 4002.30(c),</label>
          <div class="ma__tooltip__message"><p>Text 3.</p></div>
        </div>
      </td>
      <td data-label="Result">Non-Compliant</td>
    </tr>
    <tr class="nonComplaintStatementClass">
      <td><div id="Non_Compliance">Three records were missing.</div></td>
      <td><div id="CorrectiveAction">All records filed.</div></td>
    </tr>
  </tbody></table>
</div></td></tr>
</tbody></table>
</body></html>
"""


def test_multi_citation_violation_keeps_every_regulation():
    _domains, violations, deficiencies, _header, warnings = parse_visit_modal(MULTI_CITATION_MODAL_HTML)
    assert warnings == []
    assert deficiencies == 1
    citations = [r["citation"] for r in violations[0]["regulations"]]
    assert citations == ["He-C 4002.30(a)", "He-C 4002.30(b)", "He-C 4002.30(c)"]
    assert violations[0]["observations"] == "Three records were missing."
    assert violations[0]["corrective_action_plan"] == "All records filed."


BROKEN_JOIN_MODAL_HTML = """
<html><body>
<table><tbody>
<tr>
  <td data-label="Domain Category">
    <a data-toggle="collapse" data-target="#collapseMissing"><span class="categoryName">Health</span></a>
  </td>
  <td data-label="Level of Compliance">3 / 3</td>
</tr>
</tbody></table>
</body></html>
"""


def test_broken_domain_join_warns_but_does_not_crash():
    domains, violations, deficiencies, _header, warnings = parse_visit_modal(BROKEN_JOIN_MODAL_HTML)
    assert domains == [{"domain": "Health", "level_of_compliance": "3 / 3"}]
    assert violations == []
    assert deficiencies == 0
    assert len(warnings) == 1
    assert "collapseMissing" in warnings[0]


# --------------------------------------------------------------------------- #
# End-to-end spider chaining
# --------------------------------------------------------------------------- #


def test_parse_detail_with_visits_disabled_skips_phase_3():
    spider = NewHampshireSpider(visits=0)
    response = _html_response(DETAIL_URL.format("001cs00001q8LYfAAM"), _load("nh_detail_sparse.html"))
    item = ProviderItem()
    item["nh_account_id"] = "001cs00001q8LYfAAM"
    results = list(spider.parse_detail(response, item))
    assert len(results) == 1
    emitted = results[0]
    assert isinstance(emitted, ProviderItem)
    assert len(emitted["inspections"]) == 1
    assert "nh_domains" not in emitted["inspections"][0]
    assert "deficiencies" not in emitted


def test_full_serialized_visit_chain_across_three_visits(spider):
    """The rich fixture has 3 visits; each Phase 3 postback must be
    requested serially (not fanned out), threading the rotating ViewState,
    with the provider only emitted once the last visit resolves."""
    detail_url = DETAIL_URL.format("001t000000UiY2dAAF")
    response = _html_response(detail_url, _load("nh_detail_rich.html"))
    item = ProviderItem()
    item["nh_account_id"] = "001t000000UiY2dAAF"

    results = list(spider.parse_detail(response, item))
    assert len(results) == 1
    form_request = results[0]
    assert form_request.method == "POST"
    assert form_request.url == detail_url

    modal_body = _load("nh_visit_modal.html")
    seen_indexes = []
    for _ in range(3):
        seen_indexes.append(form_request.cb_kwargs["index"])
        visit_response = _text_response(detail_url, modal_body, request=form_request)
        out = list(spider.parse_visit_detail(visit_response, **form_request.cb_kwargs))
        assert len(out) == 1
        if hasattr(out[0], "cb_kwargs"):
            form_request = out[0]
        else:
            final_item = out[0]

    assert seen_indexes == [0, 1, 2]
    assert isinstance(final_item, ProviderItem)
    assert final_item["deficiencies"] == 3  # 1 violation x 3 visits (same fixture reused)
    assert len(final_item["inspections"]) == 3
    for insp in final_item["inspections"]:
        assert len(insp["nh_domains"]) == 13
        assert insp["nh_licensor"] == "Olya Mahoney"
    assert spider.providers_emitted == 1
    assert spider.visits_fetched == 3
    assert spider.violations_found == 3


def test_missing_postback_context_falls_back_to_emitting_without_phase_3(spider, caplog):
    detail_url = DETAIL_URL.format("001t000000UiY2dAAF")
    # Strip the form/script block so no postback context can be parsed, but
    # keep the Licensing History table intact.
    body = _load("nh_detail_rich.html").replace("<form", "<div data-was-form")
    response = _html_response(detail_url, body)
    item = ProviderItem()
    item["nh_account_id"] = "001t000000UiY2dAAF"

    results = list(spider.parse_detail(response, item))
    assert len(results) == 1
    emitted = results[0]
    assert isinstance(emitted, ProviderItem)
    assert len(emitted["inspections"]) == 3
    assert "nh_domains" not in emitted["inspections"][0]
    assert spider.postback_failures == 1


# --------------------------------------------------------------------------- #
# facility_category / status mappings for all 5 record types + both statuses
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "provider_type,expected_category",
    [
        ("Licensed Group Child Care Program", "center"),
        ("Licensed Family Child Care Program", "family_home"),
        ("License Exempt Family Child Care Program", "exempt"),
        ("License Exempt Facility", "exempt"),
        ("Residential Child Care Program", "other"),
    ],
)
def test_facility_category_mapping_for_all_five_record_types(provider_type, expected_category):
    assert norm.facility_category_from_type(provider_type) == expected_category


@pytest.mark.parametrize(
    "status,expected_bucket",
    [
        ("Active", "active"),
        ("Permit Issued", "provisional"),
    ],
)
def test_status_mapping_for_both_statuses(status, expected_bucket):
    assert norm.canonical_status(status) == expected_bucket


# --------------------------------------------------------------------------- #
# Coordinates -- Salesforce emits two different shapes for this field
# --------------------------------------------------------------------------- #


def test_coordinate_accepts_plain_float():
    """The shape seen on the live endpoint."""
    assert coordinate(42.962232) == "42.962232"
    assert coordinate(-71.427823) == "-71.427823"


def test_coordinate_accepts_compound_shape_and_prefers_source():
    """The shape seen in tasks/new_hampshire/search_response.json.

    `source` is unrounded, so it wins over the lossy `parsedValue`.
    """
    got = coordinate({"source": "42.995799000000000", "parsedValue": 42.995799})
    assert got == "42.995799000000000"


def test_coordinate_falls_back_to_parsed_value():
    assert coordinate({"parsedValue": 42.995799}) == "42.995799"


def test_coordinate_never_stringifies_a_dict():
    """Regression: str() on the compound shape silently wrote a dict repr
    into latitude."""
    for value in (coordinate({"source": "1.0"}), coordinate({"parsedValue": 1.0})):
        assert "{" not in value


def test_coordinate_handles_missing_and_junk():
    assert coordinate(None) is None
    assert coordinate({}) is None
    assert coordinate("") is None
    assert coordinate({"source": "   "}) is None
    assert coordinate(True) is None


def test_item_from_search_record_handles_compound_coordinates():
    """End-to-end: a record carrying the compound shape still yields clean
    latitude/longitude strings."""
    record = {
        "Id": "001t000000UiY2dAAF",
        "Name": "Compound Coords Center",
        "ShippingStreet": "673 Weston Road",
        "ShippingCity": "Manchester",
        "ShippingState": "New Hampshire",
        "ShippingPostalCode": "03103",
        "ShippingLatitude": {"source": "42.962232000000000", "parsedValue": 42.962232},
        "ShippingLongitude": {"source": "-71.427823000000000", "parsedValue": -71.427823},
        "Capacity__c": 24,
        "License_Status__c": "Active",
        "RecordTypeId": "012t00000004LplAAE",
    }
    item = item_from_search_record(record, {"012t00000004LplAAE": "Licensed Group Child Care Program"})
    assert item["latitude"] == "42.962232000000000"
    assert item["longitude"] == "-71.427823000000000"

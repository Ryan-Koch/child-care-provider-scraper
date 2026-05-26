import json
import os

import pytest
from scrapy.http import HtmlResponse, Request, TextResponse

from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.spiders.nevada import (
    POWERBI_QUERY_URL,
    QUALITY_SELECT,
    NevadaSpider,
    base_facility_type,
    build_detail_url,
    build_quality_command,
    collect_row_fields,
    decode_data_shape,
    epoch_ms_to_date,
    format_age_range,
    format_hours,
    normalize_license,
    short_name,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def _load_fixture(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return json.load(fh)


@pytest.fixture
def spider():
    return NevadaSpider()


# ---- Helpers ----


def _row_input(name, value=""):
    """Render a single hidden input field as it appears in an ASP.NET result row."""
    return f'<input type="hidden" name="{name}" value="{value}">'


def _search_row_html(ctl_suffix, fields):
    """Wrap a dict of {short_field_name: value} as a single <tr> result row."""
    prefix = f"ctl00$ContentPlaceHolder1$ucLicenseeSearchResult$ResultsGrid$ctl{ctl_suffix}"
    inputs = "".join(_row_input(f"{prefix}${k}", v) for k, v in fields.items())
    return f"<tr><td>{inputs}</td></tr>"


def _search_results_html(*rows):
    return (
        '<html><body>'
        '<input type="hidden" id="hdnTotalRecords" value="2">'
        '<table id="ctl00_ContentPlaceHolder1_ucLicenseeSearchResult_ResultsGrid">'
        '<tr><th>Header</th></tr>'
        + "".join(rows)
        + '</table>'
        '</body></html>'
    )


GOLDEN_ROW = {
    "hfName": "ACELERO LEARNING CCC - HENDERSON",
    "hfLicenseNumberToDisplay": "831-26",
    "hfLicenseNumber": "831",
    "hdnStatus": "ACT",
    "hdnStatusCode": "Active",
    "hPrimaryAddress": "180 N. WESTMINSTER WAY HENDERSON, NV 89015",
    "hPhoneNumber": "702-555-1234",
    "hEmail": "info@example.com",
    "hContactName": "JANE DOE",
    "hdCounty": "CLARK",
    "hExpiryDate": "05/31/2026",
    "hCredentialType": "CENTER (PROVISIONAL)",
    "hLicenseTypeCode": "CCC",
    "hLicenseeType": "B",
    "hdnentityType": "LSE",
    "hLicenseeId": "139953",
    "HfAddressTypeCode": "PHL",
    "hLicenseId": "168119",
    "hfProgram": "CCP",
}

# A row with several fields blank to exercise None handling.
SPARSE_ROW = {
    "hfName": "TINY DAYCARE",
    "hfLicenseNumberToDisplay": "9999-26",
    "hfLicenseNumber": "9999",
    "hdnStatus": "ACT",
    "hdnStatusCode": "Active",
    "hPrimaryAddress": "123 ELM ST LAS VEGAS, NV 89000",
    "hPhoneNumber": "",
    "hEmail": "",
    "hContactName": "",
    "hdCounty": "CLARK",
    "hExpiryDate": "12/31/2026",
    "hCredentialType": "FAMILY CARE",
    "hLicenseTypeCode": "CCC",
    "hLicenseeType": "B",
    "hdnentityType": "LSE",
    "hLicenseeId": "12345",
    "HfAddressTypeCode": "PHL",
    "hLicenseId": "678",
    "hfProgram": "CCP",
}


# ---- Pure helpers ----


def test_short_name_returns_suffix_after_last_dollar():
    assert short_name("ctl00$Foo$Bar$hCredentialType") == "hCredentialType"
    assert short_name("plain") == "plain"
    assert short_name("") == ""


def test_base_facility_type_strips_parenthetical():
    assert base_facility_type("CENTER (PROVISIONAL)") == "CENTER"
    assert base_facility_type("CENTER") == "CENTER"
    assert base_facility_type("  GROUP CARE  ") == "GROUP CARE"
    assert base_facility_type(None) is None
    assert base_facility_type("") is None


def test_format_age_range_combines_from_and_to():
    assert format_age_range("6 weeks", "5") == "6 weeks - 5 years"
    assert format_age_range("0", "12") == "0 - 12 years"
    assert format_age_range("6 weeks", None) == "from 6 weeks"
    assert format_age_range(None, "5") == "up to 5 years"
    assert format_age_range(None, None) is None


def test_format_hours_skips_closed_days_and_pads_minutes():
    rows = [
        ("Sunday", ["Closed"]),
        ("Monday", ["Open at Set Time", "7", "30", "AM", "4", "", "PM"]),
        ("Tuesday", ["Open at Set Time", "7", "30", "AM", "4", "", "PM"]),
        ("Saturday", ["Closed"]),
    ]
    formatted = format_hours(rows)
    assert formatted == "Mon 7:30 AM - 4:00 PM; Tue 7:30 AM - 4:00 PM"


def test_format_hours_returns_none_when_every_day_closed():
    rows = [(d, ["Closed"]) for d in ["Sunday", "Monday"]]
    assert format_hours(rows) is None


def test_build_detail_url_uses_row_fields():
    url = build_detail_url(
        "https://nvdpbh.aithent.com/Protected/LIC/LicenseeSearch.aspx",
        GOLDEN_ROW,
    )
    assert url.startswith("https://nvdpbh.aithent.com/Protected/INS/SODPublicView.aspx?")
    assert "LicenseeId=139953" in url
    assert "Program=CCP" in url
    assert "CredentialType=CCC" in url
    assert "LicenseeType=B" in url
    assert "EntityType=LSE" in url
    assert "LicenseNumber=831" in url
    assert "AddressTypeCode=PHL" in url
    assert "LicenseId=168119" in url
    assert "Mode=V" in url
    assert "IsPopUp=Y" in url


# ---- Search row extraction ----


def test_collect_row_fields_strips_aspnet_prefix():
    html = _search_results_html(_search_row_html("02", GOLDEN_ROW))
    response = HtmlResponse(url="http://example.com", body=html, encoding="utf-8")
    row = response.css('table[id*="ResultsGrid"] tr')[1]
    fields = collect_row_fields(row)
    assert fields["hfName"] == "ACELERO LEARNING CCC - HENDERSON"
    assert fields["hCredentialType"] == "CENTER (PROVISIONAL)"


def test_build_provider_from_row_golden_path(spider):
    item = spider.build_provider_from_row(GOLDEN_ROW)
    assert isinstance(item, ProviderItem)
    assert item["source_state"] == "NV"
    assert item["provider_name"] == "ACELERO LEARNING CCC - HENDERSON"
    assert item["license_number"] == "831-26"
    # Status should be the human-readable label, not the ACT code.
    assert item["status"] == "Active"
    assert item["address"] == "180 N. WESTMINSTER WAY HENDERSON, NV 89015"
    assert item["phone"] == "702-555-1234"
    assert item["email"] == "info@example.com"
    assert item["administrator"] == "JANE DOE"
    assert item["county"] == "CLARK"
    assert item["license_expiration"] == "05/31/2026"
    # nv_credential_type keeps the full label; nv_facility_type strips modifiers.
    assert item["nv_credential_type"] == "CENTER (PROVISIONAL)"
    assert item["nv_facility_type"] == "CENTER"
    assert item["provider_type"] == "CENTER"
    assert item["nv_operation_id"] == "168119"
    assert item["inspections"] == []


def test_build_provider_from_row_missing_optional_fields(spider):
    item = spider.build_provider_from_row(SPARSE_ROW)
    # Empty strings should normalize to None, not show up as "".
    assert item["phone"] is None
    assert item["email"] is None
    assert item["administrator"] is None
    # Required fields still present.
    assert item["provider_name"] == "TINY DAYCARE"
    assert item["nv_facility_type"] == "FAMILY CARE"


def test_build_provider_from_row_falls_back_to_status_code(spider):
    row = dict(GOLDEN_ROW)
    row["hdnStatusCode"] = ""  # display label missing
    item = spider.build_provider_from_row(row)
    assert item["status"] == "ACT"


def test_parse_search_results_dispatches_one_request_per_row(spider):
    html = _search_results_html(
        _search_row_html("02", GOLDEN_ROW),
        _search_row_html("03", SPARSE_ROW),
    )
    response = HtmlResponse(
        url="https://nvdpbh.aithent.com/Protected/LIC/LicenseeSearch.aspx",
        body=html,
        encoding="utf-8",
        request=Request(url="https://nvdpbh.aithent.com/Protected/LIC/LicenseeSearch.aspx"),
    )
    requests = list(spider.parse_search_results(response))
    # Two providers, no pagination links present.
    assert len(requests) == 2
    for req in requests:
        assert "SODPublicView.aspx" in req.url
        partial = req.meta["partial_item"]
        assert isinstance(partial, ProviderItem)
        assert partial["provider_url"] == req.url


# ---- Detail page parsing ----


def _hours_row_html(ctl_suffix, day, spans):
    """Render a single hours-of-operation grid row matching the live HTML shape."""
    inner_id = (
        f"ctl00_ContentPlaceHolder1_ucHoursOfOperation_ucHoursOfOperation_"
        f"ucGridUserControl_ResultsGrid_ctl{ctl_suffix}"
    )
    cells = [f'<td><span id="{inner_id}_lblDay">{day}</span></td>']
    for s in spans:
        cells.append(f'<td><span class="dropDownDisableSection">{s}</span></td>')
    return "<tr>" + "".join(cells) + "</tr>"


def _sod_row_html(ctl_suffix, date, number, reason, count, status_code="CLS", status_reason="POCA", wrap_count_in_font=False):
    inner_id = f"ctl00_ContentPlaceHolder1_ucSODgrid_ResultsGrid_ctl{ctl_suffix}"
    # Live site wraps the count in a <font> tag inside the span; older fixtures
    # had it as bare text. We test both shapes.
    count_inner = (
        f'<font color="Red">{count}</font>' if wrap_count_in_font else count
    )
    return (
        "<tr>"
        f'<td><span id="{inner_id}_lblInspectionEndDate">{date}</span></td>'
        f'<td><span id="{inner_id}_lblInspectionNumber">{number}</span></td>'
        f'<td><span id="{inner_id}_lblGrade"></span></td>'
        f'<td><span id="{inner_id}_InspectionReason">{reason}</span></td>'
        f'<td><span id="{inner_id}_lblCount">{count_inner}</span>'
        f'<input type="hidden" id="{inner_id}_hdSODStatusCode" value="{status_code}">'
        f'<input type="hidden" id="{inner_id}_hdSODStatusReasonCode" value="{status_reason}">'
        '</td>'
        "</tr>"
    )


def _detail_html(hours_rows, sod_rows, total="243", row1_from="6 weeks", row1_to="5"):
    return f"""
    <html><body>
        <input id="ctl00_ContentPlaceHolder1_ucChildrenAge_txtRow1Age1" type="text" value="{row1_from}">
        <input id="ctl00_ContentPlaceHolder1_ucChildrenAge_txtRow1Age2" type="text" value="{row1_to}">
        <span id="ctl00_ContentPlaceHolder1_ucChildrenAge_lblTotal">{total}</span>
        <table id="ctl00_ContentPlaceHolder1_ucHoursOfOperation_ucHoursOfOperation_ucGridUserControl_ResultsGrid">
            <thead><tr><th>Day</th></tr></thead>
            <tbody>{''.join(hours_rows)}</tbody>
        </table>
        <table id="ctl00_ContentPlaceHolder1_ucSODgrid_ResultsGrid">
            <thead><tr><th>Date</th></tr></thead>
            <tbody>{''.join(sod_rows)}</tbody>
        </table>
    </body></html>
    """


def test_parse_detail_enriches_partial_item(spider):
    partial = spider.build_provider_from_row(GOLDEN_ROW)
    hours_rows = [
        _hours_row_html("02", "Sunday", ["Closed"]),
        _hours_row_html("03", "Monday", ["Open at Set Time", "7", "30", "AM", "4", "", "PM"]),
        _hours_row_html("04", "Tuesday", ["Open at Set Time", "7", "30", "AM", "4", "", "PM"]),
        _hours_row_html("05", "Wednesday", ["Open at Set Time", "7", "30", "AM", "4", "", "PM"]),
        _hours_row_html("06", "Thursday", ["Open at Set Time", "7", "30", "AM", "4", "", "PM"]),
        _hours_row_html("07", "Friday", ["Open at Set Time", "7", "30", "AM", "4", "", "PM"]),
        _hours_row_html("08", "Saturday", ["Closed"]),
    ]
    sod_rows = [
        # Live site shape: count wrapped in <font>
        _sod_row_html("02", "03/24/2026 2:15 PM", "77473", "Ad-hoc, Annual", "(3)", wrap_count_in_font=True),
        # Bare-text shape (matches the older fixture)
        _sod_row_html("03", "02/24/2026 2:30 PM", "76735", "Annual", "(1)"),
    ]
    body = _detail_html(hours_rows, sod_rows)
    response = HtmlResponse(
        url="https://nvdpbh.aithent.com/Protected/INS/SODPublicView.aspx?LicenseeId=139953",
        body=body,
        encoding="utf-8",
        request=Request(url="https://nvdpbh.aithent.com/Protected/INS/SODPublicView.aspx", meta={"partial_item": partial}),
    )
    response.meta["partial_item"] = partial

    # parse_detail now accumulates (it no longer yields the item directly): the
    # finished provider lands in providers_by_license, keyed on the base license.
    spider.pending_details = 1
    list(spider.parse_detail(response))
    assert "831" in spider.providers_by_license
    item = spider.providers_by_license["831"]
    assert item["capacity"] == 243
    assert item["ages_served"] == "6 weeks - 5 years"
    assert item["hours"] == (
        "Mon 7:30 AM - 4:00 PM; Tue 7:30 AM - 4:00 PM; Wed 7:30 AM - 4:00 PM;"
        " Thu 7:30 AM - 4:00 PM; Fri 7:30 AM - 4:00 PM"
    )

    inspections = item["inspections"]
    assert len(inspections) == 2
    assert all(isinstance(i, InspectionItem) for i in inspections)
    first = inspections[0]
    assert first["date"] == "03/24/2026 2:15 PM"
    assert first["type"] == "Ad-hoc, Annual"
    assert first["nv_inspection_number"] == "77473"
    # Count is wrapped in <font> on the live site; parser must still extract it.
    assert first["nv_deficiency_count"] == 3
    assert first["original_status"] == "CLS"
    assert first["corrective_status"] == "POCA"
    # Second inspection has bare-text count.
    assert inspections[1]["nv_deficiency_count"] == 1


def test_parse_detail_handles_missing_sections(spider):
    """A provider with no hours, no inspections, and no age data still yields an item."""
    partial = spider.build_provider_from_row(SPARSE_ROW)
    body = _detail_html(
        hours_rows=[],
        sod_rows=[],
        total="",
        row1_from="",
        row1_to="",
    )
    response = HtmlResponse(
        url="https://nvdpbh.aithent.com/Protected/INS/SODPublicView.aspx?LicenseeId=12345",
        body=body,
        encoding="utf-8",
        request=Request(url="https://nvdpbh.aithent.com/Protected/INS/SODPublicView.aspx", meta={"partial_item": partial}),
    )
    response.meta["partial_item"] = partial

    spider.pending_details = 1
    list(spider.parse_detail(response))
    item = spider.providers_by_license["9999"]
    # Search-row fields still present.
    assert item["provider_name"] == "TINY DAYCARE"
    # Missing detail fields stay as their pre-existing values (None for unset).
    assert item.get("capacity") is None
    assert item.get("ages_served") is None
    assert item.get("hours") is None
    assert item["inspections"] == []


# ---- Quality enrichment: DSR decoding ----


def _quality_response_json(dm0, value_dicts, dict_names, restart_token=None):
    """Assemble a minimal querydata response around a hand-built DM0 row set."""
    dm0 = [dict(row) for row in dm0]
    dm0[0]["S"] = [
        ({"N": f"G{i}", "T": 1, "DN": dn} if dn else {"N": f"G{i}", "T": 7})
        for i, dn in enumerate(dict_names)
    ]
    ds = {"N": "DS0", "PH": [{"DM0": dm0}], "ValueDicts": value_dicts}
    if restart_token is not None:
        ds["RT"] = restart_token
        ds["IC"] = False
    else:
        ds["IC"] = True
    return {"results": [{"result": {"data": {"dsr": {"DS": [ds]}}}}]}


def test_decode_data_shape_golden_path_real_fixture():
    """The captured first window decodes to 500 rows with dictionaries resolved."""
    rows, restart_token = decode_data_shape(_load_fixture("nv_quality_window1.json"))
    assert len(rows) == 500
    # Window 1 is not the last; it carries a restart token to page the next window.
    assert restart_token is not None

    first = dict(zip((p for p, _, _ in QUALITY_SELECT), rows[0]))
    assert first["LicenseNumber"] == "02204-D"
    assert first["ProgramName"] == "Squires ES"
    assert first["ProgramType"] == "School Based"
    assert first["StarRatingFriendlyName"] == "Five Stars"
    # Date columns stay as raw epoch-ms until copied onto an item.
    assert epoch_ms_to_date(first["RatingPeriodStartDate"]) == "07/01/2025"


def test_decode_data_shape_second_window_is_complete():
    rows, restart_token = decode_data_shape(_load_fixture("nv_quality_window2.json"))
    assert len(rows) == 14
    # Last window: no restart token.
    assert restart_token is None


def test_decode_dm0_repeat_null_and_inline_string():
    """Exercise multi-bit R repeats, the Ø null mask, and dictionary overflow."""
    value_dicts = {"D0": ["Alpha", "Beta"], "D1": ["North", "South"]}
    dict_names = ["D0", None, "D1"]  # col1 is a date (no dictionary)
    dm0 = [
        {"C": [0, 1700000000000, 0]},                 # Alpha, date, North
        {"C": [1], "R": 0b110},                        # Beta; cols 1,2 repeat
        {"C": [1710000000000], "R": 0b001, "Ø": 0b100},  # col0 repeat, col2 null
        {"C": ["Gamma Inline", 1720000000000, 1]},     # col0 inline string overflow
    ]
    rows, _ = decode_data_shape(
        _quality_response_json(dm0, value_dicts, dict_names)
    )
    assert rows[0] == ["Alpha", 1700000000000, "North"]
    assert rows[1] == ["Beta", 1700000000000, "North"]
    assert rows[2] == ["Beta", 1710000000000, None]
    assert rows[3] == ["Gamma Inline", 1720000000000, "South"]


def test_decode_data_shape_raises_on_query_definition_error():
    bad = {
        "results": [
            {"result": {"data": {"dsr": {"DataShapes": [{"odata.error": "boom"}]}}}}
        ]
    }
    with pytest.raises(ValueError):
        decode_data_shape(bad)


def test_epoch_ms_to_date_converts_utc_midnight():
    assert epoch_ms_to_date(1777507200000) == "04/30/2026"
    assert epoch_ms_to_date(1704067200000) == "01/01/2024"
    assert epoch_ms_to_date(None) is None


# ---- Quality enrichment: license normalization ----


def test_normalize_license_strips_suffix_and_leading_zeros():
    assert normalize_license("831-26") == "831"
    assert normalize_license("028-26") == "28"      # leading zero dropped
    assert normalize_license("02204-D") == "2204"    # school-based suffix
    assert normalize_license("UTF1028517") == "UTF1028517"
    assert normalize_license(None) is None
    assert normalize_license("") is None


# ---- Quality enrichment: query builder ----


def test_build_quality_command_literals_and_selection():
    command = build_quality_command(2026, "April")["SemanticQueryDataShapeCommand"]
    select = command["Query"]["Select"]
    # LicenseNumber must be the first projection so the join key is column 0.
    assert select[0]["Column"]["Property"] == "LicenseNumber"
    where = command["Query"]["Where"]
    # Long literal carries an L suffix and no quotes; month is single-quoted.
    assert where[0]["Condition"]["In"]["Values"][0][0]["Literal"]["Value"] == "2026L"
    assert where[1]["Condition"]["In"]["Values"][0][0]["Literal"]["Value"] == "'April'"
    window = command["Binding"]["DataReduction"]["Primary"]["Window"]
    assert "RestartTokens" not in window


def test_build_quality_command_threads_restart_token():
    token = [["'tok'"]]
    command = build_quality_command(2026, "April", restart_token=token)[
        "SemanticQueryDataShapeCommand"
    ]
    window = command["Binding"]["DataReduction"]["Primary"]["Window"]
    assert window["RestartTokens"] == token


# ---- Quality enrichment: join + dedupe ----


def _quality_row(license_number, **overrides):
    row = {
        "LicenseNumber": license_number,
        "ProgramName": "Example",
        "ProgramType": "Center",
        "County": "Clark",
        "Region": "South",
        "StarRatingFriendlyName": "Three Stars",
        "StatusFriendlyName": "Maintenance",
        "RatingPeriodStartDate": 1704067200000,  # 01/01/2024
        "RatingPeriodEndDate": 1777507200000,    # 04/30/2026
    }
    row.update(overrides)
    return row


def test_enrich_and_finish_copies_fields_onto_match(spider):
    provider = spider.build_provider_from_row(GOLDEN_ROW)  # license 831-26
    spider.providers_by_license = {"831": provider}
    spider.quality_rows = [_quality_row("831")]

    items = list(spider._enrich_and_finish())

    assert len(items) == 1
    item = items[0]
    assert item["nv_star_rating"] == "Three Stars"
    assert item["nv_program_type"] == "Center"
    assert item["nv_region"] == "South"
    assert item["nv_qris_status"] == "Maintenance"
    assert item["nv_rating_period_start"] == "01/01/2024"
    assert item["nv_rating_period_end"] == "04/30/2026"
    # Licensing fields are never overwritten by the quality source.
    assert item["county"] == "CLARK"
    assert item["status"] == "Active"


def test_enrich_and_finish_drops_unmatched_quality_rows(spider):
    provider = spider.build_provider_from_row(GOLDEN_ROW)  # license 831
    spider.providers_by_license = {"831": provider}
    spider.quality_rows = [_quality_row("999")]  # no licensed counterpart

    items = list(spider._enrich_and_finish())

    # The provider is still emitted, just without quality fields.
    assert len(items) == 1
    assert "nv_star_rating" not in items[0]


def test_enrich_and_finish_keeps_latest_rating_period(spider):
    provider = spider.build_provider_from_row(GOLDEN_ROW)
    spider.providers_by_license = {"831": provider}
    spider.quality_rows = [
        _quality_row(
            "831",
            StarRatingFriendlyName="Two Stars",
            RatingPeriodEndDate=1704067200000,  # older
        ),
        _quality_row(
            "831",
            StarRatingFriendlyName="Four Stars",
            RatingPeriodEndDate=1777507200000,  # newer -> wins
        ),
    ]

    items = list(spider._enrich_and_finish())
    assert items[0]["nv_star_rating"] == "Four Stars"


# ---- Quality enrichment: lifecycle ----


def test_quality_gate_waits_for_pagination_and_pending_details(spider):
    provider = spider.build_provider_from_row(GOLDEN_ROW)
    spider.pending_details = 1

    # Detail returns but pagination isn't finished yet -> no quality kickoff.
    assert list(spider._accumulate_provider(provider)) == []
    assert spider.providers_by_license["831"] is provider
    assert spider.quality_started is False

    # Pagination finishes; the gate trips exactly once and emits the discovery POST.
    spider.pagination_done = True
    requests = list(spider._maybe_start_quality())
    assert len(requests) == 1
    assert requests[0].method == "POST"
    assert requests[0].url == POWERBI_QUERY_URL
    assert spider.quality_started is True
    # Firing again is a no-op.
    assert list(spider._maybe_start_quality()) == []


def _full_quality_window(restart_token=None):
    """A one-row window matching the full nine-column quality projection."""
    value_dicts = {
        "D0": ["831"],            # LicenseNumber
        "D1": ["Example"],        # ProgramName
        "D2": ["Center"],         # ProgramType
        "D3": ["Clark"],          # County
        "D4": ["South"],          # Region
        "D5": ["Three Stars"],    # StarRatingFriendlyName
        "D6": ["Maintenance"],    # StatusFriendlyName
    }
    dict_names = ["D0", "D1", "D2", "D3", "D4", "D5", "D6", None, None]
    dm0 = [{"C": [0, 0, 0, 0, 0, 0, 0, 1704067200000, 1777507200000]}]
    return _quality_response_json(
        dm0, value_dicts, dict_names, restart_token=restart_token
    )


def test_parse_quality_window_pages_then_finishes(spider):
    provider = spider.build_provider_from_row(GOLDEN_ROW)
    spider.providers_by_license = {"831": provider}

    # First window carries a restart token -> spider issues the next window.
    win1 = _full_quality_window(restart_token=[["'831'"]])
    request = Request(
        url=POWERBI_QUERY_URL,
        method="POST",
        body=json.dumps(
            {
                "queries": [
                    {"Query": {"Commands": [build_quality_command(2026, "April")]}}
                ]
            }
        ),
    )
    resp1 = TextResponse(
        url=POWERBI_QUERY_URL,
        body=json.dumps(win1).encode(),
        encoding="utf-8",
        request=request,
    )
    out1 = list(spider.parse_quality_window(resp1))
    assert len(out1) == 1
    assert out1[0].method == "POST"
    assert spider.quality_window == 1
    assert len(spider.quality_rows) == 1

    # Final window has no restart token -> enrichment runs and providers emit.
    win2 = _full_quality_window()
    resp2 = TextResponse(
        url=POWERBI_QUERY_URL, body=json.dumps(win2).encode(), encoding="utf-8"
    )
    out2 = list(spider.parse_quality_window(resp2))
    assert out2 == [provider]


def test_parse_period_discovery_picks_latest_then_queries(spider):
    resp = TextResponse(
        url=POWERBI_QUERY_URL,
        body=json.dumps(_load_fixture("nv_period_discovery.json")).encode(),
        encoding="utf-8",
    )
    requests = list(spider.parse_period_discovery(resp))
    assert len(requests) == 1
    body = json.loads(requests[0].body)
    where = body["queries"][0]["Query"]["Commands"][0][
        "SemanticQueryDataShapeCommand"
    ]["Query"]["Where"]
    # Latest data month in the fixture is April 2026.
    assert where[0]["Condition"]["In"]["Values"][0][0]["Literal"]["Value"] == "2026L"
    assert where[1]["Condition"]["In"]["Values"][0][0]["Literal"]["Value"] == "'April'"


def test_parse_period_discovery_falls_back_on_bad_response(spider):
    bad = {"results": [{"result": {"data": {"dsr": {"DataShapes": []}}}}]}
    resp = TextResponse(
        url=POWERBI_QUERY_URL, body=json.dumps(bad).encode(), encoding="utf-8"
    )
    requests = list(spider.parse_period_discovery(resp))
    # Falls back to the hardcoded period rather than failing outright.
    assert len(requests) == 1
    body = json.loads(requests[0].body)
    where = body["queries"][0]["Query"]["Commands"][0][
        "SemanticQueryDataShapeCommand"
    ]["Query"]["Where"]
    assert where[0]["Condition"]["In"]["Values"][0][0]["Literal"]["Value"] == "2026L"

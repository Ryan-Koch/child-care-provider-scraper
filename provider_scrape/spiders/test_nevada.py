import pytest
from scrapy.http import HtmlResponse, Request

from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.spiders.nevada import (
    NevadaSpider,
    base_facility_type,
    build_detail_url,
    collect_row_fields,
    format_age_range,
    format_hours,
    short_name,
)


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

    items = list(spider.parse_detail(response))
    assert len(items) == 1
    item = items[0]
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

    items = list(spider.parse_detail(response))
    assert len(items) == 1
    item = items[0]
    # Search-row fields still present.
    assert item["provider_name"] == "TINY DAYCARE"
    # Missing detail fields stay as their pre-existing values (None for unset).
    assert item.get("capacity") is None
    assert item.get("ages_served") is None
    assert item.get("hours") is None
    assert item["inspections"] == []

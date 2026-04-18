import pytest
from scrapy.http import HtmlResponse, Request

from provider_scrape.items import ProviderItem
from provider_scrape.spiders.minnesota import (
    COUNTIES,
    CSV_COLUMNS,
    MinnesotaSpider,
    _compose_address,
    _parse_csv_body,
    _results_url,
    _row_to_item,
)


@pytest.fixture
def spider():
    return MinnesotaSpider()


def _make_response(url, body, meta=None):
    request = Request(url=url, meta=meta or {})
    return HtmlResponse(url=url, request=request, body=body, encoding="utf-8")


# ---- County list sanity ----

def test_counties_count():
    assert len(COUNTIES) == 90


def test_counties_ids_no_duplicates():
    ids = [cid for (cid, _) in COUNTIES]
    assert len(ids) == len(set(ids))


def test_counties_names_no_duplicates():
    names = [name for (_, name) in COUNTIES]
    assert len(names) == len(set(names))


# ---- _results_url ----

def test_results_url_includes_both_co_and_con():
    url = _results_url(25, "Goodhue")
    assert "&co=25" in url
    assert "&con=Goodhue" in url


def test_results_url_url_encodes_county_name():
    url = _results_url(18, "Crow Wing")
    # space must be percent-encoded, not rendered as '+'
    assert "&con=Crow%20Wing" in url
    assert "&co=18" in url


def test_results_url_encodes_special_characters():
    url = _results_url(69, "St. Louis")
    assert "&con=St.%20Louis" in url
    url = _results_url(92, "Faribault & Martin")
    assert "&con=Faribault%20%26%20Martin" in url


# ---- Spider args ----

def test_default_county_delay_is_120():
    s = MinnesotaSpider()
    assert s.county_delay == 120.0


def test_county_delay_arg_is_respected():
    s = MinnesotaSpider(county_delay="45")
    assert s.county_delay == 45.0


def test_counties_arg_filters_by_id():
    s = MinnesotaSpider(counties="25,1")
    # filter preserves COUNTIES list order
    assert s._debug_counties == [(1, "Aitkin"), (25, "Goodhue")]


# ---- start_requests ----

def test_start_requests_yields_one_request_per_county(spider):
    requests = list(spider.start_requests())
    assert len(requests) == len(COUNTIES)
    for req in requests:
        assert "Results.aspx" in req.url
        assert "&co=" in req.url
        assert "&con=" in req.url
        assert req.meta["playwright"] is True
        assert req.meta["playwright_include_page"] is True
        assert req.callback == spider.parse_county
        assert "county_id" in req.meta
        assert "county_name" in req.meta


def test_start_requests_county_ids_in_meta(spider):
    requests = list(spider.start_requests())
    meta_pairs = [(r.meta["county_id"], r.meta["county_name"]) for r in requests]
    assert meta_pairs == COUNTIES
    for req, (county_id, county_name) in zip(requests, COUNTIES):
        assert req.url == _results_url(county_id, county_name)


# ---- _compose_address ----

def test_compose_address_full():
    row = {
        "AddressLine1": "520 E 5th St",
        "AddressLine2": "",
        "AddressLine3": "",
        "City": "Wanamingo",
        "State": "MN",
        "Zip": "55983-0000",
    }
    assert _compose_address(row) == "520 E 5th St, Wanamingo, MN 55983-0000"


def test_compose_address_with_secondary_line():
    row = {
        "AddressLine1": "300 Red Wing Ave S",
        "AddressLine2": "Unit 218",
        "AddressLine3": "",
        "City": "Red Wing",
        "State": "MN",
        "Zip": "55066",
    }
    assert (
        _compose_address(row)
        == "300 Red Wing Ave S, Unit 218, Red Wing, MN 55066"
    )


def test_compose_address_missing_street_foster_care():
    """Foster care rows often only carry County — no street/city/state/zip."""
    row = {
        "AddressLine1": "",
        "AddressLine2": "",
        "AddressLine3": "",
        "City": "",
        "State": "",
        "Zip": "",
    }
    assert _compose_address(row) is None


# ---- _parse_csv_body ----

def _tsv(rows):
    return "\n".join("\t".join(r) for r in rows)


def test_parse_csv_body_golden_path():
    header = CSV_COLUMNS
    row = [
        "1073255", "Home and Community Based Services",
        "5th Street House", "520 E 5th St", "", "",
        "Wanamingo", "MN", "55983-0000", "Goodhue",
        "(507) 824-2482", "Active", "Riverview Services Inc",
        "4", "Satellite", "None", "",
        "Goodhue County Social Services",
        "7/1/14", "6/1/24", "5/31/26", "No", "",
    ]
    body = _tsv([header, row])
    parsed = _parse_csv_body(body)
    assert len(parsed) == 1
    assert parsed[0]["License Number"] == "1073255"
    assert parsed[0]["Name of Program"] == "5th Street House"
    assert parsed[0]["License Holder Lives Onsite"] == "No"


def test_parse_csv_body_empty():
    assert _parse_csv_body("") == []


def test_parse_csv_body_unexpected_header():
    body = "some\thtml\tdoc\nvalue1\tvalue2\tvalue3\n"
    assert _parse_csv_body(body) == []


def test_parse_csv_body_multiple_rows():
    header = CSV_COLUMNS
    row1 = ["1"] + [""] * (len(header) - 1)
    row1[2] = "Program One"
    row2 = ["2"] + [""] * (len(header) - 1)
    row2[2] = "Program Two"
    body = _tsv([header, row1, row2])
    parsed = _parse_csv_body(body)
    assert [r["Name of Program"] for r in parsed] == ["Program One", "Program Two"]


def test_parse_csv_body_quoted_csv_with_trailing_comma():
    """Mirrors the real MN DHS response: comma-separated, double-quoted,
    trailing comma on every line."""
    header = ",".join(f'"{c}"' for c in CSV_COLUMNS) + ","
    row_values = [
        "1073255",
        "Home and Community Based Services - Community Residential Setting",
        "5th Street House",
        "520 E 5th St",
        "",
        "",
        "Wanamingo",
        "MN",
        "55983-0000",
        "Goodhue",
        "(507) 824-2482",
        "Active",
        "Riverview Services Inc",
        "4  ",
        "Satellite of 245D-HCBS Program 1073254",
        "None",
        "",
        "Goodhue County Social Services",
        "07/01/2014",
        "06/01/2024",
        "05/31/2026",
        "No ",
        "",
    ]
    row = ",".join(f'"{v}"' for v in row_values) + ","
    body = header + "\n" + row + "\n"
    parsed = _parse_csv_body(body)
    assert len(parsed) == 1
    assert parsed[0]["License Number"] == "1073255"
    assert parsed[0]["Name of Program"] == "5th Street House"
    assert parsed[0]["County"] == "Goodhue"
    # Trailing whitespace on capacity/onsite should be preserved here — the
    # row→item converter is responsible for stripping it.
    assert parsed[0]["Capacity"] == "4  "
    assert parsed[0]["License Holder Lives Onsite"] == "No "


# ---- _row_to_item ----

def _sample_row(**overrides):
    row = {
        "License Number": "1101850",
        "License Type": "Child Care Center",
        "Name of Program": "AB.SEE Preschool & Early Learning Center",
        "AddressLine1": "300 Park St W",
        "AddressLine2": "",
        "AddressLine3": "",
        "City": "Cannon Falls",
        "State": "MN",
        "Zip": "55009-2429",
        "County": "Goodhue",
        "Phone": "(507) 757-1110",
        "License Status": "Active",
        "License Holder": "AB.SEE Preschool & Early Learning Center LLC",
        "Capacity": "42",
        "Type Of License": "Ages Served: Infants Toddlers Preschool",
        "Restrictions": "None",
        "Services": "Services: Day Program",
        "Licensing Authority": "Minnesota Department of Human Services",
        "Initial Effective Date": "6/29/20",
        "Current Effective Date": "1/1/26",
        "Expiration Date": "12/31/26",
        "License Holder Lives Onsite": "",
        "EmailAddress": "info@absee.example.com",
    }
    row.update(overrides)
    return row


def test_row_to_item_golden_path():
    item = _row_to_item(_sample_row())
    assert isinstance(item, ProviderItem)
    assert item["source_state"] == "Minnesota"
    assert item["provider_url"] is None
    assert item["license_number"] == "1101850"
    assert item["provider_type"] == "Child Care Center"
    assert item["provider_name"] == "AB.SEE Preschool & Early Learning Center"
    assert item["address"] == "300 Park St W, Cannon Falls, MN 55009-2429"
    assert item["mn_county"] == "Goodhue"
    assert item["phone"] == "(507) 757-1110"
    assert item["status"] == "Active"
    assert item["mn_license_holder"] == "AB.SEE Preschool & Early Learning Center LLC"
    assert item["capacity"] == "42"
    assert item["mn_type_of_license"] == "Ages Served: Infants Toddlers Preschool"
    assert item["mn_restrictions"] == "None"
    assert item["mn_licensed_to_provide"] == "Services: Day Program"
    assert item["mn_initial_effective_date"] == "6/29/20"
    assert item["mn_last_renewed_date"] == "1/1/26"
    assert item["mn_next_renewal_due"] == "12/31/26"
    assert item["mn_license_holder_onsite"] is None
    assert item["email"] == "info@absee.example.com"


def test_row_to_item_foster_care_no_address():
    """Foster care rows omit street/city/state/zip — only County is present."""
    row = _sample_row(
        **{
            "License Number": "1103070",
            "License Type": "Child Foster Care",
            "Name of Program": "Andring Angela Louise & Andring Jared Adam",
            "AddressLine1": "",
            "City": "",
            "State": "",
            "Zip": "",
            "Phone": "",
            "EmailAddress": "",
            "Capacity": "",
            "Type Of License": "",
            "Restrictions": "",
            "Services": "",
        }
    )
    item = _row_to_item(row)
    assert item["provider_name"].startswith("Andring")
    assert item["address"] is None
    assert item["phone"] is None
    assert item["email"] is None
    assert item["capacity"] is None
    assert item["mn_restrictions"] is None
    assert item["mn_licensed_to_provide"] is None
    assert item["mn_county"] == "Goodhue"


def test_row_to_item_closed_status():
    row = _sample_row(**{"License Status": "Closed as of 11/07/2025"})
    item = _row_to_item(row)
    assert item["status"] == "Closed as of 11/07/2025"


def test_row_to_item_with_secondary_address_line():
    row = _sample_row(
        **{
            "AddressLine1": "300 Red Wing Ave S",
            "AddressLine2": "Unit 218",
            "City": "Red Wing",
            "Zip": "55066",
        }
    )
    item = _row_to_item(row)
    assert item["address"] == "300 Red Wing Ave S, Unit 218, Red Wing, MN 55066"


def test_row_to_item_license_holder_lives_onsite_yes():
    row = _sample_row(**{"License Holder Lives Onsite": "Yes"})
    assert _row_to_item(row)["mn_license_holder_onsite"] == "Yes"

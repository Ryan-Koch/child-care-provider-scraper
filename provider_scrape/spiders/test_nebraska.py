import json
import os

import pytest
from scrapy.http import HtmlResponse, Request, TextResponse

from provider_scrape import normalization as norm
from provider_scrape.items import ProviderItem
from provider_scrape.spiders.nebraska import (
    NRRS_DETAIL_URL,
    NRRS_SEARCH_URL,
    STEPUP_SEARCH_URL,
    NebraskaSpider,
    age_flags_from_nrrs_range,
    apply_stepup_standalone_fields,
    build_stepup_index,
    build_stepup_record,
    derive_county,
    derive_provider_type,
    extract_search_results,
    extract_zip5,
    format_nrrs_hours,
    is_real_coordinate,
    license_prefix,
    norm_name,
    normalize_zip,
    parse_stepup_search_entry,
    stepup_provider_type,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def _load_json_fixture(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return json.load(fh)


def _load_text_fixture(name):
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as fh:
        return fh.read()


@pytest.fixture
def spider():
    return NebraskaSpider()


# --- response builders --------------------------------------------------- #


def nrrs_search_response(payload, page):
    req = Request(NRRS_SEARCH_URL, meta={"page": page})
    return TextResponse(url=NRRS_SEARCH_URL, body=json.dumps(payload).encode(), encoding="utf-8", request=req)


def nrrs_detail_response(detail_fixture_name, summary=None):
    payload = _load_json_fixture(detail_fixture_name)
    url = NRRS_DETAIL_URL.format(id=payload["id"])
    req = Request(url, meta={"summary": summary or {"id": payload["id"], "license_no": payload.get("license_no")}})
    return TextResponse(url=url, body=json.dumps(payload).encode(), encoding="utf-8", request=req)


def stepup_search_response(html_text=None):
    html_text = html_text if html_text is not None else _load_text_fixture("ne_stepup_search.html")
    req = Request(STEPUP_SEARCH_URL)
    return TextResponse(url=STEPUP_SEARCH_URL, body=html_text.encode("utf-8"), encoding="utf-8", request=req)


def stepup_detail_response(html_text, url, summary):
    req = Request(url, meta={"summary": summary})
    return HtmlResponse(url=url, body=html_text.encode("utf-8"), encoding="utf-8", request=req)


# --- pure helper unit tests ----------------------------------------------- #


def test_extract_search_results_balanced_brace_scan():
    # The embedded JSON's own info-window HTML contains the two-character
    # sequence `"};` (a closing </strong> then a semicolon-terminated tag),
    # which is exactly what breaks a lazy `var searchResults = (\{.*?\});`
    # regex (plan Sec 2.2/12). A trailing sibling statement after the real
    # `};` must also NOT be swallowed.
    html = (
        "<script>var searchResults = "
        '{"center":{"lat":1,"lng":2},"providers":[{"lat":"1","lng":"2",'
        '"icon":"x.svg","html":"<p><strong>A\\"};B</strong><a href=\'u\'>Learn More</a></p>"}]};'
        'var other = {"a": 1};</script>'
    )
    data = extract_search_results(html)
    assert data is not None
    assert len(data["providers"]) == 1
    assert data["providers"][0]["lat"] == "1"


def test_extract_search_results_missing_marker_returns_none():
    assert extract_search_results("<html><body>no js here</body></html>") is None


def test_extract_search_results_real_fixture():
    html = _load_text_fixture("ne_stepup_search.html")
    data = extract_search_results(html)
    assert data is not None
    assert len(data["providers"]) == 5
    icons = {p["icon"].rsplit("/", 1)[-1] for p in data["providers"]}
    assert icons == {
        "marker-child-care-center.svg",
        "marker-family-child-care-home.svg",
        "marker-preschool.svg",
        "marker-public-school.svg",
        "marker-head-start.svg",
    }


@pytest.mark.parametrize(
    "html_snippet,expected_name,expected_url",
    [
        (
            "<p><strong>Trinity Child Care</strong><br>addr<br>"
            "<a href='https://stepuptoquality.ne.gov/child-care-facility/trinity/'>Learn More</a></p>",
            "Trinity Child Care",
            "https://stepuptoquality.ne.gov/child-care-facility/trinity/",
        ),
        ("<p>no strong or href here</p>", None, None),
        ("", None, None),
        (None, None, None),
    ],
)
def test_parse_stepup_search_entry(html_snippet, expected_name, expected_url):
    name, url = parse_stepup_search_entry(html_snippet)
    assert name == expected_name
    assert url == expected_url


@pytest.mark.parametrize(
    "license_number,expected",
    [
        ("CCC9970", "CCC"),
        ("FI12610", "FI"),
        ("FII10096", "FII"),  # greedy alpha run -> "FII", not "FI"
        (None, None),
        ("", None),
        ("9970CCC", None),  # doesn't start with letters
    ],
)
def test_license_prefix(license_number, expected):
    assert license_prefix(license_number) == expected


def test_derive_provider_type_from_license_prefix_center():
    provider_type, is_non_provider = derive_provider_type("CCC9237", [])
    assert provider_type == "Child Care Center"
    assert is_non_provider is False


def test_derive_provider_type_from_license_prefix_family_home_i():
    provider_type, is_non_provider = derive_provider_type("FI12610", [])
    assert provider_type == "Family Child Care Home I"
    assert is_non_provider is False


def test_derive_provider_type_unmapped_prefix_falls_back_to_subcategory():
    rcc = [
        {"is_from_licensure": True, "category": {"id": 13, "category_name": "Child Care Center"}},
        {"is_from_licensure": True, "category": {"id": 12, "category_name": "Child Care"}},
    ]
    provider_type, is_non_provider = derive_provider_type("ZZZ999", rcc)
    assert provider_type == "Child Care Center"
    assert is_non_provider is False


def test_derive_provider_type_ambiguous_childcare_term_first_resource_25986_shape():
    # Live-observed shape (resource 25986, 2026-09-15 smoke run): a
    # multi-service record whose sub-categories already lead with the real
    # child-care term -- must still resolve to it (baseline the tie-break
    # fix below must not regress).
    rcc = [
        {"is_from_licensure": False, "category": {"id": 13, "category_name": "Child Care Center"}},
        {"is_from_licensure": False, "category": {"id": 900, "category_name": "In Home Provider"}},
        {"is_from_licensure": False, "category": {"id": 901, "category_name": "Respite Services"}},
        {"is_from_licensure": False, "category": {"id": 12, "category_name": "Child Care"}},
    ]
    provider_type, is_non_provider = derive_provider_type(None, rcc, resource_id=25986)
    assert provider_type == "Child Care Center"
    assert is_non_provider is False


def test_derive_provider_type_ambiguous_childcare_term_buried_resource_9670_shape():
    # Live-observed shape (resource 9670, "Sarpy Community YMCA",
    # 2026-09-15 smoke run): a multi-service community agency whose
    # resourceCategoryCounty rows mix several unrelated registry categories
    # in with a genuine "Child Care Center" row that sorts LAST, not first.
    # Before the tie-break fix this resolved to the wrong category
    # ("Social Development", the first entry) -- it must now restrict to
    # the known child-care vocabulary and resolve to "Child Care Center"
    # regardless of position.
    rcc = [
        {
            "is_from_licensure": False,
            "county_number": 59,
            "category": {"id": 379, "category_name": "Social Development"},
        },
        {"is_from_licensure": False, "county_number": 59, "category": {"id": 383, "category_name": "Recreation"}},
        {
            "is_from_licensure": False,
            "county_number": 59,
            "category": {"id": 384, "category_name": "Recreation & Community Centers"},
        },
        {
            "is_from_licensure": False,
            "county_number": 59,
            "category": {"id": 415, "category_name": "Recreation-Children/Youth"},
        },
        {"is_from_licensure": False, "county_number": 59, "category": {"id": 13, "category_name": "Child Care Center"}},
        {"is_from_licensure": False, "county_number": 59, "category": {"id": 12, "category_name": "Child Care"}},
    ]
    provider_type, is_non_provider = derive_provider_type(None, rcc, resource_id=9670)
    assert provider_type == "Child Care Center"
    assert is_non_provider is False
    assert norm.facility_category_from_type(provider_type) == "center"


def test_derive_provider_type_no_license_no_subcategory_data_leaves_unset():
    rcc = [{"is_from_licensure": True, "category": {"id": 12, "category_name": "Child Care"}}]
    provider_type, is_non_provider = derive_provider_type(None, rcc)
    assert provider_type is None
    assert is_non_provider is False  # uncategorized, NOT a locator skip


def test_derive_provider_type_locator_is_skipped():
    rcc = [
        {"is_from_licensure": False, "category": {"id": 15, "category_name": "Child Care Locators"}},
        {"is_from_licensure": False, "category": {"id": 12, "category_name": "Child Care"}},
    ]
    provider_type, is_non_provider = derive_provider_type(None, rcc)
    assert provider_type is None
    assert is_non_provider is True


@pytest.mark.parametrize(
    "value,expected",
    [
        ("68825", "68825"),
        ("685160000", "68516"),  # NRRS's 9-digit-no-dash form (Sec 12)
        ("68516-1234", "68516"),  # a normal ZIP+4
        (None, None),
        ("", None),
    ],
)
def test_normalize_zip(value, expected):
    assert normalize_zip(value) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("1-2-3 Baby And Me Owned By Kailey Bonde", "1 2 3 baby and me kailey bonde"),
        ("AARON NORMAN DAYCARE owned by AARON L NORMAN", "aaron norman daycare aaron l norman"),
        ("Bright Beginnings, LLC", "bright beginnings"),
        ("A & B Child Care Inc.", "a and b child care"),
        ("", None),
        (None, None),
    ],
)
def test_norm_name(value, expected):
    assert norm_name(value) == expected


def test_extract_zip5():
    assert extract_zip5("7130 Kentwell Lane, Lincoln, NE, 68516") == "68516"
    assert extract_zip5("no zip here") is None
    assert extract_zip5(None) is None


@pytest.mark.parametrize(
    "lower,upper,expected",
    [
        (0, 1, {"infant", "toddler"}),
        (6, 13, {"school"}),  # the live-observed ambiguous case (Sec 12)
        (2, 4, {"toddler", "preschool"}),  # lo=2 sits on the toddler/preschool boundary -- both overlap
        (None, None, set()),
    ],
)
def test_age_flags_from_nrrs_range(lower, upper, expected):
    flags = age_flags_from_nrrs_range(lower, upper)
    assert set(flags) == expected
    assert all(v is True for v in flags.values())


def test_format_nrrs_hours_collapses_same_time_days():
    response = {
        "Monday": {"open": "6:00 am", "close": "6:00 pm"},
        "Tuesday": {"open": "6:00 am", "close": "6:00 pm"},
        "Wednesday": {"open": "6:00 am", "close": "6:00 pm"},
        "Thursday": {"open": "6:00 am", "close": "6:00 pm"},
        "Friday": {"open": "6:00 am", "close": "6:00 pm"},
    }
    assert format_nrrs_hours(response) == "Monday-Friday 6:00 am-6:00 pm"


def test_format_nrrs_hours_mixed_days_lists_each():
    response = {"Monday": {"open": "6:00 am", "close": "6:00 pm"}, "Saturday": {"open": "8:00 am", "close": "12:00 pm"}}
    assert format_nrrs_hours(response) == "Monday 6:00 am-6:00 pm; Saturday 8:00 am-12:00 pm"


def test_format_nrrs_hours_empty_or_wrong_type():
    assert format_nrrs_hours({}) is None
    assert format_nrrs_hours(None) is None
    assert format_nrrs_hours("not a dict") is None


@pytest.mark.parametrize(
    "latitude,longitude,expected",
    [
        ("40.7387888", "-96.6401756", True),
        ("0", "0", False),
        ("0.0", "0.0", False),
        (None, None, False),
        ("1.0", "", False),
    ],
)
def test_is_real_coordinate(latitude, longitude, expected):
    assert is_real_coordinate(latitude, longitude) is expected


def test_derive_county_anchors():
    # Official Nebraska county-number anchors (plan Sec 5.2/16), verified
    # live against real addresses: 1=Douglas, 2=Lancaster, 9=Buffalo.
    county_names = {1: "Douglas", 2: "Lancaster", 9: "Buffalo"}
    for number, name in county_names.items():
        rcc = [
            {
                "is_from_licensure": True,
                "county_number": number,
                "category": {"id": 13, "category_name": "Child Care Center"},
            }
        ]
        assert derive_county(rcc, "Child Care Center", county_names) == name


def test_derive_county_provisional_prefix_mismatch_still_matches():
    # Live-verified quirk (resource 73989): the license prefix gives the base
    # provider_type ("Family Child Care Home II") while the resourceCategoryCounty
    # row is tagged with the "Provisional " variant of that same category.
    county_names = {9: "Buffalo"}
    rcc = [
        {
            "is_from_licensure": True,
            "county_number": 9,
            "category": {"id": 22, "category_name": "Provisional Family Child Care Home II "},
        }
    ]
    assert derive_county(rcc, "Family Child Care Home II", county_names) == "Buffalo"


def test_derive_county_ambiguous_leaves_unset():
    county_names = {1: "Douglas", 59: "Sarpy"}
    rcc = [
        {"is_from_licensure": True, "county_number": 1, "category": {"id": 13, "category_name": "Child Care Center"}},
        {"is_from_licensure": True, "county_number": 59, "category": {"id": 13, "category_name": "Child Care Center"}},
    ]
    assert derive_county(rcc, "Child Care Center", county_names) is None


def test_derive_county_no_provider_type_is_unset():
    assert derive_county([{"is_from_licensure": True, "county_number": 1}], None, {1: "Douglas"}) is None


def test_facility_category_mapping_for_nebraska_provider_types():
    assert norm.facility_category_from_type("Child Care Center") == "center"
    assert norm.facility_category_from_type("Family Child Care Home I") == "family_home"
    assert norm.facility_category_from_type("Family Child Care Home II") == "family_home"
    assert norm.facility_category_from_type("Provisional Family Child Care Home I") == "family_home"
    assert norm.facility_category_from_type("Provisional Family Child Care Home II") == "family_home"
    assert norm.facility_category_from_type("Family Child Care Home") == "family_home"
    assert norm.facility_category_from_type("Public School") == "center"
    assert norm.facility_category_from_type("Preschool") == "center"
    assert norm.facility_category_from_type("Head Start") == "center"


# --- case 1: NRRS search pagination -------------------------------------- #


def test_nrrs_search_page1_fans_out_pages_and_schedules_details(spider):
    payload = _load_json_fixture("ne_nrrs_search.json")
    results = list(spider.parse_nrrs_search(nrrs_search_response(payload, 1)))
    search_requests = [r for r in results if r.url == NRRS_SEARCH_URL]
    detail_requests = [r for r in results if r.url != NRRS_SEARCH_URL]

    assert [r.meta["page"] for r in search_requests] == [2]
    assert {r.meta["summary"]["id"] for r in detail_requests} == {15450, 73989, 47427}
    assert spider.nrrs_seen_ids == {15450, 73989, 47427}
    assert spider.nrrs_pending_details == 3
    assert spider.nrrs_search_pages_remaining == 1
    assert spider.nrrs_search_done is False
    for r in detail_requests:
        assert r.dont_filter is True


def test_nrrs_search_page2_dedupes_repeated_id_across_pages(spider):
    page1 = _load_json_fixture("ne_nrrs_search.json")
    list(spider.parse_nrrs_search(nrrs_search_response(page1, 1)))

    page2 = _load_json_fixture("ne_nrrs_search_page2.json")
    results = list(spider.parse_nrrs_search(nrrs_search_response(page2, 2)))
    detail_requests = [r for r in results if r.url != NRRS_SEARCH_URL]

    # id 73989 repeats on page 2 (the plan's documented "2,531 rows / 2,523
    # distinct ids" boundary drift) -- only the new id 62707 schedules a
    # second detail request.
    assert {r.meta["summary"]["id"] for r in detail_requests} == {62707}
    assert spider.nrrs_seen_ids == {15450, 73989, 47427, 62707}
    assert spider.nrrs_search_pages_remaining == 0
    assert spider.nrrs_search_done is True
    # search is done, but a detail request is still pending -- must not
    # finish/emit leftovers yet.
    assert spider._leftovers_emitted is False


# --- case 2: NRRS detail -> item ------------------------------------------ #


def test_nrrs_detail_center_golden(spider):
    item = spider._build_nrrs_item(
        {"id": 15450, "license_no": "CCC9237"}, _load_json_fixture("ne_nrrs_detail_center.json")
    )
    assert isinstance(item, ProviderItem)
    assert item["source_state"] == "Nebraska"
    assert item["provider_name"] == "4 VIEWS ACADEMY"
    assert item["license_number"] == "CCC9237"
    assert item["provider_type"] == "Child Care Center"
    assert norm.facility_category_from_type(item["provider_type"]) == "center"
    assert item["ne_resource_id"] == 15450
    assert item["ne_from_licensure"] is True
    assert item["address"] == "4330 CORNHUSKER HWY, Lincoln, NE 68504"
    assert item["city"] == "Lincoln"
    assert item["state"] == "NE"
    assert item["zip"] == "68504"
    assert item["county"] == "Lancaster"  # county_number 2
    assert item["phone"] == "4024640174"
    assert item["ages_served"] == "6-13"
    assert item["school"] is True  # best-effort fallback (no Step Up match)
    assert item["hours"] == "Monday-Friday 6:00 am-6:00 pm"
    assert item["provider_url"] == "https://nrrs.ne.gov/resource/15450/12.0"
    assert item["ne_match_method"] == "nrrs_only"
    # mailing address is identical to the physical address on this record --
    # must NOT be emitted as a separate (redundant) mailing_address.
    assert "mailing_address" not in item


def test_nrrs_detail_family_home_fii_provisional(spider):
    item = spider._build_nrrs_item(
        {"id": 73989, "license_no": "FII10096"}, _load_json_fixture("ne_nrrs_detail_family_home.json")
    )
    assert item["provider_name"] == "2.0 ob Little Pups Daycare LLC"
    assert item["license_number"] == "FII10096"
    # license-prefix wins over the record's own "Provisional Family Child
    # Care Home II " sub-category label (Sec 5.1).
    assert item["provider_type"] == "Family Child Care Home II"
    assert norm.facility_category_from_type(item["provider_type"]) == "family_home"
    assert item["county"] == "Buffalo"  # county_number 9 (Pleasanton, NE)
    assert item["zip"] == "68866"


def test_nrrs_detail_missing_category_field_responses(spider):
    detail = {
        "id": 99001,
        "name1": "No Fields Provider",
        "license_no": "CCC5000",
        "is_from_licensure": True,
        "addressPhysical": {"address1": "1 Main St", "city": "Omaha", "state": "NE", "zip": "68101"},
        "resourceCategoryCounty": [
            {
                "is_from_licensure": True,
                "county_number": 1,
                "category": {"id": 13, "category_name": "Child Care Center"},
            },
        ],
        # categoryFieldResponses is entirely absent -- NRRS is sparse.
    }
    item = spider._build_nrrs_item({"id": 99001, "license_no": "CCC5000"}, detail)
    assert item["provider_name"] == "No Fields Provider"
    assert item["county"] == "Douglas"
    assert "ages_served" not in item
    assert "hours" not in item
    assert "capacity" not in item
    assert "meals" not in item
    assert "transportation" not in item
    assert "languages" not in item


def test_nrrs_detail_no_id_returns_none(spider):
    assert spider._build_nrrs_item({}, {}) is None


# --- case 3: non-provider "Child Care Locators" is skipped --------------- #


def test_nrrs_detail_locator_record_is_skipped(spider):
    detail = _load_json_fixture("ne_nrrs_detail_locator.json")
    item = spider._build_nrrs_item({"id": detail["id"], "license_no": None}, detail)
    assert item is None
    assert spider.nrrs_skipped_non_provider == 1


def test_parse_nrrs_detail_locator_yields_nothing(spider):
    response = nrrs_detail_response("ne_nrrs_detail_locator.json", summary={"id": 25059, "license_no": None})
    results = list(spider.parse_nrrs_detail(response))
    assert results == []


# --- case 4: Step Up searchResults parse ---------------------------------- #


def test_parse_stepup_search_schedules_all_detail_requests(spider):
    results = list(spider.parse_stepup_search(stepup_search_response()))
    assert len(results) == 5
    assert spider.stepup_total_parsed == 5
    assert spider.stepup_pending_details == 5

    by_slug = {r.meta["summary"]["slug"]: r for r in results}
    assert set(by_slug) == {
        "trinity-child-care-at-village-gardens",
        "tiny-steps",
        "wee-wisdom",
        "eagle-elementary-school",
        "community-action-head-start-center-at-k-street",
    }
    trinity_req = by_slug["trinity-child-care-at-village-gardens"]
    assert (
        trinity_req.url == "https://stepuptoquality.ne.gov/child-care-facility/trinity-child-care-at-village-gardens/"
    )
    assert trinity_req.meta["summary"]["lat"] == "40.7387888"
    assert trinity_req.meta["summary"]["icon"].endswith("marker-child-care-center.svg")


def test_parse_stepup_search_missing_variable_starts_nrrs_phase_directly(spider):
    results = list(spider.parse_stepup_search(stepup_search_response("<html><body>no js here</body></html>")))
    # no Step Up detail requests scheduled -- falls straight through to
    # Phase B's first NRRS search request.
    assert len(results) == 1
    assert results[0].url == NRRS_SEARCH_URL
    assert spider._nrrs_phase_started is True
    assert spider.stepup_by_license == {}
    assert spider.stepup_by_namezip == {}


# --- case 5: Step Up detail parse ----------------------------------------- #


def test_stepup_detail_parse_trinity_golden():
    html_text = _load_text_fixture("ne_stepup_detail.html")
    url = "https://stepuptoquality.ne.gov/child-care-facility/trinity-child-care-at-village-gardens/"
    summary = {
        "slug": "trinity-child-care-at-village-gardens",
        "detail_url": url,
        "name": "Trinity Child Care At Village Gardens",
        "lat": "40.7387888",
        "lng": "-96.6401756",
        "icon": "https://stepuptoquality.ne.gov/wp-content/uploads/2024/06/marker-child-care-center.svg",
    }
    record = build_stepup_record(stepup_detail_response(html_text, url, summary), summary)
    assert record["provider_name"] == "Trinity Child Care At Village Gardens"
    assert record["facility_type"] == "Child Care Center"
    assert record["license_number"] == "CCC9194"
    assert record["step_rating"] == 2
    assert record["administrator"] == "Amanda Mahlin"
    assert record["full_time_staff"] == 16
    assert record["part_time_staff"] == 13
    assert record["capacity"] == 144
    assert record["age_tags"] == ["Infants", "Toddlers", "Preschool"]
    assert record["checklist"] == ["Serves children with special needs", "Accepts child care subsidy"]
    assert record["phone"] == "(402) 421-0184"
    assert record["email"] == "amahlin@trinitylincoln.org"
    assert record["address"] == "7130 Kentwell Lane, Lincoln, NE, 68516"
    assert extract_zip5(record["address"]) == "68516"


def test_stepup_detail_parse_no_license_public_school():
    html_text = _load_text_fixture("ne_stepup_detail_no_license.html")
    url = "https://stepuptoquality.ne.gov/child-care-facility/eagle-elementary-school/"
    summary = {
        "slug": "eagle-elementary-school",
        "detail_url": url,
        "name": "Eagle Elementary School",
        "lat": "1",
        "lng": "2",
        "icon": "x",
    }
    record = build_stepup_record(stepup_detail_response(html_text, url, summary), summary)
    assert record["provider_name"] == "Eagle Elementary School"
    assert record["facility_type"] == "Public School"
    assert "license_number" not in record
    assert record["step_rating"] == 3
    assert record["age_tags"] == ["Preschool"]
    assert stepup_provider_type(record) == "Public School"


def test_stepup_detail_blank_badge_rating_unset():
    # No live example of a blank step-badge was found across >100 live
    # samples during this build (the plan's ~13% figure could not be
    # reproduced -- see the build report); this constructs the documented
    # edge case directly to prove the parser handles it (Sec 12): the badge
    # div renders with no digit inside, and only `ne_step_rating` is
    # affected -- `ne_step_participating` still gets set True elsewhere
    # (apply_stepup_common_fields) purely from the record being present.
    html_text = """<!DOCTYPE html><html><body>
    <main id="main-content">
    <div id="internal-hero"><h1>Blank Rating Provider</h1><h2>Child Care Center</h2>
    <div class="step-badge nitro-lazy"> </div></div>
    <div class="content"><div>
    <div class="row columns-6-6 plumb-columns">
    <div class="column"><p><span class="label">Director</span> Jane Doe</p></div>
    <div class="column"><p><span class="label">License Number</span> CCC0001</p></div>
    </div>
    </div></div>
    <div class="sidebar"><div class="box"><ul class="simple-list"></ul></div></div>
    </main></body></html>"""
    url = "https://stepuptoquality.ne.gov/child-care-facility/blank-rating-provider/"
    summary = {"slug": "blank-rating-provider", "detail_url": url, "name": None, "lat": "1", "lng": "2", "icon": "x"}
    record = build_stepup_record(stepup_detail_response(html_text, url, summary), summary)
    assert "step_rating" not in record
    assert record["license_number"] == "CCC0001"

    item = ProviderItem()
    apply_stepup_standalone_fields(item, record)
    from provider_scrape.spiders.nebraska import apply_stepup_common_fields

    apply_stepup_common_fields(item, record)
    assert "ne_step_rating" not in item
    assert item["ne_step_participating"] is True


# --- case 6: merge (license / name+zip / no match / leftovers) ----------- #


def test_merge_license_match(spider):
    # Synthetic: isolates the license-match tier by using a name/zip that
    # would NOT also satisfy the name+zip tier, proving license wins on its
    # own signal (plan Sec 7 tier 1).
    record = {
        "slug": "license-match-provider",
        "detail_url": "https://stepuptoquality.ne.gov/child-care-facility/license-match-provider/",
        "provider_name": "Totally Different Name Childcare",
        "license_number": "CCC9237",
        "step_rating": 4,
        "age_tags": [],
        "checklist": [],
        "latitude": "41.0",
        "longitude": "-96.0",
    }
    spider.stepup_records["license-match-provider"] = record
    spider.stepup_by_license, spider.stepup_by_namezip = build_stepup_index(spider.stepup_records, spider.logger)

    item = spider._build_nrrs_item(
        {"id": 15450, "license_no": "CCC9237"}, _load_json_fixture("ne_nrrs_detail_center.json")
    )
    assert item["ne_match_method"] == "license"
    assert item["ne_step_rating"] == 4
    assert item["ne_step_participating"] is True
    # NRRS's own (richer) name is kept, not overwritten by the Step Up name.
    assert item["provider_name"] == "4 VIEWS ACADEMY"
    assert "license-match-provider" in spider.stepup_consumed
    assert spider.match_license_count == 1


def test_merge_name_zip_match_real_trinity_example(spider):
    # The plan's flagship documented example (Sec 3): "Trinity Child Care At
    # Village Gardens" is CCC9194 in Step Up and NRRS id 15076 with
    # license_no: null -- the same provider, reachable only via name+ZIP.
    html_text = _load_text_fixture("ne_stepup_detail.html")
    url = "https://stepuptoquality.ne.gov/child-care-facility/trinity-child-care-at-village-gardens/"
    summary = {
        "slug": "trinity-child-care-at-village-gardens",
        "detail_url": url,
        "name": "Trinity Child Care At Village Gardens",
        "lat": "40.7387888",
        "lng": "-96.6401756",
        "icon": "https://stepuptoquality.ne.gov/wp-content/uploads/2024/06/marker-child-care-center.svg",
    }
    record = build_stepup_record(stepup_detail_response(html_text, url, summary), summary)
    spider.stepup_records[summary["slug"]] = record
    spider.stepup_by_license, spider.stepup_by_namezip = build_stepup_index(spider.stepup_records, spider.logger)

    detail = _load_json_fixture("ne_nrrs_detail_namezip_match.json")
    assert detail["license_no"] is None  # the documented null-license half of the pair
    item = spider._build_nrrs_item({"id": 15076, "license_no": None}, detail)

    assert item["ne_match_method"] == "name_zip"
    assert item["zip"] == "68516"  # normalized from the source's "685160000"
    assert item["ne_step_rating"] == 2
    assert item["capacity"] == 144  # Step Up's value, preferred over NRRS
    assert item["latitude"] == "40.7387888"
    assert item["geocode_source"] == "state"
    assert item["infant"] is True  # from Step Up's age tags, not the NRRS range
    assert item["scholarships_accepted"] is True
    # this record's resourceCategoryCounty rows are is_from_licensure=False
    # (even though the record's own top-level is_from_licensure is True) --
    # a live-verified discrepancy; county correctly stays unset rather than
    # trusting the top-level flag.
    assert "county" not in item
    assert summary["slug"] in spider.stepup_consumed
    assert spider.match_namezip_count == 1


def test_merge_no_match_is_nrrs_only(spider):
    spider.stepup_records["unrelated"] = {
        "slug": "unrelated",
        "provider_name": "Somebody Else",
        "license_number": "CCC0000",
        "age_tags": [],
        "checklist": [],
    }
    spider.stepup_by_license, spider.stepup_by_namezip = build_stepup_index(spider.stepup_records, spider.logger)

    item = spider._build_nrrs_item(
        {"id": 15450, "license_no": "CCC9237"}, _load_json_fixture("ne_nrrs_detail_center.json")
    )
    assert item["ne_match_method"] == "nrrs_only"
    assert "ne_step_rating" not in item
    assert "ne_step_participating" not in item
    assert spider.match_none_count == 1


def test_stepup_leftover_emitted_standalone_and_consumed_not_emitted_twice(spider):
    spider.stepup_records["leftover-provider"] = {
        "slug": "leftover-provider",
        "detail_url": "https://stepuptoquality.ne.gov/child-care-facility/leftover-provider/",
        "provider_name": "Leftover Provider",
        "facility_type": "Preschool",
        "license_number": "PRE1234",
        "address": "100 Elm St, Omaha, NE, 68101",
        "age_tags": ["Preschool"],
        "checklist": ["Accepts child care subsidy"],
        "step_rating": 3,
    }
    spider.stepup_records["consumed-provider"] = {
        "slug": "consumed-provider",
        "detail_url": "https://stepuptoquality.ne.gov/child-care-facility/consumed-provider/",
        "provider_name": "Consumed Provider",
        "license_number": "CCC9237",
        "age_tags": [],
        "checklist": [],
    }
    spider.stepup_by_license, spider.stepup_by_namezip = build_stepup_index(spider.stepup_records, spider.logger)

    # Consume "consumed-provider" via a license match first.
    spider._build_nrrs_item({"id": 15450, "license_no": "CCC9237"}, _load_json_fixture("ne_nrrs_detail_center.json"))
    assert "consumed-provider" in spider.stepup_consumed

    leftovers = list(spider._emit_stepup_leftovers())
    slugs_emitted = {item.get("provider_url", "").rstrip("/").rsplit("/", 1)[-1] for item in leftovers}
    assert len(leftovers) == 1  # only the un-consumed one
    assert "leftover-provider" in slugs_emitted
    assert "consumed-provider" not in slugs_emitted

    leftover_item = leftovers[0]
    assert leftover_item["ne_stepup_only"] is True
    assert leftover_item["ne_match_method"] == "stepup_only"
    assert leftover_item["source_state"] == "Nebraska"
    assert leftover_item["provider_name"] == "Leftover Provider"
    assert leftover_item["provider_type"] == "Preschool"
    assert norm.facility_category_from_type(leftover_item["provider_type"]) == "center"
    assert leftover_item["license_number"] == "PRE1234"
    assert leftover_item["ne_step_rating"] == 3
    assert leftover_item["preschool"] is True
    assert leftover_item["scholarships_accepted"] is True


def test_stepup_standalone_head_start_sets_head_start_flag():
    item = ProviderItem()
    record = {
        "slug": "head-start-provider",
        "detail_url": "https://stepuptoquality.ne.gov/child-care-facility/head-start-provider/",
        "provider_name": "Head Start Provider",
        "facility_type": "Head Start",
        "age_tags": [],
        "checklist": [],
    }
    apply_stepup_standalone_fields(item, record)
    assert item["provider_type"] == "Head Start"
    assert item["head_start"] is True
    assert norm.facility_category_from_type(item["provider_type"]) == "center"


# --- case 8: no undefined item fields ------------------------------------- #


def test_no_undefined_item_fields_across_archetypes(spider):
    fixtures_and_summaries = [
        ("ne_nrrs_detail_center.json", {"id": 15450, "license_no": "CCC9237"}),
        ("ne_nrrs_detail_family_home.json", {"id": 73989, "license_no": "FII10096"}),
        ("ne_nrrs_detail_namezip_match.json", {"id": 15076, "license_no": None}),
    ]
    for fixture, summary in fixtures_and_summaries:
        item = spider._build_nrrs_item(summary, _load_json_fixture(fixture))
        assert dict(item)  # raises on an undefined field name


def test_undefined_field_raises():
    item = ProviderItem()
    with pytest.raises(KeyError):
        item["ne_not_a_real_field"] = "boom"


# --- closed() logging smoke test ------------------------------------------ #


def test_closed_smoke(spider):
    spider.nrrs_seen_ids = {1, 2, 3}
    spider.stepup_total_parsed = 5
    spider._leftovers_emitted = True
    spider.closed("finished")  # smoke: no raise

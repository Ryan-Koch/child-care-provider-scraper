import pytest
from scrapy.http import HtmlResponse, TextResponse
from provider_scrape.spiders.virginia import VadssSpider
from provider_scrape.items import ProviderItem, InspectionItem

def create_response(html_content, url='http://example.com'):
    """Helper function to create a Scrapy response from HTML content."""
    return HtmlResponse(url=url, body=html_content.encode('utf-8'))


PROFILE_URL = (
    "https://earlychildhoodquality.doe.virginia.gov/profiles/a-childs-dream-8609/"
)


def make_profile_html(
    dss_id="46940",
    rating="Meets Expectations",
    funding="VA CCSP; MCCYN",
    interactions=(("Infant Classrooms", "Observations have met expectations."),),
    points=(("Interactions Points", "465"), ("Curriculum Points", "0"), ("Total Points", "465")),
):
    """Build a minimal VQB5 profile page exercising the enrichment selectors."""
    dss_anchor = (
        f'<p class="public-default"><a href="https://www.dss.virginia.gov/facility/'
        f'search/cc2.cgi?rm=Details;ID={dss_id}">Learn more</a></p>'
        if dss_id is not None
        else ""
    )
    # Rendered twice to mimic the page's modal + visible duplication, so tests
    # guard against the rating value being concatenated from both copies.
    rating_block = (
        (
            f'<div class="card-body"><p class="card-text">'
            f"VQB5 Quality Rating: {rating}</p></div>"
        )
        * 2
        if rating is not None
        else ""
    )
    funding_block = (
        f"<p><strong>Public Funding Information:</strong> {funding}</p>"
        if funding is not None
        else ""
    )
    interaction_block = "".join(
        f"<h4>{label}</h4><p>{desc}</p>" for label, desc in interactions
    )
    # Render each points card twice to mimic the real page's modal + visible
    # duplication, so tests exercise the "take first numeric value" dedup.
    one_points_set = "".join(
        f'<div class="card"><div class="card-body">'
        f'<p class="card-text">{label}:<br>\n        {value} points\n      </p>'
        f"</div></div>"
        for label, value in points
    )
    points_block = one_points_set + one_points_set
    return f"""
    <html><body>
        {dss_anchor}
        {rating_block}
        {funding_block}
        <div class="card-normal-points">{interaction_block}</div>
        {points_block}
    </body></html>
    """


def make_programs_response(body):
    return TextResponse(
        url=VadssSpider.PROGRAMS_JSON_URL, body=body.encode("utf-8"), encoding="utf-8"
    )

def test_successful_extraction():
    """Test case: Successful extraction of all fields, including inspection data."""
    html_content = """
    <html>
    <body>
        <table border="0" style="float:left; margin-right: 17px; min-width: 350px">
            <tr>
                <td valign="top" colspan="2">
                    <b>4 Rs Preschool</b><br>
                    6745 Jefferson Street
                </td>
            </tr>
            <tr>
                <td valign="top" colspan="2">
                    HAYMARKET, VA  20169
                </td>
            </tr>
            <tr>
                <td valign="top" colspan="2">
                    (703) 754-2497
                </td>
            </tr>
        </table>
        <table width="500px" class="cc_search">
            <tr width="160px">
                <td valign="top">
                    Facility Type:
                </td>
                <td valign="top">
                    <span><span><font color = blue><u>Child Day Center</u></font></span></span>
                </td>
            </tr>
            <tr>
                <td valign="top">
                    License Type:
                </td>
                <td valign="top">
                    <span><span><font color = blue><u>Two Year</u></font></span></span>
                </td>
            </tr>
            <tr>
                <td valign="top">
                    Administrator:
                </td>
                <td valign="top">
                    Robyn Frazier
                </td>
            </tr>
            <tr>
                <td valign="top">
                    Business Hours:
                </td>
                <td valign="top">
                    9:00 a.m. - 3:30 p.m.
                </td>
            </tr>
            <tr>
                <td valign="top">
                    Capacity:
                </td>
                <td valign="top">
                    26
                </td>
            </tr>
            <tr>
                <td valign="top">
                    Ages:
                </td>
                <td valign="top">
                    3 years - 6 years 11 months
                </td>
            </tr>
            <tr>
                <td valign="top">
                    Inspector:
                </td>
                <td valign="top">
                    Morgan Bryson: (540) 270-0057
                </td>
            </tr>
            <tr>
                <td valign="top">
                    Current Subsidy Provider
                </td>
                <td>
                    No
                </td>
            </tr>
            <tr>
                <td valign="top">
                    License/Facility ID#
                </td>
                <td>
                    1106312
                </td>
            </tr>
        </table>

        <table width="80%">
            <tr>
                <td valign="top" colspan="2">
                    <table border="0" cellspacing="5" width="100%" class="cc_search">
                        <tr>
                            <td valign="top" align="middle"><b>Inspection Date</b></td>
                            <td valign="top" align="middle"><b>SHSI</b></td>
                            <td valign="top" align="middle"><b>Complaint Related</b></td>
                            <td valign="top" align="middle"><b>Violations</b></td>
                        </tr>
                        <tr>
                            <td align="middle"><a href="#">Jan. 30, 2025</a></td>
                            <td align="middle">No</td>
                            <td align="middle">No</td>
                            <td align="middle">No</td>
                        </tr>
                        <tr>
                            <td align="middle"><a href="#">Sept. 24, 2020</a></td>
                            <td align="middle">Yes</td>
                            <td align="middle">No</td>
                            <td align="middle">Yes</td>
                        </tr>
                    </table>
                </td>
            </tr>
        </table>
    </body>
    </html>
    """
    response = create_response(html_content)
    spider = VadssSpider()
    # parse_provider_page accumulates into providers_by_ID rather than returning;
    # use pending_providers > 1 so this isn't treated as the final (enrichment-
    # triggering) provider.
    spider.pending_providers = 2
    list(spider.parse_provider_page(response))

    assert len(spider.providers_by_ID) == 1
    extracted_data = next(iter(spider.providers_by_ID.values()))
    assert isinstance(extracted_data, ProviderItem)
    assert extracted_data['provider_name'] == '4 Rs Preschool'
    assert extracted_data['address'] == '6745 Jefferson Street HAYMARKET, VA  20169'
    assert extracted_data['phone'] == '(703) 754-2497'
    assert extracted_data['provider_type'] == 'Child Day Center'
    assert extracted_data['va_license_type'] == 'Two Year'
    assert extracted_data['administrator'] == 'Robyn Frazier'
    assert extracted_data['hours'] == '9:00 a.m. - 3:30 p.m.'
    assert extracted_data['capacity'] == '26'
    assert extracted_data['ages_served'] == '3 years - 6 years 11 months'
    assert extracted_data['va_inspector'] == 'Morgan Bryson: (540) 270-0057'
    assert extracted_data['va_current_subsidy_provider'] == 'No'
    assert extracted_data['license_number'] == '1106312'

    # Assertions for inspection data
    assert len(extracted_data['inspections']) == 2
    inspections = extracted_data['inspections']

    assert isinstance(inspections[0], InspectionItem)
    assert inspections[0]['date'] == 'Jan. 30, 2025'
    assert inspections[0]['va_shsi'] == 'No'
    assert inspections[0]['va_complaint_related'] == 'No'
    assert inspections[0]['va_violations'] == 'No'

    assert isinstance(inspections[1], InspectionItem)
    assert inspections[1]['date'] == 'Sept. 24, 2020'
    assert inspections[1]['va_shsi'] == 'Yes'
    assert inspections[1]['va_complaint_related'] == 'No'
    assert inspections[1]['va_violations'] == 'Yes'

def test_missing_fields():
    """Test case: Handling missing fields and missing inspection data."""
    html_content = """
    <html>
    <body>
        <table border="0" style="float:left; margin-right: 17px; min-width: 350px">
            <tr>
                <td valign="top" colspan="2">
                    <b>4 Rs Preschool</b><br>

                </td>
            </tr>
            <tr>
                <td valign="top" colspan="2">
                    HAYMARKET, VA  20169
                </td>
            </tr>
            <tr>
                <td valign="top" colspan="2">
                    (703) 754-2497
                </td>
            </tr>
        </table>
        <table width="500px" class="cc_search">
            <tr width="160px">
                <td valign="top">
                    Facility Type:
                </td>
            </tr>
            <tr>
                <td valign="top">
                    License/Facility ID#
                </td>
                <td>
                    1106312
                </td>
            </tr>
        </table>
    </body>
    </html>
    """
    response = create_response(html_content)
    spider = VadssSpider()
    # parse_provider_page accumulates into providers_by_ID rather than returning;
    # use pending_providers > 1 so this isn't treated as the final (enrichment-
    # triggering) provider.
    spider.pending_providers = 2
    list(spider.parse_provider_page(response))

    assert len(spider.providers_by_ID) == 1
    extracted_data = next(iter(spider.providers_by_ID.values()))
    assert isinstance(extracted_data, ProviderItem)
    assert extracted_data['provider_name'] == '4 Rs Preschool'
    assert extracted_data['address'] == 'N/A HAYMARKET, VA  20169'
    assert extracted_data['phone'] == '(703) 754-2497'
    assert extracted_data['provider_type'] == 'N/A'
    assert extracted_data['va_license_type'] == 'N/A'
    assert extracted_data['administrator'] == 'N/A'
    assert extracted_data['hours'] == 'N/A'
    assert extracted_data['capacity'] == 'N/A'
    assert extracted_data['ages_served'] == 'N/A'
    assert extracted_data['va_inspector'] == 'N/A'
    assert extracted_data['va_current_subsidy_provider'] == 'N/A'
    assert extracted_data['license_number'] == '1106312'

    # Assert that inspection data is an empty list when missing
    assert extracted_data['inspections'] == []


def test_parse_quality_programs_queues_profiles():
    """programs.json is parsed (despite its trailing comma), de-duped, and every
    profile is queued for enrichment without Playwright."""
    body = """
    {
        "programs": [
            {"courseURL": "/profiles/alpha-1/"},
            {"courseURL": "/profiles/beta-2/"},
            {"courseURL": "/profiles/alpha-1/"},
            {"courseName": "no url here"},
        ]
    }
    """
    spider = VadssSpider()
    requests = list(spider.parse_quality_programs(make_programs_response(body)))

    assert len(requests) == 2  # duplicate collapsed, url-less entry skipped
    assert spider.pending_enrichments == 2
    assert all(r.callback == spider.parse_quality_detail for r in requests)
    assert all("playwright" not in r.meta for r in requests)
    assert requests[0].url == (
        "https://earlychildhoodquality.doe.virginia.gov/profiles/alpha-1/"
    )


def test_parse_quality_programs_empty_yields_providers():
    """With no programs, enrichment is skipped and accumulated providers are emitted."""
    spider = VadssSpider()
    spider.providers_by_ID = {"1": ProviderItem(provider_name="Solo", source_state="VA")}

    out = list(spider.parse_quality_programs(make_programs_response('{"programs": []}')))

    assert spider.pending_enrichments == 0
    assert out == [spider.providers_by_ID["1"]]


def test_parse_provider_page_keys_on_bare_id_matching_enrichment():
    """The DSS dict key must be the bare ID (not the trailing query string) so it
    matches the ID parsed from a VQB5 enrichment link for the same facility."""
    messy_url = (
        "https://legacy.dss.virginia.gov/facility/search/cc2.cgi?rm=Details;ID=35291;"
        "search_require_client_code-2106=1;search_require_client_code-2102=1"
    )
    provider_html = """
    <html><body>
        <table border="0"><tr><td colspan="2"><b>Round Trip Daycare</b><br>1 Main St</td></tr></table>
        <table class="cc_search"><tr><td>License/Facility ID#</td><td>35291</td></tr></table>
    </body></html>
    """
    spider = VadssSpider()
    spider.pending_providers = 1  # this is the last provider, so enrichment is triggered
    list(spider.parse_provider_page(create_response(provider_html, url=messy_url)))

    assert "35291" in spider.providers_by_ID
    assert spider.providers_by_ID["35291"]["va_ID"] == "35291"

    # A VQB5 profile pointing at the same facility should now enrich it.
    spider.pending_enrichments = 1
    profile = create_response(make_profile_html(dss_id="35291"), url=PROFILE_URL)
    list(spider.parse_quality_detail(profile))

    assert spider.providers_by_ID["35291"]["va_quality_rating"] == "Meets Expectations"


def test_parse_quality_detail_enriches_match():
    """A profile whose DSS link matches a known provider copies quality fields over."""
    spider = VadssSpider()
    provider = ProviderItem(provider_name="A Childs Dream", source_state="VA")
    spider.providers_by_ID = {"46940": provider}
    spider.pending_enrichments = 2  # another enrichment still outstanding

    response = create_response(make_profile_html(dss_id="46940"), url=PROFILE_URL)
    out = list(spider.parse_quality_detail(response))

    assert provider["va_quality_rating"] == "Meets Expectations"
    assert provider["va_public_funding"] == "VA CCSP; MCCYN"
    assert provider["va_interactions"] == (
        "Infant Classrooms: Observations have met expectations."
    )
    assert provider["va_interactions_points"] == "465"
    assert provider["va_curriculum_points"] == "0"
    assert provider["va_total_points"] == "465"
    assert spider.pending_enrichments == 1
    assert out == []  # not the final enrichment, so nothing emitted yet


def test_parse_quality_detail_extracts_points_from_first_of_duplicates():
    """Point cards appear twice (modal + visible); the first numeric value wins and
    comma-grouped values are normalized."""
    spider = VadssSpider()
    provider = ProviderItem(provider_name="Big Center", source_state="VA")
    spider.providers_by_ID = {"46940": provider}
    spider.pending_enrichments = 1

    html = make_profile_html(
        dss_id="46940",
        points=(
            ("Interactions Points", "1,200"),
            ("Curriculum Points", "0"),
            ("Total Points", "1,200"),
        ),
    )
    list(spider.parse_quality_detail(create_response(html, url=PROFILE_URL)))

    assert provider["va_interactions_points"] == "1200"
    assert provider["va_curriculum_points"] == "0"
    assert provider["va_total_points"] == "1200"


def test_parse_quality_detail_missing_points_leaves_fields_unset():
    """A profile without point cards should not set the point fields at all."""
    spider = VadssSpider()
    provider = ProviderItem(provider_name="No Points", source_state="VA")
    spider.providers_by_ID = {"46940": provider}
    spider.pending_enrichments = 1

    html = make_profile_html(dss_id="46940", points=())
    list(spider.parse_quality_detail(create_response(html, url=PROFILE_URL)))

    assert "va_interactions_points" not in provider
    assert "va_curriculum_points" not in provider
    assert "va_total_points" not in provider


def test_parse_quality_detail_no_matching_provider():
    """A DSS link with no corresponding provider leaves data untouched but still
    decrements the pending counter."""
    spider = VadssSpider()
    spider.providers_by_ID = {"111": ProviderItem(provider_name="Other", source_state="VA")}
    spider.pending_enrichments = 1

    response = create_response(make_profile_html(dss_id="99999"), url=PROFILE_URL)
    out = list(spider.parse_quality_detail(response))

    assert "va_quality_rating" not in spider.providers_by_ID["111"]
    assert spider.pending_enrichments == 0
    # Counter hit zero, so the (unenriched) providers are emitted.
    assert out == [spider.providers_by_ID["111"]]


def test_parse_quality_detail_final_enrichment_emits_all_providers():
    """When the last enrichment completes, every accumulated provider is yielded."""
    spider = VadssSpider()
    matched = ProviderItem(provider_name="A Childs Dream", source_state="VA")
    other = ProviderItem(provider_name="Untouched", source_state="VA")
    spider.providers_by_ID = {"46940": matched, "222": other}
    spider.pending_enrichments = 1

    response = create_response(make_profile_html(dss_id="46940"), url=PROFILE_URL)
    out = list(spider.parse_quality_detail(response))

    assert spider.pending_enrichments == 0
    assert set(id(p) for p in out) == {id(matched), id(other)}


def test_parse_quality_detail_handles_missing_quality_fields():
    """A profile with no DSS link is a no-op enrichment that only moves the counter."""
    spider = VadssSpider()
    spider.providers_by_ID = {"1": ProviderItem(provider_name="Solo", source_state="VA")}
    spider.pending_enrichments = 1

    html = make_profile_html(dss_id=None, rating=None, funding=None, interactions=())
    out = list(spider.parse_quality_detail(create_response(html, url=PROFILE_URL)))

    assert spider.pending_enrichments == 0
    assert out == [spider.providers_by_ID["1"]]

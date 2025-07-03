import pytest
from scrapy.http import HtmlResponse
from provider_scrape.spiders.vadss import VadssSpider
from provider_scrape.items import ProviderItem, InspectionItem

def create_response(html_content, url='http://example.com'):
    """Helper function to create a Scrapy response from HTML content."""
    return HtmlResponse(url=url, body=html_content.encode('utf-8'))

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
    extracted_data = spider.parse_provider_page(response)

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
    extracted_data = spider.parse_provider_page(response)

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
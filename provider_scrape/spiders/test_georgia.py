import pytest
import scrapy
from scrapy.http import HtmlResponse, Request, TextResponse
from provider_scrape.spiders.georgia import (
    GeorgiaSpider,
    extract_checked_labels,
    extract_list_items,
    extract_numeric_id,
    extract_radio_checked,
    parse_weekly_rates,
)
from provider_scrape.items import ProviderItem


@pytest.fixture
def spider():
    return GeorgiaSpider()


DATA_PAGE_HTML = """
<html>
<body>
<form method="post" action="/Provider/Data" id="form1">
<input type="hidden" name="__VIEWSTATE" value="abc123" />
<input type="hidden" name="__VIEWSTATEGENERATOR" value="ABCD" />
<input type="hidden" name="__EVENTVALIDATION" value="xyz789" />
<div>
    <input type="checkbox" name="ctl00$Content_Main$cblProgramType$0" value="CCLC" />
    <label for="cblProgramType_0">Child Care Learning Center</label>
    <input type="checkbox" name="ctl00$Content_Main$cblProgramType$1" value="DOD" />
    <label for="cblProgramType_1">Department of Defense</label>
    <input type="checkbox" name="ctl00$Content_Main$cblProgramType$2" value="GAHS" />
    <label for="cblProgramType_2">GA Head Start</label>
    <input type="checkbox" name="ctl00$Content_Main$cblProgramType$3" value="FCCLH" />
    <label for="cblProgramType_3">Family Child Care Learning Home</label>
</div>
<input type="submit" name="ctl00$Content_Main$btnExportToExcel" value="Download File" />
</form>
</body>
</html>
"""

DETAIL_HTML = """
<html>
<body>
<div class="panel panel-primary">
    <div class="panel-heading">
        <span id="Content_Main_lblFacilityName">1 Love Childcare &amp; Learning Center</span>
        <span id="Content_Main_lblLicenseNumber">CCLC-38436</span>
    </div>
    <div class="panel-body">
        <address>
            <span id="Content_Main_lblAddress">485 East Frontage Road, </span>
            <span id="Content_Main_lblCity">Sylvania, </span>
            <span id="Content_Main_lblState">GA</span>
            <span id="Content_Main_lblZip">30467</span>
        </address>
        <span id="Content_Main_lblPhone">(912) 564-2273</span>
        <span id="Content_Main_lblMonthsOfOperation">Year Round</span>
        <span id="Content_Main_lblDaysOfOperation">Mon - Fri</span>
        <span id="Content_Main_lblHoursOfOperation">Weekday: 05:45 AM - 05:00 PM</span>
        <span id="Content_Main_lblAdmin">Shevella Young</span>
        <span id="Content_Main_lblCapacity">43</span>
        <span id="Content_Main_lblLiabilityInsurance">Yes</span>
        <input id="Content_Main_chkIsAcceptingNewChildren" type="checkbox" checked="checked" />
        <span id="Content_Main_lblMailStreet">PO Box 788</span>
        <span id="Content_Main_lblMailCityStateZip">Sylvania, GA - 30467</span>
        <span id="Content_Main_lblProgramType">Child Care Learning Center</span>
        <span id="Content_Main_lblRegistrationFee">$95.00</span>
        <span id="Content_Main_lblActivityFee">$5.00</span>
        <span id="Content_Main_lblCurrentProgramStatus">Open</span>
        <img id="Content_Main_imgQRLevel" alt="Quality Rating Level Awarded: 3" />
        <img id="Content_Main_imgCompliance" title="Program is demonstrating an acceptable level of performance in meeting the rules." />

        <table id="Content_Main_cblServicesProvided">
            <tr>
                <td><input id="sp_0" type="checkbox" checked="checked" value="1" /><label for="sp_0">Enrolled in Childcare Subsidies (CAPS)</label></td>
                <td><input id="sp_5" type="checkbox" value="6" /><label for="sp_5">Head Start</label></td>
            </tr>
            <tr>
                <td><input id="sp_2" type="checkbox" checked="checked" value="3" /><label for="sp_2">CACFP</label></td>
            </tr>
        </table>

        <table id="Content_Main_cblAgesServed">
            <tr><td><input id="as_0" type="checkbox" checked="checked" value="Infant" /><label for="as_0">Infant (0 -12 months)</label></td></tr>
            <tr><td><input id="as_1" type="checkbox" checked="checked" value="Toddler" /><label for="as_1">Toddler (13 months - 2 years)</label></td></tr>
            <tr><td><input id="as_4" type="checkbox" checked="checked" value="School" /><label for="as_4">School Age (5+)</label></td></tr>
        </table>

        <table id="Content_Main_cblTransportation">
            <tr><td><input id="tr_5" type="checkbox" checked="checked" value="6" /><label for="tr_5">On School Bus Route</label></td></tr>
        </table>

        <table id="Content_Main_cblMeals">
            <tr><td><input id="ml_0" type="checkbox" checked="checked" value="1" /><label for="ml_0">Breakfast</label></td></tr>
            <tr><td><input id="ml_1" type="checkbox" checked="checked" value="2" /><label for="ml_1">Lunch</label></td></tr>
            <tr><td><input id="ml_4" type="checkbox" checked="checked" value="5" /><label for="ml_4">PM Snack</label></td></tr>
        </table>

        <table id="Content_Main_cblEnvironment">
            <tr>
                <td><input id="ev_0" type="checkbox" checked="checked" value="1" /><label for="ev_0">No pets</label></td>
                <td><input id="ev_1" type="checkbox" checked="checked" value="2" /><label for="ev_1">Outdoor Play area</label></td>
            </tr>
        </table>

        <table id="Content_Main_cblCampCare">
            <tr><td><input id="cc_0" type="checkbox" value="3" /><label for="cc_0">Summer Camp</label></td></tr>
        </table>

        <table id="Content_Main_cblAcceptingChildrenType">
            <tr><td><input id="act_0" type="checkbox" checked="checked" value="Full Time" /><label for="act_0">Full Time</label></td></tr>
            <tr><td><input id="act_1" type="checkbox" value="Part Time" /><label for="act_1">Part Time</label></td></tr>
        </table>

        <span id="Content_Main_lblAccreditation">N/A</span>

        <table id="Content_Main_rblForProfit">
            <tr><td><input id="fp_0" type="radio" checked="checked" value="P" /><label for="fp_0">For Profit</label></td></tr>
            <tr><td><input id="fp_1" type="radio" value="N" /><label for="fp_1">Non Profit</label></td></tr>
        </table>

        <table id="Content_Main_gvFacilityRates" class="grid">
            <tr>
                <th>Age</th><th>Weekly Full Day</th><th>Weekly Before School</th>
                <th>Weekly After School</th><th>Vacancies</th><th># of Rooms</th>
                <th>Staff/Child Ratio</th><th>Daily Drop In Care</th><th>Day Camp (Min-Max)</th>
            </tr>
            <tr>
                <td><span class="visible-xs-inline-block">Age:</span> Under 1 year</td>
                <td><span class="visible-xs-inline-block">Weekly Full Day:</span><div>$110.00</div></td>
                <td><span class="visible-xs-inline-block">Weekly Before School:</span></td>
                <td><span class="visible-xs-inline-block">Weekly After School:</span></td>
                <td><span class="visible-xs-inline-block">Vacancies:</span></td>
                <td><span class="visible-xs-inline-block"># of Rooms:</span><div>1</div></td>
                <td><span class="visible-xs-inline-block">Staff/Child Ratio:</span></td>
                <td><span class="visible-xs-inline-block">Daily Drop In Care:</span></td>
                <td><span class="visible-xs-inline-block">Day Camp (Min-Max):</span></td>
            </tr>
            <tr>
                <td><span class="visible-xs-inline-block">Age:</span> 1 year</td>
                <td><span class="visible-xs-inline-block">Weekly Full Day:</span><div>$95.00</div></td>
                <td><span class="visible-xs-inline-block">Weekly Before School:</span></td>
                <td><span class="visible-xs-inline-block">Weekly After School:</span></td>
                <td><span class="visible-xs-inline-block">Vacancies:</span></td>
                <td><span class="visible-xs-inline-block"># of Rooms:</span><div>1</div></td>
                <td><span class="visible-xs-inline-block">Staff/Child Ratio:</span></td>
                <td><span class="visible-xs-inline-block">Daily Drop In Care:</span></td>
                <td><span class="visible-xs-inline-block">Day Camp (Min-Max):</span></td>
            </tr>
        </table>

        <span id="Content_Main_lblActivities"><ul><li>Academic</li><li>Outdoor Adventure</li></ul></span>
        <span id="Content_Main_lblOtherChildCareType"><ul><li>After-school Program</li></ul></span>
        <span id="Content_Main_lblFinancialInformation">N/A</span>
        <span id="Content_Main_lblLanguages"><ul><li>English</li><li>Spanish</li></ul></span>
        <span id="Content_Main_lblSpecialHours"><ul><li>Open school holidays</li></ul></span>
        <span id="Content_Main_lblCurriculum"><ul><li>GELDS</li><li>Kaplan</li></ul></span>
        <span id="Content_Main_lblFamilyEngagement"><ul><li>Communication using flyers</li></ul></span>

        <div id="Content_Main_pnlTransportationNotes"><label>Transportation:</label><br><span>Screven County Elementary School</span></div>
        <div id="Content_Main_pnlSchoolBreakNotes"><br><label>School Break:</label><br><span>Drop-in services are discontinued.</span></div>
    </div>
</div>

<table id="Content_Main_gvReports">
    <tr>
        <th>View Report</th><th>Report Date</th><th>Arrival Time</th><th>Visit Status</th><th>Report Type</th>
    </tr>
    <tr>
        <td><a href="#">view</a></td>
        <td><span class="visible-xs-inline-block">Report Date:</span> Dec 02, 2025</td>
        <td><span class="visible-xs-inline-block">Arrival Time:</span> 11:50 AM</td>
        <td><span class="visible-xs-inline-block">Visit Status:</span> Completed</td>
        <td><span class="visible-xs-inline-block">Report Type:</span> Monitoring Visit</td>
    </tr>
    <tr>
        <td><a href="#">view</a></td>
        <td><span class="visible-xs-inline-block">Report Date:</span> Jun 23, 2025</td>
        <td><span class="visible-xs-inline-block">Arrival Time:</span> 12:50 PM</td>
        <td><span class="visible-xs-inline-block">Visit Status:</span> Completed</td>
        <td><span class="visible-xs-inline-block">Report Type:</span> Licensing Study</td>
    </tr>
</table>
</body>
</html>
"""

CSV_CONTENT = (
    "Provider_Number,Location,County,Address,City,State,Zip,MailingAddress,"
    "MailingCity,MailingState,MailingZip,Email,Phone,LicenseCapacity,"
    "Operation_Months,Operation_Days,Hours_Open,Hours_Close,"
    "Infant_0_To_12mos,Toddler_13mos_To_2yrs,Preschool_3yrs_To_4yrs,"
    "Pre_K_Served,School_Age_5yrs_Plus,Ages_Other_Than_Pre_K_Served,"
    "CAPS_Enrolled,Has_Evening_Care,Has_Drop_In_Care,"
    "Has_School_Age_Summer_Care,Has_Transport_ToFrom_School,"
    "Has_Transport_ToFrom_Home,Has_Cacfp,Accreditation_Status,"
    "Program_Type,Provider_Type,Exemption_Category,Available_PreK_Slots,"
    "Funded_PreK_Slots,QR_Participant,QR_Rated,QR_Rating,Region,"
    "IsTemporarilyClosed,TemporaryClosure_StartDate,"
    "TemporaryClosure_EndDate,CurrentProgramStatus\r\n"

    "CCLC-38436,1 Love Childcare & Learning Center,Screven,"
    "485 East Frontage Road,Sylvania,GA,30467,PO Box 788,Sylvania,GA,30467,"
    "test@example.com,(912) 564-2273,43,Year Round,Mon - Fri,05:45 AM,05:00 PM,"
    "True,True,True,False,True,True,True,False,False,False,False,False,True,N/A,"
    "Child Care Learning Center,CCLC,,0,0,True,True,3,Southeast,"
    "False,,,Open\r\n"

    "FCCLH-12345,Happy Kids Daycare,Fulton,"
    "123 Main St,Atlanta,GA,30301,,,,,,"
    "(404) 555-0100,12,Year Round,Mon - Fri,07:00 AM,06:00 PM,"
    "False,True,False,False,False,False,False,False,False,False,False,False,False,N/A,"
    "Family Child Care Learning Home,FCCLH,,0,0,False,False,,Northwest,"
    "False,,,Open\r\n"

    ",No ID Provider,,,,,,,,,,,,,,,,,"
    "False,False,False,False,False,False,False,False,False,False,False,False,False,,"
    "Unknown Type,,,0,0,False,False,,,"
    "False,,,Closed\r\n"
)


def test_extract_numeric_id():
    assert extract_numeric_id("CCLC-38436") == "38436"
    assert extract_numeric_id("FCCLH-12345") == "12345"
    assert extract_numeric_id("DOD-99999") == "99999"
    assert extract_numeric_id("12345") == "12345"
    assert extract_numeric_id("") is None
    assert extract_numeric_id(None) is None
    assert extract_numeric_id("NO-DIGITS") is None


def test_extract_checked_labels():
    request = Request(url="https://families.decal.ga.gov/ChildCare/detail/38436")
    response = HtmlResponse(
        url=request.url, body=DETAIL_HTML, encoding="utf-8", request=request
    )

    services = extract_checked_labels(response, "Content_Main_cblServicesProvided")
    assert "Enrolled in Childcare Subsidies (CAPS)" in services
    assert "CACFP" in services
    assert "Head Start" not in services

    ages = extract_checked_labels(response, "Content_Main_cblAgesServed")
    assert "Infant (0 -12 months)" in ages
    assert "School Age (5+)" in ages


def test_extract_list_items():
    request = Request(url="https://families.decal.ga.gov/ChildCare/detail/38436")
    response = HtmlResponse(
        url=request.url, body=DETAIL_HTML, encoding="utf-8", request=request
    )

    activities = extract_list_items(response, "Content_Main_lblActivities")
    assert "Academic" in activities
    assert "Outdoor Adventure" in activities

    languages = extract_list_items(response, "Content_Main_lblLanguages")
    assert "English" in languages
    assert "Spanish" in languages


def test_extract_radio_checked():
    request = Request(url="https://families.decal.ga.gov/ChildCare/detail/38436")
    response = HtmlResponse(
        url=request.url, body=DETAIL_HTML, encoding="utf-8", request=request
    )

    profit = extract_radio_checked(response, "Content_Main_rblForProfit")
    assert profit == "For Profit"


def test_parse_weekly_rates():
    request = Request(url="https://families.decal.ga.gov/ChildCare/detail/38436")
    response = HtmlResponse(
        url=request.url, body=DETAIL_HTML, encoding="utf-8", request=request
    )

    rates = parse_weekly_rates(response)
    assert len(rates) == 2
    assert rates[0]["age"] == "Under 1 year"
    assert rates[0]["weekly_full_day"] == "$110.00"
    assert rates[0]["num_rooms"] == "1"
    assert rates[1]["age"] == "1 year"
    assert rates[1]["weekly_full_day"] == "$95.00"


def test_parse_data_page(spider):
    """Test that parse_data_page finds checkboxes and submits form."""
    request = Request(url="https://families.decal.ga.gov/Provider/Data")
    response = HtmlResponse(
        url=request.url, body=DATA_PAGE_HTML, encoding="utf-8", request=request
    )

    results = list(spider.parse_data_page(response))
    assert len(results) == 1

    form_request = results[0]
    assert isinstance(form_request, scrapy.FormRequest)
    # The form request body should contain the event target for download
    body = form_request.body.decode("utf-8")
    assert "btnExportToExcel" in body


def test_parse_csv(spider):
    """Test CSV parsing yields detail requests for providers with IDs."""
    request = Request(url="https://families.decal.ga.gov/Provider/Data")
    response = TextResponse(
        url=request.url,
        body=CSV_CONTENT.encode("utf-8"),
        encoding="utf-8",
        request=request,
        headers={"Content-Type": "text/csv"},
    )

    results = list(spider.parse_csv(response))

    # Two providers have numeric IDs -> detail requests, one has no ID -> direct item
    detail_requests = [r for r in results if isinstance(r, scrapy.Request)]
    items = [r for r in results if isinstance(r, ProviderItem)]

    assert len(detail_requests) == 2
    assert len(items) == 1

    # Check first detail request
    assert "detail/38436" in detail_requests[0].url
    first_item = detail_requests[0].cb_kwargs["item"]
    assert first_item["provider_name"] == "1 Love Childcare & Learning Center"
    assert first_item["license_number"] == "CCLC-38436"
    assert first_item["provider_type"] == "Child Care Learning Center"
    assert first_item["county"] == "Screven"
    assert "485 East Frontage Road" in first_item["address"]
    assert "Sylvania" in first_item["address"]
    assert first_item["phone"] == "(912) 564-2273"
    assert first_item["capacity"] == "43"
    assert first_item["email"] == "test@example.com"
    assert first_item["hours"] == "05:45 AM - 05:00 PM"
    assert first_item["ga_operating_months"] == "Year Round"
    assert first_item["ga_operating_days"] == "Mon - Fri"
    assert "Infant" in first_item["ages_served"]
    assert "School Age" in first_item["ages_served"]
    assert first_item["infant"] == "Yes"
    assert first_item["school"] == "Yes"
    assert "CAPS Enrolled" in first_item["ga_services"]
    assert "CACFP" in first_item["ga_services"]
    assert first_item["scholarships_accepted"] == "Yes"
    assert first_item["ga_quality_rated_level"] == "3"
    assert first_item["ga_mailing_address"] is not None
    assert "PO Box 788" in first_item["ga_mailing_address"]

    # Check second detail request
    assert "detail/12345" in detail_requests[1].url

    # Check direct item (no ID)
    assert items[0]["source_state"] == "Georgia"
    assert items[0]["provider_type"] == "Unknown Type"
    assert items[0]["status"] == "Closed"


def test_parse_detail(spider):
    """Test parsing a detail page enriches the item with additional fields."""
    item = ProviderItem()
    item["source_state"] = "Georgia"
    item["provider_name"] = "1 Love Childcare & Learning Center"
    item["license_number"] = "CCLC-38436"

    request = Request(url="https://families.decal.ga.gov/ChildCare/detail/38436")
    response = HtmlResponse(
        url=request.url, body=DETAIL_HTML, encoding="utf-8", request=request
    )

    results = list(spider.parse_detail(response, item=item))
    assert len(results) == 1

    item = results[0]
    assert isinstance(item, ProviderItem)

    # Basic fields
    assert item["administrator"] == "Shevella Young"
    assert item["capacity"] == "43"
    assert item["phone"] == "(912) 564-2273"
    assert item["ga_quality_rated_level"] == "3"
    assert item["ga_liability_insurance"] == "Yes"
    assert item["ga_accepting_new_children"] == "Yes"
    assert item["ga_mailing_address"] == "PO Box 788 Sylvania, GA - 30467"
    assert item["ga_registration_fee"] == "$95.00"
    assert item["ga_activity_fee"] == "$5.00"
    assert item["ga_program_status"] == "Open"
    assert "acceptable level" in item["ga_compliance_status"]

    # Operating schedule
    assert item["ga_operating_months"] == "Year Round"
    assert item["ga_operating_days"] == "Mon - Fri"
    assert "05:45 AM" in item["hours"]

    # Checkbox lists
    assert "CAPS" in item["ga_services"]
    assert "CACFP" in item["ga_services"]
    assert "Infant (0 -12 months)" in item["ages_served"]
    assert "School Age (5+)" in item["ages_served"]
    assert "On School Bus Route" in item["ga_transportation"]
    assert "Breakfast" in item["ga_meals"]
    assert "PM Snack" in item["ga_meals"]
    assert "No pets" in item["ga_environment"]
    assert item["ga_summer_camp"] is None  # none checked
    assert "Full Time" in item["ga_accepts_children_type"]

    # Other fields
    assert item["ga_accreditation"] == "N/A"
    assert item["ga_profit_status"] == "For Profit"
    assert "Academic" in item["ga_activities"]
    assert "After-school Program" in item["ga_other_care_type"]
    assert item["ga_financial_info"] is None  # N/A gets set to None
    assert "English" in item["languages"]
    assert "Open school holidays" in item["ga_special_hours"]
    assert "GELDS" in item["ga_curriculum"]
    assert "flyers" in item["ga_family_engagement"]
    assert "Screven County" in item["ga_transportation_notes"]
    assert "discontinued" in item["ga_school_break_notes"]

    # Weekly rates
    assert len(item["ga_weekly_rates"]) == 2
    assert item["ga_weekly_rates"][0]["age"] == "Under 1 year"
    assert item["ga_weekly_rates"][0]["weekly_full_day"] == "$110.00"

    # Inspections
    assert len(item["inspections"]) == 2
    assert item["inspections"][0]["date"] == "Dec 02, 2025"
    assert item["inspections"][0]["type"] == "Monitoring Visit"
    assert item["inspections"][0]["original_status"] == "Completed"
    assert item["inspections"][1]["date"] == "Jun 23, 2025"
    assert item["inspections"][1]["type"] == "Licensing Study"


def test_parse_detail_no_panel(spider):
    """Test that parse_detail yields item unchanged if page has no detail panel."""
    item = ProviderItem()
    item["source_state"] = "Georgia"
    item["provider_name"] = "Test Provider"

    html = "<html><body><div>Not a detail page</div></body></html>"
    request = Request(url="https://families.decal.ga.gov/ChildCare/detail/99999")
    response = HtmlResponse(
        url=request.url, body=html, encoding="utf-8", request=request
    )

    results = list(spider.parse_detail(response, item=item))
    assert len(results) == 1
    assert results[0]["provider_name"] == "Test Provider"
    assert results[0]["source_state"] == "Georgia"


def test_parse_detail_not_accepting(spider):
    """Test accepting new children is 'No' when checkbox is not checked."""
    item = ProviderItem()
    item["source_state"] = "Georgia"

    html = """
    <html><body>
    <div class="panel panel-primary">
        <div class="panel-body">
            <input id="Content_Main_chkIsAcceptingNewChildren" type="checkbox" />
        </div>
    </div>
    <table id="Content_Main_gvReports"><tr><th>Header</th></tr></table>
    </body></html>
    """
    request = Request(url="https://families.decal.ga.gov/ChildCare/detail/11111")
    response = HtmlResponse(
        url=request.url, body=html, encoding="utf-8", request=request
    )

    results = list(spider.parse_detail(response, item=item))
    assert results[0]["ga_accepting_new_children"] == "No"

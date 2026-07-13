import json

import pytest
import scrapy
from scrapy.http import JsonRequest, Request, TextResponse
from twisted.python.failure import Failure

from provider_scrape.items import ProviderItem
from provider_scrape.spiders.georgia import (
    COMPLIANCE_URL,
    EXPORT_URL,
    SEARCH_URL,
    VISITS_URL,
    GeorgiaSpider,
    build_mailing_address,
    parse_weekly_rates,
    _clean,
    _format_fee,
    _split_multi,
    _yes_no,
)


@pytest.fixture
def spider():
    return GeorgiaSpider()


# A trimmed but structurally faithful /provider/search record (from the live
# API for CCLC-38436), covering every field the spider maps.
SEARCH_RECORD = {
    "id": 38436,
    "providerNumber": "CCLC-38436",
    "locationName": "1 Love Childcare & Learning Center",
    "locationPhone": "(912) 564-2273",
    "capacity": 43,
    "accreditations": "GAC",
    "servicesProvided": "Enrolled in Childcare Subsidies (CAPS)|CACFP",
    "transportation": "On School Bus Route",
    "agesServed": "Infant (0 -12 months), Toddler (13 months - 2 years), School Age (5+)",
    "financialInfo": "Multi-Child Discount|Scholarship",
    "specialHourInfo": "Open school holidays|Open school breaks",
    "environmentInfo": "No pets|Outdoor Play area",
    "mealInfo": "Breakfast|Lunch|PM Snack",
    "campCareInfo": "Summer Camp",
    "activities": "Academic|Outdoor Adventure",
    "languages": "English (Taught and Spoken by Staff)|Spanish (Taught and Spoken by Staff)",
    "otherChildCareTypes": "Before-school Program|After-school Program",
    "weeklyFullDayRates": "Under 1 year - $110.00|1 year - $95.00|5 years & older - $85.00",
    "acceptingChildrenTimeType": "Full Time|Part Time",
    "adminFirstName": "Shevella",
    "adminLastName": "Young",
    "mlAddress": "PO Box 788",
    "mlCity": "Sylvania",
    "mlState": "GA",
    "mlZip": "30467",
    "liabilityInsurance": True,
    "rateRegistrationFee": 95.0,
    "rateActivityFee": 5.0,
    "qualityRating": 3,
    "profitStatus": None,
    "familyEngagement": None,
    "isAcceptingNewChildren": False,
    "transportToFromSchool": None,
    "schoolCareBreakAdditionalSchedulingInfo": None,
}

VISITS = [
    {
        "locationId": 38436,
        "visitType": "Licensing Study",
        "visitStatus": "Completed",
        "visitDate": "2026-05-06T00:00:00",
    },
    {
        "locationId": 38436,
        "visitType": "Monitoring Visit",
        "visitStatus": "Completed",
        "visitDate": "2025-12-02T00:00:00",
    },
]

COMPLIANCE = {
    "imageUrl": "PDGLevels/Good-Standing_FINAL.webp",
    "tooltip": "Program is demonstrating an acceptable level of performance in meeting the rules.",
    "alternateText": None,
}

# Header quoted like the live export; DictReader strips the quotes.
CSV_CONTENT = (
    '"Provider_Number","Location","County","Address","City","State","Zip",'
    '"MailingAddress","MailingCity","MailingState","MailingZip","Email","Phone",'
    '"LicenseCapacity","Operation_Months","Operation_Days","Hours_Open",'
    '"Hours_Close","Infant_0_To_12mos","Toddler_13mos_To_2yrs",'
    '"Preschool_3yrs_To_4yrs","Pre_K_Served","School_Age_5yrs_Plus",'
    '"Ages_Other_Than_Pre_K_Served","CAPS_Enrolled","Has_Evening_Care",'
    '"Has_Drop_In_Care","Has_School_Age_Summer_Care","Has_Transport_ToFrom_School",'
    '"Has_Transport_ToFrom_Home","Has_Cacfp","Accreditation_Status",'
    '"Program_Type","Provider_Type","Exemption_Category","Available_PreK_Slots",'
    '"Funded_PreK_Slots","QR_Participant","QR_Rated","QR_Rating","Region",'
    '"IsTemporarilyClosed","TemporaryClosure_StartDate",'
    '"TemporaryClosure_EndDate","CurrentProgramStatus"\r\n'
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


def _json_response(url, payload, request=None):
    request = request or Request(url=url)
    return TextResponse(
        url=url,
        body=json.dumps(payload).encode("utf-8"),
        encoding="utf-8",
        request=request,
        headers={"Content-Type": "application/json"},
    )


# -- helper functions --------------------------------------------------------


def test_clean():
    assert _clean("  hi ") == "hi"
    assert _clean("") is None
    assert _clean("   ") is None
    assert _clean(None) is None
    assert _clean(43) == "43"


def test_split_multi():
    assert _split_multi("Breakfast|Lunch|PM Snack") == "Breakfast; Lunch; PM Snack"
    assert _split_multi("A, B, C", sep=",") == "A; B; C"
    assert _split_multi("") is None
    assert _split_multi(None) is None
    assert _split_multi("Solo") == "Solo"


def test_yes_no():
    assert _yes_no(True) == "Yes"
    assert _yes_no(False) == "No"
    assert _yes_no(None) is None


def test_format_fee():
    assert _format_fee(95.0) == "$95.00"
    assert _format_fee(5) == "$5.00"
    assert _format_fee(None) is None
    assert _format_fee("n/a") == "n/a"


def test_parse_weekly_rates():
    rates = parse_weekly_rates(
        "Under 1 year - $110.00|1 year - $95.00|5 years & older - $85.00"
    )
    assert rates == [
        {"age": "Under 1 year", "weekly_full_day": "$110.00"},
        {"age": "1 year", "weekly_full_day": "$95.00"},
        {"age": "5 years & older", "weekly_full_day": "$85.00"},
    ]
    assert parse_weekly_rates("") == []
    assert parse_weekly_rates(None) == []


def test_build_mailing_address():
    assert (
        build_mailing_address(SEARCH_RECORD) == "PO Box 788, Sylvania, GA 30467"
    )
    assert build_mailing_address({}) is None
    assert build_mailing_address({"mlState": "GA"}) == "GA"


# -- auth / export -----------------------------------------------------------


def test_parse_token_yields_export_request(spider):
    response = _json_response(
        "https://dcle2-decalapiprd.azurewebsites.net/api/Token",
        {"access_token": "TOK", "token_type": "Bearer", "expires_in": 3600},
    )
    results = list(spider.parse_token(response))
    assert spider.token == "TOK"
    assert len(results) == 1
    req = results[0]
    assert isinstance(req, JsonRequest)
    assert req.url == EXPORT_URL
    assert req.headers.get("Authorization") == b"Bearer TOK"
    body = json.loads(req.body)
    assert body["selectedProgramTypes"] == [100, 102, 104, 110, 111, 112, 113, 115, 116]


def test_parse_export(spider):
    """Export CSV yields a search request per provider with an id, else an item."""
    spider.token = "TOK"
    request = Request(url=EXPORT_URL)
    response = TextResponse(
        url=EXPORT_URL,
        # Mirror the live export: UTF-8 with a BOM.
        body=CSV_CONTENT.encode("utf-8-sig"),
        encoding="utf-8",
        request=request,
        headers={"Content-Type": "text/csv"},
    )

    results = list(spider.parse_export(response))
    search_requests = [r for r in results if isinstance(r, scrapy.Request)]
    items = [r for r in results if isinstance(r, ProviderItem)]

    # Two providers have a Provider_Number -> search requests; one has none.
    assert len(search_requests) == 2
    assert len(items) == 1

    first = search_requests[0]
    assert first.url == SEARCH_URL
    assert first.headers.get("Authorization") == b"Bearer TOK"
    assert json.loads(first.body) == {"ProviderNumber": "CCLC-38436"}

    base_item = first.cb_kwargs["item"]
    assert base_item["provider_name"] == "1 Love Childcare & Learning Center"
    assert base_item["license_number"] == "CCLC-38436"
    assert base_item["county"] == "Screven"
    assert "485 East Frontage Road" in base_item["address"]
    assert base_item["phone"] == "(912) 564-2273"
    assert base_item["hours"] == "05:45 AM - 05:00 PM"
    assert "Infant" in base_item["ages_served"]
    assert base_item["infant"] == "Yes"
    assert base_item["scholarships_accepted"] == "Yes"

    # The row without a Provider_Number is emitted directly.
    assert items[0]["provider_name"] == "No ID Provider"
    assert items[0]["status"] == "Closed"


# -- detail enrichment -------------------------------------------------------


def _base_item():
    item = ProviderItem()
    item["source_state"] = "Georgia"
    item["provider_name"] = "1 Love Childcare & Learning Center"
    item["license_number"] = "CCLC-38436"
    # Values the CSV would already have populated.
    item["ga_services"] = "CAPS Enrolled; CACFP"
    item["ga_accreditation"] = "N/A"
    return item


def test_parse_detail_enriches_and_requests_visits(spider):
    spider.token = "TOK"
    item = _base_item()
    request = Request(url=SEARCH_URL)
    response = _json_response(SEARCH_URL, [SEARCH_RECORD], request=request)

    results = list(
        spider.parse_detail(response, item=item, provider_number="CCLC-38436")
    )
    assert len(results) == 1
    visits_req = results[0]
    assert isinstance(visits_req, scrapy.Request)
    assert visits_req.url == f"{VISITS_URL}/38436"
    assert visits_req.headers.get("Authorization") == b"Bearer TOK"

    enriched = visits_req.cb_kwargs["item"]
    assert visits_req.cb_kwargs["location_id"] == 38436
    assert enriched["provider_url"] == "https://families.decal.ga.gov/ChildCare/detail/38436"
    assert enriched["administrator"] == "Shevella Young"
    assert enriched["capacity"] == "43"
    assert enriched["ga_quality_rated_level"] == "3"
    assert enriched["ga_liability_insurance"] == "Yes"
    assert enriched["ga_accepting_new_children"] == "No"
    assert enriched["ga_registration_fee"] == "$95.00"
    assert enriched["ga_activity_fee"] == "$5.00"
    assert enriched["ga_services"] == "Enrolled in Childcare Subsidies (CAPS); CACFP"
    assert enriched["ga_meals"] == "Breakfast; Lunch; PM Snack"
    assert enriched["ga_environment"] == "No pets; Outdoor Play area"
    assert enriched["ga_summer_camp"] == "Summer Camp"
    assert enriched["ga_accepts_children_type"] == "Full Time; Part Time"
    assert enriched["ga_activities"] == "Academic; Outdoor Adventure"
    assert enriched["ga_other_care_type"] == "Before-school Program; After-school Program"
    assert enriched["ga_financial_info"] == "Multi-Child Discount; Scholarship"
    assert enriched["ga_special_hours"] == "Open school holidays; Open school breaks"
    assert "English" in enriched["languages"] and "Spanish" in enriched["languages"]
    assert enriched["ages_served"] == (
        "Infant (0 -12 months); Toddler (13 months - 2 years); School Age (5+)"
    )
    # API supplied an accreditation code -> overrides the CSV "N/A".
    assert enriched["ga_accreditation"] == "GAC"
    assert enriched["ga_mailing_address"] == "PO Box 788, Sylvania, GA 30467"
    assert enriched["ga_weekly_rates"][0] == {
        "age": "Under 1 year",
        "weekly_full_day": "$110.00",
    }
    # Unpopulated API fields map to None.
    assert enriched["ga_profit_status"] is None
    assert enriched["ga_transportation_notes"] is None


def test_parse_detail_keeps_csv_services_when_api_empty(spider):
    spider.token = "TOK"
    item = _base_item()
    record = dict(SEARCH_RECORD, servicesProvided="", accreditations="")
    response = _json_response(SEARCH_URL, [record], request=Request(url=SEARCH_URL))

    results = list(
        spider.parse_detail(response, item=item, provider_number="CCLC-38436")
    )
    enriched = results[0].cb_kwargs["item"]
    # Falls back to the CSV-derived values when the API field is blank.
    assert enriched["ga_services"] == "CAPS Enrolled; CACFP"
    assert enriched["ga_accreditation"] == "N/A"


def test_parse_detail_no_record_yields_item(spider):
    spider.token = "TOK"
    item = _base_item()
    response = _json_response(SEARCH_URL, [], request=Request(url=SEARCH_URL))

    results = list(
        spider.parse_detail(response, item=item, provider_number="CCLC-38436")
    )
    assert len(results) == 1
    assert isinstance(results[0], ProviderItem)
    assert results[0]["license_number"] == "CCLC-38436"


# -- visits / compliance -----------------------------------------------------


def test_parse_visits_attaches_inspections(spider):
    """Inspections are attached from the visits list regardless of the next hop."""
    item = _base_item()
    response = _json_response(f"{VISITS_URL}/38436", VISITS)

    results = list(spider.parse_visits(response, item=item, location_id=38436))
    assert len(results) == 1
    inspections = results[0].cb_kwargs["item"]["inspections"]
    assert len(inspections) == 2
    assert inspections[0]["date"] == "2026-05-06"
    assert inspections[0]["type"] == "Licensing Study"
    assert inspections[0]["original_status"] == "Completed"
    assert inspections[1]["date"] == "2025-12-02"


def test_parse_visits_default_requests_compliance(spider):
    """Compliance is fetched by default (fetch_compliance defaults to True)."""
    assert spider.fetch_compliance is True
    spider.token = "TOK"
    item = _base_item()
    response = _json_response(f"{VISITS_URL}/38436", VISITS)

    results = list(spider.parse_visits(response, item=item, location_id=38436))
    assert len(results) == 1
    comp_req = results[0]
    assert isinstance(comp_req, scrapy.Request)
    assert comp_req.url == f"{COMPLIANCE_URL}/38436"
    assert comp_req.headers.get("Authorization") == b"Bearer TOK"
    # Inspections were still attached before the compliance hop.
    assert len(comp_req.cb_kwargs["item"]["inspections"]) == 2


def test_parse_visits_opt_out_yields_item_directly():
    """With fetch_compliance=0 the item is emitted after visits, no extra call."""
    spider = GeorgiaSpider(fetch_compliance="0")
    assert spider.fetch_compliance is False
    item = _base_item()
    response = _json_response(f"{VISITS_URL}/38436", VISITS)

    results = list(spider.parse_visits(response, item=item, location_id=38436))
    assert len(results) == 1
    assert isinstance(results[0], ProviderItem)
    assert len(results[0]["inspections"]) == 2


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, True),  # not passed -> default on
        ("0", False),
        ("false", False),
        ("no", False),
        ("1", True),
        ("true", True),
    ],
)
def test_fetch_compliance_flag_parsing(value, expected):
    kwargs = {} if value is None else {"fetch_compliance": value}
    assert GeorgiaSpider(**kwargs).fetch_compliance is expected


def test_parse_compliance_sets_status(spider):
    item = _base_item()
    response = _json_response(f"{COMPLIANCE_URL}/38436", COMPLIANCE)
    results = list(spider.parse_compliance(response, item=item))
    assert len(results) == 1
    assert "acceptable level of performance" in results[0]["ga_compliance_status"]


def test_build_inspections_skips_empty(spider):
    inspections = spider._build_inspections(
        [{"visitDate": None, "visitType": None, "visitStatus": None}, VISITS[0]]
    )
    assert len(inspections) == 1
    assert inspections[0]["type"] == "Licensing Study"


# -- errback -----------------------------------------------------------------


def test_errback_enrich_emits_carried_item(spider):
    item = _base_item()
    request = Request(url=f"{VISITS_URL}/38436", cb_kwargs={"item": item})
    failure = Failure(Exception("boom"))
    failure.request = request

    results = spider.errback_enrich(failure)
    assert results == [item]


def test_errback_enrich_without_item_returns_empty(spider):
    request = Request(url=f"{VISITS_URL}/1", cb_kwargs={})
    failure = Failure(Exception("boom"))
    failure.request = request
    assert spider.errback_enrich(failure) == []

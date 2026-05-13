import pytest

from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.spiders.florida import (
    FL_COUNTIES,
    FloridaSpider,
    build_county_url,
    build_spa_search_url,
    response_matches_county,
    _num,
)


@pytest.fixture
def spider():
    return FloridaSpider()


# ---- Sample data (trimmed but realistic) ----

FL_PROVIDER_FULL = {
    "providerName": "BH Academy LLC",
    "dba": "BH Academy LLC",
    "providerType": "Child Care Facility",
    "licenseStatus": "Licensed",
    "providerPhone": 3057474709,
    "licenseNumber": "C11MD3331",
    "alternateProviderNumber": None,
    "emailAddress": None,
    "capacity": 61,
    "providerStatus": "Operational",
    "licenseExpirationDate": "2027-04-07T00:00:00",
    "displayAddressOnWeb": True,
    "displayEmailOnWeb": False,
    "displayPhoneOnWeb": True,
    "isReligiousExempt": False,
    "isFaithBased": False,
    "isHeadStart": True,
    "isOfferingSchoolReadiness": True,
    "isVPK": True,
    "isGoldSeal": False,
    "isPublicSchool": False,
    "originationDate": "2024-04-08T00:00:00",
    "city": "Aventura",
    "county": "Miami-Dade",
    "state": "FL",
    "zipCode": "33180",
    "fullAddress": "20295 NE 29th Pl, Aventura FL, 33180",
    "vpkSchoolYearCompositeScore": 5.39,
    "vpkSchoolYearWelsRatingDate": "2025-03-05T00:00:00",
    "vpkSummerCompositeScore": {"source": "0.0", "parsedValue": 0},
    "vpkSummerWelsRatingDate": None,
    "compositesScore": None,
    "welsRatingDate": None,
    "isTraumaBadge": False,
    "isInclusionBadge": False,
    "isDualLanguageLearnersBadge": False,
    "isInfantToddlerBadge": False,
    "traumaBadgeDate": None,
    "inclusionBadgeDate": None,
    "dualLanguageLearnersBadgeDate": None,
    "infantToddlerBadgeDate": None,
    "mondayHours": "8:00AM to 6:00PM",
    "tuesdayHours": "8:00AM to 6:00PM",
    "wednesdayHours": "8:00AM to 6:00PM",
    "thursdayHours": "8:00AM to 6:00PM",
    "fridayHours": "8:00AM to 6:00PM",
    "saturdayHours": "Closed",
    "sundayHours": "8:00AM to 6:00PM",
    "latitude": 25.96298,
    "longitude": -80.14203,
    "goldSeal": {
        "accreditationName": None,
        "effectiveDate": None,
        "expirationDate": None,
    },
    "vpk": {
        "accreditation": None,
        "classRoom": [
            {"classType": "540", "class": [{"classRoomCode": "AF24"}]}
        ],
        "curriculum": [
            {"name": "The Creative Curriculum for Preschool (3 to K)"}
        ],
        "instructorCredential": [
            {"name": "ClassAF24: Received a B.S. or a B.A"}
        ],
    },
    "service": [
        {"name": "Drop In"},
        {"name": "Full Day"},
        {"name": "Infant Care"},
    ],
    "program": [
        {"name": "School Readiness"},
        {"name": "VPK"},
        {"name": "Head Start"},
    ],
    "inspection": [
        {
            "year": 2024,
            "inspectionReport": [
                {
                    "inspectionDate": "2024-08-06T12:00:00",
                    "id": "4140d975-8671-4a73-b4d3-0b22fa690c90",
                    "hasViolation": False,
                },
                {
                    "inspectionDate": "2024-12-06T15:57:00",
                    "id": "832f086f-9c7b-4534-a583-48a287e0ed86",
                    "hasViolation": True,
                },
            ],
        },
        {
            "year": 2025,
            "inspectionReport": [
                {
                    "inspectionDate": "2025-04-03T14:29:00",
                    "id": "c4da3353-0a3b-426d-88e7-521c3ac8308b",
                    "hasViolation": False,
                }
            ],
        },
    ],
}

# Sparse exempt record — many null/missing fields, no inspections,
# and a parsedValue-shaped score on the family-level composite.
FL_PROVIDER_EXEMPT = {
    "providerName": "Chabad Chayil-Community Hebrew Afterschool Program CHAP",
    "dba": "",
    "providerType": "Child Care Facility",
    "licenseStatus": "Exempt",
    "providerPhone": 3057701919,
    "licenseNumber": "X11MD0124",
    "alternateProviderNumber": None,
    "emailAddress": None,
    "capacity": None,
    "providerStatus": "Operational",
    "licenseExpirationDate": "2026-10-14T00:00:00",
    "displayAddressOnWeb": True,
    "displayEmailOnWeb": False,
    "displayPhoneOnWeb": True,
    "isReligiousExempt": False,
    "isFaithBased": False,
    "isHeadStart": False,
    "isOfferingSchoolReadiness": False,
    "isVPK": False,
    "isGoldSeal": False,
    "isPublicSchool": False,
    "originationDate": "2021-09-10T00:00:00",
    "city": "Miami",
    "county": "Miami-Dade",
    "state": "FL",
    "zipCode": "33180",
    "fullAddress": "2600 NE 212th Ter, Miami FL, 33180",
    "vpkSchoolYearCompositeScore": {"source": "0.0", "parsedValue": 0},
    "vpkSchoolYearWelsRatingDate": None,
    "vpkSummerCompositeScore": {"source": "0.0", "parsedValue": 0},
    "vpkSummerWelsRatingDate": None,
    "compositesScore": {"source": "5.0", "parsedValue": 5},
    "welsRatingDate": None,
    "mondayHours": "2:00PM to 6:00PM",
    "tuesdayHours": "2:00PM to 6:00PM",
    "wednesdayHours": "2:00PM to 6:00PM",
    "thursdayHours": "2:00PM to 6:00PM",
    "fridayHours": "2:00PM to 6:00PM",
    "saturdayHours": "Closed",
    "sundayHours": "Closed",
    "latitude": 25.97164,
    "longitude": -80.14857,
    "goldSeal": {
        "accreditationName": None,
        "effectiveDate": None,
        "expirationDate": None,
    },
    "vpk": {
        "accreditation": None,
        "classRoom": None,
        "curriculum": None,
        "instructorCredential": None,
    },
    "service": [{"name": "After School"}],
    "program": None,
    "inspection": None,
}


# ---- _num ----

def test_num_unwraps_parsed_value():
    assert _num({"source": "5.0", "parsedValue": 5}) == 5
    assert _num({"source": "0.0", "parsedValue": 0}) == 0


def test_num_passes_through_plain_numbers():
    assert _num(5.39) == 5.39
    assert _num(0) == 0
    assert _num(None) is None


def test_num_passes_through_unrelated_dicts():
    # dict without parsedValue shouldn't be unwrapped
    payload = {"accreditationName": None}
    assert _num(payload) is payload


# ---- counties / URL builder ----

def test_county_list_has_67_unique_entries():
    assert len(FL_COUNTIES) == 67
    assert len(set(FL_COUNTIES)) == 67


def test_build_county_url_simple():
    url = build_county_url("Miami-Dade")
    assert "searchText=Miami-Dade" in url
    assert "tag=Counties" in url


def test_build_county_url_encodes_spaces_as_percent_20():
    url = build_county_url("St. Johns")
    # Confirm the API-friendly %20 encoding, not '+', for the space
    assert "searchText=St.%20Johns" in url
    assert "+" not in url.split("?", 1)[1]


def test_build_spa_search_url_simple():
    url = build_spa_search_url("Miami-Dade")
    assert url == (
        "https://caressearch.myflfamilies.com/PublicSearch/Search"
        "?term=Miami-Dade"
    )


def test_build_spa_search_url_encodes_space():
    url = build_spa_search_url("Palm Beach")
    assert "term=Palm%20Beach" in url
    assert "+" not in url.split("?", 1)[1]


# ---- response_matches_county ----

def test_response_matches_county_plain():
    api_url = (
        "https://caresapi.myflfamilies.com/api/publicSearch/Search"
        "?searchText=Miami-Dade&tag=Counties"
    )
    assert response_matches_county(api_url, "Miami-Dade") is True


def test_response_matches_county_encoded_space():
    api_url = (
        "https://caresapi.myflfamilies.com/api/publicSearch/Search"
        "?searchText=Palm%20Beach&tag=Counties"
    )
    assert response_matches_county(api_url, "Palm Beach") is True


def test_response_matches_county_case_insensitive():
    api_url = (
        "https://caresapi.myflfamilies.com/api/publicSearch/Search"
        "?searchText=miami-dade&tag=Counties"
    )
    assert response_matches_county(api_url, "Miami-Dade") is True


def test_response_matches_county_wrong_county():
    api_url = (
        "https://caresapi.myflfamilies.com/api/publicSearch/Search"
        "?searchText=Lake&tag=Counties"
    )
    assert response_matches_county(api_url, "Liberty") is False


def test_response_matches_county_non_api_url():
    # Resource on the SPA's domain — not the API call we care about
    assert response_matches_county(
        "https://caressearch.myflfamilies.com/PublicSearch/Search?term=Lake",
        "Lake",
    ) is False


def test_response_matches_county_missing_searchtext():
    # Some unrelated call to the same path without a searchText param
    assert response_matches_county(
        "https://caresapi.myflfamilies.com/api/publicSearch/Search?tag=Counties",
        "Lake",
    ) is False


# ---- parse_provider (full record) ----

def test_parse_provider_core_fields(spider):
    item = spider.parse_provider(FL_PROVIDER_FULL, "Miami-Dade")

    assert isinstance(item, ProviderItem)
    assert item["source_state"] == "Florida"
    assert item["county"] == "Miami-Dade"
    assert item["provider_name"] == "BH Academy LLC"
    assert item["fl_dba"] == "BH Academy LLC"
    assert item["provider_type"] == "Child Care Facility"
    assert item["license_number"] == "C11MD3331"
    assert item["fl_license_status"] == "Licensed"
    assert item["status"] == "Operational"
    assert item["capacity"] == 61
    assert item["address"] == "20295 NE 29th Pl, Aventura FL, 33180"
    assert item["fl_city"] == "Aventura"
    assert item["fl_zip_code"] == "33180"
    assert item["latitude"] == 25.96298
    assert item["longitude"] == -80.14203
    assert item["license_expiration"] == "2027-04-07T00:00:00"
    assert item["license_begin_date"] == "2024-04-08T00:00:00"


def test_parse_provider_phone_coerces_int_to_str(spider):
    item = spider.parse_provider(FL_PROVIDER_FULL, "Miami-Dade")
    assert item["phone"] == "3057474709"


def test_parse_provider_phone_none_stays_none(spider):
    record = dict(FL_PROVIDER_FULL)
    record["providerPhone"] = None
    item = spider.parse_provider(record, "Miami-Dade")
    assert item["phone"] is None


def test_parse_provider_hours_dict(spider):
    item = spider.parse_provider(FL_PROVIDER_FULL, "Miami-Dade")
    assert item["hours"] == {
        "monday": "8:00AM to 6:00PM",
        "tuesday": "8:00AM to 6:00PM",
        "wednesday": "8:00AM to 6:00PM",
        "thursday": "8:00AM to 6:00PM",
        "friday": "8:00AM to 6:00PM",
        "saturday": "Closed",
        "sunday": "8:00AM to 6:00PM",
    }


def test_parse_provider_unwraps_parsed_value_scores(spider):
    item = spider.parse_provider(FL_PROVIDER_FULL, "Miami-Dade")
    # School-year score is a plain number; summer score is a parsedValue dict.
    assert item["fl_vpk_school_year_composite_score"] == 5.39
    assert item["fl_vpk_summer_composite_score"] == 0


def test_parse_provider_sutq_rating_handles_parsed_value(spider):
    item = spider.parse_provider(FL_PROVIDER_EXEMPT, "Miami-Dade")
    assert item["sutq_rating"] == 5


def test_parse_provider_sutq_rating_none_when_missing(spider):
    item = spider.parse_provider(FL_PROVIDER_FULL, "Miami-Dade")
    assert item["sutq_rating"] is None


def test_parse_provider_program_and_service_lists(spider):
    item = spider.parse_provider(FL_PROVIDER_FULL, "Miami-Dade")
    assert item["fl_programs"] == ["School Readiness", "VPK", "Head Start"]
    assert item["fl_services"] == ["Drop In", "Full Day", "Infant Care"]


def test_parse_provider_infant_inferred_from_services(spider):
    full = spider.parse_provider(FL_PROVIDER_FULL, "Miami-Dade")
    assert full["infant"] is True

    exempt = spider.parse_provider(FL_PROVIDER_EXEMPT, "Miami-Dade")
    assert exempt["infant"] is False


def test_parse_provider_vpk_structure_kept_as_dict(spider):
    item = spider.parse_provider(FL_PROVIDER_FULL, "Miami-Dade")
    assert item["fl_vpk_classrooms"] == [
        {"classType": "540", "class": [{"classRoomCode": "AF24"}]}
    ]
    assert item["fl_vpk_curriculum"] == [
        {"name": "The Creative Curriculum for Preschool (3 to K)"}
    ]
    assert item["fl_vpk_instructor_credentials"] == [
        {"name": "ClassAF24: Received a B.S. or a B.A"}
    ]


def test_parse_provider_gold_seal_dict_preserved(spider):
    item = spider.parse_provider(FL_PROVIDER_FULL, "Miami-Dade")
    assert item["fl_gold_seal"] == {
        "accreditationName": None,
        "effectiveDate": None,
        "expirationDate": None,
    }


# ---- parse_provider (sparse record) ----

def test_parse_provider_exempt_minimal_fields(spider):
    item = spider.parse_provider(FL_PROVIDER_EXEMPT, "Miami-Dade")

    assert item["fl_license_status"] == "Exempt"
    assert item["fl_dba"] == ""
    assert item["capacity"] is None
    assert item["email"] is None
    assert item["fl_programs"] == []
    assert item["fl_services"] == ["After School"]
    assert item["inspections"] == []
    # VPK sub-fields should be None when the vpk block is empty
    assert item["fl_vpk_classrooms"] is None
    assert item["fl_vpk_curriculum"] is None


# ---- parse_inspections ----

def test_parse_inspections_flattens_year_buckets(spider):
    inspections = spider.parse_inspections(FL_PROVIDER_FULL["inspection"])
    assert len(inspections) == 3
    assert all(isinstance(i, InspectionItem) for i in inspections)

    by_id = {i["fl_inspection_id"]: i for i in inspections}
    first = by_id["4140d975-8671-4a73-b4d3-0b22fa690c90"]
    assert first["date"] == "2024-08-06T12:00:00"
    assert first["fl_has_violation"] is False

    violator = by_id["832f086f-9c7b-4534-a583-48a287e0ed86"]
    assert violator["fl_has_violation"] is True


def test_parse_inspections_handles_none_and_missing_reports(spider):
    assert spider.parse_inspections(None) == []
    assert spider.parse_inspections([]) == []
    assert spider.parse_inspections([{"year": 2024, "inspectionReport": None}]) == []



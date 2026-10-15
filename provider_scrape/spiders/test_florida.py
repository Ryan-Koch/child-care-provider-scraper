import pytest

from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.spiders.florida import (
    FL_COUNTIES,
    FloridaSpider,
    _num,
    build_county_url,
    build_spa_search_url,
    choose_surviving_county,
    dedupe_inspection_reports,
    dedupe_providers,
    response_matches_county,
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
        "classRoom": [{"classType": "540", "class": [{"classRoomCode": "AF24"}]}],
        "curriculum": [{"name": "The Creative Curriculum for Preschool (3 to K)"}],
        "instructorCredential": [{"name": "ClassAF24: Received a B.S. or a B.A"}],
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
    assert url == ("https://caressearch.myflfamilies.com/PublicSearch/Search?term=Miami-Dade")


def test_build_spa_search_url_encodes_space():
    url = build_spa_search_url("Palm Beach")
    assert "term=Palm%20Beach" in url
    assert "+" not in url.split("?", 1)[1]


# ---- response_matches_county ----


def test_response_matches_county_plain():
    api_url = "https://caresapi.myflfamilies.com/api/publicSearch/Search?searchText=Miami-Dade&tag=Counties"
    assert response_matches_county(api_url, "Miami-Dade") is True


def test_response_matches_county_encoded_space():
    api_url = "https://caresapi.myflfamilies.com/api/publicSearch/Search?searchText=Palm%20Beach&tag=Counties"
    assert response_matches_county(api_url, "Palm Beach") is True


def test_response_matches_county_case_insensitive():
    api_url = "https://caresapi.myflfamilies.com/api/publicSearch/Search?searchText=miami-dade&tag=Counties"
    assert response_matches_county(api_url, "Miami-Dade") is True


def test_response_matches_county_wrong_county():
    api_url = "https://caresapi.myflfamilies.com/api/publicSearch/Search?searchText=Lake&tag=Counties"
    assert response_matches_county(api_url, "Liberty") is False


def test_response_matches_county_non_api_url():
    # Resource on the SPA's domain — not the API call we care about
    assert (
        response_matches_county(
            "https://caressearch.myflfamilies.com/PublicSearch/Search?term=Lake",
            "Lake",
        )
        is False
    )


def test_response_matches_county_missing_searchtext():
    # Some unrelated call to the same path without a searchText param
    assert (
        response_matches_county(
            "https://caresapi.myflfamilies.com/api/publicSearch/Search?tag=Counties",
            "Lake",
        )
        is False
    )


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
    assert item["fl_vpk_composite_score"] == 5


def test_parse_provider_sutq_rating_none_when_missing(spider):
    item = spider.parse_provider(FL_PROVIDER_FULL, "Miami-Dade")
    assert item["fl_vpk_composite_score"] is None


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
    assert item["fl_vpk_classrooms"] == [{"classType": "540", "class": [{"classRoomCode": "AF24"}]}]
    assert item["fl_vpk_curriculum"] == [{"name": "The Creative Curriculum for Preschool (3 to K)"}]
    assert item["fl_vpk_instructor_credentials"] == [{"name": "ClassAF24: Received a B.S. or a B.A"}]


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


# ---- bug_12: cause B -- duplicate inspection reports within one provider ----


def test_dedupe_inspection_reports_drops_exact_repeat_id():
    reports = [
        {"inspectionDate": "2024-11-20T00:00:00", "id": "dup-1", "hasViolation": False},
        {"inspectionDate": "2024-11-20T00:00:00", "id": "dup-1", "hasViolation": False},
    ]
    kept, dropped = dedupe_inspection_reports(reports)
    assert dropped == 1
    assert len(kept) == 1
    assert kept[0]["id"] == "dup-1"


def test_dedupe_inspection_reports_keeps_different_ids_same_date():
    reports = [
        {"inspectionDate": "2024-05-17T00:00:00", "id": "a", "hasViolation": False},
        {"inspectionDate": "2024-05-17T00:00:00", "id": "b", "hasViolation": True},
    ]
    kept, dropped = dedupe_inspection_reports(reports)
    assert dropped == 0
    assert {r["id"] for r in kept} == {"a", "b"}


def test_dedupe_inspection_reports_never_collapses_missing_ids():
    # Reports with no id must each be kept -- None is not a real identity.
    reports = [
        {"inspectionDate": "2024-01-01T00:00:00", "id": None, "hasViolation": False},
        {"inspectionDate": "2024-02-02T00:00:00", "id": None, "hasViolation": False},
        {"inspectionDate": "2024-03-03T00:00:00", "id": None, "hasViolation": True},
    ]
    kept, dropped = dedupe_inspection_reports(reports)
    assert dropped == 0
    assert len(kept) == 3


def test_parse_inspections_integration_dedupes_and_counts(spider):
    year_buckets = [
        {
            "year": 2024,
            "inspectionReport": [
                {"inspectionDate": "2024-11-20T00:00:00", "id": "rep-1", "hasViolation": False},
                {"inspectionDate": "2024-11-20T00:00:00", "id": "rep-1", "hasViolation": False},
                {"inspectionDate": "2024-12-01T00:00:00", "id": "rep-2", "hasViolation": True},
            ],
        }
    ]
    inspections = spider.parse_inspections(year_buckets)
    assert len(inspections) == 2
    assert {i["fl_inspection_id"] for i in inspections} == {"rep-1", "rep-2"}
    assert spider._inspection_dupes_dropped == 1


# ---- bug_12: cause A -- same license returned under more than one county ----


def _raw_record(license_number, address, city="", provider_name="", dba="", county=None):
    record = {
        "licenseNumber": license_number,
        "fullAddress": address,
        "city": city,
        "providerName": provider_name,
        "dba": dba,
    }
    if county is not None:
        record["county"] = county
    return record


def test_choose_surviving_county_tier1_record_owns_county():
    # The record's own `county` field wins outright, even though it isn't
    # one of the search candidates at all.
    record = _raw_record("C11MD0431", "22940 Old Dixie Hwy, Miami FL, 33170", county="Miami-Dade")
    county, tier = choose_surviving_county(["Dixie", "Orange"], record)
    assert (county, tier) == ("Miami-Dade", 1)


def test_choose_surviving_county_tier2_license_code_wins_regardless_of_order():
    # C11MD... -> district 11, code MD -> Miami-Dade, per
    # FL_LICENSE_DISTRICT_COUNTY_CODES. No `county` field on the record, so
    # it must fall through to the code tier.
    record = _raw_record(
        "C11MD0431",
        "22940 Old Dixie Hwy, Miami FL, 33170",
        city="Miami",
        provider_name="Children's Rainbow Day School",
    )
    county, tier = choose_surviving_county(["Dixie", "Miami-Dade"], record, "C11MD0431")
    assert (county, tier) == ("Miami-Dade", 2)

    # Order of the candidates must not matter.
    county, tier = choose_surviving_county(["Miami-Dade", "Dixie"], record, "C11MD0431")
    assert (county, tier) == ("Miami-Dade", 2)


def test_choose_surviving_county_tier3_substring_fallback():
    # License number doesn't match the known-code pattern, so tier 2 can't
    # resolve it. "Pinellas" doesn't appear anywhere in the address/city/
    # name text, but "Martin" does (via "Dr Martin Luther King Jr St") --
    # that's the search's own false-positive signature, so the
    # non-matching candidate is the real one.
    record = _raw_record(
        "F99ZZ9999",
        "5025 Dr Martin Luther King Jr St N, St Petersburg FL, 33703",
        city="St Petersburg",
        provider_name="Baker, Tamsen",
    )
    county, tier = choose_surviving_county(["Pinellas", "Martin"], record, "F99ZZ9999")
    assert (county, tier) == ("Pinellas", 3)


def test_choose_surviving_county_tier4_first_candidate_when_unresolvable():
    # Both candidates' names appear in the text and the license code is
    # unknown -- nothing can resolve it, so the first-seen candidate wins
    # (and callers log this at debug).
    record = _raw_record(
        "F99ZZ9999",
        "5025 Dr Martin Luther King Jr St N, St Petersburg FL, 33703",
        city="St Petersburg",
        provider_name="Baker, Tamsen",
    )
    county, tier = choose_surviving_county(["Baker", "Martin"], record, "F99ZZ9999")
    assert (county, tier) == ("Baker", 4)


def test_dedupe_providers_collapses_same_license_two_counties():
    record = _raw_record("C11MD0431", "22940 Old Dixie Hwy, Miami FL, 33170", provider_name="Children's Rainbow")
    buffered = [("Dixie", record), ("Miami-Dade", record)]
    resolved, tier_counts = dedupe_providers(buffered)
    assert len(resolved) == 1
    assert resolved[0][0] == "Miami-Dade"
    assert tier_counts[2] == 1


def test_dedupe_providers_keeps_unique_licenses_untouched():
    record_a = _raw_record("C01ES0001", "100 Main St, Pensacola FL, 32501")
    record_b = _raw_record("C02LE0002", "200 Main St, Tallahassee FL, 32301")
    buffered = [("Escambia", record_a), ("Leon", record_b)]
    resolved, tier_counts = dedupe_providers(buffered)
    assert len(resolved) == 2
    assert sum(tier_counts.values()) == 0  # nothing needed a tie-break


def test_dedupe_providers_integration_yields_one_item_per_license(spider):
    record = _raw_record(
        "C11MD0431",
        "22940 Old Dixie Hwy, Miami FL, 33170",
        city="Miami",
        provider_name="Children's Rainbow Day School",
    )
    buffered = [("Dixie", record), ("Miami-Dade", record)]
    resolved, _tier_counts = dedupe_providers(buffered)
    assert len(resolved) == 1

    county, raw_record = resolved[0]
    item = spider.parse_provider(raw_record, county)
    assert item["county"] == "Miami-Dade"
    assert item["license_number"] == "C11MD0431"


def test_parse_provider_prefers_record_county_over_search_county(spider):
    # bug_12: a singleton found only by a false-positive search ("Dixie" via
    # "Old Dixie Hwy") must still carry the record's own county.
    item = spider.parse_provider(FL_PROVIDER_FULL, "Dixie")
    assert item["county"] == "Miami-Dade"


def test_parse_provider_falls_back_to_search_county_without_record_county(spider):
    record = {k: v for k, v in FL_PROVIDER_FULL.items() if k != "county"}
    item = spider.parse_provider(record, "Miami-Dade")
    assert item["county"] == "Miami-Dade"

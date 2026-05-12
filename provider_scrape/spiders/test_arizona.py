import json

import pytest

from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.spiders.arizona import (
    ArizonaSpider,
    build_search_message,
    build_search_post_body,
    extract_form_field,
)


@pytest.fixture
def spider():
    return ArizonaSpider()


# ---- Sample Data ----

AZ_PROVIDER_RECORD = {
    "title": "Arcadia Montessori School",
    "value": "0bScs000000PlWnEAK",
    "providertype": "Child Care Center",
    "slotcapacity": 171,
    "AgeServed": "3 Years To 12 Years",
    "licensetype": "Center - Licensed by AZ Dept. of Health Services",
    "owner": "John Doe",
    "rating": None,
    "languages": "English;Spanish",
    "phone": "602-840-2342",
    "website": "https://www.arcadiamontessori.com",
    "address": "2257 E Cedar Ave\nFlagstaff, AZ, 86004",
    "location": {
        "Latitude": "33.475900000000000",
        "Longitude": "-111.971100000000000"
    },
    "operatinghourid": "0OHcs000002qQrhGAE",
    "affiliation": None,
    "regionalpartnership": "South Phoenix",
    "shiftcomment": "Application fee: $200 applied to your registration fee;",
    "headstart": False,
    "desprovider": False,
    "statusLabel": "Open Spots",
    "firstSlotStart": "07:30:00.000Z",
    "firstSlotEnd": "14:50:00.000Z",
    "dhsenforcements": [
        {
            "InspectionDate__c": "2025-02-12",
            "InspectionType__c": "Compliance (Annual)",
            "Regulation__c": "AAC R9-5-502.A.9.",
            "Decision_Correction__c": "Findings: Menu lacking components",
            "DateResolved__c": "2025-02-14",
            "CIVIL_PENALTY__c": None,
            "Name": "D-424195"
        },
        {
            "InspectionDate__c": "2021-10-28",
            "InspectionType__c": "Enforcement",
            "Regulation__c": None,
            "Decision_Correction__c": "LICENSEE AGREES TO PAY...",
            "DateResolved__c": None,
            "CIVIL_PENALTY__c": 100.0,
            "Name": "D-436770"
        }
    ]
}


# ---- extract_form_field ----

def test_extract_form_field_success():
    post_data = "message=xyz&aura.context=the_context_blob&aura.token=null"
    assert extract_form_field(post_data, "aura.context") == "the_context_blob"

def test_extract_form_field_url_decodes():
    post_data = "aura.context=some%20encoded%2Bstring"
    assert extract_form_field(post_data, "aura.context") == "some encoded+string"

def test_extract_form_field_missing():
    post_data = "message=xyz&aura.token=null"
    assert extract_form_field(post_data, "aura.context") is None

def test_extract_form_field_empty():
    assert extract_form_field("", "aura.context") is None
    assert extract_form_field(None, "aura.context") is None


# ---- build_search_message / build_search_post_body ----

def test_build_search_message_structure():
    msg_str = build_search_message(page_size=50, page_number=2)
    msg = json.loads(msg_str)
    
    assert "actions" in msg
    action = msg["actions"][0]
    assert action["id"] == "2;a"
    assert action["params"]["classname"] == "PVM_ProviderSearchControllerMain"
    assert action["params"]["method"] == "getProvidersfromZip"
    
    params = action["params"]["params"]
    assert params["pageSize"] == 50
    assert params["pageNumber"] == 2

def test_build_search_post_body():
    body = build_search_post_body(page_size=50, page_number=2, aura_context="ctx_blob")
    
    # Check that required urlencoded parts are present
    assert "aura.context=ctx_blob" in body
    assert "aura.token=null" in body
    assert "message=" in body
    assert "aura.pageURI=" in body


# ---- parse_provider ----

def test_parse_provider_full_record(spider):
    item = spider.parse_provider(AZ_PROVIDER_RECORD)
    
    assert isinstance(item, ProviderItem)
    assert item["source_state"] == "Arizona"
    assert item["provider_name"] == "Arcadia Montessori School"
    assert item["az_facility_id"] == "0bScs000000PlWnEAK"
    assert item["provider_type"] == "Child Care Center"
    assert item["capacity"] == 171
    assert item["ages_served"] == "3 Years To 12 Years"
    assert item["az_license_type"] == "Center - Licensed by AZ Dept. of Health Services"
    assert item["license_holder"] == "John Doe"
    assert item["sutq_rating"] is None
    assert item["languages"] == "English;Spanish"
    assert item["phone"] == "602-840-2342"
    assert item["provider_website"] == "https://www.arcadiamontessori.com"
    assert item["address"] == "2257 E Cedar Ave\nFlagstaff, AZ, 86004"
    assert item["latitude"] == "33.475900000000000"
    assert item["longitude"] == "-111.971100000000000"
    
    # AZ specific fields
    assert item["az_operatinghourid"] == "0OHcs000002qQrhGAE"
    assert item["az_affiliation"] is None
    assert item["az_regionalpartnership"] == "South Phoenix"
    assert item["az_shiftcomment"] == "Application fee: $200 applied to your registration fee;"
    assert item["az_headstart"] is False
    assert item["az_desprovider"] is False
    assert item["az_status_label"] == "Open Spots"
    assert item["az_first_slot_start"] == "07:30:00.000Z"
    assert item["az_first_slot_end"] == "14:50:00.000Z"
    
    # Inspections
    assert len(item["inspections"]) == 2

def test_parse_provider_empty_owner_to_none(spider):
    record = dict(AZ_PROVIDER_RECORD)
    record["owner"] = "   "
    item = spider.parse_provider(record)
    assert item["license_holder"] is None

def test_parse_provider_rating_to_string(spider):
    record = dict(AZ_PROVIDER_RECORD)
    record["rating"] = 4.5
    item = spider.parse_provider(record)
    assert item["sutq_rating"] == "4.5"


# ---- parse_inspections ----

def test_parse_inspections(spider):
    inspections = spider.parse_inspections(AZ_PROVIDER_RECORD["dhsenforcements"])
    
    assert len(inspections) == 2
    
    first = inspections[0]
    assert isinstance(first, InspectionItem)
    assert first["date"] == "2025-02-12"
    assert first["type"] == "Compliance (Annual)"
    assert first["az_regulation"] == "AAC R9-5-502.A.9."
    assert first["az_decision_correction"] == "Findings: Menu lacking components"
    assert first["az_date_resolved"] == "2025-02-14"
    assert first["az_civil_penalty"] is None
    assert first["az_enforcement_name"] == "D-424195"

    second = inspections[1]
    assert second["date"] == "2021-10-28"
    assert second["type"] == "Enforcement"
    assert second["az_regulation"] is None
    assert second["az_civil_penalty"] == 100.0

def test_parse_inspections_empty(spider):
    assert spider.parse_inspections([]) == []

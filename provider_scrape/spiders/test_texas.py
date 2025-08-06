
import pytest
import json
import base64
from scrapy.http import Response, Request
from provider_scrape.spiders.texas import TxhhsSpider
from provider_scrape.items import ProviderItem

# Happy path CSV content
HAPPY_PATH_CSV = """
"Operation #","Agency Number","Operation/Caregiver Name","Address","City","State","Zip","County","Phone","Type","Status","Issue Date","Capacity","Email Address","Infant","Toddler","Preschool","School","Hours","Texas Rising Star ","Accepts ChildCare Scholarships","Deficiencies"
"12345","67890","Test Provider","123 Main St","Anytown","TX","12345","Test County","(555) 555-5555","Licensed Center","Active","2023-01-01","50","test@example.com","Yes","Yes","Yes","No","Mon-Fri 8am-5pm","4 Star","Yes","0"
""".strip()

# CSV content with missing fields
MISSING_FIELDS_CSV = """
"Operation #","Agency Number","Operation/Caregiver Name","Address","City","State","Zip","County","Phone","Type","Status","Issue Date","Capacity","Email Address","Infant","Toddler","Preschool","School","Hours","Texas Rising Star ","Accepts ChildCare Scholarships","Deficiencies"
"54321","","Missing Info Provider","","","","","","","","","","","","","","","","","","",""
""".strip()

# Empty CSV content
EMPTY_CSV = '"Operation #","Agency Number","Operation/Caregiver Name","Address","City","State","Zip","County","Phone","Type","Status","Issue Date","Capacity","Email Address","Infant","Toddler","Preschool","School","Hours","Texas Rising Star ","Accepts ChildCare Scholarships","Deficiencies"'

def create_csv_response(csv_data, url='http://example.com'):
    """Helper function to create a Scrapy response containing a base64 encoded CSV."""
    csv_bytes = csv_data.encode('utf-8')
    encoded_csv = base64.b64encode(csv_bytes).decode('utf-8')
    json_payload = json.dumps({"fileBytes": encoded_csv})
    return Response(url=url, body=json_payload.encode('utf-8'))

@pytest.fixture
def spider():
    return TxhhsSpider()

def test_parse_csv_happy_path(spider):
    response = create_csv_response(HAPPY_PATH_CSV)
    items = list(spider.parse_csv(response))

    assert len(items) == 1
    provider = items[0]

    assert isinstance(provider, ProviderItem)
    assert provider['provider_url'] == 'https://childcare.hhs.texas.gov/Public/OperationDetails?operationId=12345'
    assert provider['tx_operation_id'] == '12345'
    assert provider['tx_agency_number'] == '67890'
    assert provider['provider_name'] == 'Test Provider'
    assert provider['address'] == '123 Main St Anytown, TX 12345'
    assert provider['county'] == 'Test County'
    assert provider['phone'] == '(555) 555-5555'
    assert provider['provider_type'] == 'Licensed Center'
    assert provider['status'] == 'Active'
    assert provider['status_date'] == '2023-01-01'
    assert provider['capacity'] == '50'
    assert provider['email'] == 'test@example.com'
    assert provider['infant'] == 'Yes'
    assert provider['toddler'] == 'Yes'
    assert provider['preschool'] == 'Yes'
    assert provider['school'] == 'No'
    assert provider['hours'] == 'Mon-Fri 8am-5pm'
    assert provider['tx_rising_star'] == '4 Star'
    assert provider['scholarships_accepted'] == 'Yes'
    assert provider['deficiencies'] == '0'

def test_parse_csv_missing_fields(spider):
    response = create_csv_response(MISSING_FIELDS_CSV)
    items = list(spider.parse_csv(response))

    assert len(items) == 1
    provider = items[0]

    assert isinstance(provider, ProviderItem)
    assert provider['provider_url'] == 'https://childcare.hhs.texas.gov/Public/OperationDetails?operationId=54321'
    assert provider['tx_operation_id'] == '54321'
    assert provider['tx_agency_number'] == ''
    assert provider['provider_name'] == 'Missing Info Provider'
    assert provider['address'] == ' ,  '
    assert provider['county'] == ''
    assert provider['phone'] == ''
    assert provider['provider_type'] == ''
    assert provider['status'] == ''
    assert provider['status_date'] == ''
    assert provider['capacity'] == ''
    assert provider['email'] == ''
    assert provider['infant'] == ''
    assert provider['toddler'] == ''
    assert provider['preschool'] == ''
    assert provider['school'] == ''
    assert provider['hours'] == ''
    assert provider['tx_rising_star'] == ''
    assert provider['scholarships_accepted'] == ''
    assert provider['deficiencies'] == ''

def test_parse_csv_empty(spider):
    response = create_csv_response(EMPTY_CSV)
    items = list(spider.parse_csv(response))
    assert len(items) == 0

def test_parse_csv_invalid_json(spider):
    response = Response(url='http://example.com', body=b'{"fileBytes": "not-base64}')
    items = list(spider.parse_csv(response))
    assert len(items) == 0

def test_parse_csv_no_filebytes(spider):
    response = Response(url='http://example.com', body=b'{"other_key": "value"}')
    items = list(spider.parse_csv(response))
    assert len(items) == 0

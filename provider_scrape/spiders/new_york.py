import scrapy
import csv
from io import StringIO
from provider_scrape.items import ProviderItem

class NewYorkSpider(scrapy.Spider):
    name = 'new_york'
    allowed_domains = ['data.ny.gov']

    start_url = 'https://data.ny.gov/api/v3/views/cb42-qumz/export.csv?cacheBust=1768022137&accessType=DOWNLOAD'

    def start_requests(self):
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:146.0) Gecko/20100101 Firefox/146.0',
            'Accept': 'text/csv',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Referer': 'https://data.ny.gov/Human-Services/Child-Care-Regulated-Programs/cb42-qumz/about_data',
            'Content-Type': 'application/json',
            'X-App-Token': 'U29jcmF0YS0td2VraWNrYXNz0',
            'Origin': 'https://data.ny.gov',
            'Connection': 'keep-alive',
        }

        body = '{"serializationOptions":{"defaultGroupSeparator":",","defaultDecimalSeparator":"."}}'

        yield scrapy.Request(
            url=self.start_url,
            method='POST',
            headers=headers,
            body=body,
            callback=self.parse
        )

    def parse(self, response):
        csv_data = StringIO(response.text)
        reader = csv.DictReader(csv_data)

        for row in reader:
            item = ProviderItem()

            item['source_state'] = 'NY'
            item['ny_facility_id'] = row.get('Facility ID')
            item['provider_type'] = row.get('Program Type')
            item['ny_region_code'] = row.get('Region Code')
            item['county'] = row.get('County')
            item['status'] = row.get('Facility Status')
            item['provider_name'] = row.get('Facility Name')
            item['ny_facility_opened_date'] = row.get('Facility Opened Date')
            item['license_begin_date'] = row.get('License Issue Date')
            item['license_expiration'] = row.get('License Expiration Date')
            item['ny_address_omitted'] = row.get('Address Omitted')

            # Construct Address
            street_number = row.get('Street Number', '').strip()
            street_name = row.get('Street Name', '').strip()
            additional_address = row.get('Additional Address', '').strip()
            floor = row.get('Floor', '').strip()
            apartment = row.get('Apartment', '').strip()
            city = row.get('City', '').strip()
            state = row.get('State', '').strip()
            zip_code = row.get('Zip Code', '').strip()

            address_parts = [part for part in [street_number, street_name, additional_address, floor, apartment] if part]
            street_address = " ".join(address_parts)

            full_address_parts = [part for part in [street_address, city, state, zip_code] if part]
            item['address'] = ", ".join(full_address_parts)

            item['ny_phone_number_omitted'] = row.get('Phone Number Omitted')
            item['phone'] = row.get('Phone Number')
            item['ny_phone_extension'] = row.get('Phone Extension')
            item['license_holder'] = row.get('Provider Name')
            item['ny_school_district_name'] = row.get('School District Name')
            item['ny_capacity_description'] = row.get('Capacity Description')
            item['infant'] = row.get('Infant Capacity')
            item['toddler'] = row.get('Toddler Capacity')
            item['preschool'] = row.get('Preschool Capacity')
            item['school'] = row.get('School Age Capacity')
            item['capacity'] = row.get('Total Capacity')
            item['provider_url'] = row.get('Program Profile')
            item['latitude'] = row.get('Latitude')
            item['longitude'] = row.get('Longitude')

            yield item

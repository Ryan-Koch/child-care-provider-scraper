import unittest
from scrapy.http import Request
from provider_scrape.spiders.alaska import AlaskaSpider
from provider_scrape.items import ProviderItem, InspectionItem

class AlaskaSpiderTest(unittest.TestCase):
    def setUp(self):
        self.spider = AlaskaSpider()

    def test_extract_detail(self):
        # Mock HTML content based on discovered Blazor structure
        html = """
        <html>
        <body>
            <div id="app">
                <div class="mud-paper">
                    <h1>Provider Info</h1>
                    <p>Facility Name: SUNSHINE DAYCARE</p>
                    <p>First Name: JANE</p>
                    <p>Last Name: DOE</p>
                    <p>Provider License Effective Date: 01/01/2023</p>
                    <p>Provider License Expiration Date: 12/31/2025</p>
                    <p>Capacity: 30</p>
                    <p>Children Age Range: 0 Weeks - 12 Years</p>
                    <p>Phone Number: (907) 555-0100</p>
                    <p>Facility Status: Active</p>
                    <p>Address: 123 Glacier Hwy, Juneau, AK 99801</p>
                </div>
                <div class="inspections">
                    <h2>Compliance Events Found: 2</h2>
                    <p>Date of Event Compliance Type Findings Action Taken</p>
                    <p>01/01/2023 INSPECTION ROUTINE IN-COMPLIANCE NONE NEEDED Details</p>
                    <p>05/05/2023 COMPLAINT SUBSTANTIATED PLAN OF CORRECTION Details</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        url = 'https://findccprovider.health.alaska.gov/ProviderInfo/12345'
        item = self.spider.extract_detail(html, url)
        
        self.assertIsInstance(item, ProviderItem)
        self.assertEqual(item['provider_name'], 'SUNSHINE DAYCARE')
        self.assertEqual(item['license_number'], '12345') # From URL fallback
        self.assertEqual(item['status'], 'Active')
        self.assertEqual(item['phone'], '(907) 555-0100')
        self.assertEqual(item['capacity'], '30')
        self.assertEqual(item['address'], '123 Glacier Hwy, Juneau, AK 99801')
        self.assertEqual(item['administrator'], 'JANE DOE')
        self.assertEqual(item['status_date'], '01/01/2023')
        self.assertEqual(item['license_expiration'], '12/31/2025')
        self.assertEqual(item['ages_served'], '0 Weeks - 12 Years')
        
        # Inspections
        self.assertEqual(len(item['inspections']), 2)
        self.assertIsInstance(item['inspections'][0], InspectionItem)
        self.assertEqual(item['inspections'][0]['date'], '01/01/2023')
        self.assertEqual(item['inspections'][0]['type'], 'INSPECTION ROUTINE')
        self.assertEqual(item['inspections'][0]['original_status'], 'IN-COMPLIANCE')
        self.assertEqual(item['inspections'][0]['corrective_status'], 'NONE NEEDED')
        
        self.assertEqual(item['inspections'][1]['date'], '05/05/2023')
        self.assertEqual(item['inspections'][1]['type'], 'COMPLAINT')
        self.assertEqual(item['inspections'][1]['original_status'], 'SUBSTANTIATED')
        self.assertEqual(item['inspections'][1]['corrective_status'], 'PLAN OF CORRECTION')

    def test_start_requests(self):
        requests = list(self.spider.start_requests())
        self.assertEqual(len(requests), 1)
        self.assertTrue(requests[0].meta['playwright'])

if __name__ == '__main__':
    unittest.main()

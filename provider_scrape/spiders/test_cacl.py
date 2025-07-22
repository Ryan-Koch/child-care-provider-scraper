import unittest
from scrapy.http import TextResponse
from provider_scrape.spiders.cacl import CaclSpider
from provider_scrape.items import ProviderItem

class CaclSpiderTest(unittest.TestCase):
    def setUp(self):
        self.spider = CaclSpider()

    def test_happy_path(self):
        # Mock CSV data for the happy path scenario
        csv_data = """"Facility Type","Facility Number","Facility Name","Licensee","Facility Administrator","Facility Telephone Number","Facility Address","Facility City","Facility State","Facility Zip","County Name","Regional Office","Facility Capacity","Facility Status","License First Date","Closed Date","Citation Numbers","All Visit Dates","Inspection Visit Dates","Other Visit Dates","Complaint Info- Date, #Sub Aleg, # Inc Aleg, # Uns Aleg, # TypeA, # TypeB ...","Inspect TypeA","Inspect TypeB","Other TypeA","Other TypeB"
"CHILD CARE CENTER","123456789","HAPPY KIDS CENTER","HAPPY KIDS INC","JOHN DOE","(555) 123-4567","123 SUNNY ST","SUNNYVALE","CA","94086","SANTA CLARA","BAY AREA OFFICE","50","LICENSED","2020-01-01","","","2023-05-10","2023-05-10","","","0","0","0","0"
"CHILD CARE CENTER","987654321","PLAYFUL LEARNERS","PLAYFUL LEARNERS LLC","JANE SMITH","(555) 765-4321","456 OAK AVE","MOUNTAIN VIEW","CA","94043","SANTA CLARA","BAY AREA OFFICE","30","LICENSED","2019-03-15","","","2023-06-20","2023-06-20","","","0","0","0","0"
"""
        # Create a mock Scrapy response
        response = TextResponse(url='http://www.example.com/test.csv',
                                body=csv_data,
                                encoding='utf-8')

        # Parse the response
        results = list(self.spider.parse(response))

        # Assertions
        self.assertEqual(len(results), 2)

        # Check first provider
        provider1 = results[0]
        self.assertIsInstance(provider1, ProviderItem)
        self.assertEqual(provider1['provider_type'], 'CHILD CARE CENTER')
        self.assertEqual(provider1['license_number'], '123456789')
        self.assertEqual(provider1['provider_name'], 'HAPPY KIDS CENTER')
        self.assertEqual(provider1['license_holder'], 'HAPPY KIDS INC')
        self.assertEqual(provider1['administrator'], 'JOHN DOE')
        self.assertEqual(provider1['phone'], '(555) 123-4567')
        self.assertEqual(provider1['address'], '123 SUNNY ST, SUNNYVALE, CA 94086')
        self.assertEqual(provider1['county'], 'SANTA CLARA')
        self.assertEqual(provider1['ca_regional_office'], 'BAY AREA OFFICE')
        self.assertEqual(provider1['capacity'], '50')
        self.assertEqual(provider1['status'], 'LICENSED')
        self.assertEqual(provider1['ca_license_first_date'], '2020-01-01')

        # Check second provider
        provider2 = results[1]
        self.assertIsInstance(provider2, ProviderItem)
        self.assertEqual(provider2['provider_name'], 'PLAYFUL LEARNERS')
        self.assertEqual(provider2['address'], '456 OAK AVE, MOUNTAIN VIEW, CA 94043')


    def test_missing_data(self):
        # Mock CSV data with missing values
        csv_data = """"Facility Type","Facility Number","Facility Name","Licensee","Facility Administrator","Facility Telephone Number","Facility Address","Facility City","Facility State","Facility Zip","County Name","Regional Office","Facility Capacity","Facility Status","License First Date","Closed Date","Citation Numbers","All Visit Dates","Inspection Visit Dates","Other Visit Dates","Complaint Info- Date, #Sub Aleg, # Inc Aleg, # Uns Aleg, # TypeA, # TypeB ...","Inspect TypeA","Inspect TypeB","Other TypeA","Other TypeB"
"CHILD CARE CENTER","111222333","KID ZONE","","","(555) 111-2222","","SUNNYVALE","CA","94086","SANTA CLARA","BAY AREA OFFICE","","LICENSED","2021-02-01","","","","","","","0","0","0","0"
"CHILD CARE CENTER","444555666","","LEARNING TREE LLC","","","456 MAPLE AVE","","CA","94043","SANTA CLARA","BAY AREA OFFICE","25","","2018-07-20","","","","","","","0","0","0","0"
"""
        # Create a mock Scrapy response
        response = TextResponse(url='http://www.example.com/test.csv',
                                body=csv_data,
                                encoding='utf-8')

        # Parse the response
        results = list(self.spider.parse(response))

        # Assertions
        self.assertEqual(len(results), 2)

        # Check first provider for missing data
        provider1 = results[0]
        self.assertIsInstance(provider1, ProviderItem)
        self.assertEqual(provider1['license_holder'], '')
        self.assertEqual(provider1['administrator'], '')
        self.assertEqual(provider1['address'], ', SUNNYVALE, CA 94086') # Address is constructed, so check for partial
        self.assertEqual(provider1['capacity'], '')

        # Check second provider for missing data
        provider2 = results[1]
        self.assertIsInstance(provider2, ProviderItem)
        self.assertEqual(provider2['provider_name'], '')
        self.assertEqual(provider2['phone'], '')
        self.assertEqual(provider2['address'], '456 MAPLE AVE, , CA 94043') # Address is constructed
        self.assertEqual(provider2['status'], '')

if __name__ == '__main__':
    unittest.main()
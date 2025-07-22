import unittest
from scrapy.http import TextResponse
from scrapy.utils.test import get_crawler
from provider_scrape.spiders.cacl import CaclSpider

class CaclSpiderTest(unittest.TestCase):
    def setUp(self):
        self.spider = CaclSpider()

    def test_happy_path(self):
        # Mock CSV data for the happy path scenario
        csv_data = """FacilityNumber,FacilityName,FacilityAddress,FacilityCity,FacilityState,FacilityZipCode
123,Test Facility 1,123 Main St,Anytown,CA,12345
456,Test Facility 2,456 Oak Ave,Someville,CA,67890
"""
        # Create a mock Scrapy response
        response = TextResponse(url='http://www.example.com/test.csv',
                                body=csv_data,
                                encoding='utf-8')

        # Parse the response
        results = list(self.spider.parse(response))

        # Assertions
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['FacilityNumber'], '123')
        self.assertEqual(results[0]['FacilityName'], 'Test Facility 1')
        self.assertEqual(results[1]['FacilityZipCode'], '67890')

    def test_missing_data(self):
        # Mock CSV data with missing values
        csv_data = """FacilityNumber,FacilityName,FacilityAddress,FacilityCity,FacilityState,FacilityZipCode
123,Test Facility 1,,Anytown,CA,12345
456,,456 Oak Ave,Someville,,67890
"""
        # Create a mock Scrapy response
        response = TextResponse(url='http://www.example.com/test.csv',
                                body=csv_data,
                                encoding='utf-8')

        # Parse the response
        results = list(self.spider.parse(response))

        # Assertions
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['FacilityAddress'], '')
        self.assertEqual(results[1]['FacilityName'], '')
        self.assertEqual(results[1]['FacilityState'], '')

if __name__ == '__main__':
    unittest.main()

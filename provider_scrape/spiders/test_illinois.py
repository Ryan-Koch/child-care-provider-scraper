import unittest
from scrapy.http import TextResponse, Headers
from provider_scrape.spiders.illinois import IllinoisSpider
from provider_scrape.items import ProviderItem

class IllinoisSpiderTest(unittest.TestCase):
    def setUp(self):
        self.spider = IllinoisSpider()

    def test_happy_path(self):
        # Mock CSV data for the happy path scenario
        csv_data = """ProviderID,DoingBusinessAs,Street,City,Zip,County,Phone,FacilityType,DayAgeRange,NightAgeRange,DayCapacity,NightCapacity,Status,Language1,Language2,Language3
12345,"HAPPY KIDS DAYCARE","123 Main St","Springfield","62704","Sangamon","(217) 555-1234","Day Care Center","0-5","","60","","Licensed","English","Spanish",""
67890,"SUNNYSIDE LEARNING","456 Oak Ave","Chicago","60611","Cook","(312) 555-5678","Day Care Home","1-6","1-6","12","12","Licensed","English","",""
"""
        # Create a mock Scrapy response
        headers = Headers({'Content-Disposition': 'attachment; filename="DayCareProviderList.csv"'})
        response = TextResponse(url='http://www.example.com/test.csv',
                                headers=headers,
                                body=csv_data,
                                encoding='utf-8')

        # Parse the response
        results = list(self.spider.parse_csv(response))

        # Assertions
        self.assertEqual(len(results), 2)

        # Check first provider
        provider1 = results[0]
        self.assertIsInstance(provider1, ProviderItem)
        self.assertEqual(provider1['il_provider_id'], '12345')
        self.assertEqual(provider1['provider_name'], 'HAPPY KIDS DAYCARE')
        self.assertEqual(provider1['address'], '123 Main St, Springfield, CA 62704')
        self.assertEqual(provider1['county'], 'Sangamon')
        self.assertEqual(provider1['phone'], '(217) 555-1234')
        self.assertEqual(provider1['il_facility_type'], 'Day Care Center')
        self.assertEqual(provider1['il_day_age_range'], '0-5')
        self.assertEqual(provider1['il_night_age_range'], '')
        self.assertEqual(provider1['il_day_capacity'], '60')
        self.assertEqual(provider1['il_night_capacity'], '')
        self.assertEqual(provider1['status'], 'Licensed')
        self.assertEqual(provider1['languages'], ['English', 'Spanish'])

        # Check second provider
        provider2 = results[1]
        self.assertIsInstance(provider2, ProviderItem)
        self.assertEqual(provider2['provider_name'], 'SUNNYSIDE LEARNING')
        self.assertEqual(provider2['address'], '456 Oak Ave, Chicago, CA 60611')
        self.assertEqual(provider2['languages'], ['English'])


    def test_missing_data(self):
        # Mock CSV data with missing values
        csv_data = """ProviderID,DoingBusinessAs,Street,City,Zip,County,Phone,FacilityType,DayAgeRange,NightAgeRange,DayCapacity,NightCapacity,Status,Language1,Language2,Language3
98765,"","","Springfield","","Sangamon","","Day Care Center","","","","","","","",""
54321,"KIDZ KORNER","555 Elm St","Chicago","60620","Cook","(312) 555-0000","Day Care Home","0-5","","","","Pending","English","",""
"""
        # Create a mock Scrapy response
        headers = Headers({'Content-Disposition': 'attachment; filename="DayCareProviderList.csv"'})
        response = TextResponse(url='http://www.example.com/test.csv',
                                headers=headers,
                                body=csv_data,
                                encoding='utf-8')

        # Parse the response
        results = list(self.spider.parse_csv(response))

        # Assertions
        self.assertEqual(len(results), 2)

        # Check first provider for missing data
        provider1 = results[0]
        self.assertIsInstance(provider1, ProviderItem)
        self.assertEqual(provider1['provider_name'], '')
        self.assertEqual(provider1['address'], ', Springfield, CA ')
        self.assertEqual(provider1['phone'], '')
        self.assertEqual(provider1['status'], '')
        self.assertEqual(provider1['languages'], [])

        # Check second provider for missing data
        provider2 = results[1]
        self.assertIsInstance(provider2, ProviderItem)
        self.assertEqual(provider2['il_day_capacity'], '')
        self.assertEqual(provider2['il_night_capacity'], '')
        self.assertEqual(provider2['status'], 'Pending')


if __name__ == '__main__':
    unittest.main()
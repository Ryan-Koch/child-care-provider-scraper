import unittest
import json
from scrapy.http import TextResponse, Request
from provider_scrape.spiders.utah import UtahSpider
from provider_scrape.items import ProviderItem

class UtahSpiderTest(unittest.TestCase):
    def setUp(self):
        self.spider = UtahSpider()

    def test_start_requests(self):
        requests = list(self.spider.start_requests())
        # The current spider yields ALL zip codes initially
        self.assertEqual(len(requests), 343)
        for req in requests:
            self.assertIn("https://cac-api.jobs.utah.gov/program/v1/public/programs", req.url)
            self.assertEqual(req.method, 'POST')
            self.assertIn(req.meta.get('zip_code'), self.spider.UT_ZIP_CODES)

    def test_parse_search(self):
        # Mock search response
        search_json = {
            "content": [
                {
                    "programId": 12345,
                    "program": "Test Academy",
                    "zipCode": "84003",
                    "vacancy": 5,
                    "ageAccept": "Infants, Toddlers"
                }
            ],
            "number": 0,
            "totalPages": 1
        }
        
        request = Request(
            url="https://cac-api.jobs.utah.gov/program/v1/public/programs?page=0&size=20",
            meta={'zip_code': '84003', 'page': 0}
        )
        response = TextResponse(
            url=request.url,
            request=request,
            body=json.dumps(search_json),
            encoding='utf-8'
        )
        
        results = list(self.spider.parse_search(response))
        
        # Should yield 1 detail request
        self.assertEqual(len(results), 1)
        self.assertIn("program-details/programs/12345", results[0].url)
        self.assertEqual(results[0].meta['search_data']['programId'], 12345)

    def test_parse_detail(self):
        search_data = {
            "programId": 12345,
            "program": "Test Academy",
            "zipCode": "84003",
            "vacancy": 5,
            "ageAccept": "Infants, Toddlers",
            "school": "School A",
            "meals": "Lunch",
            "environment": "No Pets"
        }
        
        detail_json = {
            "programId": 12345,
            "name": "Test Academy Structured",
            "addressOne": "123 Main St",
            "city": "Lehi",
            "state": "Utah",
            "zipCode": "84003",
            "phone": "8015551234",
            "licenseType": "Child Care Center",
            "qrl": "High Quality",
            "licenseStartDate": "2023-08-11T00:00:00",
            "totalChildren": 50,
            "vacancies": 5,
            "acpDwsSub": "Y"
        }
        
        request = Request(
            url="https://cac-api.jobs.utah.gov/program/v1/public/program-details/programs/12345",
            meta={'search_data': search_data}
        )
        response = TextResponse(
            url=request.url,
            request=request,
            body=json.dumps(detail_json),
            encoding='utf-8'
        )
        
        results = list(self.spider.parse_detail(response))
        
        self.assertEqual(len(results), 1)
        item = results[0]
        self.assertIsInstance(item, ProviderItem)
        self.assertEqual(item['provider_name'], "Test Academy Structured")
        self.assertEqual(item['address'], "123 Main St Lehi Utah 84003")
        self.assertEqual(item['phone'], "(801) 555-1234")
        self.assertEqual(item['ut_license_type'], "Child Care Center")
        self.assertEqual(item['capacity'], 50)
        self.assertEqual(item['scholarships_accepted'], "Yes")
        self.assertEqual(item['ages_served'], "Infants, Toddlers")
        self.assertEqual(item['ut_school_district'], "School A")

if __name__ == '__main__':
    unittest.main()
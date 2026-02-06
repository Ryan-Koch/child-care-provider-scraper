import unittest
from scrapy.http import TextResponse
from scrapy.selector import Selector
from provider_scrape.spiders.utah import UtahSpider
from provider_scrape.items import ProviderItem

class UtahSpiderTest(unittest.TestCase):
    def setUp(self):
        self.spider = UtahSpider()

    def test_parse_provider_data(self):
        # Mock HTML based on utah_detail_page.html structure
        html = """
        <html>
        <body>
            <cac-public-search-root>
                <main>
                    <section class="program-details">
                        <div class="card-body">
                            <h2 class="fw-normal fs-custom-1-58 mb-2">A Brighter Tomorrow</h2>
                            
                            <p data-testid="program-detail-qrl-license-type"> 
                                Quality Rating: Default Foundation of Quality <br> 
                                License Type: Child Care Center 
                            </p>
                            
                            <div class="contact-info">
                                <a href="tel:8013766767"> (801) 376-6767 </a>
                                <ul>
                                    <li data-testid="program-detail-address">
                                        <a href="..."> 50 N 200 E <br> Lehi, Utah 84043 </a>
                                    </li>
                                </ul>
                            </div>
                            
                            <div class="capacity-info">
                                <ul>
                                    <li data-testid="program-detail-capacity"> Total Capacity: <strong> 75 </strong></li>
                                    <li data-testid="program-detail-vacancies"> Current Vacancies: <strong> 14 </strong></li>
                                </ul>
                            </div>
                        </div>
                    </section>
                    
                    <aside class="program-attributes">
                        <span class="badge bg-faded-warning me-2 mb-2"> Accepts DWS Subsidy </span>
                        
                        <div class="card-body">
                            <h5>Age Groups</h5>
                            <ul>
                                <li>Infants (0-11 months)</li>
                                <li>Toddlers (12-23 months)</li>
                            </ul>
                        </div>
                        
                        <div class="card-body">
                            <h5>School(s) Served</h5>
                            <ul>
                                <li>Alpine - Lehi</li>
                                <li>Alpine - Meadow</li>
                            </ul>
                        </div>
                        
                        <div class="card-body">
                            <h5>Meals Offered</h5>
                            <ul>
                                <li>Breakfast</li>
                                <li>Lunch</li>
                            </ul>
                        </div>
                        
                        <div class="card-body">
                            <h5>Environment</h5>
                            <ul>
                                <li>Smoke-free Environment</li>
                                <li>No Pets</li>
                            </ul>
                        </div>
                        
                        <ul>
                            <li data-testid="program-detail-licensed-since"> Licensed Since: <strong>Aug 11, 2023</strong></li>
                        </ul>
                    </aside>
                </main>
            </cac-public-search-root>
        </body>
        </html>
        """
        
        sel = Selector(text=html)
        url = "https://jobs.utah.gov/occ/cac/search/program-detail/104964"
        
        # We need to iterate the generator
        results = list(self.spider.parse_provider_data(sel, url))
        
        self.assertEqual(len(results), 1)
        item = results[0]
        
        self.assertIsInstance(item, ProviderItem)
        self.assertEqual(item['provider_name'], 'A Brighter Tomorrow')
        self.assertEqual(item['ut_quality_rating'], 'Default Foundation of Quality')
        self.assertEqual(item['ut_license_type'], 'Child Care Center')
        self.assertEqual(item['provider_type'], 'Child Care Center')
        self.assertEqual(item['phone'], '(801) 376-6767')
        self.assertIn('50 N 200 E', item['address'])
        self.assertIn('Lehi, Utah 84043', item['address'])
        self.assertEqual(item['capacity'], '75')
        self.assertEqual(item['ut_vacancies'], '14')
        self.assertEqual(item['scholarships_accepted'], 'Yes')
        self.assertIn('Infants', item['ages_served'])
        self.assertIn('Toddlers', item['ages_served'])
        self.assertIn('Alpine - Lehi', item['ut_school_district'])
        self.assertIn('Breakfast', item['ut_meals'])
        self.assertIn('No Pets', item['ut_environment'])
        self.assertEqual(item['ut_licensed_since'], 'Aug 11, 2023')
        self.assertEqual(item['license_begin_date'], 'Aug 11, 2023')

    def test_start_requests(self):
        requests = list(self.spider.start_requests())
        # We expect 344 zip codes based on the hardcoded list
        self.assertEqual(len(requests), 344)
        for req in requests:
            self.assertEqual(req.url, "https://jobs.utah.gov/occ/cac/search/programs-list")
            self.assertTrue(req.meta.get('playwright'))
            self.assertIn(req.meta.get('zip_code'), self.spider.UT_ZIP_CODES)

if __name__ == '__main__':
    unittest.main()

import unittest
from urllib.parse import parse_qs
from scrapy.http import TextResponse, Request, FormRequest
from provider_scrape.spiders.colorado import ColoradoSpider
from provider_scrape.items import ProviderItem

class ColoradoSpiderTest(unittest.TestCase):
    def setUp(self):
        self.spider = ColoradoSpider()

    def test_parse_list(self):
        # Mock HTML for the list page
        html = """
        <html>
        <body>
            <form id="page:searchForm" method="post">
                <input type="hidden" id="com.salesforce.visualforce.ViewState" name="com.salesforce.visualforce.ViewState" value="mock_viewstate">
                <input type="hidden" id="com.salesforce.visualforce.ViewStateVersion" name="com.salesforce.visualforce.ViewStateVersion" value="mock_version">
                <input type="hidden" id="com.salesforce.visualforce.ViewStateMAC" name="com.salesforce.visualforce.ViewStateMAC" value="mock_mac">
                <li class="result">
                    <div class="result-left">
                        <h1>Thorne Nature Experience</h1>
                        <p class="result-rating">
                            <span><span>Licensed Program</span></span>
                        </p>
                        <p class="result-location">1241 Ceres Drive Lafayette, CO 80026</p>
                        <p>County: Boulder</p>
                        <p class="result-phone">(303) 499-3647</p>
                        <p><strong>Care Setting</strong><br>School Age Program</p>
                        <p><strong>Ages Served</strong><br><span>Preschool</span><span>School Age</span></p>
                        <p><strong>languages spoken</strong><br>English<br>Spanish</p>
                        <p><strong>Accepts CCCAP</strong><br><span>Yes</span></p>
                        
                        <p><strong>Infant Openings Available</strong><br>0</p>
                        <p><strong>Toddler Openings Available</strong><br>0</p>
                        <p><strong>Preschool Openings Available</strong><br>5</p>
                        <p><strong>School Age Openings Available</strong><br>80</p>

                        <a class="view-details" href="/program_details?id=123">View Details</a>
                    </div>
                </li>
                <li class="next last"><a href="#" onclick="if(typeof jsfcljs == 'function'){jsfcljs(document.getElementById('page:searchForm'),'page:searchForm:j_id169,page:searchForm:j_id169','');}return false">next</a></li>
            </form>
        </body>
        </html>
        """
        response = TextResponse(url='https://www.coloradoshines.com/search?program=a',
                                body=html,
                                encoding='utf-8')
        
        results = list(self.spider.parse(response))
        
        # Expect 1 Request for detail and 1 FormRequest for next page
        self.assertEqual(len(results), 2)
        
        # Check Detail Request
        req = results[0]
        self.assertIsInstance(req, Request)
        self.assertEqual(req.url, 'https://www.coloradoshines.com/program_details?id=123')
        
        # Check Item in Meta
        item = req.meta['item']
        self.assertEqual(item['provider_name'], 'Thorne Nature Experience')
        self.assertEqual(item['co_quality_rating'], 'Licensed Program')
        self.assertEqual(item['address'], '1241 Ceres Drive Lafayette, CO 80026')
        self.assertEqual(item['county'], 'Boulder')
        self.assertEqual(item['phone'], '(303) 499-3647')
        self.assertEqual(item['provider_type'], 'School Age Program')
        self.assertIn('Preschool', item['ages_served'])
        self.assertIn('English', item['languages'])
        self.assertEqual(item['scholarships_accepted'], 'Yes')
        self.assertEqual(item['co_preschool_openings'], '5')
        self.assertEqual(item['co_school_age_openings'], '80')

        # Check Pagination Request
        form_req = results[1]
        self.assertIsInstance(form_req, FormRequest)
        self.assertEqual(form_req.method, 'POST')
        self.assertEqual(form_req.url, 'https://www.coloradoshines.com/search?program=a')
        
        # Verify body content
        body_params = parse_qs(form_req.body.decode('utf-8'))
        self.assertEqual(body_params['page:searchForm:j_id169'][0], 'page:searchForm:j_id169')
        self.assertEqual(body_params['com.salesforce.visualforce.ViewState'][0], 'mock_viewstate')
        self.assertEqual(body_params['com.salesforce.visualforce.ViewStateVersion'][0], 'mock_version')
        self.assertEqual(body_params['com.salesforce.visualforce.ViewStateMAC'][0], 'mock_mac')

    def test_parse_detail(self):
        # Mock HTML for detail page
        html = """
        <html>
        <body>
            <p><strong>License Number:</strong> 1694465</p>
            <div class="field-website"><span><a href="http://www.thornenature.org"></a></span></div>
            <p><strong>Accepting New Children: </strong><span>Yes</span></p>
            <p><strong>Capacity:</strong> 80</p>
            <p><strong>Head Start: </strong><span>No</span></p>
            <p><strong>Licensed to Serve:</strong> Day Camp – Building Program</p>
            <div class="field-name-field-info"><strong>Special Needs:</strong> Diabetes; Seizure Disorders</div>
            <p><strong>License Type:</strong> Permanent</p>
            <p><strong>License Issue Date: </strong><span>5/15/2024</span></p>
        </body>
        </html>
        """
        
        item = ProviderItem()
        response = TextResponse(url='https://www.coloradoshines.com/program_details?id=123',
                                body=html,
                                encoding='utf-8',
                                request=Request('http://example.com', meta={'item': item}))
        
        results = list(self.spider.parse_detail(response))
        self.assertEqual(len(results), 1)
        
        final_item = results[0]
        self.assertEqual(final_item['license_number'], '1694465')
        self.assertEqual(final_item['provider_website'], 'http://www.thornenature.org')
        self.assertEqual(final_item['co_accepting_new_children'], 'Yes')
        self.assertEqual(final_item['capacity'], '80')
        self.assertEqual(final_item['co_head_start'], 'No')
        self.assertEqual(final_item['co_licensed_to_serve'], 'Day Camp – Building Program')
        self.assertIn('Diabetes', final_item['co_special_needs'])
        self.assertEqual(final_item['co_license_type'], 'Permanent')
        self.assertEqual(final_item['co_license_issue_date'], '5/15/2024')

if __name__ == '__main__':
    unittest.main()
import unittest
from scrapy.http import TextResponse, Request
from provider_scrape.spiders.alabama import AlabamaSpider
from provider_scrape.items import ProviderItem

class AlabamaSpiderTest(unittest.TestCase):
    def setUp(self):
        self.spider = AlabamaSpider()

    def test_parse_results(self):
        html = """
        <html>
        <body>
            <form action="daycare_search" method="post">
            <table id="MainContent_GridView1">
                <tr><th>Header</th></tr>
                <tr>
                    <td>Star 2</td>
                    <td><a href="daycare_results.aspx?ID=128">DENNIS, CONSTANCE <br />GLOBAL LEARNING ACADEMY</a></td>
                    <td>MT. VERNON</td>
                    <td>(251) 635-3595</td>
                </tr>
                <tr>
                    <td>
                        <table>
                            <tr><td><span>1</span></td><td><a href="javascript:__doPostBack('ctl00$MainContent$GridView1','Page$2')">2</a></td></tr>
                        </table>
                    </td>
                </tr>
            </table>
            </form>
        </body>
        </html>
        """
        response = TextResponse(url='https://apps.dhr.alabama.gov/daycare/daycare_search',
                                body=html,
                                encoding='utf-8')
        
        results = list(self.spider.parse_results(response))
        
        # Expect 1 Request for detail and 1 Request for next page
        self.assertEqual(len(results), 2)
        
        # Check detail request
        req1 = results[0]
        self.assertIsInstance(req1, Request)
        self.assertIn('daycare_results.aspx?ID=128', req1.url)
        
        # Check pagination request
        req2 = results[1]
        self.assertIsInstance(req2, Request)
        # FormRequest is a subclass of Request, checks logic for pagination

    def test_parse_detail(self):
        html = """
        <div id="MainContent_Label1">
            <div style='width:100%;clear:both;overflow:hidden;'>
                <div style='width:17%;float:left;margin-right:10px;'><span style='font-size:16px;font-weight:bold;'>Licensee:</span></div>
                <div style='float:left;'><span style='font-size:14px;font-weight:bold;'>JOHNSON, DARRYL </span></div>
            </div>
            <div style='width:100%;clear:both;overflow: hidden;'>
                <div style='width:17%;float:left;margin-right:10px;'><span style='font-size:16px;font-weight:bold;'>Facility:</span></div>
                <div style='float:left;'><span style='font-size:14px;font-weight:bold;'>GLAD TIDINGS DAY CARE</span></div>
            </div>
            <br /><b>Status:</b> Licensed<br />
            <b>JOHNSON, KATRINA M - Director</b><br />
            <b>Phone:</b> (205) 798-1248<br />
            <span style='font-size:12px;font-weight:bold;'>Alabama Quality Star Rating:   </span><span style='font-size:12px;'> &nbsp;&nbsp; 1 Star</span>
            <span style='font-size:12px;font-weight:bold; &nbsp;&nbsp;'>     Rating Expiration Date:    </span><span style='font-size:12px;'>&nbsp;&nbsp;&nbsp;</span>
            
            <table style='padding:1px 1px 5px 1px;border:1px solid #999999;width:100%;border-collapse:collapse;' border='1'>
                <tr><td width='50%'><b>Daytime Hours:</b> &nbsp;07:00 AM - 06:00 PM</td><td width='50%'><b>Nighttime Hours:</b> &nbsp;N/A - N/A</td></tr>
                <tr><td width='50%'><b>Daytime Ages:</b> &nbsp;2 Years To 14 Years</td><td width='50%'><b>Nighttime Ages:</b> &nbsp;N/A</td></tr>
            </table>
            
            <span style='font-size:12px;font-weight:bold;'>Mailing Address:</span><br /><span style='font-size:12px;'>1400 BRISBANE AVENUE</span><br /><span style='font-size:12px;'>BIRMINGHAM</span>, <span style='font-size:12px;'>AL</span> <span style='font-size:12px;'>35214</span><br /><br />
            <span style='font-size:12px;font-weight:bold;'>Street Address:</span><br /><span style='font-size:12px;'>1400 BRISBANE AVENUE</span><br /><span style='font-size:12px;'>BIRMINGHAM</span>, <span style='font-size:12px;'>AL</span> <span style='font-size:12px;'>35214</span>
            <br /><br /><a href='http://map.google.com/maps?q=1400+BRISBANE+AVENUE,BIRMINGHAM, AL 35214' target='_blank'>Click for Interactive Map</a>
        </div>
        
        <table id="MainContent_GridView1"><tr><td>No Accreditations</td></tr></table>
        <table id="MainContent_GridView3"><tr><td>No Adverse Actions</td></tr></table>
        <table id="MainContent_GridView2"><tr><td>No Substantiated Complaints</td></tr></table>
        <table id="MainContent_GridView4"><tr><td>No Evaluation/Deficiency Reports</td></tr></table>
        """
        
        response = TextResponse(url='https://apps.dhr.alabama.gov/daycare/daycare_results?ID=589',
                                body=html,
                                encoding='utf-8')
        
        results = list(self.spider.parse_detail(response))
        item = results[0]
        
        self.assertIsInstance(item, ProviderItem)
        self.assertEqual(item['license_holder'], 'JOHNSON, DARRYL ')
        self.assertEqual(item['provider_name'], 'GLAD TIDINGS DAY CARE')
        self.assertEqual(item['status'], 'Licensed')
        self.assertEqual(item['administrator'], 'JOHNSON, KATRINA M')
        self.assertEqual(item['phone'], '(205) 798-1248')
        self.assertEqual(item['al_quality_rating'], '1 Star')
        self.assertEqual(item['hours'], '07:00 AM - 06:00 PM')
        self.assertEqual(item['ages_served'], '2 Years To 14 Years')
        self.assertIn('1400 BRISBANE AVENUE', item['al_mailing_address'])
        self.assertIn('1400 BRISBANE AVENUE', item['address'])
        self.assertEqual(item['al_accreditations'], [])

if __name__ == '__main__':
    unittest.main()

import unittest
from scrapy.selector import Selector
from provider_scrape.spiders.pennsylvania import PennsylvaniaSpider
from provider_scrape.items import ProviderItem

class PennsylvaniaSpiderTest(unittest.TestCase):
    def setUp(self):
        self.spider = PennsylvaniaSpider()

    def test_parse_provider_details(self):
        html = """
        <div role="contentinfo" class="container provider">
            <h1 role="heading" aria-level="1">Bright Horizons At Southside Works</h1>
            <div class="row">
                <div class="col-12 col-md-4 large-mb prov-detail">
                    <p class="stars-rating text-prim-mid fw-bold">
                        <i class="fa-solid fa-star"></i><i class="fa-solid fa-star"></i><i class="fa-solid fa-star"></i><i class="fa-solid fa-star"></i>
                    </p>
                    <div>
                        <a target="_blank" href="https://maps.google.com/maps?q=2629 E CARSON ST ,+PITTSBURGH,+PA,+15203">
                            <span class="d-block"> 2629 E CARSON ST </span>
                            <span class="d-block"> PITTSBURGH, PA 15203 </span>
                        </a>
                    </div>
                    <div>
                        <div class="prov-info md-mb">
                            <a href="tel:+14124888690"> (412) 488-8690 </a>
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="prov-page-section">
                <div class="row">
                    <div class="col-6 col-md">
                        <h3 role="contentinfo" aria-level="3" class="section-title-prov"> Provider Type </h3>
                        <p class="prov-data"> Center </p>
                    </div>
                    <div class="col-6 col-md">
                        <h3 role="contentinfo" aria-level="3" class="section-title-prov"> Certification </h3>
                        <div><p class="prov-data"> Active </p></div>
                    </div>
                    <div class="col-6 col-md">
                        <h3 role="contentinfo" aria-level="3" class="section-title-prov"> Maximum Capacity </h3>
                        <div><p class="prov-data"> 132 </p></div>
                    </div>
                </div>
            </div>

            <div class="prov-page-section">
                <div class="row">
                    <div class="col-12 col-md-6">
                        <h3 class="section-title"> School District(s) Served </h3>
                    </div>
                    <div class="col-12 col-md-6">
                        <p class="prov-data"> - </p>
                    </div>
                </div>
            </div>

            <div class="prov-page-section">
                <div class="row">
                    <div class="col-12 col-md-6">
                        <h3 class="section-title"> Meal Options </h3>
                    </div>
                    <div class="col-12 col-md-6">
                        <ul class="side-list">
                            <li>AM Snack</li>
                            <li>Lunch</li>
                            <li>PM Snack</li>
                        </ul>
                    </div>
                </div>
            </div>

            <div class="prov-page-section">
                <div class="row">
                    <div class="col-12 col-md-6">
                        <h3 class="section-title"> Schedule </h3>
                        <ul>
                            <li> School District Holidays </li>
                            <li> Full Year </li>
                        </ul>
                    </div>
                </div>
            </div>

            <div class="prov-page-section">
                <h3 class="section-title"> Cost </h3>
                <div class="card standard-mb">
                    <div class="card-body">
                        <div class="record">
                            <div class="row no-border data-row">
                                <div class="col-12 col-md-5">
                                    <span class="head"><span class="d-none d-md-block"> Infant (6 Weeks-12 mos.) </span></span>
                                </div>
                                <div class="col-4 col-md-2"> $87.69 </div>
                                <div class="col-4 col-md-2"> $65.77 </div>
                                <div class="col-4 col-md-3"> Call for Availability </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        """
        
        item = self.spider.parse_provider_details(html)
        
        self.assertIsInstance(item, ProviderItem)
        self.assertEqual(item['provider_name'], 'Bright Horizons At Southside Works')
        self.assertIn('2629 E CARSON ST', item['address'])
        self.assertIn('PITTSBURGH, PA 15203', item['address'])
        self.assertEqual(item['phone'], '(412) 488-8690')
        self.assertEqual(item['pa_stars_rating'], '4')
        self.assertEqual(item['capacity'], '132')
        self.assertEqual(item['provider_type'], 'Center')
        self.assertEqual(item['pa_certificate_status'], 'Active')
        self.assertEqual(item['pa_school_district'], '-')
        self.assertIn('AM Snack', item['pa_meal_options'])
        self.assertIn('Lunch', item['pa_meal_options'])
        self.assertIn('School District Holidays', item['pa_schedule'])
        
        # Check Cost Table
        cost_table = item['pa_cost_table']
        self.assertEqual(len(cost_table), 1)
        self.assertEqual(cost_table[0]['age_group'], 'Infant (6 Weeks-12 mos.)')
        self.assertEqual(cost_table[0]['full_time_rate'], '$87.69')
        self.assertEqual(cost_table[0]['part_time_rate'], '$65.77')
        self.assertEqual(cost_table[0]['openings'], 'Call for Availability')

if __name__ == '__main__':
    unittest.main()

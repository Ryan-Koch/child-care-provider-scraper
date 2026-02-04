import unittest
from scrapy.http import TextResponse
from provider_scrape.spiders.new_mexico import NewMexicoSpider
from provider_scrape.items import ProviderItem

class NewMexicoSpiderTest(unittest.TestCase):
    def setUp(self):
        self.spider = NewMexicoSpider()

    def test_parse_detail(self):
        html = """
        <header>...</header>
        <main>
            <h1 id="listing-name">Little Saints Preschool</h1>
            <div id="availablity-banner-desktop">Accepting new enrollments: 3 years to 4 years</div>
            <div id="program-type-text">Licensed Center</div>
            <div id="license-number"><a href="...">#4000715</a></div>
            <div id="location">
                <div class="flex flex-row"><h4>Location</h4></div>
                <div class="flex flex-col">
                    <div>1000 E 8th St</div>
                    <div>Alamogordo, NM 88310</div>
                </div>
            </div>
            <div id="hours">
                <div class="flex flex-row"><h4>Hours</h4></div>
                <div>
                    <div>Monday - Friday</div>
                    <div>6:30am - 6:00pm</div>
                </div>
            </div>
            <div id="network_rating">
                <div>Star Level 2</div>
            </div>
            <div id="meals-provided"><div>Lunch</div></div>
            <div id="snacks-provided"><div>2 per day</div></div>
            <div id="potty-training-required"><div>Required</div></div>
            <div id="days-per-week"><div>1, 2, 3, 4 and 5 days per week</div></div>
            <div id="language-supported"><div>English</div></div>
            <div id="pay-schedules"><div>Monthly</div></div>
            <div id="subsidy-accepted"><div>Accepted</div></div>
            <div class="aside">
                <div>
                    <div class="text-lg">Stephnora Ogbonna</div>
                    <div>Program Director</div>
                </div>
            </div>
        </main>
        """
        
        # Note: My XPath for administrator might need to be more precise based on the provided HTML
        # In my spider: //div[div/div/text()="Program Director"]//div[contains(@class, "text-lg")]/text()
        # User snippet:
        # <div class="flex flex-col" data-phx-id="m74-phx-GJEFJP5sYPrqyLUl">
        #   <div class="text-lg font-bold mb-1">Stephnora Ogbonna</div>
        #   <div class="uppercase font-bold text-gray-500 text-xs">Program Director</div>
        # </div>
        
        # Let's adjust the mock HTML to match the user's snippet exactly for the administrator
        html = """
        <h1 id="listing-name">Little Saints Preschool</h1>
        <div id="availablity-banner-desktop">Accepting new enrollments: 3 years to 4 years</div>
        <div id="program-type-text">Licensed Center</div>
        <div id="license-number"><a href="...">#4000715</a></div>
        <div id="location">
            <h4 class="flex flex-row gap-x-2 font-bold ">Location</h4>
            <div class="flex flex-col pl-12 -mt-2">
                <div>1000 E 8th St</div>
                <div>Alamogordo, NM 88310</div>
            </div>
            <img src="https://maps.googleapis.com/maps/api/staticmap?center=32.8975773,-105.9495625&zoom=15">
        </div>
        <div id="hours">
            <h4 class="flex flex-row gap-x-2 font-bold ">Hours</h4>
            <div class="pl-12 -mt-2">
                <div>Monday - Friday</div>
                <div>6:30am - 6:00pm</div>
            </div>
        </div>
        <div id="network_rating">
            <div class="flex flex-row items-center gap-x-2">Star Level 2</div>
        </div>
        <div id="meals-provided"><div>Lunch</div></div>
        <div id="snacks-provided"><div>2 per day</div></div>
        <div id="potty-training-required"><div>Required</div></div>
        <div id="days-per-week"><div>1, 2, 3, 4 and 5 days per week</div></div>
        <div id="language-supported"><div>English</div></div>
        <div id="pay-schedules"><div>Monthly</div></div>
        <div id="subsidy-accepted"><div>Accepted</div></div>
        <div class="flex flex-col">
            <div class="text-lg font-bold mb-1">Stephnora Ogbonna</div>
            <div class="uppercase font-bold text-gray-500 text-xs">Program Director</div>
        </div>
        """

        response = TextResponse(url='https://childcare.ececd.nm.gov/nm/alamogordo/little-saints-preschool-26613',
                                body=html,
                                encoding='utf-8')
        
        results = list(self.spider.parse_detail(response))
        item = results[0]
        
        self.assertIsInstance(item, ProviderItem)
        self.assertEqual(item['provider_name'], 'Little Saints Preschool')
        self.assertEqual(item['administrator'], 'Stephnora Ogbonna')
        self.assertEqual(item['provider_type'], 'Licensed Center')
        self.assertEqual(item['license_number'], '4000715')
        self.assertEqual(item['address'], '1000 E 8th St, Alamogordo, NM 88310')
        self.assertEqual(item['latitude'], '32.8975773')
        self.assertEqual(item['longitude'], '-105.9495625')
        self.assertEqual(item['hours'], 'Monday - Friday, 6:30am - 6:00pm')
        self.assertEqual(item['nm_star_level'], 'Star Level 2')
        self.assertEqual(item['ages_served'], '3 years to 4 years')
        self.assertEqual(item['nm_meals'], 'Lunch')
        self.assertEqual(item['nm_snacks'], '2 per day')
        self.assertEqual(item['nm_potty_training'], 'Required')
        self.assertEqual(item['nm_schedule'], '1, 2, 3, 4 and 5 days per week')
        self.assertEqual(item['languages'], 'English')
        self.assertEqual(item['nm_pay_schedules'], 'Monthly')
        self.assertEqual(item['scholarships_accepted'], 'Accepted')

if __name__ == '__main__':
    unittest.main()

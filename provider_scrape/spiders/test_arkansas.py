import unittest
from unittest.mock import MagicMock, AsyncMock
from provider_scrape.spiders.arkansas import ArkansasSpider
from provider_scrape.items import ProviderItem, InspectionItem

class TestArkansasSpider(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.spider = ArkansasSpider()

    async def test_parse_detail_salesforce_standard_structure(self):
        """
        Tests extraction using the standard Salesforce 'test-id__field-label' structure.
        """
        # We need distinct containers to ensure XPaths don't cross-contaminate
        html = """
        <html>
            <body>
                <div class="forceHighlightsPanel">
                    <h1 class="slds-page-header__title">Salesforce Child Care</h1>
                </div>
                
                <!-- Address Container -->
                <div class="field-wrapper">
                    <div>
                        <span class="test-id__field-label">Address</span>
                    </div>
                    <div class="test-id__field-value">123 SF Street</div>
                </div>

                <!-- Phone Container -->
                <div class="field-wrapper">
                    <div>
                        <span class="test-id__field-label">Phone</span>
                    </div>
                    <div class="test-id__field-value">555-1234</div>
                </div>
                
                <!-- License Container -->
                <div class="field-wrapper">
                    <div>
                        <span class="test-id__field-label">License Number</span>
                    </div>
                    <div class="test-id__field-value">LIC-100</div>
                </div>

                <!-- Capacity Container -->
                <div class="field-wrapper">
                    <div>
                        <span class="test-id__field-label">Total Allowed Capacity</span>
                    </div>
                    <div class="test-id__field-value">45</div>
                </div>

                <!-- Rating Container -->
                <div class="field-wrapper">
                    <div>
                        <span class="test-id__field-label">Star Level</span>
                    </div>
                    <div class="test-id__field-value">3 Star</div>
                </div>
            </body>
        </html>
        """

        # Mock the Playwright Page
        # Note: page.locator must be synchronous, others async
        mock_page = MagicMock()
        mock_page.content = AsyncMock(return_value=html)
        mock_page.wait_for_selector = AsyncMock()
        mock_page.close = AsyncMock()
        
        # Mock locator for facility visits (return empty to skip logic)
        mock_visits_locator = MagicMock()
        mock_visits_locator.count = AsyncMock(return_value=0)
        mock_page.locator = MagicMock(return_value=mock_visits_locator)

        # Mock the Response object
        mock_response = MagicMock()
        mock_response.url = "http://example.com/details"
        mock_response.meta = {"playwright_page": mock_page}

        # Run the spider method
        results = []
        async for item in self.spider.parse_detail(mock_response):
            results.append(item)

        self.assertEqual(len(results), 1)
        item = results[0]
        
        self.assertIsInstance(item, ProviderItem)
        self.assertEqual(item["provider_name"], "Salesforce Child Care")
        self.assertEqual(item["address"], "123 SF Street")
        self.assertEqual(item["phone"], "555-1234")
        self.assertEqual(item["license_number"], "LIC-100")
        self.assertEqual(item["capacity"], "45")
        self.assertEqual(item["ar_quality_rating"], "3 Star")

    async def test_parse_detail_lwc_structure(self):
        """
        Tests extraction using the LWC structure (Label div + Sibling Value).
        """
        html = """
        <html>
            <body>
                <!-- Missing primary title, fallback to h2 -->
                <h2 class="slds-align-middle">LWC Child Care</h2>
                
                <!-- Address -->
                <div>Address</div>
                <lightning-formatted-rich-text>456 LWC Lane</lightning-formatted-rich-text>

                <!-- Phone -->
                <div>Site Phone</div>
                <lightning-formatted-rich-text>555-5678</lightning-formatted-rich-text>
                
                <!-- Capacity -->
                <div>Capacity</div>
                <lightning-formatted-rich-text>60</lightning-formatted-rich-text>
                
                <!-- Rating -->
                <div>Better Beginnings</div>
                <lightning-formatted-rich-text>2 Star</lightning-formatted-rich-text>
            </body>
        </html>
        """

        mock_page = MagicMock()
        mock_page.content = AsyncMock(return_value=html)
        mock_page.wait_for_selector = AsyncMock()
        mock_page.close = AsyncMock()
        
        mock_visits_locator = MagicMock()
        mock_visits_locator.count = AsyncMock(return_value=0)
        mock_page.locator = MagicMock(return_value=mock_visits_locator)

        mock_response = MagicMock()
        mock_response.url = "http://example.com/details"
        mock_response.meta = {"playwright_page": mock_page}

        results = []
        async for item in self.spider.parse_detail(mock_response):
            results.append(item)

        self.assertEqual(len(results), 1)
        item = results[0]
        
        self.assertEqual(item["provider_name"], "LWC Child Care")
        self.assertEqual(item["address"], "456 LWC Lane")
        self.assertEqual(item["phone"], "555-5678")
        self.assertEqual(item["capacity"], "60")
        self.assertEqual(item["ar_quality_rating"], "2 Star")

    async def test_parse_detail_missing_fields(self):
        """
        Tests that the spider gracefully handles missing fields.
        """
        html = """
        <html>
            <body>
                 <div class="forceHighlightsPanel">
                    <h1 class="slds-page-header__title">Minimal Provider</h1>
                </div>
            </body>
        </html>
        """

        mock_page = MagicMock()
        mock_page.content = AsyncMock(return_value=html)
        mock_page.wait_for_selector = AsyncMock()
        mock_page.close = AsyncMock()
        
        mock_visits_locator = MagicMock()
        mock_visits_locator.count = AsyncMock(return_value=0)
        mock_page.locator = MagicMock(return_value=mock_visits_locator)

        mock_response = MagicMock()
        mock_response.url = "http://example.com/details"
        mock_response.meta = {"playwright_page": mock_page}

        results = []
        async for item in self.spider.parse_detail(mock_response):
            results.append(item)

        self.assertEqual(len(results), 1)
        item = results[0]
        
        self.assertEqual(item["provider_name"], "Minimal Provider")
        self.assertIsNone(item.get("phone"))
        self.assertIsNone(item.get("capacity"))
        self.assertIsNone(item.get("license_number"))
        self.assertEqual(item["inspections"], [])

    async def test_parse_detail_with_inspections(self):
        """
        Tests extraction of inspection data when the tab/link exists.
        """
        html_main = """
        <html>
            <body>
                <h1 class="slds-page-header__title">Provider With Inspections</h1>
            </body>
        </html>
        """
        
        html_visits = """
        <html>
            <body>
                <table>
                    <tbody>
                        <tr>
                            <td>01/01/2025</td>
                            <td>Routine</td>
                            <td><a href="/report1.pdf">View Report</a></td>
                        </tr>
                    </tbody>
                </table>
            </body>
        </html>
        """

        mock_page = MagicMock()
        mock_page.wait_for_selector = AsyncMock()
        mock_page.close = AsyncMock()
        mock_page.wait_for_load_state = AsyncMock()
        mock_page.wait_for_timeout = AsyncMock()
        
        # First call to content() is for main page, second is for visits
        mock_page.content = AsyncMock(side_effect=[html_main, html_visits])
        
        # Mock locator for "View Facility Visits" finding something
        mock_visits_link = MagicMock()
        mock_visits_link.count = AsyncMock(return_value=1)
        
        # Configure .first.click to be an AsyncMock so it can be awaited
        mock_first = MagicMock()
        mock_first.click = AsyncMock()
        mock_visits_link.first = mock_first
        
        # When page.locator is called, return our link mock
        mock_page.locator = MagicMock(return_value=mock_visits_link)

        mock_response = MagicMock()
        mock_response.url = "http://example.com/details"
        mock_response.meta = {"playwright_page": mock_page}
        mock_response.urljoin = lambda x: f"http://example.com{x}"

        results = []
        async for item in self.spider.parse_detail(mock_response):
            results.append(item)

        self.assertEqual(len(results), 1)
        item = results[0]
        
        # Check that we attempted to click
        mock_first.click.assert_called()
        
        # Check inspections extraction
        self.assertEqual(len(item["inspections"]), 1)
        insp = item["inspections"][0]
        self.assertEqual(insp["date"], "01/01/2025")
        self.assertEqual(insp["type"], "Routine")
        self.assertEqual(insp["report_url"], "http://example.com/report1.pdf")

if __name__ == '__main__':
    unittest.main()
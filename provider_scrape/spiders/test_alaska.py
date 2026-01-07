import unittest
from unittest.mock import MagicMock, AsyncMock, patch
from scrapy.http import Request, Response
from provider_scrape.spiders.alaska import AlaskaSpider
import asyncio
import re

class AlaskaSpiderTest(unittest.TestCase):
    def setUp(self):
        self.spider = AlaskaSpider()

    def test_pagination_loop(self):
        # Mock Response and Playwright Page
        mock_response = MagicMock(spec=Response)
        mock_response.meta = {}
        mock_page = AsyncMock()
        # Ensure page.locator is NOT async (it returns a Locator object synchronously)
        mock_page.locator = MagicMock() 
        mock_response.meta['playwright_page'] = mock_page

        # Mock Agreement Button (count=0 to skip)
        mock_agreement_btn = MagicMock()
        mock_agreement_btn.count = AsyncMock(return_value=0)

        # Mock Search Button
        mock_search_btn = MagicMock()
        mock_search_btn.count = AsyncMock(return_value=1)
        mock_search_btn.first.click = AsyncMock()
        
        # Mock Pagination Buttons
        # mock_pagination_buttons is a Locator, so it should be MagicMock
        # but .count() is async
        mock_pagination_buttons = MagicMock()
        mock_pagination_buttons.count = AsyncMock(return_value=4)
        
        # Next button mocks
        mock_next_btn_enabled = MagicMock()
        mock_next_btn_enabled.is_disabled = AsyncMock(return_value=False)
        mock_next_btn_enabled.click = AsyncMock()
        
        mock_next_btn_disabled = MagicMock()
        mock_next_btn_disabled.is_disabled = AsyncMock(return_value=True)

        # nth(2) is the Next button
        mock_pagination_buttons.nth.side_effect = [mock_next_btn_enabled, mock_next_btn_disabled]

        def locator_side_effect(*args, **kwargs):
            # args[0] is the selector string
            selector = args[0]
            has_text = kwargs.get('has_text')
            
            # Check for Agreement button
            if has_text and isinstance(has_text, re.Pattern) and 'Accept' in has_text.pattern:
                return mock_agreement_btn

            # Check for Search button
            if has_text and isinstance(has_text, re.Pattern) and 'Search' in has_text.pattern:
                return mock_search_btn
            
            # Check for Pagination
            if '.mud-table-pagination-actions button' in selector:
                return mock_pagination_buttons
            
            return MagicMock()

        mock_page.locator.side_effect = locator_side_effect

        # Mock Content
        # We'll return a simple table for content with correct classes
        mock_page.content.return_value = """
            <html>
                <div class="mud-table-page-number-information">1-10 of 50</div>
                <table>
                    <tr class="mud-table-row">
                        <td>Provider 1</td>
                        <td>Type</td>
                        <td><a href="/ProviderInfo/1">Details</a></td>
                    </tr>
                </table>
            </html>
        """

        # Run the async parse method
        async def run_test():
            # We need to mock response.follow as well since it's used in yield
            with patch('scrapy.http.Response.follow') as mock_follow:
                mock_follow.return_value = Request(url="http://mock-url")
                
                results = []
                async for item in self.spider.parse(mock_response):
                    results.append(item)
                
                # Assertions
                # Should have yielded 1 request per page loop * 2 loops (initial + 1 next click) = 2 requests
                self.assertEqual(len(results), 2)
                self.assertEqual(mock_next_btn_enabled.click.call_count, 1)

        asyncio.run(run_test())

if __name__ == '__main__':
    unittest.main()
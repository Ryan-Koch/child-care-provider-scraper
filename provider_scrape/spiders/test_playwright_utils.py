import logging
import unittest
from unittest.mock import MagicMock, AsyncMock

from provider_scrape.playwright_utils import PlaywrightErrbackMixin


class _MixinHost(PlaywrightErrbackMixin):
    """Bare mixin host.

    Deliberately not a ``scrapy.Spider`` subclass so it is not picked up by the
    spider registry (the sources-drift test would otherwise flag it).
    """

    def __init__(self):
        self.logger = logging.getLogger("mixin_test")
        # Silence the errback's logging during the give-up / non-retry paths.
        self.logger.setLevel(logging.CRITICAL)


class TestPlaywrightErrbackMixin(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.spider = _MixinHost()

    def _failure(self, meta):
        page = MagicMock()
        page.is_closed = MagicMock(return_value=False)
        page.close = AsyncMock()
        failure = MagicMock()
        failure.value = TimeoutError("Page.goto: Timeout 30000ms exceeded")
        request = MagicMock()
        request.url = "https://example.com/detail?pid=abc"
        request.meta = {"playwright_page": page, **meta}
        replaced = MagicMock()
        request.replace = MagicMock(return_value=replaced)
        failure.request = request
        return failure, request, page, replaced

    async def test_closes_page_and_retries_when_opted_in(self):
        """A failed request that opted into retries closes its leaked page
        (freeing the semaphore slot) and re-schedules itself."""
        failure, request, page, replaced = self._failure({"playwright_retry": True})

        results = [out async for out in self.spider.errback_close_page(failure)]

        page.close.assert_awaited_once()
        self.assertEqual(results, [replaced])
        replace_kwargs = request.replace.call_args.kwargs
        self.assertEqual(replace_kwargs["meta"]["playwright_retry_count"], 1)
        self.assertNotIn("playwright_page", replace_kwargs["meta"])
        self.assertTrue(replace_kwargs["dont_filter"])

    async def test_gives_up_after_max_retries(self):
        """Once the retry budget is spent the request is dropped, not
        re-scheduled, so the crawl can drain and finish."""
        failure, request, page, _ = self._failure(
            {"playwright_retry": True,
             "playwright_retry_count": self.spider.playwright_max_retries}
        )

        results = [out async for out in self.spider.errback_close_page(failure)]

        page.close.assert_awaited_once()  # still frees the slot
        request.replace.assert_not_called()
        self.assertEqual(results, [])

    async def test_closes_page_without_retry_when_not_opted_in(self):
        """The critical anti-hang behaviour: even without retry opt-in the page
        is always closed so its semaphore slot is released."""
        failure, request, page, _ = self._failure({})

        results = [out async for out in self.spider.errback_close_page(failure)]

        page.close.assert_awaited_once()
        request.replace.assert_not_called()
        self.assertEqual(results, [])

    async def test_skips_close_when_page_already_closed(self):
        """Guard against double-closing a page that Playwright already closed."""
        failure, request, page, _ = self._failure({})
        page.is_closed = MagicMock(return_value=True)

        results = [out async for out in self.spider.errback_close_page(failure)]

        page.close.assert_not_awaited()
        self.assertEqual(results, [])


if __name__ == "__main__":
    unittest.main()

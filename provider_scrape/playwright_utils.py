"""Shared helpers for Playwright-backed spiders.

The important export is :class:`PlaywrightErrbackMixin`. Any request that sets
``playwright_include_page=True`` MUST also set ``errback=self.errback_close_page``.

Why this matters
----------------
When a ``playwright_include_page`` request fails at download time (typically a
``page.goto`` timeout on a slow JS SPA), scrapy-playwright deliberately does
*not* close the page -- it leaves that to the callback/errback. But the callback
never runs for a failed download, so with no errback nobody closes the page. The
page's slot in the per-context page semaphore (``PLAYWRIGHT_MAX_PAGES_PER_CONTEXT``,
which defaults to ``CONCURRENT_REQUESTS``) is only released on the page's
``close`` event, so every such failure permanently leaks a slot. After enough
timeouts the pool is exhausted and every remaining request blocks forever on
``semaphore.acquire()`` -- the crawl wedges at ``0 pages/min, 0 items/min`` and
never finishes.

Why not just use RetryMiddleware
--------------------------------
Scrapy's built-in retrying does not help here for two reasons: Playwright's
``TimeoutError`` is not in the default ``RETRY_EXCEPTIONS`` (so these failures
are never retried), and even if it were, a RetryMiddleware retry re-schedules
the request without running the errback -- so the failed attempt's page would
leak on every retry. The only leak-safe way to retry is to close the page first
and then re-yield, which is exactly what this errback does.
"""


class PlaywrightErrbackMixin:
    """Mixin providing a leak-safe errback for ``playwright_include_page`` requests.

    Usage:
        * inherit before ``scrapy.Spider``:
          ``class FooSpider(PlaywrightErrbackMixin, scrapy.Spider): ...``
        * pass ``errback=self.errback_close_page`` on every request that sets
          ``playwright_include_page=True``.
        * to have a request retried on failure, set ``playwright_retry=True`` in
          its ``meta``. Retries are capped at :attr:`playwright_max_retries`.
    """

    #: How many times a request that opted into retries (``meta["playwright_retry"]``)
    #: is re-scheduled before it is given up on.
    playwright_max_retries = 3

    async def errback_close_page(self, failure):
        request = failure.request
        page = request.meta.get("playwright_page")
        if page is not None and not page.is_closed():
            await page.close()

        if request.meta.get("playwright_retry"):
            retries = request.meta.get("playwright_retry_count", 0)
            if retries < self.playwright_max_retries:
                self.logger.warning(
                    "Retrying Playwright request (%d/%d) after %r: %s",
                    retries + 1,
                    self.playwright_max_retries,
                    failure.value,
                    request.url,
                )
                meta = dict(request.meta)
                meta.pop("playwright_page", None)  # drop the now-closed page
                meta["playwright_retry_count"] = retries + 1
                yield request.replace(meta=meta, dont_filter=True)
                return
            self.logger.error(
                "Giving up on Playwright request after %d retries: %s",
                self.playwright_max_retries,
                request.url,
            )
        else:
            self.logger.error(repr(failure))

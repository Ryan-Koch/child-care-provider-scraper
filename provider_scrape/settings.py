# Scrapy settings for va_scrape project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "provider_scrape"

SPIDER_MODULES = ["provider_scrape.spiders"]
NEWSPIDER_MODULE = "provider_scrape.spiders"


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = "va_scrape (+http://www.yourdomain.com)"

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    "va_scrape.middlewares.VaScrapeSpiderMiddleware": 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    "provider_scrape.middlewares.VaScrapeDownloaderMiddleware": 543,
    # Cooldown-and-retry for per-IP rate-limit 403s. Disabled unless a spider
    # sets RATELIMIT_BACKOFF_ENABLED (e.g. maryland). Placed just after the
    # built-in RetryMiddleware (550) so it inspects the response first.
    "provider_scrape.middlewares.RateLimitBackoffMiddleware": 560,
    # Optional per-proxy egress + download slot for rate-limited hosts. No-op
    # unless a spider exposes a truthy `proxy_pool`. Must run before the built-in
    # HttpProxyMiddleware (750) so the proxy it sets is honored.
    "provider_scrape.middlewares.ProxyPoolMiddleware": 610,
}

# Seconds between ProxyPoolMiddleware's per-IP activity reports (INFO). Only
# emitted when a proxy pool is active; makes detail-phase fan-out visible at a
# glance (which IPs are still completing responses vs went quiet under throttle).
# Set to 0 to disable the periodic report (per-proxy run totals still land in
# the final stats dump).
PROXY_POOL_STATS_INTERVAL = 300.0

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    # Runs after item construction so exporters see normalized values.
    "provider_scrape.pipelines.NormalizationPipeline": 300,
}

# Master switch for scrape-time normalization (data cleanup pipeline). Set to
# False for a non-normalized run when raw scraped values are needed (D4).
NORMALIZE_ENABLED = True

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}
PLAYWRIGHT_BROWSER_TYPE = "chromium"
PLAYWRIGHT_LAUNCH_OPTIONS = {
    "headless": True,
    "timeout": 20 * 1000,  # 20 seconds
}

PLAYWRIGHT_CONTEXT_ARGS = {
    "ignore_https_errors": True,
}
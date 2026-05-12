import asyncio
import json
import logging
import platform
import random
from urllib.parse import urlencode

import scrapy
import scrapy.signals
from playwright_stealth import Stealth
from scrapy_playwright.page import PageMethod

from provider_scrape.items import InspectionItem, ProviderItem

LANDING_PAGE_URL = "https://azchildcareprovidersearch.azdes.gov/"
SEARCH_PAGE_URL = "https://azchildcaresearch.azdes.gov/s/providersearch?language=en_US"
AURA_ENDPOINT_PATH = "/s/sfsites/aura?r=2&aura.ApexAction.execute=1"

_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/148.0.0.0 Safari/537.36"
)

_PLATFORM = "Linux x86_64"

# Playwright stealth settings to evade basic bot detection
_STEALTH_SCRIPT = Stealth(
    navigator_user_agent_override=_UA,
    navigator_platform_override=_PLATFORM,
    navigator_languages_override=("en-US", "en"),
    webgl_vendor=False,
).script_payload

assert "webdriver" in _STEALTH_SCRIPT, "Stealth script missing webdriver patch"

_CANVAS_PATCH = """
const origToDataURL = HTMLCanvasElement.prototype.toDataURL;
HTMLCanvasElement.prototype.toDataURL = function(...args) {
    const ctx = this.getContext('2d');
    if (ctx) {
        const imageData = ctx.getImageData(0, 0, this.width, this.height);
        for (let i = 0; i < imageData.data.length; i += 4) {
            imageData.data[i] ^= 1;
        }
        ctx.putImageData(imageData, 0, 0);
    }
    return origToDataURL.apply(this, args);
};
"""

_HW_PATCH = """
Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});
"""

_FETCH_SCRIPT = """
async ({url, body}) => {
    let resp;
    try {
        resp = await fetch(url, {
            method: 'POST',
            body: body,
            credentials: 'include',
            mode: 'same-origin',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
                'X-SFDC-LDS-Endpoints': 'ApexActionController.execute:PVM_ProviderSearchControllerMain.getProvidersfromZip',
            },
        });
    } catch (e) {
        return {error: 'fetch_failed', detail: String(e)};
    }
    return {status: resp.status, body: await resp.text()};
}
"""

def extract_form_field(post_data, field):
    if not post_data:
        return None
    needle = f"{field}="
    for piece in post_data.split("&"):
        if piece.startswith(needle):
            from urllib.parse import unquote
            return unquote(piece[len(needle):])
    return None

def build_search_message(page_size, page_number):
    message = {
        "actions": [
            {
                "id": f"{page_number};a",
                "descriptor": "aura://ApexActionController/ACTION$execute",
                "callingDescriptor": "UNKNOWN",
                "params": {
                    "namespace": "",
                    "classname": "PVM_ProviderSearchControllerMain",
                    "method": "getProvidersfromZip",
                    "params": {
                        "zipCode": None,
                        "providerName": None,
                        "providerCity": None,
                        "streetAddress": "",
                        "pageSize": page_size,
                        "pageNumber": page_number,
                        "filtersJson": None
                    },
                    "cacheable": False,
                    "isContinuation": False
                }
            }
        ]
    }
    return json.dumps(message, separators=(",", ":"))

def build_search_post_body(page_size, page_number, aura_context):
    return urlencode({
        "message": build_search_message(page_size, page_number),
        "aura.context": aura_context,
        "aura.pageURI": "/s/providersearch?language=en_US",
        "aura.token": "null"
    })

class StealthContextMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        mw = cls()
        crawler.signals.connect(mw.spider_opened, signal=scrapy.signals.spider_opened)
        return mw

    def spider_opened(self, spider):
        from scrapy_playwright.handler import ScrapyPlaywrightDownloadHandler

        handlers = spider.crawler.engine.downloader.handlers._handlers
        handler = handlers.get("https")
        if not isinstance(handler, ScrapyPlaywrightDownloadHandler):
            spider.logger.warning(
                "StealthContextMiddleware: scrapy-playwright handler not found; "
                "stealth patches NOT applied."
            )
            return

        original = handler._create_browser_context

        async def patched_create_context(name, context_kwargs=None, spider=None):
            wrapper = await original(name, context_kwargs=context_kwargs, spider=spider)
            await wrapper.context.add_init_script(_STEALTH_SCRIPT)
            await wrapper.context.add_init_script(_CANVAS_PATCH)
            await wrapper.context.add_init_script(_HW_PATCH)
            if spider:
                spider.logger.info(
                    "StealthContextMiddleware: stealth patches applied to context '%s'",
                    name,
                )
            return wrapper

        handler._create_browser_context = patched_create_context

class ArizonaSpider(scrapy.Spider):
    name = "arizona"
    allowed_domains = ["azchildcaresearch.azdes.gov", "azchildcareprovidersearch.azdes.gov"]
    handle_httpstatus_list = [403]

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "RETRY_TIMES": 3,
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 90 * 1000,
        "PLAYWRIGHT_PROCESS_REQUEST_HEADERS": None,
        "USER_AGENT": _UA,
        "DOWNLOADER_MIDDLEWARES": {
            "provider_scrape.spiders.arizona.StealthContextMiddleware": 100,
        },
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "headless": False,
            "channel": "chrome",
            "args": (
                ["--ozone-platform=x11"] if platform.system() == "Linux" else []
            ) + (
                ["--window-size=1920,1080"]
            ),
            "timeout": 30 * 1000,
        },
        "PLAYWRIGHT_CONTEXTS": {
            "default": {
                "ignore_https_errors": True,
                "user_agent": _UA,
                "viewport": {"width": 1920, "height": 1080},
                "device_scale_factor": 1,
                "locale": "en-US",
                "timezone_id": "America/Phoenix",
            }
        },
    }

    def __init__(self, page_size=200, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.page_size = int(page_size)

    def start_requests(self):
        yield scrapy.Request(
            LANDING_PAGE_URL,
            callback=self.parse_search_page,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_load_state", "domcontentloaded", timeout=60000),
                    PageMethod("mouse.move", 150, 150),
                    PageMethod("wait_for_timeout", 5000),
                    PageMethod("reload", wait_until="domcontentloaded", timeout=60000),
                    PageMethod("wait_for_timeout", 3000),
                ],
            },
        )

    async def _humanize_warmup(self, page):
        # Random non-linear mouse path
        for _ in range(random.randint(6, 10)):
            x = random.randint(150, 1000)
            y = random.randint(150, 700)
            try:
                await page.mouse.move(x, y, steps=random.randint(15, 35))
            except Exception:
                break
            await asyncio.sleep(random.uniform(0.3, 0.8))

        # Light scroll
        try:
            await page.mouse.wheel(0, random.randint(150, 350))
            await asyncio.sleep(random.uniform(0.8, 1.5))
            await page.mouse.wheel(0, -random.randint(50, 200))
        except Exception:
            pass

        await asyncio.sleep(random.uniform(2.0, 4.0))
        self.logger.info("Humanize warm-up phase complete")

    async def parse_search_page(self, response):
        page = response.meta["playwright_page"]
        aura_context = None

        try:
            self.logger.info(f"Loaded initial page: {page.url}")
            
            # Click the landing page search button to navigate to the Aura app
            # This helps seed Cloudflare cookies and provides a valid Referer
            landing_btn = page.locator("input[type='button'][value='Search']").first
            if await landing_btn.count() > 0:
                self.logger.info("Clicking landing page Search button to navigate to Aura app...")
                await landing_btn.click()
                
                self.logger.info("Waiting for navigation to Aura app...")
                await page.wait_for_url("**/s/providersearch**", timeout=60000)
                await page.wait_for_load_state("domcontentloaded")
                await page.wait_for_timeout(5000) # Give Aura time to bootstrap
            else:
                self.logger.warning("Landing page Search button not found. Attempting direct navigation fallback...")
                await page.goto(SEARCH_PAGE_URL)
                await page.wait_for_load_state("domcontentloaded")
                await page.wait_for_timeout(5000)

            await self._humanize_warmup(page)
            
            # The search button might take a moment to be rendered by Aura
            self.logger.info("Waiting for the Aura search button to appear...")
            button_selector = "button.search-button:has-text('Search'), input[type='button'][value='Search']"
            try:
                await page.wait_for_selector(button_selector, timeout=30000)
            except Exception:
                self.logger.error("Timed out waiting for the Aura search button to appear. The page might not have loaded correctly (check for 403s in logs).")
                # Log the current page content for debugging
                content = await page.content()
                self.logger.debug(f"Page content at failure: {content[:1000]}")
                return

            search_button = page.locator(button_selector).first
            
            self.logger.info("Clicking the search button to capture aura.context...")
            async with page.expect_response(
                lambda r: "aura" in r.url and r.request.method == "POST",
                timeout=30000
            ) as resp_info:
                await search_button.click()
            
            resp = await resp_info.value
            post_data = resp.request.post_data
            aura_context = extract_form_field(post_data, "aura.context")
            
            if not aura_context:
                self.logger.error("Failed to extract aura.context from initial request.")
                return
            
            self.logger.info("Successfully extracted aura.context. Beginning pagination.")
            
            page_number = 1
            total_extracted = 0
            
            while True:
                self.logger.info(f"Fetching page {page_number}...")
                body = build_search_post_body(self.page_size, page_number, aura_context)
                url = "https://azchildcaresearch.azdes.gov" + AURA_ENDPOINT_PATH
                
                result = await page.evaluate(_FETCH_SCRIPT, {"url": url, "body": body})
                
                if result.get("error"):
                    self.logger.error(f"Fetch failed on page {page_number}: {result.get('error')}")
                    break
                
                if result.get("status") != 200:
                    self.logger.error(f"HTTP {result.get('status')} on page {page_number}")
                    break
                
                try:
                    data = json.loads(result["body"])
                except json.JSONDecodeError as e:
                    self.logger.error(f"Failed to decode JSON on page {page_number}: {e}")
                    break
                
                actions = data.get("actions", [])
                if not actions or actions[0].get("state") != "SUCCESS":
                    self.logger.warning(f"Action state was not SUCCESS on page {page_number}")
                    break
                
                records = actions[0].get("returnValue", {}).get("returnValue", {}).get("records", [])
                
                if not records:
                    self.logger.info(f"No records returned on page {page_number}. Pagination complete.")
                    break
                
                for record in records:
                    yield self.parse_provider(record)
                    total_extracted += 1
                
                self.logger.info(f"Extracted {len(records)} records from page {page_number}. Total so far: {total_extracted}")
                
                page_number += 1
                await asyncio.sleep(random.uniform(1.0, 3.0))

        finally:
            await page.close()

    def parse_provider(self, data):
        item = ProviderItem()
        item["source_state"] = "Arizona"
        
        item["provider_name"] = data.get("title")
        item["az_facility_id"] = data.get("value")
        item["provider_type"] = data.get("providertype")
        item["capacity"] = data.get("slotcapacity")
        item["ages_served"] = data.get("AgeServed")
        item["az_license_type"] = data.get("licensetype")
        item["license_holder"] = data.get("owner")
        if item["license_holder"] and not item["license_holder"].strip():
            item["license_holder"] = None
        
        # Parse rating
        rating = data.get("rating")
        item["sutq_rating"] = str(rating) if rating is not None else None
        
        item["languages"] = data.get("languages")
        item["phone"] = data.get("phone")
        item["provider_website"] = data.get("website")
        item["address"] = data.get("address")
        
        loc = data.get("location", {})
        item["latitude"] = loc.get("Latitude")
        item["longitude"] = loc.get("Longitude")
        
        # AZ Specific Flags
        item["az_operatinghourid"] = data.get("operatinghourid")
        item["az_affiliation"] = data.get("affiliation")
        item["az_regionalpartnership"] = data.get("regionalpartnership")
        item["az_shiftcomment"] = data.get("shiftcomment")
        item["az_headstart"] = data.get("headstart")
        item["az_desprovider"] = data.get("desprovider")
        item["az_status_label"] = data.get("statusLabel")
        item["az_first_slot_start"] = data.get("firstSlotStart")
        item["az_first_slot_end"] = data.get("firstSlotEnd")
        
        item["inspections"] = self.parse_inspections(data.get("dhsenforcements", []))
        return item
    
    def parse_inspections(self, enforcements):
        inspections = []
        for enc in enforcements:
            ins = InspectionItem()
            ins["date"] = enc.get("InspectionDate__c")
            ins["type"] = enc.get("InspectionType__c")
            ins["az_regulation"] = enc.get("Regulation__c")
            ins["az_decision_correction"] = enc.get("Decision_Correction__c")
            ins["az_date_resolved"] = enc.get("DateResolved__c")
            ins["az_civil_penalty"] = enc.get("CIVIL_PENALTY__c")
            ins["az_enforcement_name"] = enc.get("Name")
            inspections.append(ins)
        return inspections
        return inspections

import scrapy
from scrapy_playwright.page import PageMethod
from ..items import ProviderItem
import logging
import re

class NewMexicoSpider(scrapy.Spider):
    name = 'new_mexico'
    allowed_domains = ['childcare.ececd.nm.gov']
    start_urls = ['https://childcare.ececd.nm.gov/search']

    custom_settings = {
        'PLAYWRIGHT_BROWSER_TYPE': 'chromium',
        'PLAYWRIGHT_LAUNCH_OPTIONS': {
            'headless': True,
        },
        'ROBOTSTXT_OBEY': False,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    }

    def start_requests(self):
        yield scrapy.Request(
            self.start_urls[0],
            meta={
                'playwright': True,
                'playwright_include_page': True,
            },
            callback=self.parse_search_results,
            errback=self.errback_close_page,
        )

    def __init__(self, max_clicks=None, *args, **kwargs):
        super(NewMexicoSpider, self).__init__(*args, **kwargs)
        self.max_clicks = int(max_clicks) if max_clicks else None

    async def parse_search_results(self, response):
        page = response.meta['playwright_page']
        self.logger.info("Visited search page. Loading results...")

        try:
            # Wait for the first set of results
            await page.wait_for_selector('.listing-card')
            
            # Get total count if possible to log progress
            total_expected = 0
            try:
                total_count_text = await page.inner_text('[data-cy="showing-listings-count"]')
                self.logger.info(f"Initial results info: {total_count_text}")
                total_match = re.search(r'of ([\d,]+) programs', total_count_text)
                if total_match:
                    total_expected = int(total_match.group(1).replace(',', ''))
                    self.logger.info(f"Targeting {total_expected} total programs.")
            except Exception as e:
                self.logger.warning(f"Could not determine total expected count: {e}")

            # Keep clicking "Show More Results" until it disappears or we reach the total/limit
            consecutive_no_increase = 0
            clicks = 0
            
            while True:
                if self.max_clicks and clicks >= self.max_clicks:
                    self.logger.info(f"Reached max_clicks limit of {self.max_clicks}. Stopping.")
                    break

                show_more_button = page.locator('a:has-text("Show More Results")')
                current_count = await page.locator('.listing-card').count()
                
                if total_expected and current_count >= total_expected:
                    self.logger.info(f"Reached expected count of {total_expected}. Stopping.")
                    break

                if await show_more_button.is_visible():
                    self.logger.info(f"Current results count: {current_count}. Clicking 'Show More Results' (Click {clicks + 1})...")
                    await show_more_button.click()
                    clicks += 1
                    
                    try:
                        await page.wait_for_function(
                            f"document.querySelectorAll('.listing-card').length > {current_count}",
                            timeout=15000
                        )
                        consecutive_no_increase = 0
                    except Exception as e:
                        self.logger.warning(f"Timeout or no increase after clicking 'Show More Results': {e}")
                        consecutive_no_increase += 1
                        if consecutive_no_increase >= 3:
                            self.logger.error("Results count not increasing after 3 attempts. Breaking loop.")
                            break
                else:
                    self.logger.info(" 'Show More Results' button no longer visible.")
                    break
                
                last_count = current_count

            # Final check of count
            final_count = await page.locator('.listing-card').count()
            self.logger.info(f"Finished loading results. Final count in DOM: {final_count}")

            # Extract links
            content = await page.content()
            sel = scrapy.Selector(text=content)
            links = sel.css('.listing-card a[href^="/nm/"]::attr(href)').getall()
            # De-duplicate links (each card might have multiple links to the same detail page)
            unique_links = list(set(links))
            self.logger.info(f"Extracted {len(unique_links)} unique detail links.")

            for link in unique_links:
                yield scrapy.Request(
                    response.urljoin(link),
                    callback=self.parse_detail,
                    meta={'source_state': 'NM'}
                )

        except Exception as e:
            self.logger.error(f"Error during loading results: {e}")
        finally:
            await page.close()

    def parse_detail(self, response):
        provider = ProviderItem()
        provider['source_state'] = 'NM'
        provider['provider_url'] = response.url

        provider['provider_name'] = response.css('h1#listing-name::text').get('').strip()
        
        # Administrator
        provider['administrator'] = response.xpath('//div[div[contains(., "Program Director")]]/div[contains(@class, "text-lg")]/text()').get('').strip()
        
        # Provider Type
        provider['provider_type'] = response.css('#program-type-text::text').get('').strip()
        
        # License Number
        license_text = response.css('#license-number a::text').get('').strip()
        if license_text.startswith('#'):
            provider['license_number'] = license_text[1:]
        else:
            provider['license_number'] = license_text

        # Address
        address_div = response.xpath('//div[@id="location"]//div[contains(@class, "flex-col") and contains(@class, "pl-12")]')
        address_lines = address_div.css('div::text').getall()
        provider['address'] = ', '.join([line.strip() for line in address_lines if line.strip()])

        # Latitude and Longitude
        static_map_url = response.xpath('//div[@id="location"]//img[contains(@src, "staticmap")]/@src').get('')
        if 'center=' in static_map_url:
            try:
                center = static_map_url.split('center=')[1].split('&')[0]
                lat, lon = center.split(',')
                provider['latitude'] = lat.strip()
                provider['longitude'] = lon.strip()
            except Exception:
                self.logger.warning(f"Failed to extract lat/lon from {static_map_url}")

        # Hours
        hours_div = response.xpath('//div[@id="hours"]//div[contains(@class, "pl-12")]')
        hours_lines = hours_div.css('div::text').getall()
        provider['hours'] = ', '.join([line.strip() for line in hours_lines if line.strip()])

        # NM Star Level
        star_level = "".join(response.xpath('//div[@id="network_rating"]//text()[contains(., "Star Level")]').getall()).strip()
        provider['nm_star_level'] = star_level

        # Ages Served
        # Primary: from availability banner (take first match to avoid mobile/desktop duplication)
        ages_banner = response.xpath('(//div[contains(@id, "availablity-banner")]//text()[contains(., "enrollments")])[1]').get('').strip()
        if ages_banner:
            ages_served = ages_banner.replace('Accepting new enrollments:', '').strip()
        else:
            ages_served = ""
        
        # Fallback: from pricing tabs
        if not ages_served:
            tab_ages = [t.strip() for t in response.css(".tab-picker-tabs::text").getall() if t.strip()]
            if tab_ages:
                ages_served = ", ".join(tab_ages)
        
        provider['ages_served'] = ages_served

        # Robust helper for structured fields
        def extract_structured_val(id_val):
            # Target text nodes within the container div, excluding those within the label div (font-semibold),
            # tooltips (absolute), and SVGs. Also normalize spaces.
            nodes = response.xpath(f'//div[@id="{id_val}"]//text()[not(ancestor::div[contains(@class, "font-semibold")]) and not(ancestor::div[contains(@class, "absolute")]) and not(ancestor::svg)]').getall()
            return " ".join([n.strip() for n in nodes if n.strip()]).strip()

        # Meals
        provider['nm_meals'] = extract_structured_val("meals-provided")
        
        # Snacks
        provider['nm_snacks'] = extract_structured_val("snacks-provided")

        # Potty Training
        provider['nm_potty_training'] = extract_structured_val("potty-training-required")

        # Schedules
        provider['nm_schedule'] = extract_structured_val("days-per-week")

        # Languages
        provider['languages'] = extract_structured_val("language-supported")

        # Pay Schedules
        provider['nm_pay_schedules'] = extract_structured_val("pay-schedules")

        # Child Care Assistance / Scholarships
        provider['scholarships_accepted'] = extract_structured_val("subsidy-accepted")

        yield provider

    async def errback_close_page(self, failure):
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()
        self.logger.error(repr(failure))

import scrapy
from scrapy_playwright.page import PageMethod
from provider_scrape.items import ProviderItem, InspectionItem
import re

class MontanaSpider(scrapy.Spider):
    name = "montana"
    allowed_domains = ["mtdphhs.my.site.com"]
    start_urls = ["https://mtdphhs.my.site.com/MAQCSChildCareLicensing/s/provider-search?language=en_US"]
    
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                },
                callback=self.parse_search_page,
            )

    async def parse_search_page(self, response):
        page = response.meta["playwright_page"]

        try:
            self.logger.info("Waiting for Age Group checkboxes...")
            await page.wait_for_selector("input[name='ageCategory']", timeout=10000)
            
            # Select all "Age Groups" checkboxes using native Playwright clicks on the associated labels
            checkboxes = await page.locator("input[name='ageCategory']").all()
            for cb in checkboxes:
                cb_id = await cb.get_attribute("id")
                if cb_id:
                    await page.locator(f"label[for='{cb_id}']").click(force=True)
                    await page.wait_for_timeout(500)
            self.logger.info("Checked Age Group checkboxes.")
            
            # Click the Search button using native Playwright click
            await page.locator("button.slds-button_brand").first.click(force=True)
            self.logger.info("Clicked Search button.")
            
            # Wait for results to load
            await page.wait_for_selector("article.provider-card", timeout=15000)
            
            # Handle potential "More" / Pagination (Salesforce communities often use infinite scroll or a 'More' button)
            # The user requested to log transitions.
            previous_count = 0
            while True:
                cards = await page.locator("article.provider-card").count()
                if cards > previous_count:
                    self.logger.info(f"Pagination/Scroll transition: Found {cards} providers so far.")
                    previous_count = cards
                
                # Scroll to bottom to trigger infinite scroll if it exists
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                await page.wait_for_timeout(2000)
                
                # Check for a "More" or "Load More" button
                more_btns = await page.evaluate('''() => {
                    return Array.from(document.querySelectorAll('button'))
                                .filter(b => b.innerText.trim().toLowerCase() === 'more' || b.innerText.trim().toLowerCase() === 'load more')
                                .length;
                }''')
                
                if more_btns > 0:
                    await page.evaluate('''() => {
                        const btns = Array.from(document.querySelectorAll('button'))
                                          .filter(b => b.innerText.trim().toLowerCase() === 'more' || b.innerText.trim().toLowerCase() === 'load more');
                        if (btns.length > 0) btns[0].click();
                    }''')
                    await page.wait_for_timeout(2000)
                else:
                    new_cards = await page.locator("article.provider-card").count()
                    if new_cards == previous_count:
                        # No new cards loaded after scroll/wait
                        break
            
            # Once fully loaded, get the HTML content
            html = await page.content()
            sel = scrapy.Selector(text=html)
            
            cards = sel.css('article.provider-card')
            self.logger.info(f"Total providers found: {len(cards)}")
            
            for card in cards:
                pid = card.css('lightning-button[data-pid]::attr(data-pid)').get()
                lat = card.css('lightning-button[data-lat]::attr(data-lat)').get()
                lon = card.css('lightning-button[data-lon]::attr(data-lon)').get()
                
                if pid:
                    detail_url = f"https://mtdphhs.my.site.com/MAQCSChildCareLicensing/s/provider-detail?language=en_US&pid={pid}"
                    yield scrapy.Request(
                        detail_url,
                        callback=self.parse_detail_page,
                        meta={
                            "latitude": lat,
                            "longitude": lon,
                            "playwright": True,
                            "playwright_include_page": True
                        }
                    )
        finally:
            await page.close()

    async def parse_detail_page(self, response):
        page = response.meta.get("playwright_page")
        
        # If we are using Playwright, wait for the page to render and extract the dynamic HTML
        if page:
            try:
                # Wait for the "Provider Name" label to appear
                await page.wait_for_selector("span:has-text('Provider Name')", timeout=15000)
                html = await page.content()
                # Overwrite the response with the fully rendered HTML
                response = response.replace(body=html)
            except Exception as e:
                self.logger.error(f"Timeout or error loading detail page {response.url}: {e}")
            finally:
                await page.close()

        item = ProviderItem()
        item['source_state'] = 'Montana'
        item['provider_url'] = response.url
        item['latitude'] = response.meta.get('latitude')
        item['longitude'] = response.meta.get('longitude')
        
        # Helper function to extract a value based on its label
        def extract_by_label(label_text):
            val = response.xpath(f'//span[text()="{label_text}"]/following-sibling::div//text()').get()
            return val.strip() if val else None

        item['provider_name'] = extract_by_label("Provider Name")
        item['license_number'] = extract_by_label("Provider Number")
        item['capacity'] = extract_by_label("Capacity")
        item['status'] = extract_by_label("License Status")
        item['license_begin_date'] = extract_by_label("Effective Date")
        item['license_expiration'] = extract_by_label("Expiration Date")
        item['mt_license_type'] = extract_by_label("Provider Type")
        item['provider_type'] = item['mt_license_type']
        item['ages_served'] = extract_by_label("Min Age to Max Age")
        item['address'] = extract_by_label("Address")
        item['phone'] = extract_by_label("Contact Information")

        # Parse Inspections table
        inspections = []
        rows = response.xpath('//table[contains(@class, "slds-table")]//tr')
        if not rows:
            # Fallback to just grabbing all tr elements since there's typically only one table
            rows = response.xpath('//table//tr')

        for row in rows[1:]:  # Skip header row
            cols = row.xpath('.//td')
            if len(cols) >= 3:
                insp_item = InspectionItem()
                insp_item['date'] = cols[0].xpath('.//text()').get(default='').strip()
                insp_item['type'] = cols[1].xpath('.//text()').get(default='').strip()
                insp_item['mt_inspector_name'] = cols[2].xpath('.//text()').get(default='').strip()
                
                # The 4th column has 'View File', we could potentially extract a report URL if it's a link
                link = cols[3].xpath('.//a/@href').get()
                if link:
                    insp_item['report_url'] = response.urljoin(link)
                
                if insp_item['date'] or insp_item['type']:
                    inspections.append(insp_item)
                    
        item['inspections'] = inspections
        yield item
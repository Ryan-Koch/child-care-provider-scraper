import scrapy
from provider_scrape.items import ProviderItem, InspectionItem
from scrapy.selector import Selector
import re

class AlaskaSpider(scrapy.Spider):
    name = 'alaska'
    allowed_domains = ['health.alaska.gov', 'findccprovider.health.alaska.gov']
    start_urls = ['https://findccprovider.health.alaska.gov/']

    def start_requests(self):
        yield scrapy.Request(
            self.start_urls[0],
            meta={
                'playwright': True,
                'playwright_include_page': True,
                'playwright_context_args': {
                    "ignore_https_errors": True,
                }
            }
        )

    async def parse(self, response):
        page = response.meta['playwright_page']
        try:
            # 1. Handle Agreement if present
            await page.wait_for_selector('#app', timeout=20000)
            
            # Wait for loader to disappear
            try:
                await page.wait_for_selector('.loader', state='hidden', timeout=45000)
                self.logger.info("Loader disappeared.")
            except:
                self.logger.warning("Loader did not disappear or wasn't found.")

            # Attempt to click agreement
            agreement_btn = page.locator("button", has_text=re.compile(r"Accept|Agree", re.IGNORECASE))
            if await agreement_btn.count() > 0:
                self.logger.info("Found Agreement button, clicking...")
                await agreement_btn.first.click()
                await page.wait_for_timeout(2000)

            # 2. Search Page
            search_btn = page.locator("#app button", has_text=re.compile(r"Search|Submit", re.IGNORECASE))
            if await search_btn.count() > 0:
                self.logger.info("Found Search button, clicking...")
                await search_btn.first.click()
                
                # 3. Wait for Results
                try:
                    await page.wait_for_selector('table', timeout=20000)
                    await page.wait_for_timeout(5000) # Wait for rows to render
                    self.logger.info("Results table loaded.")
                except:
                    self.logger.error("Results table did not load.")
                    return

                # 4. Extract Links and Names
                content = await page.content()
                sel = Selector(text=content)
                
                # Find all rows in the results table
                rows = sel.css('table tr')
                self.logger.info(f"Found {len(rows)} rows in the table.")
                
                for row in rows:
                    link_node = row.css('a[href*="ProviderInfo"]')
                    if not link_node:
                        continue
                        
                    link = link_node.css('::attr(href)').get()
                    
                    # Try to get name from cells or the link text
                    name = row.css('td:nth-child(1)::text').get()
                    if not name or name.strip() == "" or name.strip().lower() == "details":
                        name = row.css('td:nth-child(2)::text').get()
                    if not name or name.strip() == "" or name.strip().lower() == "details":
                        name = link_node.css('::text').get()
                    
                    if name and name.strip().lower() == "details":
                        name = None

                    yield response.follow(
                        link,
                        callback=self.parse_detail,
                        meta={
                            'playwright': True,
                            'playwright_include_page': True,
                            'playwright_context_args': {
                                "ignore_https_errors": True,
                            },
                            'provider_name': name.strip() if name else None
                        }
                    )
            else:
                self.logger.warning("Search button not found.")

        finally:
            await page.close()

    async def parse_detail(self, response):
        page = response.meta['playwright_page']
        try:
            await page.wait_for_selector('#app', timeout=20000)
            
            # Handle Agreement again if redirected
            agreement_btn = page.locator("button", has_text=re.compile(r"Accept|Agree", re.IGNORECASE))
            if await agreement_btn.count() > 0 and await agreement_btn.first.is_visible():
                self.logger.info("Agreement found on detail page, clicking...")
                await agreement_btn.first.click()
                await page.wait_for_timeout(2000)

            # Wait for loader to disappear
            try:
                await page.wait_for_selector('.loader', state='hidden', timeout=45000)
            except:
                self.logger.warning("Loader did not disappear on detail page.")

            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(3000)

            content = await page.content()
            item = self.extract_detail(content, response.url)
            
            # Use name from meta if detail page extraction is empty
            if not item.get('provider_name') or item['provider_name'].lower() == 'details':
                if response.meta.get('provider_name'):
                    item['provider_name'] = response.meta['provider_name']
                
            yield item
            
        finally:
            await page.close()

    def extract_detail(self, html, url):
        sel = Selector(text=html)
        item = ProviderItem()
        item['source_state'] = 'AK'
        item['provider_url'] = url
        
        app_sel = sel.css('#app')
        if not app_sel:
            app_sel = sel

        full_text = " ".join(app_sel.xpath('.//text()[not(parent::script or parent::style)]').getall())
        full_text = re.sub(r'\s+', ' ', full_text).strip()
        
        def extract_field(regex):
            match = re.search(regex, full_text, re.IGNORECASE)
            return match.group(1).strip() if match else None

        # Basic Fields
        item['provider_name'] = extract_field(r'Facility Name:\s*(.*?)(?:First Name|$)')

        first_name = extract_field(r'First Name:\s*(.*?)(?:Last Name|$)')
        last_name = extract_field(r'Last Name:\s*(.*?)(?:Provider|$)')
        if first_name and last_name:
            item['administrator'] = f"{first_name} {last_name}"
        elif first_name or last_name:
            item['administrator'] = (first_name or last_name)

        url_match = re.search(r'/ProviderInfo/(\d+)', url)
        if url_match:
            item['license_number'] = url_match.group(1)
        else:
            item['license_number'] = extract_field(r'License\s*(?:Number|#|ID)?\s*:?\s*([A-Z0-9-]+)')

        item['phone'] = extract_field(r'Phone\s*(?:Number)?\s*:?\s*(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})')
        item['address'] = extract_field(r'Address\s*:?\s*(.*?)(?:Phone|License|Email|Status|Capacity|Compliance|$)')
        item['status'] = extract_field(r'(?:Facility\s*)?Status\s*:?\s*(\w+)')
        item['capacity'] = extract_field(r'Capacity\s*:?\s*(\d+)')
        item['email'] = extract_field(r'Email\s*:?\s*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})')
        
        # New Fields identified from logs
        item['status_date'] = extract_field(r'License\s*Effective\s*Date\s*:?\s*(\d{2}/\d{2}/\d{4})')
        item['license_begin_date'] = item['status_date']
        item['license_expiration'] = extract_field(r'License\s*Expiration\s*Date\s*:?\s*(\d{2}/\d{2}/\d{4})')
        item['ages_served'] = extract_field(r'Children\s*Age\s*Range\s*:?\s*(.*?)(?:Phone|Address|Compliance|$)')
        
        # Inspections
        item['inspections'] = []
        seen_inspections = set()
        insp_pattern = r'(\d{2}/\d{2}/\d{4}|Not Available)\s+(INSPECTION|COMPLAINT)(.*?)(IN-COMPLIANCE|NON-COMPLIANCE|SUBSTANTIATED|UNSUBSTANTIATED|N/A)\s+(.*?)\s+Details'
        matches = re.finditer(insp_pattern, full_text, re.IGNORECASE)
        for m in matches:
            date = m.group(1).strip()
            type_part = m.group(3).strip()
            type_str = f"{m.group(2)} {type_part}".strip()
            # Normalize whitespace
            type_str = " ".join(type_str.split())
            
            findings = m.group(4).strip()
            action = m.group(5).strip()
            
            fingerprint = (date, type_str, findings, action)
            if fingerprint not in seen_inspections:
                insp = InspectionItem()
                insp['date'] = date
                insp['type'] = type_str
                insp['original_status'] = findings
                insp['corrective_status'] = action
                item['inspections'].append(insp)
                seen_inspections.add(fingerprint)
            
        return item

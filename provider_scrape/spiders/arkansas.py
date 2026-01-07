import scrapy
from provider_scrape.items import ProviderItem, InspectionItem
import re

class ArkansasSpider(scrapy.Spider):
    name = "arkansas"
    allowed_domains = ["ardhslicensing.my.site.com"]
    start_urls = ["https://ardhslicensing.my.site.com/elicensing/s/search-provider/find-provider-cc?language=en_US&tab=CC"]

    custom_settings = {
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "headless": True,
        },
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 60000,
    }

    def start_requests(self):
        yield scrapy.Request(
            self.start_urls[0],
            meta={
                "playwright": True,
                "playwright_include_page": True,
            },
            callback=self.parse_search_page,
        )

    async def parse_search_page(self, response):
        page = response.meta["playwright_page"]
        
        try:
            self.logger.info("Page loaded, waiting for network idle...")
            try:
                await page.wait_for_load_state("networkidle", timeout=45000)
            except:
                self.logger.warning("Network idle timeout, proceeding...")

            await page.wait_for_timeout(5000) # Give it a moment to render

            # DEBUG: Log the text content of the page to understand what we are seeing
            content_text = await page.inner_text("body")
            self.logger.info(f"VISIBLE TEXT ON PAGE:\n{content_text[:2000]}...") # Log first 2000 chars

            # 1. Search Logic
            # User instruction: "go to the star level and select 'all levels'"
            # Let's try to identify the combobox or filter.
            
            # Common Salesforce LWC combobox pattern
            # We look for a placeholder or label
            
            # Strategy:
            # 1. Interact with filters to enable Search.
            # 2. Click Search.
            # 3. Switch to List View if needed.
            
            # The Search button is disabled initially. We need to select a filter.
            # "Star Level (Better Beginnings)" -> "Select an Option"
            
            try:
                # Find the 'Select an Option' button specifically for Star Level
                # We can try to find the button that has 'Select an Option' text.
                # Since there might be multiple, we grab the one that appears after 'Star Level' text or just try the first one that works.
                # However, the log text shows "Star Level ... Select an Option".
                
                # Try finding the combobox for Star Level
                # Salesforce comboboxes often have a button with 'Select an Option'
                star_level_dropdown = page.locator("button:has-text('Select an Option')").first
                if await star_level_dropdown.count() > 0:
                    self.logger.info("Found dropdown, attempting to select option...")
                    await star_level_dropdown.click()
                    await page.wait_for_timeout(1000)
                    
                    # Select the first option that isn't 'Select an Option' or explicitly 'All Levels'
                    # Usually the dropdown items are in a listbox.
                    # We'll try to find an item with text 'All' or just the first item.
                    # Let's try to click "All Levels" or "1 Star" if all isn't there, but usually for scrapers we want everything.
                    # If we can't find specific text, we click the first available option.
                    
                    options = page.locator("lightning-base-combobox-item")
                    if await options.count() > 0:
                        # Try to find one that says "All"
                        all_option = options.filter(has_text="All")
                        if await all_option.count() > 0:
                            await all_option.first.click()
                            self.logger.info("Selected 'All' option.")
                        else:
                            # Just click the first real option
                            await options.first.click()
                            self.logger.info("Selected first available option.")
                    else:
                        self.logger.warning("No options found in dropdown.")
                        
                    await page.wait_for_timeout(1000)
            except Exception as e:
                self.logger.warning(f"Error interacting with dropdown: {e}")

            # Now try to find the Search button and wait for it to be enabled
            search_btn = page.locator("button:has-text('Search')").first
            if await search_btn.count() > 0:
                 self.logger.info("Attempting to click Search button...")
                 # Log if it's disabled
                 if not await search_btn.is_enabled():
                     self.logger.warning("Search button appears disabled. Attempting force click...")
                 
                 try:
                     await search_btn.click(force=True, timeout=5000)
                     self.logger.info("Search button clicked (forced or normal).")
                     await page.wait_for_timeout(5000) # Wait for results load
                 except Exception as e:
                     self.logger.warning(f"Search click failed: {e}")
            
            # Switch to List View
            # We want to ensure we are in list view.
            try:
                # Find the element with text "List View" and click it
                list_view_btn = page.locator("text='List View'")
                if await list_view_btn.count() > 0:
                    self.logger.info("Clicking List View toggle...")
                    await list_view_btn.first.click(force=True)
                    await page.wait_for_timeout(3000)
            except Exception as e:
                self.logger.warning(f"List View toggle failed: {e}")
            
            # Pagination Loop
            self.logger.info("Starting pagination loop...")
            while True:
                # Wait for results
                try:
                    # Wait for the "View" buttons which contain the record ID in the 'name' attribute
                    await page.wait_for_selector("button[name^='a0k']", timeout=10000)
                except:
                    self.logger.warning("No profile buttons found on this page. Dumping HTML.")
                    content = await page.content()
                    with open("arkansas_debug.html", "w", encoding="utf-8") as f:
                        f.write(content)
                    break

                # Extract links
                content = await page.content()
                sel = scrapy.Selector(text=content)
                
                # Unconditional debug dump
                # self.logger.info("Dumping HTML for inspection...")
                # with open("arkansas_debug.html", "w", encoding="utf-8") as f:
                #     f.write(content)

                # Extract IDs from the "View" buttons
                # The buttons have name="a0k..."
                ids = sel.css("button[name^='a0k']::attr(name)").getall()
                self.logger.info(f"Found {len(ids)} providers on this page.")
                
                # DEBUG: Click the first one to see where it goes
                # Removed debug click logic

                for pid in ids:
                    # Construct the URL based on what we saw in debug: 
                    # /elicensing/s/search-provider/facility-details-cc?tab=CC&fid={pid}&language=en_US
                    relative_url = f"/elicensing/s/search-provider/facility-details-cc?tab=CC&fid={pid}&language=en_US"
                    url = response.urljoin(relative_url)
                    yield scrapy.Request(
                        url,
                        meta={
                            "playwright": True,
                            "playwright_include_page": True, 
                        },
                        callback=self.parse_detail
                    )

                # Next Button
                # Based on HTML inspection, the next button is inside an anchor with class 'next-link'
                # and the button itself has title 'chevronright'.
                
                next_button = page.locator(".next-link button").first
                
                if await next_button.count() > 0:
                    # Check if disabled
                    is_disabled = await next_button.is_disabled()
                    
                    if not is_disabled:
                        # Check specific Salesforce disabled attributes/styles just in case
                        style = await page.locator(".next-link lightning-button-icon").first.get_attribute("style")
                        if style and "pointer-events: none" in style:
                            is_disabled = True

                    if not is_disabled:
                        self.logger.info("Clicking Next page (.next-link button)...")
                        await next_button.click()
                        await page.wait_for_timeout(5000) # Wait for page reload
                    else:
                        self.logger.info("Next button found but is disabled. Pagination complete.")
                        break
                else:
                    self.logger.info("No next button found (.next-link button). Pagination complete.")
                    break

        except Exception as e:
            self.logger.error(f"Error in search page parsing: {e}")
        finally:
            await page.close()

    async def parse_detail(self, response):
        page = response.meta["playwright_page"]
        try:
            # Wait for network idle to ensure hydration
            try:
                await page.wait_for_load_state("networkidle", timeout=30000)
            except:
                self.logger.warning("Timeout waiting for network idle on detail page")

            # Wait for content - "Facility Number" seems to be a reliable label
            try:
                await page.wait_for_selector("text=Facility Number", timeout=20000)
            except:
                self.logger.warning(f"Timeout waiting for Facility Number on {response.url}. Waiting a bit more...")
                await page.wait_for_timeout(5000)

            content = await page.content()
            sel = scrapy.Selector(text=content)

            item = ProviderItem()
            item["source_state"] = "AR"
            item["provider_url"] = response.url
            
            # Basic extraction
            # Try multiple selectors for title
            item["provider_name"] = sel.css(".forceHighlightsPanel .slds-page-header__title::text").get(default="").strip()
            if not item["provider_name"]:
                 # Fallback to h2
                 item["provider_name"] = sel.xpath("//h2[contains(@class, 'slds-align-middle')]/text()").get(default="").strip()
            
            # Helper to extract by label
            def get_field_by_label(label):
                # Strategy 1: Standard Salesforce View (test-id)
                val = sel.xpath(f"//span[contains(@class, 'test-id__field-label') and contains(text(), '{label}')]/../../div[contains(@class, 'test-id__field-value')]//text()").getall()
                if val: return val
                
                # Strategy 2: LWC structure (Label div followed by Value component/div)
                # Matches: <div>Label</div> <lightning-formatted-rich-text>...</lightning-formatted-rich-text>
                # Also handles cases where label is in a bold div
                val = sel.xpath(f"//div[contains(text(), '{label}')]/following-sibling::*[1]//text()").getall()
                if val: return val
                
                return []

            address_parts = get_field_by_label("Address")
            
            # Extract Website specifically
            website_parts = get_field_by_label("Website Address")
            website_url = ""
            if website_parts:
                website_url = "".join([p.strip() for p in website_parts if p.strip()])
                item["provider_website"] = website_url

            if address_parts:
                full_parts = [p.strip() for p in address_parts if p.strip()]
                combined_text = " ".join(full_parts)

                # Extract emails
                email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
                found_emails = re.findall(email_pattern, combined_text)

                # Remove emails from address text
                clean_address = combined_text
                for email in found_emails:
                    clean_address = clean_address.replace(email, "")
                
                # Remove specifically extracted website from address if present
                if website_url:
                    clean_address = clean_address.replace(website_url, "")

                # Cleanup whitespace
                clean_address = re.sub(r'\s+', ' ', clean_address).strip()

                item["address"] = clean_address
                if found_emails:
                    item["email"] = ", ".join(found_emails)

            phone = get_field_by_label("Phone") or get_field_by_label("Site Phone")
            item["phone"] = phone[0].strip() if phone else None

            license_num = get_field_by_label("License Number") or get_field_by_label("Facility Number")
            item["license_number"] = license_num[0].strip() if license_num else None

            capacity = get_field_by_label("Total Allowed Capacity") or get_field_by_label("Capacity")
            item["capacity"] = capacity[0].strip() if capacity else None
            item["ar_total_capacity"] = item["capacity"]

            rating = get_field_by_label("Quality Rating") or get_field_by_label("Star Level") or get_field_by_label("Better Beginnings")
            if rating:
                item["ar_quality_rating"] = rating[0].strip()
            else:
                # Try counting stars
                rating_imgs = sel.xpath("//div[contains(@class, 'font-bold')][contains(text(), 'Better Beginnings')]/following-sibling::*[1]//img[@alt='star']")
                if rating_imgs:
                    item["ar_quality_rating"] = str(len(rating_imgs))
                else:
                    item["ar_quality_rating"] = None
            
            regulation = get_field_by_label("Regulation Type") or get_field_by_label("Facility Type")
            item["ar_regulation_type"] = regulation[0].strip() if regulation else None

            program_type = get_field_by_label("Program Type")
            item["ar_program_type"] = program_type[0].strip() if program_type else None

            # Facility Visits
            inspections = []
            try:
                # Try locating the tab or link
                visits_link = page.locator("a:has-text('View Facility Visits'), span:has-text('View Facility Visits')")
                if await visits_link.count() > 0:
                    await visits_link.first.click()
                    await page.wait_for_load_state("networkidle")
                    await page.wait_for_timeout(2000)
                    
                    visits_content = await page.content()
                    v_sel = scrapy.Selector(text=visits_content)
                    
                    rows = v_sel.css("table tbody tr")
                    for row in rows:
                        i_item = InspectionItem()
                        cols = row.css("td")
                        if len(cols) >= 3:
                            i_item["date"] = "".join(cols[0].css("::text").getall()).strip()
                            i_item["type"] = "".join(cols[1].css("::text").getall()).strip()
                            # Report URL
                            report_link = row.css("a::attr(href)").get()
                            if report_link:
                                i_item["report_url"] = response.urljoin(report_link)
                            inspections.append(i_item)
            except Exception as e:
                self.logger.warning(f"Could not extract facility visits: {e}")

            item["inspections"] = inspections
            yield item

        except Exception as e:
            self.logger.error(f"Error parsing detail page {response.url}: {e}")
        finally:
            await page.close()
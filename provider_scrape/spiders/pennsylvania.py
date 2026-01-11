import re

import scrapy
from scrapy_playwright.page import PageMethod

from provider_scrape.items import ProviderItem


class PennsylvaniaSpider(scrapy.Spider):
    name = "pennsylvania"
    allowed_domains = ["compass.dhs.pa.gov"]
    start_url = "https://www.compass.dhs.pa.gov/providersearch/#/advancedsearch"

    def start_requests(self):
        # County IDs range from 01 to 67
        counties = [f"{i:02d}" for i in range(1, 68)]

        # Chunk counties into size 1 to isolate failures
        chunk_size = 1
        for i in range(0, len(counties), chunk_size):
            county_chunk = counties[i : i + chunk_size]
            yield scrapy.Request(
                url=self.start_url,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "counties": county_chunk,
                    "download_timeout": 3600,  # 1 hour timeout for large counties
                },
                dont_filter=True,  # Allow multiple requests to the same URL for different counties
                callback=self.search_counties,
                errback=self.errback_close_page,
            )

    async def search_counties(self, response):
        page = response.meta["playwright_page"]
        counties = response.meta["counties"]
        self.logger.info(f"Starting search for counties: {counties}")

        try:
            # Wait for initial load
            await page.wait_for_load_state("networkidle")

            # Click "By County" tab
            await page.click("#county-tab-select")
            # Wait for any re-rendering
            await page.wait_for_timeout(1000)

            # Open County Dropdown
            dropdown_toggle = (
                'label[id="lbl-county-county-form"] + div button.dropdown-toggle'
            )
            await page.wait_for_selector(dropdown_toggle, state="visible")
            await page.click(dropdown_toggle)

            # Wait for the dropdown menu to appear
            await page.wait_for_selector("#county-county-form", state="visible")

            # Select specific counties
            for county_id in counties:
                self.logger.info(f"Selecting county {county_id} and beginning search.")
                input_id = f"county-county-form{county_id}"
                label_selector = f'label[for="{input_id}"]'
                try:
                    await page.wait_for_selector(
                        label_selector, state="visible", timeout=2000
                    )
                    await page.click(label_selector)
                except:
                    self.logger.warning(f"Could not click label for county {county_id}")

            # Close County Dropdown
            await page.click(dropdown_toggle)
            await page.wait_for_selector("#county-county-form", state="hidden")

            # Select "Child Care" program
            cc_label = 'label[for="CCP_program"]'
            await page.locator(cc_label).scroll_into_view_if_needed()
            await page.click(cc_label)

            # Open "Children's Ages" Dropdown
            age_dropdown_toggle = (
                'label[id="lbl-county-carelevel"] + div button.dropdown-toggle'
            )
            await page.click(age_dropdown_toggle)
            await page.wait_for_selector("#county-carelevel", state="visible")

            # Select all age groups
            age_inputs = await page.query_selector_all(
                '#county-carelevel input[type="checkbox"]'
            )
            for inp in age_inputs:
                id_attr = await inp.get_attribute("id")
                if id_attr:
                    await page.click(f'label[for="{id_attr}"]')

            # Close Age Dropdown
            await page.click(age_dropdown_toggle)
            await page.wait_for_selector("#county-carelevel", state="hidden")

            # Click "Find a Provider"
            find_btn_selector = "#search-btn"
            await page.wait_for_selector(find_btn_selector, state="visible")
            await page.click(find_btn_selector)

            # Wait for results
            try:
                # Wait for loader to disappear first
                await page.wait_for_selector(
                    "compass-ui-cw-loader", state="hidden", timeout=60000
                )
                await page.wait_for_selector(".result-box", timeout=60000)
            except:
                self.logger.info(f"No results found for counties {counties}")
                return

            # Pagination Loop
            page_num = 1
            while True:
                self.logger.info(f"Processing page {page_num} for counties {counties}")

                # Identify current items range to verify pagination later
                try:
                    await page.wait_for_selector(
                        ".result-box", state="visible", timeout=10000
                    )
                except:
                    pass  # Continue to check count

                results_count = await page.locator(".result-box").count()
                self.logger.info(f"Found {results_count} results on page {page_num}")

                if results_count == 0:
                    self.logger.warning(
                        f"No results found on page {page_num}. Waiting and retrying..."
                    )
                    await page.wait_for_timeout(5000)
                    results_count = await page.locator(".result-box").count()
                    if results_count == 0:
                        self.logger.error(
                            "Still no results found. Ending pagination for this county to avoid infinite loop."
                        )
                        break

                for i in range(results_count):
                    # Re-query results to avoid stale references
                    results = await page.query_selector_all(".result-box")
                    if i >= len(results):
                        break

                    result = results[i]

                    # Find the "More Details" link
                    link = await result.query_selector("a.hyperlink.h4")

                    if link:
                        try:
                            # Retry mechanism for clicking details
                            for attempt in range(3):
                                try:
                                    # Ensure loader is gone before clicking
                                    try:
                                        await page.wait_for_selector(
                                            "compass-ui-cw-loader",
                                            state="hidden",
                                            timeout=5000,
                                        )
                                    except:
                                        pass

                                    # Click and wait for details page (SPA navigation)
                                    await link.click(force=True)

                                    # Wait for details page to load (look for specific details element)
                                    await page.wait_for_selector(
                                        ".prov-detail", state="visible", timeout=20000
                                    )

                                    # If we reached here, success
                                    break
                                except Exception as click_e:
                                    self.logger.warning(
                                        f"Attempt {attempt + 1} to click details failed: {click_e}"
                                    )
                                    if attempt == 2:
                                        raise click_e  # Re-raise on last attempt
                                    await page.wait_for_timeout(
                                        2000
                                    )  # Wait before retry

                            # Parse Details
                            details_content = await page.content()
                            item = self.parse_provider_details(details_content)
                            item["source_state"] = "PA"
                            yield item

                        except Exception as e:
                            self.logger.error(
                                f"Error extracting provider details (County: {counties}, Page: {page_num}, Item: {i}): {e}"
                            )
                        finally:
                            # Go Back
                            try:
                                # Check if we are already on the results page (list is visible)
                                if await page.is_visible(".result-box"):
                                    # We are already on the list page, no need to navigate back
                                    pass
                                else:
                                    # Check for return button
                                    return_btn = page.locator("#return-btn")
                                    if await return_btn.is_visible():
                                        # Verify this isn't the "Find a Provider" button on the main results page
                                        btn_text = await return_btn.inner_text()
                                        if "Find a Provider" not in btn_text:
                                            await return_btn.click()
                                            # Wait for loader after going back
                                            await page.wait_for_selector(
                                                "compass-ui-cw-loader",
                                                state="hidden",
                                                timeout=20000,
                                            )
                                            await page.wait_for_selector(
                                                ".result-box",
                                                state="visible",
                                                timeout=60000,
                                            )
                                            # Wait a bit for list to re-render
                                            await page.wait_for_timeout(500)
                                        else:
                                            # If the button says "Find a Provider", we might be on results page but .result-box isn't detected yet?
                                            # Or we are in a weird state. Try generic go_back or just wait.
                                            self.logger.warning(
                                                "Found 'Find a Provider' button but .result-box not visible. Waiting..."
                                            )
                                            await page.wait_for_selector(
                                                ".result-box",
                                                state="visible",
                                                timeout=60000,
                                            )
                                    else:
                                        self.logger.warning(
                                            "Return button not found, attempting to recover state via go_back."
                                        )
                                        await page.go_back()
                                        await page.wait_for_selector(
                                            "compass-ui-cw-loader",
                                            state="hidden",
                                            timeout=20000,
                                        )
                                        await page.wait_for_selector(
                                            ".result-box",
                                            state="visible",
                                            timeout=60000,
                                        )
                            except Exception as nav_e:
                                self.logger.error(f"Error navigating back: {nav_e}")
                            except Exception as nav_e:
                                self.logger.error(f"Error navigating back: {nav_e}")

                # Pagination
                next_btn = await page.query_selector(
                    ".pagination .next.page-item:not(.disabled) a"
                )

                if next_btn:
                    # Get current mapping text to verify change
                    try:
                        mapping_text_element = page.locator("text=/Mapping.*of.*/")
                        current_mapping_text = (
                            await mapping_text_element.text_content()
                            if await mapping_text_element.count() > 0
                            else ""
                        )
                    except:
                        current_mapping_text = ""

                    self.logger.info(
                        f"Changing pages within county {counties} search list. Moving to page {page_num + 1}."
                    )  # Log 2: Change page
                    await next_btn.click()
                    page_num += 1

                    # Wait for loader
                    await page.wait_for_selector(
                        "compass-ui-cw-loader", state="hidden", timeout=10000
                    )

                    # Wait for the mapping text to change
                    if current_mapping_text:
                        try:
                            await page.wait_for_function(
                                f"document.body.innerText.includes('Mapping') && !document.body.innerText.includes('{current_mapping_text.strip()}')",
                                timeout=10000,
                            )
                        except:
                            self.logger.warning(
                                "Timed out waiting for pagination text update, assuming page changed or last page."
                            )
                    else:
                        await page.wait_for_timeout(3000)  # Fallback wait

                else:
                    break

            # End of county processing
            self.logger.info(
                f"Finished processing county {counties}. Spider will close page and start next request if available."
            )  # Log 3: Context for next county

        except Exception as e:
            self.logger.error(f"Error processing counties {counties}: {e}")
        finally:
            await page.close()

    def parse_provider_details(self, content):
        sel = scrapy.Selector(text=content)
        item = ProviderItem()

        item["provider_name"] = sel.css("h1::text").get()

        # Address extraction
        # The address is in a link to google maps
        address_parts = sel.css(
            '.prov-detail a[href^="https://maps.google.com"] span::text'
        ).getall()
        item["address"] = ", ".join([p.strip() for p in address_parts if p.strip()])

        # Phone
        phone = sel.css('.prov-info a[href^="tel:"]::text').get()
        item["phone"] = phone.strip() if phone else None

        # Stars Rating
        # Count filled stars? The HTML shows:
        # <i class="fa-solid fa-star"></i> for filled?
        # Sample: 4 stars has 4 `fa-solid fa-star`.
        stars = len(sel.css(".stars-rating .fa-solid.fa-star"))
        item["pa_stars_rating"] = str(stars)

        # Capacity
        # Found under "Maximum Capacity" section.
        # <h3 ...> Maximum Capacity </h3> ... <p class="prov-data"> 132 </p>
        # We can look for the h3 with text "Maximum Capacity" and get the following p.prov-data
        capacity = sel.xpath(
            '//h3[contains(text(), "Maximum Capacity")]/following-sibling::div/p[@class="prov-data"]/text()'
        ).get()
        item["capacity"] = capacity.strip() if capacity else None

        # Program Type / Provider Type
        provider_type = sel.xpath(
            '//h3[contains(text(), "Provider Type")]/following-sibling::p[@class="prov-data"]/text()'
        ).get()
        item["provider_type"] = provider_type.strip() if provider_type else None

        # Certification Status
        certificate_status = sel.xpath(
            '//h3[contains(text(), "Certification")]/following-sibling::div/p[@class="prov-data"]/text()'
        ).get()
        item["pa_certificate_status"] = (
            certificate_status.strip() if certificate_status else None
        )

        # School District
        school_district = sel.xpath(
            '//h3[contains(text(), "School District(s) Served")]/parent::div/following-sibling::div/p[@class="prov-data"]/text()'
        ).get()
        item["pa_school_district"] = (
            school_district.strip() if school_district else None
        )

        # Meal Options
        # List items under "Meal Options" section
        meal_options = sel.xpath(
            '//h3[contains(text(), "Meal Options")]/parent::div/following-sibling::div//li/text()'
        ).getall()
        item["pa_meal_options"] = ", ".join([m.strip() for m in meal_options])

        # Schedule
        schedule_items = sel.xpath(
            '//h3[contains(text(), "Schedule")]/following-sibling::ul//li/text()'
        ).getall()
        item["pa_schedule"] = ", ".join([s.strip() for s in schedule_items])

        # Cost Table (Ages Served / Full Time / Part Time / Openings)
        cost_rows = sel.xpath(
            '//h3[contains(text(), "Cost")]/following-sibling::div//div[contains(@class, "data-row")]'
        )
        costs = []
        for row in cost_rows:
            # 4 columns: Age, Full Time, Part Time, Openings
            # Cols are divs.
            cols = row.css('div[class*="col-"]::text').getall()
            # Clean up
            cols = [c.strip() for c in cols if c.strip()]

            age_group = (
                row.css(".col-md-5 .d-none::text").get()
                or row.css(".col-md-5 .head::text").get()
            )
            # The prices are in col-md-2 and openings in col-md-3
            prices = row.css(".col-md-2::text").getall()
            openings = row.css(".col-md-3::text").get()

            if age_group:
                entry = {
                    "age_group": age_group.strip(),
                    "full_time_rate": prices[0].strip() if len(prices) > 0 else None,
                    "part_time_rate": prices[1].strip() if len(prices) > 1 else None,
                    "openings": openings.strip() if openings else None,
                }
                costs.append(entry)

        item["pa_cost_table"] = costs

        return item

    async def errback_close_page(self, failure):
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()
        self.logger.error(repr(failure))

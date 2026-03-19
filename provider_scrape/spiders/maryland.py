import asyncio
import logging
import os
import re

import pypdfium2 as pdfium
import scrapy
import tesserocr
from provider_scrape.items import ProviderItem, InspectionItem

# tessdata path for tesserocr — bundled fast model
TESSDATA_DIR = os.environ.get("TESSDATA_PREFIX", "/tmp/tessdata")


def extract_address_from_pdf(pdf_bytes):
    """Extract the precise address from an inspection report PDF via OCR.

    The PDFs are image-based with a consistent form layout. The ADDRESS field
    is on page 1, containing street, city, state, and zip in a row.
    """
    try:
        pdf = pdfium.PdfDocument(pdf_bytes)
        try:
            page = pdf[0]
            bitmap = page.render(scale=2)
            img = bitmap.to_pil()
        finally:
            pdf.close()

        # Crop to the ADDRESS region (consistent position across all reports)
        # At 2x scale on landscape letter (3300x2550), the ADDRESS row is ~y=1750-1950
        crop = img.crop((0, 1750, 3300, 1950))

        text = tesserocr.image_to_text(crop, path=TESSDATA_DIR)
        lines = [line.strip() for line in text.split("\n") if line.strip()]

        # Find "ADDRESS:" and extract the address from the same or next line
        for i, line in enumerate(lines):
            if "ADDRESS" in line.upper():
                # Address may be after the colon on the same line
                after_colon = line.split(":", 1)[-1].strip()
                if after_colon:
                    return _format_address(after_colon)
                # Or on the next line
                if i + 1 < len(lines):
                    return _format_address(lines[i + 1])
                break

        return None
    except Exception as e:
        logging.getLogger("maryland").warning(f"OCR address extraction failed: {e}")
        return None


def _format_address(raw):
    """Format a raw OCR address string into 'street, city, state zip'."""
    # tesserocr returns: "325 N Howard Street Baltimore MD 21201"
    # Try to parse into components using the state code as anchor
    match = re.match(
        r"^(.+?)\s+(MD)\s+(\d{5}(?:-\d{4})?)$", raw.strip()
    )
    if match:
        street_city = match.group(1).strip()
        state = match.group(2)
        zipcode = match.group(3)
        # The last word(s) before MD is the city — but city can be multi-word
        # so just return as "street city, MD zipcode"
        return f"{street_city}, {state} {zipcode}"
    return raw.strip()


class MarylandSpider(scrapy.Spider):
    name = "maryland"
    allowed_domains = ["checkccmd.org"]
    start_urls = ["https://www.checkccmd.org/"]

    custom_settings = {
        "DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS": 4,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
    }

    def parse(self, response):
        """Submit the search form with defaults to get all providers."""
        self.logger.info("Loaded search page, submitting form with defaults...")

        # Extract all options from multi-select dropdowns to search for everything
        fac_types = response.css(
            "#MainContent_ddlFacType option::attr(value)"
        ).getall()
        license_statuses = response.css(
            "#MainContent_ddlLicenseStatus option::attr(value)"
        ).getall()
        counties = response.css(
            "#MainContent_ddlCountyList option::attr(value)"
        ).getall()
        cities = response.css(
            "#MainContent_ddlCityList option::attr(value)"
        ).getall()

        self.logger.info(
            f"Form options: {len(fac_types)} types, {len(license_statuses)} statuses, "
            f"{len(counties)} counties, {len(cities)} cities"
        )

        yield scrapy.FormRequest.from_response(
            response,
            formdata={
                "ctl00$MainContent$ddlFacType": fac_types,
                "ctl00$MainContent$ddlLicenseStatus": license_statuses,
                "ctl00$MainContent$ddlCountyList": counties,
                "ctl00$MainContent$ddlCityList": cities,
                "ctl00$MainContent$SearchButton": "SEARCH",
            },
            callback=self.parse_results,
            dont_click=True,
        )

    def parse_results(self, response):
        """Parse the search results page with pagination."""
        total_text = response.css("#MainContent_lblTotalRows::text").get()
        self.logger.info(f"Total providers: {total_text}")

        # Extract provider detail links
        rows = response.css("#grdResults tr.rowStyle")
        self.logger.info(f"Found {len(rows)} provider rows on this page.")

        for row in rows:
            cols = row.css("td")
            link = cols[0].css("a::attr(href)").get() if cols else None
            if link:
                # Extract fields only available on the results page
                address = cols[2].css("::text").get("").strip() if len(cols) > 2 else None
                school_name = cols[4].css("::text").get("").strip() if len(cols) > 4 else None
                program_type = cols[5].css("::text").get("").strip() if len(cols) > 5 else None

                yield response.follow(
                    link,
                    callback=self.parse_detail,
                    cb_kwargs={
                        "address": address or None,
                        "school_name": school_name or None,
                        "program_type": program_type or None,
                    },
                )

        # Pagination - find next page via __doPostBack
        pager_row = response.css("tr.dataPager")
        if pager_row:
            # Current page is shown as a span (not a link)
            current_page_text = pager_row.css("span::text").get()
            if current_page_text and current_page_text.strip().isdigit():
                current_page = int(current_page_text.strip())
                next_page = current_page + 1

                next_link = pager_row.css(f'a[href*="Page${next_page}"]')
                if next_link:
                    self.logger.info(f"Navigating to page {next_page}...")
                    yield scrapy.FormRequest.from_response(
                        response,
                        formdata={
                            "__EVENTTARGET": "ctl00$MainContent$grdResults",
                            "__EVENTARGUMENT": f"Page${next_page}",
                        },
                        callback=self.parse_results,
                        dont_click=True,
                    )
                else:
                    # Check for "..." link which jumps to the next set of pages
                    ellipsis_links = pager_row.css("a")
                    for el_link in ellipsis_links:
                        text = el_link.css("::text").get("").strip()
                        href = el_link.attrib.get("href", "")
                        if text == "..." and f"Page${next_page}" not in href:
                            # The "..." after current page goes to next set
                            match = re.search(r"Page\$(\d+)", href)
                            if match:
                                jump_page = int(match.group(1))
                                if jump_page > current_page:
                                    self.logger.info(
                                        f"Jumping to page {jump_page} via '...' link..."
                                    )
                                    yield scrapy.FormRequest.from_response(
                                        response,
                                        formdata={
                                            "__EVENTTARGET": "ctl00$MainContent$grdResults",
                                            "__EVENTARGUMENT": f"Page${jump_page}",
                                        },
                                        callback=self.parse_results,
                                        dont_click=True,
                                    )
                                    break

    def parse_detail(self, response, address=None, school_name=None, program_type=None):
        """Parse a provider detail page."""
        # Check if we actually got the detail page (not redirected to search)
        if "FacilityDetail" not in response.url:
            self.logger.warning(
                f"Expected detail page but got: {response.url}. Skipping."
            )
            return

        item = ProviderItem()
        item["source_state"] = "Maryland"
        item["provider_url"] = response.url

        # Fields from the results page (not available on detail page)
        item["address"] = address
        item["md_school_name"] = school_name
        item["provider_type"] = program_type

        # Closed/suspended providers use a different panel with *Op suffixed IDs
        is_non_operating = response.css("#MainContent_PnlNonOperating").get() is not None

        if is_non_operating:
            item["provider_name"] = self._get_span_text(
                response, "MainContent_txtProviderNameOp"
            )
            item["license_number"] = self._get_span_text(
                response, "MainContent_txtLicenseOp"
            )
            item["status"] = self._get_span_text(
                response, "MainContent_txtProviderStatusOp"
            )
            item["md_approved_education"] = self._get_span_text(
                response, "MainContent_txtApprovedEducationProgramOp"
            )
            item["md_accreditation"] = self._get_span_text(
                response, "MainContent_txtAccreditationOp"
            )
            item["md_excels_level"] = self._get_span_text(
                response, "MainContent_txtEXCELSLevelOp"
            )
        else:
            item["provider_name"] = self._get_span_text(
                response, "MainContent_txtProviderName"
            )
            item["license_number"] = self._get_span_text(
                response, "MainContent_txtLicense"
            )
            item["county"] = self._get_span_text(response, "MainContent_txtCounty")
            item["status"] = self._get_span_text(
                response, "MainContent_txtProviderStatus"
            )
            item["phone"] = self._get_span_text(response, "MainContent_txtPhone")
            item["email"] = self._get_span_text(response, "MainContent_txtEmail")

            # Capacity - may contain total + age breakdowns separated by <br> tags
            capacity_raw = self._get_span_html(response, "MainContent_txtCapacity")
            if capacity_raw:
                parts = [
                    p.strip() for p in re.split(r"<br\s*/?>", capacity_raw) if p.strip()
                ]
                if parts:
                    item["capacity"] = parts[0]
                    if len(parts) > 1:
                        item["ages_served"] = "; ".join(parts[1:])

            # Hours
            hours_raw = self._get_span_html(response, "MainContent_txtHours")
            if hours_raw:
                parts = [
                    p.strip() for p in re.split(r"<br\s*/?>", hours_raw) if p.strip()
                ]
                item["hours"] = "; ".join(parts)

            # Maryland-specific fields
            item["md_approved_education"] = self._get_span_text(
                response, "MainContent_txtApprovedEducationProgram"
            )
            item["md_accreditation"] = self._get_span_text(
                response, "MainContent_txtAccreditation"
            )
            item["md_fatalities"] = self._get_span_text(
                response, "MainContent_txtFatalities"
            )
            item["md_serious_injuries"] = self._get_span_text(
                response, "MainContent_txtInjuries"
            )
            item["md_excels_level"] = self._get_span_text(
                response, "MainContent_txtEXCELSLevel"
            )

        # Inspections
        item["inspections"] = self._extract_inspections(response)

        # Try to get precise address from the first inspection report PDF
        first_report_url = self._get_first_report_url(response)
        if first_report_url:
            yield scrapy.Request(
                first_report_url,
                callback=self.parse_inspection_pdf,
                cb_kwargs={"item": item},
                dont_filter=True,
            )
        else:
            yield item

    async def parse_inspection_pdf(self, response, item):
        """Extract precise address from an inspection report PDF via OCR."""
        # Run CPU-bound OCR in a thread pool to avoid blocking the reactor
        pdf_bytes = response.body
        precise_address = await asyncio.to_thread(extract_address_from_pdf, pdf_bytes)
        if precise_address:
            self.logger.debug(
                f"OCR address for {item.get('provider_name')}: {precise_address}"
            )
            item["address"] = precise_address
        yield item

    def _get_first_report_url(self, response):
        """Get the URL of the first inspection report PDF from the detail page."""
        rows = response.css("#MainContent_grdInspection tr")
        for row in rows[1:]:
            cols = row.css("td")
            if len(cols) >= 1:
                link = cols[0].css("a::attr(href)").get()
                if link:
                    return response.urljoin(link)
        return None

    def _extract_inspections(self, response):
        """Extract inspection records from the detail page."""
        inspections = []
        rows = response.css("#MainContent_grdInspection tr")

        for row in rows[1:]:
            cols = row.css("td")
            if len(cols) < 7:
                continue

            insp = InspectionItem()

            report_link = cols[0].css("a::attr(href)").get()
            if report_link:
                insp["report_url"] = response.urljoin(report_link)

            insp["date"] = cols[2].css("::text").get("").strip()
            insp["type"] = cols[3].css("::text").get("").strip()

            md_regulation = cols[4].css("::text").get("").strip()
            md_finding = cols[5].css("::text").get("").strip()
            md_status = cols[6].css("::text").get("").strip()

            insp["md_regulation"] = (
                md_regulation if md_regulation and md_regulation != "\xa0" else None
            )
            insp["md_finding"] = (
                md_finding if md_finding and md_finding != "\xa0" else None
            )
            insp["md_inspection_status"] = (
                md_status if md_status and md_status != "\xa0" else None
            )

            if insp.get("date") or insp.get("type"):
                inspections.append(insp)

        return inspections

    def _get_span_text(self, response, span_id):
        """Extract text from a span element by its ID."""
        texts = response.css(f"#{span_id}::text").getall()
        if texts:
            return " ".join(t.strip() for t in texts if t.strip())
        return None

    def _get_span_html(self, response, span_id):
        """Extract inner HTML from a span element by its ID."""
        span = response.css(f"#{span_id}")
        if span:
            outer = span.get()
            if outer:
                inner = re.sub(r"^<span[^>]*>", "", outer)
                inner = re.sub(r"</span>\s*$", "", inner)
                return inner
        return None

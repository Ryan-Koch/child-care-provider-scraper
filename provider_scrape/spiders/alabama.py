import re

import scrapy

from provider_scrape.items import ProviderItem

# bug_09: AL renders a missing ZIP+4 as a literal "-____" placeholder rather
# than omitting it. A real ZIP+4 (e.g. "35601-1234") has digits after the
# dash and will not match this, so it is left untouched.
_ZIP_PLACEHOLDER_RE = re.compile(r"^(\d{5})-[\s_]*$")


def _extract_br_separated_address(label):
    """Build a "street, city, state zip" string from the four ``<span>``
    elements that follow an address label on an AL detail page, or ``None``
    when the address is blank.

    bug_09: the page lays out each address (Mailing Address / Street
    Address) as a bold label ``<span>`` followed by four flat sibling
    ``<span>`` elements -- street, city, state, ZIP -- visually separated by
    ``<br>`` tags. A ``<br>`` produces no text node of its own, so the
    original code (joining the whole container's text with ``"".join(...)``
    and regexing across it) silently ran the street and city together with
    no separator at all, e.g. "1909 CENTRAL PARKWAY SWDECATUR". Walking
    straight to the four sibling spans and joining them ourselves removes
    that ambiguity entirely -- no cross-``<br>`` regex involved.
    """
    spans = label.xpath("following-sibling::span[position() <= 4]")
    values = [span.xpath("string(.)").get("").strip() for span in spans]
    values += [""] * (4 - len(values))  # defensive: tolerate a short/odd page
    street, city, state, zip_code = values[:4]

    zip_match = _ZIP_PLACEHOLDER_RE.match(zip_code)
    if zip_match:
        zip_code = zip_match.group(1)

    locality = ", ".join(part for part in (city, state) if part)
    if zip_code:
        locality = f"{locality} {zip_code}".strip()

    pieces = [part for part in (street, locality) if part]
    return ", ".join(pieces) if pieces else None


class AlabamaSpider(scrapy.Spider):
    name = "alabama"
    allowed_domains = ["apps.dhr.alabama.gov"]
    start_urls = ["https://apps.dhr.alabama.gov/daycare/daycare_search"]

    def parse(self, response):
        """
        Initial request to the search page.
        Extracts viewstate and submits the form to search for all licensed providers.
        """
        # Prepare form data
        form_data = {
            "ctl00$MainContent$CountySelect": "ALL",
            "ctl00$MainContent$Radiolist1": "L",  # Licensed
            "ctl00$MainContent$Radiolist2": "ALL",  # Any Child Care
            "__EVENTTARGET": "ctl00$MainContent$LinkButton2",
            "__EVENTARGUMENT": "",
            "ctl00$MainContent$TextBox1": "",
            "ctl00$MainContent$TextBox2": "",
        }

        # Scrapy's FormRequest.from_response handles hidden fields (VIEWSTATE, etc.) automatically
        yield scrapy.FormRequest.from_response(
            response,
            formdata=form_data,
            callback=self.parse_results,
            dont_click=True,  # We specified the target explicitly
        )

    def parse_results(self, response):
        """
        Parses the search results page (paginated).
        """
        # Iterate over result rows
        # The table ID is MainContent_GridView1
        rows = response.css("table#MainContent_GridView1 tr")

        # Skip header row (first one) and possibly pager row (last one)
        # We can check if the row has data.

        for row in rows:
            # Check if it's a data row or a pager row
            # Data rows have 4 cells usually, pager row has 1 cell spanning multiple columns
            cells = row.css("td")
            if len(cells) == 4:
                # Extract link to details
                link = row.css("td:nth-child(2) a::attr(href)").get()
                if link:
                    yield response.follow(link, callback=self.parse_detail)

        # Pagination
        # Find the current page number
        # The current page is usually a number inside a span in the pager row
        # Pager row is inside the table, usually the last row, containing a nested table

        pager_row = response.css("table#MainContent_GridView1 tr:last-child table")
        if pager_row:
            current_page_span = pager_row.css("span::text").get()
            if current_page_span and current_page_span.isdigit():
                current_page = int(current_page_span)
                next_page = current_page + 1

                # Look for the link to the next page
                # The links invoke __doPostBack('ctl00$MainContent$GridView1','Page$N')
                # We can construct the request manually or find the link

                # Attempt to find the link with text == str(next_page)
                next_page_link = pager_row.css(f'a[href*="Page${next_page}"]')

                if next_page_link:
                    yield scrapy.FormRequest.from_response(
                        response,
                        formdata={
                            "__EVENTTARGET": "ctl00$MainContent$GridView1",
                            "__EVENTARGUMENT": f"Page${next_page}",
                        },
                        callback=self.parse_results,
                        dont_click=True,
                    )
                else:
                    # Check for "..." which might lead to next set of pages
                    # Example: Page$11 if we are at 10 and 11 is hidden behind "..."
                    # But the example showed "... 21 22 ...".
                    # If we are at 20, we see "..." (prev), 21, 22...
                    # If we can't find specific number, maybe we reached the end?
                    # Or check for the "..." link that is *after* the current page
                    pass

    def parse_detail(self, response):
        """
        Parses the provider detail page.
        """
        item = ProviderItem()
        item["source_state"] = "Alabama"
        item["provider_url"] = response.url

        # The main content is inside #MainContent_Label1
        # It's unstructured text with some <b> tags and spans.
        # We can use xpath to extract text following specific labels.

        container = response.css("#MainContent_Label1")

        def get_text_after_label(label_text):
            # XPath: Find b or span containing text, then get following text node
            # This is tricky because of the messy HTML.
            # "Licensee:" is in a span with font-weight:bold.
            # "Facility:" is in a span.

            # Try a regex approach on the inner HTML or text
            pass

        # Let's extract all text and parse it or use strict XPaths

        # Licensee
        item["license_holder"] = container.xpath(
            './/span[contains(text(), "Licensee:")]/../../div[2]/span/text()'
        ).get()

        # Facility (Provider Name)
        item["provider_name"] = container.xpath('.//span[contains(text(), "Facility:")]/../../div[2]/span/text()').get()

        # Status
        # "<b>Status:</b> Licensed<br>"
        # Find b with "Status:", get following sibling text
        item["status"] = container.xpath('.//b[contains(text(), "Status:")]/following-sibling::text()[1]').get()
        if item["status"]:
            item["status"] = item["status"].strip()

        # Director
        # "<b>JOHNSON, KATRINA M - Director</b>"
        # This one doesn't have a label "Director:". It's just bold text ending in "- Director"
        director_text = container.xpath('.//b[contains(text(), "- Director")]/text()').get()
        if director_text:
            item["administrator"] = director_text.replace("- Director", "").strip()

        # Phone
        item["phone"] = container.xpath('.//b[contains(text(), "Phone:")]/following-sibling::text()[1]').get()
        if item["phone"]:
            item["phone"] = item["phone"].strip()

        # Quality Star Rating
        # <span style='...'>Alabama Quality Star Rating:   </span><span style='...'> &nbsp;&nbsp; 1 Star</span>
        item["al_quality_rating"] = container.xpath(
            './/span[contains(text(), "Quality Star Rating:")]/following-sibling::span[1]/text()'
        ).get()
        if item["al_quality_rating"]:
            item["al_quality_rating"] = item["al_quality_rating"].strip()

        # Rating Expiration
        item["al_rating_expiration"] = container.xpath(
            './/span[contains(text(), "Rating Expiration Date:")]/following-sibling::span[1]/text()'
        ).get()
        if item["al_rating_expiration"]:
            item["al_rating_expiration"] = item["al_rating_expiration"].strip()

        # Hours & Ages table
        # There's a nested table for this.
        # Daytime Hours
        item["hours"] = container.xpath('.//b[contains(text(), "Daytime Hours:")]/following-sibling::text()').get()
        if item["hours"]:
            item["hours"] = item["hours"].strip()

        # Nighttime Hours
        item["al_nighttime_hours"] = container.xpath(
            './/b[contains(text(), "Nighttime Hours:")]/following-sibling::text()'
        ).get()
        if item["al_nighttime_hours"]:
            item["al_nighttime_hours"] = item["al_nighttime_hours"].strip()

        # Ages
        item["ages_served"] = container.xpath('.//b[contains(text(), "Daytime Ages:")]/following-sibling::text()').get()
        if item["ages_served"]:
            item["ages_served"] = item["ages_served"].strip()

        item["al_nighttime_ages"] = container.xpath(
            './/b[contains(text(), "Nighttime Ages:")]/following-sibling::text()'
        ).get()
        if item["al_nighttime_ages"]:
            item["al_nighttime_ages"] = item["al_nighttime_ages"].strip()

        # Addresses
        # bug_09: each address (Mailing Address / Street Address) is a bold
        # label <span> followed by four flat sibling <span>s -- street,
        # city, state, ZIP -- so we locate the label and walk its siblings
        # directly rather than regexing the container's joined text (see
        # _extract_br_separated_address for why that approach dropped the
        # street/city separator).
        mailing_label = container.xpath('.//span[contains(text(), "Mailing Address:")]')
        if mailing_label:
            item["al_mailing_address"] = _extract_br_separated_address(mailing_label[0])

        street_label = container.xpath('.//span[contains(text(), "Street Address:")]')
        if street_label:
            item["address"] = _extract_br_separated_address(street_label[0])

        # Tables extraction
        # Accreditations
        accreditations = []
        acc_rows = response.css(
            "#MainContent_GridView1 tr:not(:first-child)"
        )  # Skip header (if any? "No Accreditations" is a row)
        for row in acc_rows:
            text = "".join(row.css("::text").getall()).strip()
            if text and "No Accreditations" not in text:
                accreditations.append(text)
        item["al_accreditations"] = accreditations

        # Adverse Actions
        adverse = []
        adv_rows = response.css("#MainContent_GridView3 tr:not(:first-child)")
        for row in adv_rows:
            text = "".join(row.css("::text").getall()).strip()
            if text and "No Adverse Actions" not in text:
                adverse.append(text)  # Or structured dict if columns known
        item["al_adverse_actions"] = adverse

        # Substantiated Complaints
        complaints = []
        comp_rows = response.css("#MainContent_GridView2 tr:not(:first-child)")
        for row in comp_rows:
            text = "".join(row.css("::text").getall()).strip()
            if text and "No Substantiated Complaints" not in text:
                complaints.append(text)
        item["al_substantiated_complaints"] = complaints

        # Deficiencies
        # Try to map to InspectionItem if possible, otherwise list of strings
        deficiencies = []
        inspections = []
        def_rows = response.css("#MainContent_GridView4 tr:not(:first-child)")
        for row in def_rows:
            text = "".join(row.css("::text").getall()).strip()
            if text and "No Evaluation/Deficiency Reports" not in text:
                deficiencies.append(text)
                # If we could parse dates, we would create InspectionItem
                # Since we don't have example data, just storing raw text for now
        item["al_deficiency_reports"] = deficiencies
        item["inspections"] = inspections

        yield item

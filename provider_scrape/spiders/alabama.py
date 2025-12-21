import scrapy
from provider_scrape.items import ProviderItem, InspectionItem
import re

class AlabamaSpider(scrapy.Spider):
    name = 'alabama'
    allowed_domains = ['apps.dhr.alabama.gov']
    start_urls = ['https://apps.dhr.alabama.gov/daycare/daycare_search']

    def parse(self, response):
        """
        Initial request to the search page.
        Extracts viewstate and submits the form to search for all licensed providers.
        """
        # Prepare form data
        form_data = {
            'ctl00$MainContent$CountySelect': 'ALL',
            'ctl00$MainContent$Radiolist1': 'L', # Licensed
            'ctl00$MainContent$Radiolist2': 'ALL', # Any Child Care
            '__EVENTTARGET': 'ctl00$MainContent$LinkButton2',
            '__EVENTARGUMENT': '',
            'ctl00$MainContent$TextBox1': '',
            'ctl00$MainContent$TextBox2': ''
        }
        
        # Scrapy's FormRequest.from_response handles hidden fields (VIEWSTATE, etc.) automatically
        yield scrapy.FormRequest.from_response(
            response,
            formdata=form_data,
            callback=self.parse_results,
            dont_click=True # We specified the target explicitly
        )

    def parse_results(self, response):
        """
        Parses the search results page (paginated).
        """
        # Iterate over result rows
        # The table ID is MainContent_GridView1
        rows = response.css('table#MainContent_GridView1 tr')
        
        # Skip header row (first one) and possibly pager row (last one)
        # We can check if the row has data.
        
        for row in rows:
            # Check if it's a data row or a pager row
            # Data rows have 4 cells usually, pager row has 1 cell spanning multiple columns
            cells = row.css('td')
            if len(cells) == 4:
                # Extract link to details
                link = row.css('td:nth-child(2) a::attr(href)').get()
                if link:
                    yield response.follow(link, callback=self.parse_detail)
            
        # Pagination
        # Find the current page number
        # The current page is usually a number inside a span in the pager row
        # Pager row is inside the table, usually the last row, containing a nested table
        
        pager_row = response.css('table#MainContent_GridView1 tr:last-child table')
        if pager_row:
            current_page_span = pager_row.css('span::text').get()
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
                            '__EVENTTARGET': 'ctl00$MainContent$GridView1',
                            '__EVENTARGUMENT': f'Page${next_page}'
                        },
                        callback=self.parse_results,
                        dont_click=True
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
        item['source_state'] = 'AL'
        item['provider_url'] = response.url
        
        # The main content is inside #MainContent_Label1
        # It's unstructured text with some <b> tags and spans.
        # We can use xpath to extract text following specific labels.
        
        container = response.css('#MainContent_Label1')
        
        def get_text_after_label(label_text):
            # XPath: Find b or span containing text, then get following text node
            # This is tricky because of the messy HTML.
            # "Licensee:" is in a span with font-weight:bold.
            # "Facility:" is in a span.
            
            # Try a regex approach on the inner HTML or text
            pass

        # Let's extract all text and parse it or use strict XPaths
        
        # Licensee
        item['license_holder'] = container.xpath('.//span[contains(text(), "Licensee:")]/../../div[2]/span/text()').get()
        
        # Facility (Provider Name)
        item['provider_name'] = container.xpath('.//span[contains(text(), "Facility:")]/../../div[2]/span/text()').get()
        
        # Status
        # "<b>Status:</b> Licensed<br>"
        # Find b with "Status:", get following sibling text
        item['status'] = container.xpath('.//b[contains(text(), "Status:")]/following-sibling::text()[1]').get()
        if item['status']:
            item['status'] = item['status'].strip()
            
        # Director
        # "<b>JOHNSON, KATRINA M - Director</b>"
        # This one doesn't have a label "Director:". It's just bold text ending in "- Director"
        director_text = container.xpath('.//b[contains(text(), "- Director")]/text()').get()
        if director_text:
            item['administrator'] = director_text.replace('- Director', '').strip()
            
        # Phone
        item['phone'] = container.xpath('.//b[contains(text(), "Phone:")]/following-sibling::text()[1]').get()
        if item['phone']:
            item['phone'] = item['phone'].strip()
            
        # Quality Star Rating
        # <span style='...'>Alabama Quality Star Rating:   </span><span style='...'> &nbsp;&nbsp; 1 Star</span>
        item['al_quality_rating'] = container.xpath('.//span[contains(text(), "Quality Star Rating:")]/following-sibling::span[1]/text()').get()
        if item['al_quality_rating']:
            item['al_quality_rating'] = item['al_quality_rating'].strip()
            
        # Rating Expiration
        item['al_rating_expiration'] = container.xpath('.//span[contains(text(), "Rating Expiration Date:")]/following-sibling::span[1]/text()').get()
        if item['al_rating_expiration']:
            item['al_rating_expiration'] = item['al_rating_expiration'].strip()
            
        # Hours & Ages table
        # There's a nested table for this.
        # Daytime Hours
        item['hours'] = container.xpath('.//b[contains(text(), "Daytime Hours:")]/following-sibling::text()').get()
        if item['hours']:
            item['hours'] = item['hours'].strip()

        # Nighttime Hours
        item['al_nighttime_hours'] = container.xpath('.//b[contains(text(), "Nighttime Hours:")]/following-sibling::text()').get()
        if item['al_nighttime_hours']:
            item['al_nighttime_hours'] = item['al_nighttime_hours'].strip()
            
        # Ages
        item['ages_served'] = container.xpath('.//b[contains(text(), "Daytime Ages:")]/following-sibling::text()').get()
        if item['ages_served']:
            item['ages_served'] = item['ages_served'].strip()

        item['al_nighttime_ages'] = container.xpath('.//b[contains(text(), "Nighttime Ages:")]/following-sibling::text()').get()
        if item['al_nighttime_ages']:
            item['al_nighttime_ages'] = item['al_nighttime_ages'].strip()
            
        # Addresses
        # Mailing Address
        # "<b>Mailing Address:</b><br><span>1400 BRISBANE...</span><br>..."
        # This is hard to grab with simple sibling selectors because of <br>s and spans.
        # Let's try to grab the text following the label until the next label or double break.
        
        # Helper to extract address blocks
        def extract_address(label_text):
            # Find the b tag
            # Get all following siblings until we hit <br><br> or another bold tag?
            # Actually, looking at the HTML:
            # <span ...>Mailing Address:</span><br /><span ...>Line 1</span><br /><span ...>City</span>, <span ...>State</span> <span ...>Zip</span><br /><br />
            
            # We can select the following-sibling::span nodes until we hit <br><br>
            # Or just grab the text.
            
            # Let's try to get the parent text content and parse it with regex
            # Or use xpath string concatenation if possible, but that's hard in Scrapy 
            pass

        # Let's clean up the text extraction for address
        # Locate the label "Mailing Address:"
        mailing_label = container.xpath('.//span[contains(text(), "Mailing Address:")]')
        if mailing_label:
            # Get the following siblings that are spans or text, stop at "Street Address:"
            # This is getting complicated.
            # Alternative: Get the full text of the container and parse with Regex.
            full_text = "".join(container.xpath('.//text()').getall())
            
            # Normalize whitespace
            full_text = re.sub(r'\s+', ' ', full_text)
            
            # Regex for Mailing Address
            # Mailing Address: (.*?) Street Address:
            mailing_match = re.search(r'Mailing Address:(.*?)Street Address:', full_text)
            if mailing_match:
                item['al_mailing_address'] = mailing_match.group(1).strip()
                
            # Regex for Street Address
            # Street Address: (.*?) Click for Interactive Map
            # Or just end of string (but there is map link after)
            street_match = re.search(r'Street Address:(.*?)Click for Interactive Map', full_text)
            if not street_match:
                street_match = re.search(r'Street Address:(.*)$', full_text)
            
            if street_match:
                item['address'] = street_match.group(1).strip()
        
        # Tables extraction
        # Accreditations
        accreditations = []
        acc_rows = response.css('#MainContent_GridView1 tr:not(:first-child)') # Skip header (if any? "No Accreditations" is a row)
        for row in acc_rows:
            text = "".join(row.css('::text').getall()).strip()
            if text and "No Accreditations" not in text:
                accreditations.append(text)
        item['al_accreditations'] = accreditations
        
        # Adverse Actions
        adverse = []
        adv_rows = response.css('#MainContent_GridView3 tr:not(:first-child)')
        for row in adv_rows:
            text = "".join(row.css('::text').getall()).strip()
            if text and "No Adverse Actions" not in text:
                adverse.append(text) # Or structured dict if columns known
        item['al_adverse_actions'] = adverse
        
        # Substantiated Complaints
        complaints = []
        comp_rows = response.css('#MainContent_GridView2 tr:not(:first-child)')
        for row in comp_rows:
            text = "".join(row.css('::text').getall()).strip()
            if text and "No Substantiated Complaints" not in text:
                complaints.append(text)
        item['al_substantiated_complaints'] = complaints
        
        # Deficiencies
        # Try to map to InspectionItem if possible, otherwise list of strings
        deficiencies = []
        inspections = []
        def_rows = response.css('#MainContent_GridView4 tr:not(:first-child)')
        for row in def_rows:
            text = "".join(row.css('::text').getall()).strip()
            if text and "No Evaluation/Deficiency Reports" not in text:
                deficiencies.append(text)
                # If we could parse dates, we would create InspectionItem
                # Since we don't have example data, just storing raw text for now
        item['al_deficiency_reports'] = deficiencies
        item['inspections'] = inspections

        yield item

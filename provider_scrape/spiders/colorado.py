import scrapy
from provider_scrape.items import ProviderItem
import re

class ColoradoSpider(scrapy.Spider):
    name = 'colorado'
    allowed_domains = ['coloradoshines.com']
    start_urls = ['https://www.coloradoshines.com/search?program=a']
    custom_settings = {
        'ROBOTSTXT_OBEY': False
    }

    def parse(self, response):
        # Iterate over results
        results = response.xpath('//li[@class="result"]')
        for result in results:
            item = ProviderItem()
            item['source_state'] = 'CO'
            
            # Provider Name
            item['provider_name'] = result.xpath('.//h1/text()').get('').strip()
            
            # Quality Rating
            item['co_quality_rating'] = result.xpath('.//p[@class="result-rating"]/span/span/text()').get('').strip()
            
            # Address & County
            full_address = result.xpath('.//p[@class="result-location"]/text()').get('').strip()
            item['address'] = full_address
            
            # County often follows address in a p tag: "County: Boulder"
            county_text = result.xpath('.//p[contains(text(), "County:")]/text()').get()
            if county_text:
                item['county'] = county_text.replace('County:', '').strip()
            
            # Phone
            item['phone'] = result.xpath('.//p[@class="result-phone"]/text()').get('').strip()
            
            # Care Setting / Provider Type
            item['provider_type'] = result.xpath('.//p[strong[contains(text(),"Care Setting")]]/text()[normalize-space()]').get('').strip()
            
            # Ages Served
            ages = result.xpath('.//p[strong[contains(text(),"Ages Served")]]/span/text()').getall()
            item['ages_served'] = ", ".join([a.strip() for a in ages if a.strip()])
            
            # Languages
            # Languages are often in text nodes following the strong tag
            langs = result.xpath('.//p[strong[contains(text(),"languages spoken")]]/text()').getall()
            item['languages'] = ", ".join([l.strip() for l in langs if l.strip()])
            
            # CCCAP
            cccap = result.xpath('.//p[strong[contains(text(),"Accepts CCCAP")]]/span/text()').get()
            item['scholarships_accepted'] = cccap.strip() if cccap else None

            # Openings (from Summary)
            item['co_infant_openings'] = result.xpath('.//p[strong[contains(text(),"Infant Openings Available")]]/text()[normalize-space()]').get('').strip()
            item['co_toddler_openings'] = result.xpath('.//p[strong[contains(text(),"Toddler Openings Available")]]/text()[normalize-space()]').get('').strip()
            item['co_preschool_openings'] = result.xpath('.//p[strong[contains(text(),"Preschool Openings Available")]]/text()[normalize-space()]').get('').strip()
            item['co_school_age_openings'] = result.xpath('.//p[strong[contains(text(),"School Age Openings Available")]]/text()[normalize-space()]').get('').strip()

            # Follow Detail Link
            detail_link = result.xpath('.//a[contains(@class, "view-details")]/@href').get()
            if detail_link:
                url = response.urljoin(detail_link)
                item['provider_url'] = url
                yield scrapy.Request(url, callback=self.parse_detail, meta={'item': item})
            else:
                yield item

        # Pagination
        next_link = response.xpath('//li[contains(@class, "next")]/a/@onclick').get()
        view_state = response.xpath('//input[@id="com.salesforce.visualforce.ViewState"]/@value').get()
        view_state_version = response.xpath('//input[@id="com.salesforce.visualforce.ViewStateVersion"]/@value').get()
        view_state_mac = response.xpath('//input[@id="com.salesforce.visualforce.ViewStateMAC"]/@value').get()

        if next_link and view_state:
            # Extract parameters from onclick string
            # pattern: jsfcljs(document.getElementById('page:searchForm'),'page:searchForm:j_id169,page:searchForm:j_id169','');
            match = re.search(r"jsfcljs\([^,]+,'([^']+)'", next_link)
            if match:
                params_str = match.group(1)
                # params_str looks like: 'page:searchForm:j_id169,page:searchForm:j_id169'
                # We need to turn this into a dict for FormRequest
                
                parts = params_str.split(',')
                form_data = {
                    'com.salesforce.visualforce.ViewState': view_state
                }
                
                if view_state_version:
                    form_data['com.salesforce.visualforce.ViewStateVersion'] = view_state_version
                if view_state_mac:
                    form_data['com.salesforce.visualforce.ViewStateMAC'] = view_state_mac
                
                for i in range(0, len(parts), 2):
                    if i + 1 < len(parts):
                        form_data[parts[i]] = parts[i+1]
                
                yield scrapy.FormRequest.from_response(
                    response,
                    formid='page:searchForm',
                    formdata=form_data,
                    dont_click=True,
                    callback=self.parse
                )

    def parse_detail(self, response):
        item = response.meta['item']
        
        # License Number
        item['license_number'] = response.xpath('//p[strong[contains(text(),"License Number")]]/text()[normalize-space()]').get('').strip()
        
        # Website
        item['provider_website'] = response.xpath('//div[contains(@class,"field-website")]/span/a/@href').get()
        
        # Accepting New Children
        item['co_accepting_new_children'] = response.xpath('//p[strong[contains(text(),"Accepting New Children")]]/span/text()').get()
        
        # Capacity
        item['capacity'] = response.xpath('//p[strong[contains(text(),"Capacity")]]/text()[normalize-space()]').get('').strip()
        
        # Head Start
        item['co_head_start'] = response.xpath('//p[strong[contains(text(),"Head Start")]]/span/text()').get()
        
        # Licensed to Serve
        item['co_licensed_to_serve'] = response.xpath('//p[strong[contains(text(),"Licensed to Serve")]]/text()[normalize-space()]').get('').strip()
        
        # Special Needs
        # This is often just text inside a div/field
        special_needs = response.xpath('//div[contains(@class,"field-name-field-info") and contains(.,"Special Needs")]/text()').getall()
        # Filter out the label "Special Needs:"
        special_needs = [s.strip() for s in special_needs if s.strip() and 'Special Needs:' not in s]
        item['co_special_needs'] = "; ".join(special_needs)
        
        # License Type
        item['co_license_type'] = response.xpath('//p[strong[contains(text(),"License Type")]]/text()[normalize-space()]').get('').strip()
        
        # License Issue Date
        item['co_license_issue_date'] = response.xpath('//p[strong[contains(text(),"License Issue Date")]]/span/text()').get()
        
        # License Expiration (Not explicitly in detail HTML provided, sometimes implicit or missing)
        
        yield item

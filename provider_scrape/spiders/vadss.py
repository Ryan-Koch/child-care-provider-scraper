import scrapy
from ..items import ProviderItem, InspectionItem


class VadssSpider(scrapy.Spider):
    name = "vadss"
    allowed_domains = ["dss.virginia.gov"]
    start_urls = ["https://www.dss.virginia.gov/facility/search/cc2.cgi"]

    def parse(self, response):
        """
        This method is the default callback used by Scrapy to process downloaded responses
        when their requests don't specify a callback.
        """
        self.logger.info(f'Parsing URL: {response.url}')

        title = response.xpath('//title/text()').get()
        self.logger.info(f'Page title: {title}')

        acceptable_names = ['search_require_client_code-2101', 'search_require_client_code-2102', 'search_require_client_code-2106',
                          # 'search_require_client_code-2105', 'search_require_client_code-2201', 'search_require_client_code-2104',
                          # 'search_require_client_code-3001', 'search_require_client_code-3002'
                          ]

        form = response.xpath('//form[@action="/facility/search/cc2.cgi"]')
        if form:

            yield scrapy.FormRequest.from_response(
                response,
                formxpath='//form[@action="/facility/search/cc2.cgi"]',
                callback=self.after_submit,
                formdata=self.get_submission_data(response, acceptable_names),
            )
        else:
            self.logger.error("couldn't find form")

    def get_submission_data(self, response, acceptable_names):
        form = response.xpath('//form[@action="/facility/search/cc2.cgi"]')
        formdata = {}

        if form:
            checkboxes = form.xpath('.//input[@type="checkbox"]')
            for checkbox in checkboxes:
                name = checkbox.xpath('./@name').get()
                value = checkbox.xpath('@value').get()
                if name in acceptable_names:
                    formdata.setdefault(name, []).append(value)

        else:
            self.logger.warning('Could not find the ')

        return formdata

    def after_submit(self, response):
        """
        Callback function to handle the response after the form submission.
        """
        self.logger.info(f'Form submitted. Response URL: {response.url}')
        # You can now parse the results page here
        title = response.xpath('//title/text()').get()
        self.logger.info(f'Results page title: {title}')

        links = response.xpath('//table[contains(@class, "cc_search")]/tbody//a[contains(@href, "ID=")]/@href')
        self.logger.info(f"Number of providers found: {len(links)}")
        for i,link in enumerate(links):
            self.logger.info(f"Provider {i+1} of {len(links)}")
            yield scrapy.Request(url=response.urljoin(str(link)), callback = self.parse_provider_page)

    def parse_provider_page(self, response):
        self.logger.info(f"Parsing provider page: {response.url}")

        def extract_with_xpath(query, row=None):
            try:
                # if row is not None:
                #     self.logger.info(f"Extracting data from row: {row}")
                #     result = row.xpath(query).get()
                # else:
                #     result = response.xpath(query).get()

                result = response.xpath(query).get(default='N/A').strip()
                return result if result else 'N/A'
            except:
                return 'N/A'

        def extract_inspection_data():
            inspection_data = []
            table = response.xpath('//table[@class="cc_search"]/following::table[not(@class)]')
            if table:
                rows = table.xpath('.//tr[position()>1]')
                for row in rows:
                    # Get the 'violations' cell (td[4]) and 'complaint_related' cell td[3] first
                    violations_td = row.xpath('./td[4]')
                    complaint_related_td = row.xpath('./td[3]')

                    # Try to get text from the 'a' tag within td[4]
                    violations_text = violations_td.xpath('./a/text()').get()
                    complaint_related_text = complaint_related_td.xpath('./a/text()').get()

                    # If no text found in 'a' tag, try direct text or normalize-space
                    if not violations_text:
                        violations_text = violations_td.xpath('./text()').get() # For cases like 'No'
                        if not violations_text:
                            violations_text = violations_td.xpath('normalize-space()').get() # General cleanup if other text found

                    # If no text found in 'a' tag for complaint_related_td
                    if not complaint_related_text:
                        complaint_related_text = complaint_related_td.xpath('./text()').get() # For cases like 'No'
                        if not complaint_related_text:
                            complaint_related_text = complaint_related_td.xpath('normalize-space()').get() # General cleanup if other text found

                    inspection = InspectionItem(
                        date=row.xpath('./td[1]/a/text()').get().strip() if row.xpath('./td[1]/a/text()').get() else None,
                        va_shsi=row.xpath('./td[2]/text()').get().strip() if row.xpath('./td[2]/text()').get() else None,
                        va_complaint_related=complaint_related_text.strip() if complaint_related_text else None,
                        va_violations=violations_text.strip() if violations_text else None
                    )
                    inspection_data.append(inspection)
            return inspection_data

        provider = ProviderItem(
            provider_name=extract_with_xpath('//table[not(@class)]/tr[1]/td/b/text()'),
            address=f"{extract_with_xpath('//table[not(@class)]/tr[1]/td/br/following-sibling::text()')} {extract_with_xpath('//table[not(@class)]/tr[2]/td/text()')}",
            phone=extract_with_xpath('//table[not(@class)]/tr[3]/td/text()'),
            provider_type=extract_with_xpath('//table[@class="cc_search"]/tr[1]/td[2]/span/span/font/u/text()'),
            va_license_type=extract_with_xpath('//table[@class="cc_search"]/tr[2]/td[2]/span/span/font/u/text()'),
            administrator=extract_with_xpath('//table[@class="cc_search"]/tr/td[contains(text(), "Administrator:")]/following-sibling::td/text()'),
            hours=extract_with_xpath('//table[@class="cc_search"]/tr/td[contains(text(), "Business Hours:")]/following-sibling::td/text()'),
            capacity=extract_with_xpath('//table[@class="cc_search"]/tr/td[contains(text(), "Capacity:")]/following-sibling::td/text()'),
            ages_served=extract_with_xpath('//table[@class="cc_search"]/tr/td[contains(text(), "Ages:")]/following-sibling::td/text()'),
            va_inspector=extract_with_xpath('//table[@class="cc_search"]/tr/td[contains(text(), "Inspector:")]/following-sibling::td/text()'),
            va_current_subsidy_provider=extract_with_xpath('//table[@class="cc_search"]/tr/td[contains(text(), "Current Subsidy Provider")]/following-sibling::td/text()'),
            license_number=extract_with_xpath('//table[@class="cc_search"]/tr/td[contains(text(), "License/Facility ID#")]/following-sibling::td/text()'),
            inspections=extract_inspection_data(),
            provider_url=response.url,
            source_state='VA'
        )

        return provider

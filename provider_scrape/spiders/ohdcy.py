from scrapy import Spider, FormRequest, Request
import re
from cgi import log


class OhdcySpider(Spider):
    name = "ohdcy"
    # allowed_domains = ["childcaresearch.ohio.gov"]
    start_urls = ["https://childcaresearch.ohio.gov/search?q=fVLNbtQwEM62%2bxf6KxDqASF64FKpVLTi2kNwt%2bpS2I2aCKkgDt54NmvhtSPH2ZIb74C4cOE1eAWOvAFvAjNuUypRdSKN7fnGX77xTNAKguAPGq1kq0voWCayC5gwM58bvbv9FmwpjT7c33tO3%2b42q5SrLBxqqJzlanc7riZKZqdQp%2bYj6ENdKdUhxic3ifaO0vFeAtxmM2alAyv5U8xZj61ZSAF2VM0nYLvMVNrVbSZd3XsnC2YErDYpaV1A94w7qfOVmFsnuRrxOfSGesq1K9fO8W6eGiEUSl4bK7pydQpjC2U2M0aFiV%2biHDaOeF2Op%2bMCLHIavXliKnsz0HkDXJWbL2FqLFxeY9zCymABGjXQvp9cyPkcDw%2bTAjISBCBKNpNKELyeWq7LwljnCTeiKRb%2bj2ltvACrZT5zdLp3LEGJ1Mqi3Bpwq2pPg7li8KlAGiQIT4CLxGHtD2IrF9zBqdRYZo4R0FuXjVD1cYVBca3i%2fmvJJ1Lhkw51WaGiDDqjaHDOOqMBiyPcMxajP2ZsmY2jdsSSITYi15K3hwgFrRa283JGeu3gVuv0%2by1vj%2f9run%2fWM65zeP%2bB5qt1qxGy9Gy%2fu0xs4XLjaDqvI95CUhB2bpeBtkF4L2gE497n0omQ9qM7BJKIsEuOCMI%2buZCqo9hdF71e35hU4kQOtKC1n4CCzIHwPL7EoPj26oDW3y%2b%2bCq%2bL%2ftS9GfnxJfrukX6DXEU%2b7%2fza%2bemRsEEaa1hX%2fgI%3d"]
    base_url = "https://childcaresearch.ohio.gov"

    providers = []

    def parse(self, response):
        # We need to push the search button to get the list
        form_data = {
            '__EventTarget': '__Page',
            '__EventArgument': 'search'
        }
        yield FormRequest(
            url=response.url,
            method='POST',
            formdata=form_data,
            callback=self.after_submit
        )

    def after_submit(self, response):
        # Process the response after the form submission
        # This could involve extracting data, following redirects, etc.
        self.logger.info("Visiting results page and getting list of facilities' links...")


        max_pages = response.xpath('//a[@id="ContentPlaceHolder1_pagerPrograms_ctl00_PagingFieldForDataPager_lnkLast"]')
        max_pages_num = 0
        if max_pages:
            max_pages_href = max_pages.xpath('@href').get()
            match = re.search(r'&p=(\d+)$', max_pages_href) # Should pull number at the end of the href string after the &p=
            if match:
                max_pages_num = int(match.group(1))
                self.logger.info(f"Extracted page number: {max_pages_num}")


        if max_pages_num != 0:
            # for i in range(max_pages_num):
            #     yield Request(
            #         url=f"{self.start_urls[0]}&p={i}",
            #         callback=self.parse_page_list
            #     )

            for i in range(3):
                self.logger.info(f"{self.start_urls[0]}&p={i}")
                yield Request(
                    url=f"{self.start_urls[0]}&p={i}",
                    callback=self.parse_page_list
                )

    def parse_page_list(self, response):
        # Process the response after visiting each page
        # This could involve extracting data, following redirects, etc.
        self.logger.info(f"Parsing links for page at {response.url}")


        links = response.xpath('//div[@class="resultsList"]/div[@class="resultsListRow"]//a/@href').getall()
        self.logger.info(links)

        if len(links) > 0:
            self.logger.info(f"Found {len(links)} links on page")
            self.logger.info("Extracting data from links...")
            for i, link in enumerate(links):
                self.logger.info(link)
                if i < 3:
                    yield Request(
                        url=f"{self.base_url}{link}",
                        callback=self.parse_provider_page
                    )

    def parse_provider_page(self, response):
        # Process the response after visiting each provider page
        # This could involve extracting data, following redirects, etc.
        self.logger.info(f"Parsing provider page at {response.url}")

        detail_rows = response.xpath('.//div[@class="detailGroupContainer"]/div[@class="detailGroup"]/div[@class="detailRow"]')

        data = {}
        for row in detail_rows:
            label = row.xpath('.//span[@class="detailLabel"]/text()').get()
            info_selector = row.xpath('.//span[@class="detailInfo"]')

            if info_selector:
                self.logger.info(f"Processing row with label '{label}'")
                label = label.strip()
                if label == "Program Status:" or label == 'SUTQ Rating:':
                    self.logger.info(f"{label} found")
                    info = info_selector.xpath('./span/text()').get()
                    self.logger.info(f"{label}: {info}")

                if label == "Current Inspections:":
                    self.logger.info("Current Inspections found")
                    link = info_selector.xpath('./a/@href').get()
                    self.logger.info(f"Current Inspections link: {link}")
                    yield Request(url=self.base_url + link,
                        callback=self.parse_inspections,
                        meta={'parent_data': response.meta.get('parent_data', response.meta)})

                if info_selector.xpath('./a/text()').get():  # Check if it's a link
                    info = info_selector.xpath('./a/text()')
                    href = info_selector.xpath('./a/@href').get()
                    if href.startswith('mailto:'):
                        info = href[7:]
                    else:
                        info = info.get()
                # if info_selector.xpath('./text()').get():
                else:
                    info = info_selector.xpath('./text()').get()
            else:
                info = ""

            if label and info:
                data[label.strip()] = info.strip()
                self.logger.info(f"Provider data: {data}")
                self.providers.append(data)


    def parse_inspections(self, response):
        self.logger.info(f"Parsing inspections at {response.url}")
        self.logger.info(f"Context data: {response.meta['parent_data']}")

        parent_data = response.meta['parent_data']
        inspection_data = []

        inspection_rows = response.xpath('//div[@class="resultsListRow"]')
        self.logger.info(f"Found {len(inspection_rows)} inspection rows")

        for row in inspection_rows:
            columns = row.xpath('.//div[@class="resultsListColumn"]')
            if len(columns) >= 6: # Ensure enough columns are present
                self.logger.info("Processing row. Found enough columns.")

                # Extract data from the columns and remove unwanted characters
                inspection_date = columns[0].xpath('./span/following-sibling::text()').get().strip().replace('\r\n', ' ')
                inspection_type = columns[1].xpath('./span/following-sibling::text()').get().strip().replace('\r\n', ' ')
                original_status = columns[2].xpath('./span/following-sibling::text()').get().strip().replace('\r\n', ' ')
                corrective_status = columns[3].xpath('./span/following-sibling::text()').get().strip().replace('\r\n', ' ')
                status_updated = columns[4].xpath('./span/following-sibling::text()').get().strip().replace('\r\n', ' ')
                report_link = columns[5].xpath('.//a/@href').get()  # Get report link

                inspection_data.append({
                    'inspection_date': inspection_date,
                    'inspection_type': inspection_type,
                    'original_status': original_status,
                    'corrective_status': corrective_status,
                    'status_updated': status_updated,
                    'report_link': report_link
                })

            self.logger.info(f"Inspection data: {inspection_data}")
            parent_data.update({'inspection_data': inspection_data})
            return None

    def close_spider(self, spider):
        self.logger.info("spider finished")

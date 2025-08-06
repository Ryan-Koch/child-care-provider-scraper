import scrapy
from scrapy_playwright.page import PageMethod
import io
import csv

from ..items import ProviderItem

class IllinoisSpider(scrapy.Spider):
    name = 'illinois'

    custom_settings = {
        'DOWNLOAD_HANDLERS': {
            'http': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
            'https': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
        },
        'TWISTED_REACTOR': 'twisted.internet.asyncioreactor.AsyncioSelectorReactor',
        'PLAYWRIGHT_LAUNCH_OPTIONS': {
            'headless': True
        },
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'ROBOTSTXT_OBEY': False,
    }

    async def start(self):
        """
        Initiates the first request. We use Playwright here only to render
        the page and get the dynamic __VIEWSTATE values. We do not click.
        """
        yield scrapy.Request(
            url="https://sunshine.dcfs.illinois.gov/Content/Licensing/Daycare/ProviderLookup.aspx",
            callback=self.start_download,
            meta={
                'playwright': True,
                'playwright_page_methods': [
                    PageMethod("wait_for_load_state", "domcontentloaded"),
                ]
            }
        )

    def start_download(self, response):
        """
        This callback takes the rendered page, extracts the necessary
        form values, and then submits a POST request to trigger the download.
        This replaces the problematic Playwright click.
        """
        self.logger.info(f"Successfully loaded initial page: {response.url}")

        viewstate = response.css('input#__VIEWSTATE::attr(value)').get()
        viewstate_generator = response.css('input#__VIEWSTATEGENERATOR::attr(value)').get()
        event_validation = response.css('input#__EVENTVALIDATION::attr(value)').get()
        event_target = 'ctl00$ContentPlaceHolderContent$ASPxButtonExport'

        yield scrapy.FormRequest(
            url=response.url,
            method='POST',
            callback=self.parse_csv,
            formdata={
                '__EVENTTARGET': event_target,
                '__EVENTARGUMENT': '',
                '__VIEWSTATE': viewstate,
                '__VIEWSTATEGENERATOR': viewstate_generator,
                '__EVENTVALIDATION': event_validation,
                'ctl00$ContentPlaceHolderContent$ASPxProviderName': 'Enter Provider Name',
                'ctl00$ContentPlaceHolderContent$ASPxCity': 'Enter City Name',
                'ctl00$ContentPlaceHolderContent$ASPxCounty': 'Enter County Name',
                'ctl00$ContentPlaceHolderContent$ASPxZip': 'Enter 5 Digit Zip Code',
            }
        )

    def parse_csv(self, response):
            """
            Receives the CSV data in the response body and parses it in memory
            using the exact headers from the provided sample file.
            """
            content_disposition = response.headers.get('Content-Disposition', b'').decode()
            if 'attachment' not in content_disposition:
                self.logger.error("Failed to download CSV. The response was not a file attachment.")
                return

            self.logger.info("Successfully received CSV data. Parsing now.")

            csv_data = response.body.decode('utf-8')
            csv_file = io.StringIO(csv_data)

            dict_reader = csv.DictReader(csv_file)

            for row in dict_reader:
                # Consolidate language fields into a single list, filtering out empty values.
                languages = [
                    lang for lang in
                    [row.get('Language1'), row.get('Language2'), row.get('Language3')]
                    if lang
                ]

                provider = ProviderItem()
                provider['source_state'] = 'CA'
                provider['il_provider_id'] = row.get('ProviderID')
                provider['provider_name'] = row.get('DoingBusinessAs')
                provider['address'] = f"{row.get('Street')}, {row.get('City')}, CA {row.get('Zip')}"
                provider['county'] = row.get('County')
                provider['phone'] = row.get('Phone')
                provider['il_facility_type'] = row.get('FacilityType')
                provider['il_day_age_range'] = row.get('DayAgeRange')
                provider['il_night_age_range'] = row.get('NightAgeRange')
                provider['il_day_capacity'] = row.get('DayCapacity')
                provider['il_night_capacity'] = row.get('NightCapacity')
                provider['status'] = row.get('Status')
                provider['languages'] = languages

                yield provider

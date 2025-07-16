import scrapy
import csv
import io
import json
import base64
from scrapy_playwright.page import PageMethod
from ..items import ProviderItem, InspectionItem


class TxhhsSpider(scrapy.Spider):
    name = "txhhs"

    custom_settings = {
        'DOWNLOAD_HANDLERS': {
            'http': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
            'https': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
        },
        'TWISTED_REACTOR': 'twisted.internet.asyncioreactor.AsyncioSelectorReactor',
        'PLAYWRIGHT_LAUNCH_OPTIONS': {
            'headless': True
        }
    }

    def __init__(self, *args, **kwargs_):
        super().__init__(*args, **kwargs_)
        self.intercepted_auth_header = None

    async def intercept_post_request(self, route, request):
        if request.method == "POST":
            self.logger.info(f"Checking POST on {request.url} for auth token")
            if self.intercepted_auth_header:
                await route.continue_()

            self.intercepted_auth_header = request.headers.get('authorization')
            self.logger.info(f"Auth header: {self.intercepted_auth_header}")
            self.logger.info(f"Headers: {request.headers}")
            await route.continue_()


    async def start(self):
        search_api_url = "**/**"
        wait_for_api = "**/__endpoint/reftable/getReferenceTables"
        # This is a bit brittle right now. I'm going to come back to it. Further down is a retry mechanic I'll use for now.
        # ToDo: Revisit the page methods here and explore making it less vulnerable to the auth header not being available.
        yield scrapy.Request(
            url="https://childcare.hhs.texas.gov/public/childcaresearch",
            callback=self.make_download_request,
            meta={
                "playwright": True,
                "playwright_handle_downloads": True,
                "playwright_page_methods": [
                    # Inteception method
                    PageMethod("route", search_api_url, self.intercept_post_request),
                    PageMethod("wait_for_selector", 'div.ux-btn-label-wrapper:has-text("Search")'),
                    PageMethod("click", 'div.ux-btn-label-wrapper:has-text("Search")'),
                    PageMethod("wait_for_load_state", "domcontentloaded")
                ],
            }
        )

    def make_download_request(self, response):
        """
        Callback method for request in start(). This method gets an authorization token and prepares a download request.
        The download is meant to pull a CSV file containing provider records.
        """
        download_url = 'https://childcare.hhs.texas.gov/__endpoint/ps/download/CDC/CSV?compare=false'
        self.logger.info(f"Preparing download request for {download_url}")
        # auth token

        if self.intercepted_auth_header:
            download_headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en',
                'Content-Type': 'application/json',
                'Origin': 'https://childcare.hhs.texas.gov',
                'Connection': 'keep-alive',
                'Referer': 'https://childcare.hhs.texas.gov/Public/ChildCareSearchResults',
                # Our auth token goes here
                'Authorization': self.intercepted_auth_header
            }
            # This is the exact payload from your cURL command
            download_body = {
                "operationNumber": "",
                "operationName": "",
                "providerName": "",
                "address": "",
                "city": "",
                "sortColumn": "",
                "sortOrder": "ASC",
                "pageSize": "",
                "pageNumber": 1,
                "includeApplicants": False,
                "providerAdrressOpt": "",
                "nearMeAddress": "",
                "commuteFromAddress": "",
                "commuteToAddress": "",
                "latLong": [],
                "radius": "",
                "providerTypes": [],
                "primaryCaregiverFirstName": "",
                "primaryCaregiverMiddleName": "",
                "primaryCaregiverLastName": "",
                "issuanceTypes": [],
                "agesServed": [],
                "mealOptions": [],
                "schedulesServed": [],
                "programProvided": [],
                "isAccredited": "",
                "providerWrkngDays": "",
                "providerWrkngHrs": "",
                "isDownload": True
            }
            yield scrapy.Request(url=download_url, method='POST', headers=download_headers, body=json.dumps(download_body), callback=self.parse_csv)
        else:
            self.logger.error("Retryable Error")
            self.logger.error("No auth token provided")


    def parse_csv(self, response):
        base_url = "https://childcare.hhs.texas.gov/Public/OperationDetails?operationId="
        self.logger.info(f"CSV download response from {response.url} was: {response.status}")
        providers = []
        csv.field_size_limit(1024 * 1024 * 1024)
        try:
            json_response = json.loads(response.body)
            csv_data = json_response.get("fileBytes")

            csv_bytes = base64.b64decode(csv_data)
            csv_data = csv_bytes.decode('utf-8-sig')

            csv_files = io.StringIO(csv_data)
            reader = csv.DictReader(csv_files)

            for row in reader:
                provider = ProviderItem()
                provider['provider_url'] = base_url + row['Operation #']
                provider['tx_operation_id'] = row['Operation #']
                provider['tx_agency_number'] = row['Agency Number']
                provider['provider_name'] = row['Operation/Caregiver Name']
                provider['address'] = f"{row['Address']} {row['City']}, {row['State']} {row['Zip']}"
                provider['county'] = row['County']
                provider['phone'] = row['Phone']
                provider['provider_type'] = row['Type']
                provider['status'] = row['Status']
                provider['status_date'] = row['Issue Date']
                provider['capacity'] = row['Capacity']
                provider['email'] = row['Email Address']
                provider['infant'] = row['Infant']
                provider['toddler'] = row['Toddler']
                provider['preschool'] = row['Preschool']
                provider['school'] = row['School']
                provider['hours'] = row['Hours']
                provider['tx_rising_star'] = row['Texas Rising Star '] # yes the trailing space is necessary for the key to match
                provider['scholarships_accepted'] = row['Accepts ChildCare Scholarships']
                provider['deficiencies'] = row['Deficiencies']

                yield provider




        except Exception as e:
            self.logger.error(f"Error parsing CSV: {e}")

        self.logger.info("Parsing complete")

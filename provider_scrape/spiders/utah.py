import scrapy
import json
import urllib.parse
from provider_scrape.items import ProviderItem

class UtahSpider(scrapy.Spider):
    name = "utah"
    allowed_domains = ["jobs.utah.gov"]
    
    # API Endpoints
    SEARCH_URL = "https://cac-api.jobs.utah.gov/program/v1/public/programs"
    DETAIL_URL_TEMPLATE = "https://cac-api.jobs.utah.gov/program/v1/public/program-details/programs/{}"

    custom_settings = {
        'DOWNLOAD_HANDLERS': {
            'http': 'scrapy.core.downloader.handlers.http.HTTPDownloadHandler',
            'https': 'scrapy.core.downloader.handlers.http.HTTPDownloadHandler',
        },
        'CONCURRENT_REQUESTS': 16,  # We can go faster with API
        'DOWNLOAD_DELAY': 0.5,      # Gentle delay
    }

    UT_ZIP_CODES = [
        '84001', '84002', '84003', '84004', '84005', '84006', '84007', '84008', '84009', '84010',
        '84011', '84013', '84014', '84015', '84016', '84017', '84018', '84020', '84021', '84022',
        '84023', '84024', '84025', '84026', '84027', '84028', '84029', '84031', '84032', '84033',
        '84034', '84035', '84036', '84037', '84038', '84039', '84040', '84041', '84042', '84043',
        '84044', '84045', '84046', '84047', '84048', '84049', '84050', '84051', '84052', '84053',
        '84054', '84055', '84056', '84057', '84058', '84059', '84060', '84061', '84062', '84063',
        '84064', '84065', '84066', '84067', '84068', '84069', '84070', '84071', '84072', '84073',
        '84074', '84075', '84076', '84078', '84079', '84080', '84081', '84082', '84083', '84084',
        '84085', '84086', '84087', '84088', '84089', '84090', '84091', '84092', '84093', '84094',
        '84095', '84096', '84097', '84098', '84101', '84102', '84103', '84104', '84105', '84106',
        '84107', '84108', '84109', '84110', '84111', '84112', '84113', '84114', '84115', '84116',
        '84117', '84118', '84119', '84120', '84121', '84122', '84123', '84124', '84125', '84126',
        '84127', '84128', '84129', '84130', '84131', '84132', '84133', '84134', '84138', '84139',
        '84143', '84145', '84147', '84148', '84150', '84151', '84152', '84157', '84158', '84165',
        '84170', '84171', '84180', '84184', '84190', '84199', '84201', '84244', '84301', '84302',
        '84304', '84305', '84306', '84308', '84309', '84310', '84311', '84312', '84313', '84314',
        '84315', '84316', '84317', '84318', '84319', '84320', '84321', '84322', '84323', '84324',
        '84325', '84326', '84327', '84328', '84329', '84330', '84331', '84332', '84333', '84334',
        '84335', '84336', '84337', '84338', '84339', '84340', '84341', '84401', '84402', '84403',
        '84404', '84405', '84407', '84408', '84409', '84412', '84414', '84415', '84501', '84510',
        '84511', '84512', '84513', '84515', '84516', '84518', '84520', '84521', '84522', '84523',
        '84525', '84526', '84528', '84529', '84530', '84531', '84532', '84533', '84534', '84535',
        '84536', '84537', '84539', '84540', '84542', '84601', '84602', '84603', '84604', '84605',
        '84606', '84620', '84621', '84622', '84623', '84624', '84626', '84627', '84628', '84629',
        '84630', '84631', '84632', '84633', '84634', '84635', '84636', '84637', '84638', '84639',
        '84640', '84642', '84643', '84644', '84645', '84646', '84647', '84648', '84649', '84651',
        '84652', '84653', '84654', '84655', '84656', '84657', '84660', '84662', '84663', '84664',
        '84665', '84667', '84701', '84710', '84711', '84712', '84713', '84714', '84715', '84716',
        '84718', '84719', '84720', '84721', '84722', '84723', '84724', '84725', '84726', '84728',
        '84729', '84730', '84731', '84732', '84733', '84734', '84735', '84736', '84737', '84738',
        '84739', '84740', '84741', '84742', '84743', '84744', '84745', '84746', '84747', '84749',
        '84750', '84751', '84752', '84753', '84754', '84755', '84756', '84757', '84758', '84759',
        '84760', '84761', '84762', '84763', '84764', '84765', '84766', '84767', '84770', '84771',
        '84772', '84773', '84774', '84775', '84776', '84779', '84780', '84781', '84782', '84783',
        '84784', '84790', '84791'
    ]

    def start_requests(self):
        for zip_code in self.UT_ZIP_CODES:
            # We can run these concurrently because we aren't using a browser
            # We start at page 0
            yield self.generate_search_request(zip_code, page=0)

    def generate_search_request(self, zip_code, page):
        # Construct URL with pagination parameters
        # Note: The 'sort' parameter in the curl was ','. I'll leave it empty or minimal.
        # Original: page=0&size=10&sort=,&miles=1&latitude=0&longitude=0
        params = {
            'page': page,
            'size': 20, # Increased size to reduce pagination requests
            'sort': ',',
            'miles': 1,
            'latitude': 0,
            'longitude': 0
        }
        query_string = urllib.parse.urlencode(params)
        url = f"{self.SEARCH_URL}?{query_string}"
        
        payload = {
            "andPredicates": {
                "filters": [
                    {
                        "fieldName": "zipCode",
                        "operator": ":",
                        "value": zip_code
                    }
                ]
            }
        }
        
        return scrapy.Request(
            url,
            method='POST',
            body=json.dumps(payload),
            headers={
                'Content-Type': 'application/json',
                'Referer': 'https://jobs.utah.gov/'
            },
            callback=self.parse_search,
            meta={'zip_code': zip_code, 'page': page}
        )

    def parse_search(self, response):
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error(f"Failed to parse JSON for {response.meta['zip_code']} page {response.meta['page']}")
            return

        providers = data.get('content', [])
        self.logger.info(f"Found {len(providers)} providers in zip {response.meta['zip_code']} page {response.meta['page']}")

        for provider in providers:
            program_id = provider.get('programId')
            if program_id:
                detail_url = self.DETAIL_URL_TEMPLATE.format(program_id)
                yield scrapy.Request(
                    detail_url,
                    callback=self.parse_detail,
                    meta={'search_data': provider}
                )

        # Pagination
        current_page = data.get('number', 0)
        total_pages = data.get('totalPages', 0)
        
        if current_page < total_pages - 1:
            yield self.generate_search_request(response.meta['zip_code'], current_page + 1)

    def parse_detail(self, response):
        search_data = response.meta['search_data']
        try:
            detail_data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error(f"Failed to parse detail JSON for {search_data.get('programId')}")
            # Fallback to search data only? Or return partial? 
            # Ideally we want the capacity from detail.
            # Let's try to proceed with what we have if detail fails, but it's unlikely.
            return 

        item = ProviderItem()
        item['source_state'] = 'UT'
        item['provider_url'] = f"https://jobs.utah.gov/occ/cac/search/program-detail/{search_data.get('programId')}"
        
        # Merge data strategies: Detail data > Search data
        # Common fields
        item['provider_name'] = detail_data.get('name') or search_data.get('program')
        
        # Address
        addr1 = detail_data.get('addressOne') or search_data.get('address')
        addr2 = detail_data.get('addressTwo')
        city = detail_data.get('city') or search_data.get('city')
        state = detail_data.get('state') or search_data.get('state')
        zip_code = detail_data.get('zipCode') or search_data.get('zipCode')
        
        full_address_parts = [addr1, addr2, city, state, zip_code]
        item['address'] = " ".join([p for p in full_address_parts if p])
        
        item['phone'] = self.format_phone(detail_data.get('phone') or search_data.get('phone'))
        item['email'] = detail_data.get('email') or search_data.get('email')
        
        # License/Quality
        item['ut_license_type'] = detail_data.get('licenseType') or search_data.get('licenseType')
        item['provider_type'] = item['ut_license_type'] # Common field mapping
        item['ut_quality_rating'] = detail_data.get('qrl') or search_data.get('qrl')
        
        # Dates
        item['license_begin_date'] = detail_data.get('licenseStartDate') or detail_data.get('initialRegulationDate')
        item['ut_licensed_since'] = item['license_begin_date']
        
        # Capacity & Vacancies
        # Detail has 'totalChildren' which is capacity
        item['capacity'] = detail_data.get('totalChildren')
        item['ut_vacancies'] = detail_data.get('vacancies') if 'vacancies' in detail_data else search_data.get('vacancy')
        
        # Boolean/Yes-No fields
        subsidy = detail_data.get('acpDwsSub') or search_data.get('acpDwsSub')
        if subsidy == 'Y':
            item['scholarships_accepted'] = 'Yes'
        elif subsidy == 'N':
            item['scholarships_accepted'] = 'No'
            
        # Attributes handling
        # Search data has comma-separated strings for many of these.
        # Detail data has 'attributes' list of objects.
        # We can use search data for simplicity as it's already comma-separated, 
        # or process attributes from detail if search is missing.
        
        item['ages_served'] = search_data.get('ageAccept')
        item['ut_school_district'] = search_data.get('school')
        item['ut_meals'] = search_data.get('meals')
        item['ut_environment'] = search_data.get('environment')
        
        # If any of the above are None, try to extract from detail attributes (optional enhancement)
        # For now, the search data covers these well.
        
        yield item

    def format_phone(self, phone):
        if not phone:
            return None
        # Basic formatting if it's just digits
        phone = str(phone).strip()
        if len(phone) == 10 and phone.isdigit():
            return f"({phone[:3]}) {phone[3:6]}-{phone[6:]}"
        return phone
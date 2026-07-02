import json
import re

import scrapy

from ..items import InspectionItem, ProviderItem


def _clean(value):
    """Collapse runs of whitespace (newlines/tabs from the DSS markup) to single
    spaces and strip the ends. Returns None for falsy input."""
    if not value:
        return value
    return re.sub(r"\s+", " ", value).strip()


class VadssSpider(scrapy.Spider):
    name = "virginia"
    allowed_domains = ["dss.virginia.gov", "earlychildhoodquality.doe.virginia.gov"]
    start_urls = ["https://www.dss.virginia.gov/facility/search/cc2.cgi"]

    PROGRAMS_JSON_URL = "https://earlychildhoodquality.doe.virginia.gov/vqb5-2025/site-search/programs.json"

    def __init__(self):
        self.providers_by_ID = {}
        self.pending_providers = 0
        self.pending_enrichments = 0

    def parse(self, response):
        """
        This method is the default callback used by Scrapy to process downloaded responses
        when their requests don't specify a callback.
        """
        self.logger.info(f"Parsing URL: {response.url}")

        title = response.xpath("//title/text()").get()
        self.logger.info(f"Page title: {title}")

        acceptable_names = [
            # Licensed
            "search_require_client_code-2101",
            "search_require_client_code-2102",
            "search_require_client_code-2106",
            # Regulated Unlicensed types
            "search_require_client_code-2105",
            "search_require_client_code-2201",
            "search_require_client_code-2104",
            # unregulated and unlicensed (commented out for now)
            # 'search_require_client_code-3001',
            # 'search_require_client_code-3002'
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
                name = checkbox.xpath("./@name").get()
                value = checkbox.xpath("@value").get()
                if name in acceptable_names:
                    formdata.setdefault(name, []).append(value)

        else:
            self.logger.warning("Could not find the ")

        return formdata

    def after_submit(self, response):
        """
        Callback function to handle the response after the form submission.
        """
        self.logger.info(f"Form submitted. Response URL: {response.url}")
        # You can now parse the results page here
        title = response.xpath("//title/text()").get()
        self.logger.info(f"Results page title: {title}")

        raw_links = response.xpath(
            '//table[contains(@class, "cc_search")]/tbody//a[contains(@href, "ID=")]/@href'
        ).getall()
        unique_links = list(dict.fromkeys(response.urljoin(link) for link in raw_links))
        self.logger.info(
            f"Number of providers found: {len(unique_links)} ({len(raw_links) - len(unique_links)} duplicates removed)"
        )
        self.pending_providers = len(unique_links)

        for link in unique_links:
            yield scrapy.Request(url=link, callback=self.parse_provider_page)

    def parse_provider_page(self, response):
        self.logger.info(f"Parsing provider page: {response.url}")
        # DSS detail URLs look like ...?rm=Details;ID=35291;search_require_client_code-...
        # Key on the bare ID so it matches the ID parsed from VQB5 enrichment links.
        id_match = re.search(r"[?;&]ID=(\w+)", response.url)
        provider_id = id_match.group(1) if id_match else response.url

        def extract_with_xpath(query, row=None):
            try:
                result = _clean(response.xpath(query).get(default="N/A"))
                return result if result else "N/A"
            except:
                return "N/A"

        def extract_inspection_data():
            inspection_data = []
            table = response.xpath(
                '//table[@class="cc_search"]/following::table[not(@class)]'
            )
            if table:
                rows = table.xpath(".//tr[position()>1]")
                for row in rows:
                    # Get the 'violations' cell (td[4]) and 'complaint_related' cell td[3] first
                    violations_td = row.xpath("./td[4]")
                    complaint_related_td = row.xpath("./td[3]")

                    # Try to get text from the 'a' tag within td[4]
                    violations_text = violations_td.xpath("./a/text()").get()
                    complaint_related_text = complaint_related_td.xpath(
                        "./a/text()"
                    ).get()

                    # If no text found in 'a' tag, try direct text or normalize-space
                    if not violations_text:
                        violations_text = violations_td.xpath(
                            "./text()"
                        ).get()  # For cases like 'No'
                        if not violations_text:
                            violations_text = violations_td.xpath(
                                "normalize-space()"
                            ).get()  # General cleanup if other text found

                    # If no text found in 'a' tag for complaint_related_td
                    if not complaint_related_text:
                        complaint_related_text = complaint_related_td.xpath(
                            "./text()"
                        ).get()  # For cases like 'No'
                        if not complaint_related_text:
                            complaint_related_text = complaint_related_td.xpath(
                                "normalize-space()"
                            ).get()  # General cleanup if other text found

                    inspection = InspectionItem(
                        date=_clean(row.xpath("./td[1]/a/text()").get()),
                        va_shsi=_clean(row.xpath("./td[2]/text()").get()),
                        va_complaint_related=_clean(complaint_related_text),
                        va_violations=_clean(violations_text),
                    )
                    inspection_data.append(inspection)
            return inspection_data

        provider = ProviderItem(
            va_ID=provider_id,
            provider_name=extract_with_xpath("//table[not(@class)]/tr[1]/td/b/text()"),
            address=f"{extract_with_xpath('//table[not(@class)]/tr[1]/td/br/following-sibling::text()')} {extract_with_xpath('//table[not(@class)]/tr[2]/td/text()')}",
            phone=extract_with_xpath("//table[not(@class)]/tr[3]/td/text()"),
            provider_type=extract_with_xpath(
                '//table[@class="cc_search"]/tr[1]/td[2]/span/span/font/u/text()'
            ),
            va_license_type=extract_with_xpath(
                '//table[@class="cc_search"]/tr[2]/td[2]/span/span/font/u/text()'
            ),
            administrator=extract_with_xpath(
                '//table[@class="cc_search"]/tr/td[contains(text(), "Administrator:")]/following-sibling::td/text()'
            ),
            hours=extract_with_xpath(
                '//table[@class="cc_search"]/tr/td[contains(text(), "Business Hours:")]/following-sibling::td/text()'
            ),
            capacity=extract_with_xpath(
                '//table[@class="cc_search"]/tr/td[contains(text(), "Capacity:")]/following-sibling::td/text()'
            ),
            ages_served=extract_with_xpath(
                '//table[@class="cc_search"]/tr/td[contains(text(), "Ages:")]/following-sibling::td/text()'
            ),
            va_inspector=extract_with_xpath(
                '//table[@class="cc_search"]/tr/td[contains(text(), "Inspector:")]/following-sibling::td/text()'
            ),
            va_current_subsidy_provider=extract_with_xpath(
                '//table[@class="cc_search"]/tr/td[contains(text(), "Current Subsidy Provider")]/following-sibling::td/text()'
            ),
            license_number=extract_with_xpath(
                '//table[@class="cc_search"]/tr/td[contains(text(), "License/Facility ID#")]/following-sibling::td/text()'
            ),
            inspections=extract_inspection_data(),
            provider_url=response.url,
            source_state="Virginia",
        )

        self.providers_by_ID[provider_id] = provider
        self.pending_providers -= 1
        self.logger.info(f"Pending providers left: {self.pending_providers}")

        if self.pending_providers == 0:
            self.logger.info("About to enrich from quality source")
            yield scrapy.Request(
                url=self.PROGRAMS_JSON_URL,
                callback=self.parse_quality_programs,
            )

    def parse_quality_programs(self, response):
        """Read the full VQB5 program list and queue every profile for enrichment.

        The VQB5 site paginates entirely client-side from this single JSON file
        (the ``?page=N`` URL param is ignored), so we pull every program's profile
        URL here rather than walking the rendered search-result pages.
        """
        # The endpoint emits a trailing comma before the closing bracket, which is
        # invalid JSON, so strip trailing commas before parsing.
        cleaned = re.sub(r",(\s*[}\]])", r"\1", response.text)
        programs = json.loads(cleaned).get("programs", [])

        urls = list(
            dict.fromkeys(
                response.urljoin(p["courseURL"]) for p in programs if p.get("courseURL")
            )
        )
        self.pending_enrichments = len(urls)
        self.logger.info(
            f"Queuing {self.pending_enrichments} VQB5 profiles for enrichment"
        )

        if self.pending_enrichments == 0:
            yield from self._yield_all_providers()
            return

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_quality_detail)

    def parse_quality_detail(self, response):
        """Enrich a DSS provider with VQB5 quality data from its detail page."""
        dss_link = response.css(
            'p.public-default a[href*="dss.virginia.gov"]::attr(href)'
        ).get()

        if dss_link:
            self.logger.info(f"Found DSS link {dss_link}")
            match = re.search(r"[?;&]ID=(\w+)", dss_link)
            provider_id = match.group(1) if match else None

            if provider_id and provider_id in self.providers_by_ID:
                self.logger.info(f"Found match for provider_id: {provider_id}")
                provider = self.providers_by_ID[provider_id]

                # VQB5 quality rating — text nodes from the first matching <p>.
                # The rating block is duplicated (modal + visible card), so we scope
                # to the first match to avoid repeating the value.
                rating_nodes = response.xpath(
                    '(//div[contains(@class,"card-body")]'
                    '//p[contains(@class,"card-text") and contains(.,"VQB5 Quality Rating")])[1]/text()'
                ).getall()
                if rating_nodes:
                    self.logger.info(f"Found rating data for {provider_id}")
                    raw = " ".join(t.strip() for t in rating_nodes if t.strip())
                    provider["va_quality_rating"] = raw.replace(
                        "VQB5 Quality Rating:", ""
                    ).strip()

                # Public Funding Information — text node(s) inside the <p> that
                # contains the <strong> label
                funding_nodes = response.xpath(
                    '//p[strong[contains(text(),"Public Funding Information:")]]/text()'
                ).getall()
                if funding_nodes:
                    self.logger.info(f"Found funding data for {provider_id}")
                    provider["va_public_funding"] = " ".join(
                        f.strip() for f in funding_nodes if f.strip()
                    )

                # Interaction observations per classroom type
                interactions = []
                for h4 in response.css(".card-normal-points h4"):
                    label = h4.css("::text").get("").strip()
                    desc = h4.xpath("following-sibling::p[1]/text()").get("").strip()
                    if label and desc:
                        interactions.append(f"{label}: {desc}")
                if interactions:
                    self.logger.info(f"Found interactions data for {provider_id}")
                    provider["va_interactions"] = "; ".join(interactions)

                # Quality point breakdown — these map to the VQB5 rating tier.
                # Each appears in a <p class="card-text"> as "<label>:<br> N points".
                # The label/value pair is duplicated (modal + visible card), so we
                # take the first numeric value found.
                def extract_points(label):
                    nodes = response.xpath(
                        f'//p[contains(@class,"card-text") and '
                        f'starts-with(normalize-space(.),"{label}:")]/text()'
                    ).getall()
                    for node in nodes:
                        m = re.search(r"\d[\d,]*", node)
                        if m:
                            return m.group(0).replace(",", "")
                    return None

                for label, field in (
                    ("Interactions Points", "va_interactions_points"),
                    ("Curriculum Points", "va_curriculum_points"),
                    ("Total Points", "va_total_points"),
                ):
                    value = extract_points(label)
                    if value is not None:
                        provider[field] = value

        self.pending_enrichments -= 1
        self.logger.info(f"Pending enrichments: {self.pending_enrichments}")
        if self.pending_enrichments == 0:
            yield from self._yield_all_providers()

    def _yield_all_providers(self):
        self.logger.info(
            f"Yielding all providers for output. Count: {len(self.providers_by_ID)}"
        )
        yield from self.providers_by_ID.values()

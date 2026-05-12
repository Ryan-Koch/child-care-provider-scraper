import scrapy

from ..items import InspectionItem, ProviderItem


SEARCH_URL = (
    "https://www.wvdhhr.org/bcf/ece/cccenters/eceWVIsearch.asp"
)

PROVIDER_LABEL_MAP = {
    "Agency Name": "provider_name",
    "County": "county",
    "Phone": "phone",
    "DHHR Licensing Specialist": "wv_licensing_specialist",
    "License Type": "wv_license_type",
    "License Expires": "license_expiration",
    "Contact": "wv_contact",
    "Title": "wv_contact_title",
}

CAPACITY_LABEL_MAP = {
    "Capacity": "capacity",
    "Age From": "wv_age_from",
    "Age To": "wv_age_to",
}

INSPECTION_LABEL_MAP = {
    "Corrective Action Plan Start": "wv_corrective_action_plan_start",
    "Corrective Action Plan End": "wv_corrective_action_plan_end",
    "Non Compliance Code": "wv_non_compliance_code",
    "Outcome Code": "wv_outcome_code",
    "Issue Completed Date": "wv_issue_completed_date",
}


class WestVirginiaSpider(scrapy.Spider):
    name = "west_virginia"
    allowed_domains = ["wvdhhr.org"]

    def start_requests(self):
        yield scrapy.FormRequest(
            url=SEARCH_URL,
            formdata={"view_all": "View All Providers"},
            callback=self.parse_results,
        )

    def parse_results(self, response):
        rows = response.css("tbody#center_data")
        self.logger.info(
            "Search results loaded from %s; found %d provider rows.",
            response.url,
            len(rows),
        )

        for index, row in enumerate(rows, start=1):
            href = row.css("a::attr(href)").get()
            if not href:
                self.logger.warning(
                    "Skipping result row %d: no detail link found.", index
                )
                continue

            detail_url = response.urljoin(href.strip())
            self.logger.debug(
                "Queueing detail page %d/%d: %s", index, len(rows), detail_url
            )
            yield scrapy.Request(
                url=detail_url, callback=self.parse_details
            )

    def parse_details(self, response):
        provider = ProviderItem()
        provider["source_state"] = "WV"
        provider["provider_url"] = response.url

        provider_data = self._parse_label_table(response, "centertablea")
        for label, field in PROVIDER_LABEL_MAP.items():
            value = provider_data.get(label)
            if value is not None:
                provider[field] = value

        address = self._build_address(provider_data)
        if address:
            provider["address"] = address

        capacity_data = self._parse_label_table(response, "centertableb")
        for label, field in CAPACITY_LABEL_MAP.items():
            value = capacity_data.get(label)
            if value is not None:
                provider[field] = value

        provider["inspections"] = self._parse_inspections(response)

        yield provider

    def _parse_label_table(self, response, table_id):
        """Return a {label: value} dict for a `<strong>` keyed two-column table."""
        data = {}
        rows = response.css(f"table#{table_id} tr")
        for row in rows:
            label = row.css("td strong::text").get()
            if not label:
                continue
            label = label.strip()
            value = " ".join(
                t.strip() for t in row.css("td:nth-of-type(2) ::text").getall()
            ).strip()
            data[label] = value
        return data

    def _build_address(self, provider_data):
        parts = [
            provider_data.get("Address 1", ""),
            provider_data.get("Address 2", ""),
        ]
        street = " ".join(p for p in (s.strip() for s in parts) if p)
        city = provider_data.get("City", "").strip()
        zip_code = provider_data.get("Zip Code", "").strip()
        city_zip = " ".join(p for p in (city, zip_code) if p)
        full = ", ".join(p for p in (street, city_zip) if p)
        return full

    def _parse_inspections(self, response):
        inspections = []
        rows = response.css("table#centertablec tr")
        current = None
        for row in rows:
            if row.css("td[bgcolor]"):
                if current is not None:
                    inspections.append(current)
                current = InspectionItem()
                continue

            label = row.css("td strong::text").get()
            if not label or current is None:
                continue
            label = label.strip()
            field = INSPECTION_LABEL_MAP.get(label)
            if field is None:
                continue
            value = " ".join(
                t.strip() for t in row.css("td:nth-of-type(2) ::text").getall()
            ).strip()
            current[field] = value

        if current is not None:
            inspections.append(current)
        return inspections

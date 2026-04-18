import json
import re

import scrapy

from provider_scrape.items import InspectionItem, ProviderItem

SEARCH_URL = "https://www.findchildcarewa.org/PSS_Search?p=DEL%20Licensed"
APEXREMOTE_URL = "https://www.findchildcarewa.org/apexremote"
DETAIL_URL = "https://www.findchildcarewa.org/PSS_Provider?id={}"


def extract_field(response, label_text):
    """Extract the form-control-static value following a label with the given text.

    The detail page uses this pattern throughout:
        <label class="...control-label">Label:</label>
        <div ...><p class="form-control-static">Value</p></div>
    """
    p = response.xpath(
        f'//label[contains(text(),"{label_text}")]'
        '/following-sibling::div/p[@class="form-control-static"]/text()'
    ).get()
    return p.strip() if p and p.strip() else None


class WashingtonSpider(scrapy.Spider):
    name = "washington"
    allowed_domains = ["findchildcarewa.org"]

    custom_settings = {
        "DOWNLOAD_DELAY": 0.25,
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "RETRY_TIMES": 5,
        "ROBOTSTXT_OBEY": False,
    }

    def start_requests(self):
        yield scrapy.Request(
            SEARCH_URL,
            callback=self.parse_search_page,
            dont_filter=True,
        )

    def parse_search_page(self, response):
        """Extract Visualforce remoting tokens and request all provider IDs."""
        match = re.search(
            r"RemotingProviderImpl\((\{.*?\})\)\);", response.text
        )
        if not match:
            self.logger.error("Could not find Visualforce remoting config")
            return

        config = json.loads(match.group(1))
        vid = config["vf"]["vid"]
        methods = config["actions"]["PSS_SearchController"]["ms"]
        sosl_method = next(m for m in methods if m["name"] == "getSOSLKeys")

        payload = json.dumps({
            "action": "PSS_SearchController",
            "method": "getSOSLKeys",
            "data": ["", "", ["DEL Licensed"], [], None, None, None, []],
            "type": "rpc",
            "tid": 2,
            "ctx": {
                "csrf": sosl_method["csrf"],
                "vid": vid,
                "ns": sosl_method["ns"],
                "ver": int(sosl_method["ver"]),
                "authorization": sosl_method["authorization"],
            },
        })

        yield scrapy.Request(
            APEXREMOTE_URL,
            method="POST",
            body=payload,
            headers={
                "Content-Type": "application/json",
                "X-User-Agent": "Visualforce-Remoting",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": SEARCH_URL,
                "Origin": "https://www.findchildcarewa.org",
            },
            callback=self.parse_provider_ids,
            dont_filter=True,
        )

    def parse_provider_ids(self, response):
        """Parse the getSOSLKeys response and yield detail page requests."""
        data = json.loads(response.text)
        provider_ids = data[0]["result"]
        self.logger.info(f"Found {len(provider_ids)} provider IDs")

        for provider_id in provider_ids:
            url = DETAIL_URL.format(provider_id)
            yield scrapy.Request(url, callback=self.parse_detail)

    def parse_detail(self, response):
        """Parse a provider detail page."""
        item = ProviderItem()
        item["source_state"] = "Washington"
        item["provider_url"] = response.url

        # Provider name from the h1 in the panel heading
        name = response.css(
            "div.panel-heading h1::text"
        ).get()
        if name:
            item["provider_name"] = name.strip()

        # Early Achievers status from the heading area
        ea_status = response.css(
            "div.panel-heading div.text-right > p::text"
        ).get()
        if ea_status and ea_status.strip():
            item["wa_early_achievers_status"] = ea_status.strip()

        # Address from the panel body
        addr_p = response.css(
            "div.panel-body div.col-xs-4 p[style='display:block']"
        )
        if addr_p:
            addr_html = addr_p[0].css("::text").getall()
            addr_text = " ".join(t.strip() for t in addr_html if t.strip())
            if addr_text:
                item["address"] = addr_text

        # Phone from the panel body
        if len(addr_p) > 1:
            phone = addr_p[1].css("::text").get()
            if phone and phone.strip():
                item["phone"] = phone.strip()

        # Provider status
        status_div = response.xpath(
            '//div[@class="panel-body"]'
            '//label[contains(text(),"Provider Status")]'
            "/following-sibling::div/text()"
        ).get()
        if status_div and status_div.strip():
            item["wa_provider_status"] = status_div.strip()

        # Hours of operation
        hours_items = response.css(
            "div.panel-body ul.list-unstyled li"
        )
        hours_parts = []
        for li in hours_items:
            texts = li.css("::text").getall()
            day_label = texts[0].strip().rstrip("\xa0") if texts else ""
            time_val = texts[1].strip() if len(texts) > 1 else ""
            if time_val:
                hours_parts.append(f"{day_label} {time_val}")
        if hours_parts:
            item["hours"] = "; ".join(hours_parts)

        # Lat/Lng from inline JavaScript
        lat_match = re.search(r"var lat = ([\d.-]+)", response.text)
        lng_match = re.search(r"var lng = ([\d.-]+)", response.text)
        if lat_match:
            item["latitude"] = lat_match.group(1)
        if lng_match:
            item["longitude"] = lng_match.group(1)

        # Fields from the pageContentSource hidden div
        self._parse_detail_fields(response, item)

        # Provider contacts table
        item["wa_contacts"] = self._parse_contacts(response)

        # Inspections table
        item["inspections"] = self._parse_inspections(response)

        # License history table
        item["wa_license_history"] = self._parse_license_history(response)

        yield item

    def _parse_detail_fields(self, response, item):
        """Parse the form fields from the detail section."""
        item["email"] = extract_field(response, "Email:")
        item["administrator"] = extract_field(response, "Primary Contact:")
        item["wa_head_start"] = extract_field(response, "Head Start Funding:")
        item["wa_early_head_start"] = extract_field(
            response, "Early Head Start Funding:"
        )
        item["wa_eceap"] = extract_field(response, "ECEAP Funding:")
        item["wa_available_slots"] = extract_field(
            response, "Total Available Slots:"
        )
        item["wa_slot_age_groups"] = extract_field(
            response, "Age Groups of Available Slots:"
        )
        item["languages"] = extract_field(response, "Languages Spoken:")
        item["wa_languages_of_instruction"] = extract_field(
            response, "Languages of Instruction:"
        )
        item["wa_license_name"] = extract_field(response, "License Name:")
        item["license_number"] = extract_field(response, "License Number:")
        item["wa_provider_id"] = extract_field(response, "Provider ID:")
        item["provider_type"] = extract_field(response, "Facility Type:")
        item["ages_served"] = extract_field(response, "Ages:")
        item["license_begin_date"] = extract_field(
            response, "Initial License Date:"
        )
        item["status"] = extract_field(response, "License Status:")
        item["wa_license_type"] = extract_field(response, "License Type:")
        item["capacity"] = extract_field(response, "Licensed Capacity:")
        item["wa_school_district"] = extract_field(
            response, "School District:"
        )
        item["wa_food_program"] = extract_field(
            response, "Food Program Participation:"
        )
        item["wa_subsidy"] = extract_field(
            response, "Subsidy Participation:"
        )

        # Website is in a form-control-static inside a special form
        website_p = response.xpath(
            '//label[contains(text(),"Website:")]'
            "/following-sibling::div//p[@class='form-control-static']"
        )
        if website_p:
            link = website_p.css("a::attr(href)").get()
            text = website_p.css("::text").get()
            if link:
                item["provider_website"] = link
            elif text and text.strip():
                item["provider_website"] = text.strip()

    def _parse_contacts(self, response):
        """Parse the provider contacts table."""
        contacts = []
        rows = response.css("#ProviderContactsTable tbody tr")
        for row in rows:
            tds = row.css("td")
            if len(tds) < 5:
                continue

            def cell_text(idx):
                text = tds[idx].css("::text").get()
                return text.strip() if text and text.strip() else ""

            contacts.append({
                "name": cell_text(0),
                "role": cell_text(1),
                "email": cell_text(2),
                "phone": cell_text(3),
                "start_date": cell_text(4),
            })
        return contacts

    def _parse_inspections(self, response):
        """Parse the inspections table from the inspections tab."""
        inspections = []
        rows = response.css("#inspections table.table-striped tbody tr")
        for row in rows:
            cells = row.css("td")
            if not cells:
                continue

            insp = InspectionItem()

            date_text = cells[0].css("::text").getall()
            date_vals = [t.strip() for t in date_text if t.strip()]
            if date_vals:
                insp["date"] = date_vals[0]

            if len(cells) > 1:
                type_text = cells[1].css("::text").getall()
                type_vals = [t.strip() for t in type_text if t.strip()]
                if type_vals:
                    insp["type"] = type_vals[0]

            if len(cells) > 2:
                checklist = cells[2].css("::text").getall()
                checklist_vals = [t.strip() for t in checklist if t.strip()]
                if checklist_vals:
                    insp["original_status"] = checklist_vals[0]

            # Document link
            doc_link = row.css("a::attr(href)").get()
            if doc_link:
                insp["report_url"] = doc_link

            if insp.get("date") or insp.get("type"):
                inspections.append(insp)

        return inspections

    def _parse_license_history(self, response):
        """Parse the license history table."""
        history = []
        rows = response.css("#license_history table.table-striped tbody tr")
        for row in rows:
            tds = row.css("td")
            if len(tds) < 9:
                continue

            def cell_text(idx):
                text = tds[idx].css("::text").get()
                return text.strip() if text and text.strip() else ""

            history.append({
                "license_id": cell_text(0),
                "regulation_type": cell_text(1),
                "regulation_authority": cell_text(2),
                "facility_type": cell_text(3),
                "license_type": cell_text(4),
                "license_status": cell_text(5),
                "issue_date": cell_text(6),
                "closure_date": cell_text(7),
                "status_reason": cell_text(8),
            })
        return history

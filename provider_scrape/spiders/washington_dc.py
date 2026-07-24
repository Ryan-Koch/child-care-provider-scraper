"""Washington DC child care provider spider.

Source: the DC ``mychildcare.dc.gov`` facility search
(https://mychildcare.dc.gov/Home/SearchFacilities), an ASP.NET MVC site.

The site renders its map/list from two plain ``POST`` endpoints that return HTML
fragments (no cookie, anti-forgery token, or CAPTCHA required), so this is a
pure-Scrapy spider -- no Playwright needed:

  * ``POST /Home/RunQuickSearch`` with ``facilityName=&distance=0`` returns
    **every** facility (~445) as a list of cards in one response. Each card
    carries the facility id, name, address, phone, latitude/longitude, and a
    set of program badges (Capital Quality, subsidies, food program, pay-equity
    fund, Pre-K enhancement, Montessori, nontraditional hours).
  * ``POST /MyChildCare/FacilityDetail`` with ``facilityID=<id>`` returns the
    detail panel for one facility: facility type, contact, email, hours, the
    Capital Quality *designation* (DC's quality rating), ages served, languages,
    subsidy acceptance, capacity, and an enrollment/openings/tuition table.

The ``FacilityProfile?FacilityId=<id>`` GET page (linked by the "Facility
Details" button) is server-broken -- it resets the connection -- so we never
use it, though it remains the canonical public per-facility URL we emit as
``provider_url``.

DC exposes no license number, license dates, status, or inspection data; it is
a directory/quality portal, not a licensing/enforcement one.
"""
import re

import scrapy

from provider_scrape.items import ProviderItem

BASE = "https://mychildcare.dc.gov"
LIST_URL = f"{BASE}/Home/RunQuickSearch"
DETAIL_URL = f"{BASE}/MyChildCare/FacilityDetail"
# Canonical public per-facility page (server-broken for scraping; emitted as a
# human-facing link only).
PROFILE_URL = f"{BASE}/MychildCare/FacilityProfile?FacilityId={{}}"

# List-card program badges (matched by their alt/title text) -> where the datum
# lands. Booleans become True when the badge is present on the card.
DAYS = ("Mon", "Tues", "Wed", "Thurs", "Fri", "Sat", "Sun")
ENROLLMENT_AGE_GROUPS = ("Infant", "Toddler", "Preschool", "School Age")
# Age-group label prefix -> the ProviderItem boolean age field it drives.
AGE_GROUP_TO_FIELD = {
    "Infant": "infant",
    "Toddler": "toddler",
    "Preschool": "preschool",
    "School Age": "school",
}
# Values the site uses to mean "no data" / "does not apply".
_EMPTY_VALUES = {"", "no data available", "not applicable"}


def _clean(value):
    """Collapse whitespace; return None for empty/None."""
    if value is None:
        return None
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def _has_value(value):
    """True when a cell carries real data (not blank / "No Data Available")."""
    return value is not None and value.strip().lower() not in _EMPTY_VALUES


class WashingtonDcSpider(scrapy.Spider):
    name = "washington_dc"
    allowed_domains = ["mychildcare.dc.gov"]

    custom_settings = {
        "DOWNLOAD_DELAY": 0.25,
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
    }

    # --------------------------------------------------------------------- #
    # Roster: one POST returns every facility.
    # --------------------------------------------------------------------- #
    def start_requests(self):
        yield scrapy.FormRequest(
            LIST_URL,
            formdata={"facilityName": "", "distance": "0"},
            callback=self.parse_list,
        )

    def parse_list(self, response):
        cards = response.xpath('//div[@class="facility"]')
        self.logger.info("parse_list: %d facilities in roster", len(cards))
        for card in cards:
            fid = card.xpath("@id").get()
            if not fid:
                continue
            badges = set(card.xpath(".//@title").getall()) | set(
                card.xpath(".//@alt").getall())
            address = _clean(
                card.xpath(f'.//span[@id="addr_{fid}"]//text()').get())
            if address:
                address = address.rstrip(", ").strip()
            meta = {
                "fid": fid,
                "name": _clean(
                    card.xpath(f'.//a[@id="name_{fid}"]//text()').get()),
                "list_address": address,
                "phone": _clean(
                    card.xpath(f'.//span[@id="phone_{fid}"]//text()').get()),
                "latitude": card.xpath("@lat").get(),
                "longitude": card.xpath("@lng").get(),
                "badges": badges,
            }
            yield scrapy.FormRequest(
                DETAIL_URL,
                formdata={"facilityID": fid},
                callback=self.parse_detail,
                meta=meta,
            )

    # --------------------------------------------------------------------- #
    # Detail: rich fields for one facility.
    # --------------------------------------------------------------------- #
    def parse_detail(self, response):
        fid = response.meta["fid"]
        badges = response.meta["badges"]

        item = ProviderItem()
        item["source_state"] = "Washington DC"
        item["provider_url"] = PROFILE_URL.format(fid)
        # DC publishes no license number; the internal facility id is the
        # closest stable registration id.
        item["license_number"] = fid

        # From the roster card.
        item["provider_name"] = response.meta["name"]
        item["phone"] = response.meta["phone"]
        item["latitude"] = response.meta["latitude"]
        item["longitude"] = response.meta["longitude"]

        # Address: prefer the detail header (carries ", DC ZIP"); fall back to
        # the roster card. Every facility is in Washington, DC, and the address
        # has no city token (only a quadrant), so set the components explicitly
        # rather than let the parser mistake the quadrant for a city.
        address = self._detail_address(response) or response.meta["list_address"]
        item["address"] = address
        item["city"] = "Washington"
        item["state"] = "DC"
        item["zip"] = self._zip(address)

        # Header key/values.
        item["provider_type"] = self._header_value(response, "Facility Type:")
        item["administrator"] = self._header_value(response, "Contact:")
        item["email"] = self._header_email(response)

        # Two-up label/value grid.
        item["dc_capital_quality_designation"] = self._grid_value(
            response, "Capital Quality")
        item["ages_served"] = self._grid_value(response, "Ages")
        item["capacity"] = self._grid_value(response, "Facility")
        subsidies = self._grid_value(response, "Accepts Subsidies")
        if subsidies is not None:
            item["scholarships_accepted"] = subsidies
        item["languages"] = self._languages(response)

        item["hours"] = self._hours(response)

        enrollment = self._enrollment(response)
        if enrollment:
            item["dc_enrollment"] = enrollment
            for row in enrollment:
                field = self._age_field(row["age_group"])
                if field:
                    item[field] = any(
                        _has_value(row[k]) for k in
                        ("openings", "current_enrollment",
                         "desired_enrollment", "monthly_tuition"))

        # Program badges from the roster card.
        item["dc_capital_quality_participant"] = (
            "Capital Quality Participant" in badges)
        item["dc_pay_equity_fund"] = (
            "Participating in Pay Equity Fund" in badges)
        item["dc_prek_enhancement"] = "Pre-K Enhancement" in badges
        item["dc_nontraditional_hours"] = "Nontraditional" in badges
        if "Child and Adult Care Food Program" in badges:
            item["meals"] = "Child and Adult Care Food Program"
        if "Montessori" in badges:
            item["curriculum"] = "Montessori"

        yield item

    # --------------------------------------------------------------------- #
    # Detail parsing helpers.
    # --------------------------------------------------------------------- #
    @staticmethod
    def _detail_address(response):
        """The address span in the detail header, cleaned (" , DC" -> ", DC")."""
        # The header shows name, then the address, in the first two spans.
        for text in response.xpath('//span/text()').getall():
            text = _clean(text)
            if text and re.search(r",\s*DC\s+\d{5}", text):
                return re.sub(r"\s+,", ",", text)
        return None

    @staticmethod
    def _zip(address):
        if not address:
            return None
        match = re.search(r"\b(\d{5})\b", address)
        return match.group(1) if match else None

    @staticmethod
    def _header_value(response, label):
        """Value after a ``Label:&nbsp;`` header span (e.g. Facility Type:)."""
        for span in response.xpath("//span"):
            text = _clean(" ".join(span.xpath(".//text()").getall()))
            if text and label in text:
                return _clean(text.split(label, 1)[1])
        return None

    @staticmethod
    def _header_email(response):
        for span in response.xpath("//span"):
            text = _clean(" ".join(span.xpath(".//text()").getall()))
            if text and "@" in text and " " not in text:
                return text
        return None

    @staticmethod
    def _grid_value(response, label_substring):
        """Value from the two-up label/value grid.

        Labels sit in ``<b>`` cells; the matching values are in the *next*
        ``<tr>``. That value row omits the rowspanned "Facility Hours" cell, so
        column indices shift -- instead we match by rank: the Nth label cell
        (cells containing a ``<b>``) maps to the Nth non-empty value cell.
        """
        bold = response.xpath(f'.//b[contains(., "{label_substring}")]')
        if not bold:
            return None
        label_td = bold[0].xpath("ancestor::td[1]")
        if not label_td:
            return None
        label_td = label_td[0]
        label_tr = label_td.xpath("ancestor::tr[1]")[0]
        label_cells = label_tr.xpath("td[.//b]")
        roots = [cell.root for cell in label_cells]
        if label_td.root not in roots:
            return None
        rank = roots.index(label_td.root)
        value_tr = label_tr.xpath("following-sibling::tr[1]")
        if not value_tr:
            return None
        values = []
        for cell in value_tr[0].xpath("td"):
            text = _clean(" ".join(cell.xpath(".//text()").getall()))
            if text:
                values.append(text)
        return values[rank] if rank < len(values) else None

    def _languages(self, response):
        """Join "Language Spoken" and "Other Languages", dropping empties."""
        spoken = self._grid_value(response, "Language")
        other = self._grid_value(response, "Other")
        langs = [lang for lang in (spoken, other) if _has_value(lang)]
        return ", ".join(langs) if langs else None

    @staticmethod
    def _hours(response):
        """Per-day operating hours joined into one string."""
        strong = response.xpath('//strong[contains(., "Facility Hours")]/parent::*')
        if not strong:
            return None
        tokens = [_clean(t) for t in strong[0].xpath(".//text()").getall()]
        tokens = [t for t in tokens if t]
        day_labels = {f"{day}:" for day in DAYS}
        parts = []
        index = 0
        while index < len(tokens):
            if tokens[index] in day_labels:
                day = tokens[index].rstrip(":")
                value = tokens[index + 1] if index + 1 < len(tokens) else ""
                parts.append(f"{day} {value}".strip())
                index += 2
            else:
                index += 1
        return "; ".join(parts) if parts else None

    @staticmethod
    def _enrollment(response):
        """The enrollment/openings/tuition table as a list of per-age dicts."""
        header = response.xpath(
            '//*[contains(text(), "Enrollment and Openings")]')
        if not header:
            return None
        table = header[0].xpath("ancestor::table[1]")
        if not table:
            return None
        rows = []
        for tr in table[0].xpath(".//tr"):
            cells = [
                _clean(" ".join(td.xpath(".//text()").getall()))
                for td in tr.xpath("td")
            ]
            cells = [cell for cell in cells if cell is not None]
            if not cells:
                continue
            if not cells[0].startswith(ENROLLMENT_AGE_GROUPS):
                continue
            cells = (cells + [None] * 5)[:5]
            rows.append({
                "age_group": cells[0],
                "openings": cells[1],
                "current_enrollment": cells[2],
                "desired_enrollment": cells[3],
                "monthly_tuition": cells[4],
            })
        return rows or None

    @staticmethod
    def _age_field(age_group):
        for prefix, field in AGE_GROUP_TO_FIELD.items():
            if age_group.startswith(prefix):
                return field
        return None

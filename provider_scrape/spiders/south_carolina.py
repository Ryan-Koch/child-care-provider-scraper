import math
import re
from urllib.parse import urljoin

import scrapy

from provider_scrape.items import InspectionItem, ProviderItem


BASE_URL = "https://www.scchildcare.org"
LISTING_URL = f"{BASE_URL}/provider-search/?all=1"

# Map ABC quality image slug → display rating.
ABC_RATING_MAP = {
    "a-plus": "A+",
    "a": "A",
    "b-plus": "B+",
    "b": "B",
    "c": "C",
    "p": "P",
}

# Inline facCoords JS entries look like:
#   '24680':{'latlng':new google.maps.LatLng(34.008744,-81.040186),...}
FAC_COORDS_RE = re.compile(
    r"'([^']+)':\{'latlng':new google\.maps\.LatLng\(([-\d.]+),([-\d.]+)\)"
)

ABC_IMG_RE = re.compile(r"/abc-([a-z-]+)\.png")

PAGE_SIZE = 8


def parse_abc_rating(src):
    """Extract "A+"/"A"/"B+"/… from a /img/abc-*.png URL."""
    if not src:
        return None
    m = ABC_IMG_RE.search(src)
    if not m:
        return None
    key = m.group(1).lower()
    return ABC_RATING_MAP.get(key, key.upper())


def parse_fac_coords(js_text):
    """Extract {provider_id: (lat, lng)} from the page's inline facCoords JS."""
    coords = {}
    for match in FAC_COORDS_RE.finditer(js_text):
        coords[match.group(1)] = (match.group(2), match.group(3))
    return coords


def extract_attribute(selector, label):
    """Return the text of the <p> that immediately follows
    <p class="attribute-title">{label}</p>, or None if that pattern isn't
    present under this selector.
    """
    parts = selector.xpath(
        './/p[@class="attribute-title" and normalize-space(.)=$label]'
        "/following-sibling::p[1]//text()",
        label=label,
    ).getall()
    text = " ".join(p.strip() for p in parts if p and p.strip())
    return text or None


class SouthCarolinaSpider(scrapy.Spider):
    """Spider for South Carolina DSS child care provider data.

    The listing at /provider-search/?all=1 is a paginated server-rendered
    list of every provider. Each result links to a per-provider detail
    page where the full attribute set lives (operator, capacity, hours,
    licensing, specialist contact, inspection history, ABC review
    history). Lat/lng for each card is only exposed via the inline
    `facCoords` JS on the listing page, so we capture it there and pass
    it through request meta to the detail parser.
    """

    name = "south_carolina"
    allowed_domains = ["scchildcare.org"]

    custom_settings = {
        "DOWNLOAD_DELAY": 0.25,
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "RETRY_TIMES": 5,
        "ROBOTSTXT_OBEY": False,
    }

    def start_requests(self):
        yield scrapy.Request(
            LISTING_URL,
            callback=self.parse_search_page,
            meta={"page": 1},
            dont_filter=True,
        )

    def parse_search_page(self, response):
        page = response.meta.get("page", 1)

        total_count_text = response.css(
            "div.results-details span.number::text"
        ).get()
        total_count = None
        total_pages = None
        if total_count_text and total_count_text.strip().isdigit():
            total_count = int(total_count_text.strip())
            total_pages = math.ceil(total_count / PAGE_SIZE)

        if page == 1:
            self.logger.info(
                "South Carolina listing: total_providers=%s total_pages=%s",
                total_count,
                total_pages,
            )

        coords = parse_fac_coords(response.text)

        results = response.css("section.search-results div.row.result")
        self.logger.info(
            "[page %s/%s] %d results on page, %d lat/lng entries from facCoords",
            page,
            total_pages,
            len(results),
            len(coords),
        )

        for row in results:
            link = row.css('a[href^="/provider/"]::attr(href)').get()
            if not link:
                continue

            m = re.match(r"^/provider/([^/]+)/", link)
            provider_id = m.group(1) if m else None

            lat, lng = (None, None)
            if provider_id and provider_id in coords:
                lat, lng = coords[provider_id]

            yield scrapy.Request(
                urljoin(response.url, link),
                callback=self.parse_detail,
                meta={
                    "sc_provider_id": provider_id,
                    "latitude": lat,
                    "longitude": lng,
                },
            )

        next_href = response.css(
            'ul.pagination a.page-link[aria-label="Next"]::attr(href)'
        ).get()
        if next_href:
            next_url = urljoin(response.url, next_href)
            m = re.search(r"[?&]page=(\d+)", next_url)
            next_page = int(m.group(1)) if m else page + 1
            self.logger.info(
                "Pagination: page %s → page %s (of %s total pages)",
                page,
                next_page,
                total_pages,
            )
            yield scrapy.Request(
                next_url,
                callback=self.parse_search_page,
                meta={"page": next_page},
            )

    def parse_detail(self, response):
        item = ProviderItem()
        item["source_state"] = "South Carolina"
        item["provider_url"] = response.url
        item["sc_provider_id"] = response.meta.get("sc_provider_id")
        item["latitude"] = response.meta.get("latitude")
        item["longitude"] = response.meta.get("longitude")

        info = response.css("section.location-info")

        name = info.css("h1::text").get()
        if name and name.strip():
            item["provider_name"] = name.strip()

        # First icon-detail beneath the h1 carries the facility type.
        ptype = info.xpath(
            './/h1/following-sibling::div[contains(@class,"row")]'
            '[1]//div[contains(@class,"icon-detail")]//p/text()'
        ).get()
        if ptype and ptype.strip():
            item["provider_type"] = ptype.strip()

        # ABC quality rating — first grey-bg block pairs "ABC Quality Rating"
        # with an img whose src encodes the rating.
        abc_src = info.xpath(
            './/p[contains(normalize-space(.),"ABC Quality Rating")]'
            '/ancestor::div[contains(@class,"row")][1]'
            '//img[contains(@src,"/abc-")]/@src'
        ).get()
        rating = parse_abc_rating(abc_src)
        if rating:
            item["sc_abc_quality_rating"] = rating

        operator = extract_attribute(info, "Operator:")
        if operator:
            item["administrator"] = operator

        capacity = extract_attribute(info, "Capacity:")
        if capacity:
            item["capacity"] = capacity

        tags = info.css("div.provider-tags span.tag img::attr(alt)").getall()
        tags = [t.strip() for t in tags if t and t.strip()]
        if tags:
            item["sc_program_participation"] = tags

        hours = self._parse_hours(info)
        if hours:
            item["hours"] = hours

        lic_type_num = extract_attribute(info, "Licensing Type & Number:")
        if lic_type_num:
            if lic_type_num.lower().startswith("not licensed"):
                item["sc_license_category"] = "Exempt"
                item["status"] = lic_type_num
            else:
                m = re.match(r"^(License|Approval)#:\s*(.+)$", lic_type_num)
                if m:
                    item["sc_license_category"] = m.group(1)
                    item["license_number"] = m.group(2).strip()
                else:
                    item["license_number"] = lic_type_num

        issue_date = extract_attribute(info, "Issue Date:")
        if issue_date:
            item["license_begin_date"] = issue_date

        exp_date = extract_attribute(info, "Expiration Date:")
        if exp_date:
            item["license_expiration"] = exp_date

        spec = info.css("div.specialists div.specialist")
        if spec:
            spec_name = extract_attribute(spec, "DSS Licensing Specialist")
            if spec_name:
                item["sc_licensing_specialist_name"] = spec_name
            spec_phone = spec.css('a[href^="tel:"]::text').get()
            if spec_phone and spec_phone.strip():
                item["sc_licensing_specialist_phone"] = spec_phone.strip()

        self._parse_contact_block(info, item)

        inspections = self._parse_inspections(response)
        if inspections:
            item["inspections"] = inspections
        else:
            item["inspections"] = []

        abc_history = self._parse_abc_history(response)
        if abc_history:
            item["sc_abc_rating_history"] = abc_history

        yield item

    def _parse_hours(self, info):
        """Join the Facility Hours table into 'Day times; …' format."""
        rows = info.css("div.facility-hours table.data-table tbody tr")
        parts = []
        for row in rows:
            day = row.css("th::text").get()
            if not day or not day.strip():
                continue
            time_vals = row.css("td li::text").getall()
            times = ", ".join(t.strip() for t in time_vals if t and t.strip())
            if times:
                parts.append(f"{day.strip()} {times}")
        return "; ".join(parts) if parts else None

    def _parse_contact_block(self, info, item):
        """Address / county / phone live in the right-hand column."""
        blocks = info.css("div.location-contact div.icon-detail")
        if not blocks:
            return

        first = blocks[0]
        addr_lines = [
            t.strip() for t in first.css("p::text").getall() if t and t.strip()
        ]
        if addr_lines:
            last = addr_lines[-1]
            if last.lower().endswith("county"):
                item["county"] = re.sub(
                    r"\s*County\s*$", "", last, flags=re.IGNORECASE
                ).strip()
                addr_lines = addr_lines[:-1]
            if addr_lines:
                item["address"] = ", ".join(addr_lines)

        if len(blocks) > 1:
            phone = blocks[1].css('a[href^="tel:"]::text').get()
            if not phone:
                phone = blocks[1].css("p::text").get()
            if phone and phone.strip():
                item["phone"] = phone.strip()

    def _parse_inspections(self, response):
        """Parse every .inspection-row in the Inspection History section."""
        inspections = []
        for row in response.css(
            "section.location-inspections div.inspection-row"
        ):
            insp = InspectionItem()

            date = row.css("div.date p::text").get()
            if date and date.strip():
                insp["date"] = date.strip()

            itype = row.css("div.type p::text").get()
            if itype and itype.strip():
                insp["type"] = itype.strip()

            report = row.css("div.download a::attr(href)").get()
            if report and report.strip():
                insp["report_url"] = report.strip()

            alert_text = " ".join(
                t.strip()
                for t in row.css("div.alerts p::text").getall()
                if t and t.strip()
            )
            if alert_text:
                m = re.search(r"(\d+)\s+Alert", alert_text)
                if m:
                    insp["sc_alert_count"] = int(m.group(1))
                m = re.search(r"(\d+)\s+Resolved", alert_text)
                if m:
                    insp["sc_alert_resolved_count"] = int(m.group(1))

            deficiencies = self._parse_deficiencies(row)
            if deficiencies:
                insp["sc_deficiencies"] = deficiencies

            if any(insp.get(k) for k in ("date", "type", "report_url")):
                inspections.append(insp)

        return inspections

    def _parse_deficiencies(self, row):
        """Each .alert-slide inside an inspection row is one deficiency."""
        deficiencies = []
        for slide in row.css("div.alert-dropdown div.alert-slide"):
            defic = {}

            severity = slide.css("a.severity::text").get()
            if severity and severity.strip():
                defic["severity"] = severity.strip()

            for info_row in slide.css("div.alert-info"):
                label = info_row.css("span.label::text").get()
                if not label:
                    continue
                label = label.strip().rstrip(":")
                if label == "Severity Level":
                    continue
                value = info_row.css("p::text").get()
                if value and value.strip():
                    key = label.lower().replace(" ", "_")
                    defic[key] = value.strip()

            if defic:
                deficiencies.append(defic)
        return deficiencies

    def _parse_abc_history(self, response):
        """Parse the 'ABC Facility Review' date/rating table."""
        history = []
        for row in response.css(
            "section.location-inspections table.data-table.deficiencies tbody tr"
        ):
            date = row.xpath('.//td[@data-title="Date"]/text()').get()
            rating_src = row.xpath(
                './/td[@data-title="Rating"]//img/@src'
            ).get()
            if not date and not rating_src:
                continue
            history.append({
                "date": date.strip() if date and date.strip() else None,
                "rating": parse_abc_rating(rating_src),
            })
        return history

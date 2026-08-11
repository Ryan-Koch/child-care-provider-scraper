"""South Dakota Child Care Provider Search spider.

Source: https://olapublic.sd.gov/child-care-provider-search/ -- a plain
server-rendered site. A ``GET .../child-care-provider-search/?search=true&
providerType=Child+Care[&status=...]`` renders *every* matching row into one
HTML results table in a single response (DataTables then paginates that table
client-side, which is irrelevant to us -- every row is already in the DOM).
Detail pages are plain server-rendered HTML at
``.../child-care-program-profile/{id}``. No JavaScript, no session/cookies, no
bot protection is involved anywhere in this crawl.

Three gotchas drive most of the design here:

1. **The detail page's "Status" field always reads "Operational"**, even for
   Closed/Pending providers -- proven live against an id that only the
   ``status=Closed`` search facet returns. The only reliable status source is
   *which search facet an id appeared under* in Phase 1 (see
   ``parse_results`` / ``_dispatch_details``). Never read status off the
   detail page.
2. **Services / Ages / Months multi-selects print every option as a visible
   badge ``<span>``**; none carry ``hidden`` in the raw (pre-JS) HTML Scrapy
   sees. The active subset lives only in a trailing ``<script>``'s
   ``showBadges('#form', 'a~*~b', 'css-class')`` call -- see ``_badges``.
3. **The detail page's "Phone Number" field is not looked up server-side at
   all.** It literally echoes back whatever ``?phone=`` query string the page
   was requested with (verified: passing a fake number renders that fake
   number). Since we deliberately drop that param to keep detail URLs clean
   and cacheable, the detail page never has a phone value to parse. The
   genuine phone number is sourced from the search result row's "Business
   Phone Number" column instead (Phase 1), carried into the detail request
   via ``meta``.
"""
import re
from html import unescape
from urllib.parse import urlencode

from scrapy import Request, Spider

from provider_scrape.items import InspectionItem, ProviderItem

BASE = "https://olapublic.sd.gov/child-care-provider-search/"
DETAIL = "https://olapublic.sd.gov/child-care-program-profile/{id}"

PROFILE_ID_RE = re.compile(r"/child-care-program-profile/(\d+)")

# Realistic desktop UA -- the site has no bot protection, but this keeps us a
# good citizen and consistent with the other spiders.
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/151.0.0.0 Safari/537.36"
)

# The three disjoint status facets the public search exposes, plus an
# unfiltered pass (``None``) that catches "straggler" ids with some other or
# blank status. Order is also the merge priority in ``_dispatch_details``: a
# real facet status always wins over the unfiltered ``None``.
STATUS_FACETS = ["Operational", "Closed", "Pending", None]


def _badges(text, form_id):
    """Active options for a multi-select badge group (gotcha #2).

    The detail page prints EVERY option as a visible badge ``<span>``; the
    active subset is recorded only in a trailing ``<script>`` call:
    ``showBadges('#form_id', 'a~*~b', 'css-class')``. Parse the second,
    ``~*~``-delimited argument -- never the spans themselves.
    """
    m = re.search(
        r"showBadges\('#%s',\s*'((?:[^'\\]|\\')*)'" % re.escape(form_id), text
    )
    if not m:
        return []
    return [unescape(v).strip() for v in m.group(1).split("~*~") if v.strip()]


def _p_text(response, label):
    """Value of a ``<p><b>Label: </b>VALUE</p>`` field, or ``None`` if blank.

    Covers Program Name / Program ID / Program Category / Physical Address /
    Capacity / Nationally Accredited -- fields whose value is a plain text
    node directly inside the ``<p>``, right after the ``<b>`` label.
    """
    value = response.xpath(
        '//p[b[contains(normalize-space(.), "%s")]]/text()' % label
    ).get()
    value = value.strip() if value else None
    return value or None


def _p_span(response, label):
    """Value of a ``<p><b>Label: </b><span>VALUE</span></p>`` field.

    Covers Website and the "child care openings?" field -- both render their
    value inside a nested ``<span>`` rather than a bare text node, and both
    use the site's own ``N/A`` placeholder for "no value", which we map to
    ``None``.
    """
    value = response.xpath(
        '//p[b[contains(normalize-space(.), "%s")]]/span/text()' % label
    ).get()
    value = value.strip() if value else None
    if not value or value == "N/A":
        return None
    return value


def _capacity(value):
    """Blank Capacity -> ``None`` (never ``0``); a clean digit string -> int."""
    if not value:
        return None
    if re.fullmatch(r"\d+", value):
        return int(value)
    return value


def _clean_phone(value):
    """Drop the site's own "no phone on file" placeholder.

    Providers with no phone on record still render a formatted-but-empty
    template in the list row -- e.g. ``+ () -`` (verified live: the profile
    link's own ``?phone=00000000000`` param is all zeros). A value with no
    digits at all carries no information, so treat it the same as a blank
    cell -- ``None``.
    """
    if not value:
        return None
    return value if re.search(r"\d", value) else None


class SouthDakotaSpider(Spider):
    """olapublic.sd.gov Child Care Program Profile search."""

    name = "south_dakota"
    allowed_domains = ["olapublic.sd.gov"]

    custom_settings = {
        "USER_AGENT": USER_AGENT,
        "ROBOTSTXT_OBEY": False,
        # Small, polite crawl: 4 list requests + ~1,044 detail requests
        # against a small state directory server.
        "CONCURRENT_REQUESTS": 6,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 6,
        "DOWNLOAD_DELAY": 0.3,
        "RETRY_TIMES": 5,
    }

    def start_requests(self):
        # ``facet_rows`` accumulates each of the 4 list responses keyed by
        # facet status; detail requests are only dispatched once all 4 have
        # arrived (``_dispatch_details``), so the status-priority merge is
        # deterministic no matter which response -- a tiny Pending page or a
        # huge unfiltered one -- happens to land first.
        self.facet_rows = {}
        for status in STATUS_FACETS:
            params = {"search": "true", "providerType": "Child Care"}
            if status:
                params["status"] = status
            url = f"{BASE}?{urlencode(params)}"
            yield Request(
                url, callback=self.parse_results, meta={"raw_status": status}
            )

    def parse_results(self, response):
        """Harvest one status-facet (or unfiltered) results table.

        Every matching row is already server-rendered into this single
        response -- the site's own DataTables pagination is client-side JS we
        never run -- so there is no follow-the-next-page step here, just row
        extraction.
        """
        status = response.meta["raw_status"]
        rows = []
        for row in response.css("tr.provider-search-row"):
            cells = row.css("td")
            if len(cells) < 3:
                continue
            href = cells[0].css("a::attr(href)").get()
            if not href:
                continue
            m = PROFILE_ID_RE.search(href)
            if not m:
                continue
            # The "Business Phone Number" column is the only genuine source
            # of phone data -- see module docstring gotcha #3.
            phone_text = cells[2].css("::text").get(default="").strip()
            rows.append((m.group(1), _clean_phone(phone_text)))

        self.facet_rows[status] = rows
        self.logger.info(
            "SD list phase: %s -> %d rows", status or "Unfiltered", len(rows)
        )

        if len(self.facet_rows) == len(STATUS_FACETS):
            yield from self._dispatch_details()

    def _dispatch_details(self):
        """Merge the 4 accumulated lists and fan out to detail requests.

        A provider id keeps the status of the first facet (in
        ``STATUS_FACETS`` order) it appears under; ids that only ever show up
        in the unfiltered pass get status ``None`` -- "stragglers" with no
        status facet at all.
        """
        merged = {}
        for status in STATUS_FACETS:
            for provider_id, phone in self.facet_rows.get(status, []):
                if provider_id in merged:
                    continue
                merged[provider_id] = (status, phone)

        unfiltered_total = len(self.facet_rows.get(None, []))
        stragglers = sum(1 for status, _ in merged.values() if status is None)
        self.logger.info(
            "SD list phase complete: %d unique providers (unfiltered "
            "total=%d, stragglers with no status facet=%d).",
            len(merged), unfiltered_total, stragglers,
        )

        for provider_id, (status, phone) in merged.items():
            yield Request(
                DETAIL.format(id=provider_id),
                callback=self.parse_detail,
                meta={
                    "raw_status": status,
                    "list_phone": phone,
                    "dont_merge_cookies": True,
                },
            )

    def parse_detail(self, response):
        """Parse a Child Care Program Profile page into a ``ProviderItem``."""
        text = response.text
        item = ProviderItem()
        item["source_state"] = "South Dakota"
        item["provider_url"] = response.url

        # Status comes ONLY from the Phase-1 search facet -- see gotcha #1.
        item["status"] = response.meta["raw_status"]
        # Phone comes from the list row -- see gotcha #3.
        item["phone"] = response.meta.get("list_phone")

        item["provider_name"] = _p_text(response, "Program Name")
        # Program ID formats vary ("011008567", "499143513"); keep as a
        # string so leading zeros survive.
        item["license_number"] = _p_text(response, "Program ID")
        item["provider_type"] = _p_text(response, "Program Category")
        # Emit the raw address (incl. trailing ", USA"); the normalization
        # pipeline's clean_address/parse_address_components handles it.
        item["address"] = _p_text(response, "Physical Address")
        item["provider_website"] = _p_span(response, "Website")
        item["capacity"] = _capacity(_p_text(response, "Capacity"))
        item["accepting_new_children"] = _p_span(response, "openings")

        nationally_accredited = _p_text(response, "Nationally Accredited")
        # Only a national-accreditation flag is exposed; no accrediting body.
        item["accreditation"] = "National" if nationally_accredited == "Yes" else None

        services = _badges(text, "servicesForm")
        ages = _badges(text, "agesChildrenForm")
        months = _badges(text, "monthsOfOperationForm")
        item["ages_served"] = ", ".join(ages) or None
        item["transportation"] = "Transportation" in services
        item["sd_services_offered"] = services
        item["sd_months_of_operation"] = months

        item["inspections"] = self._parse_documents(response)
        yield item

    def _parse_documents(self, response):
        """Documents area -> one ``InspectionItem`` per document.

        The "Documents" area has three sections (Program Certificate,
        Inspections, Compliance/Corrective/Stipulation plans); every entry is
        a ``div.list-group-item`` with an ``h5`` title, a ``small`` subtitle,
        and a download anchor. The certificate's subtitle is a bare date
        ("MM/DD/YYYY"); every other document's subtitle is
        "<Type> - MM/DD/YYYY". Walk the download anchors (scoped via their
        ancestor list-group-item) rather than a broad regex, so the site nav
        is never accidentally swallowed.
        """
        inspections = []
        for anchor in response.css("a.btn-download"):
            href = anchor.attrib.get("href")
            if not href:
                continue
            block = anchor.xpath(
                './ancestor::div[contains(@class,"list-group-item")][1]'
            )
            subtitle = block.css("small::text").get(default="").strip()
            if " - " in subtitle:
                doc_type, _, date = subtitle.rpartition(" - ")
            else:
                # The Program Certificate's <small> is a bare date.
                doc_type, date = "Program Certificate", subtitle
            insp = InspectionItem()
            insp["type"] = doc_type or None
            insp["date"] = date or None
            insp["report_url"] = response.urljoin(href)
            inspections.append(insp)
        return inspections

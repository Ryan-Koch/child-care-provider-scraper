"""Vermont Bright Futures child care provider spider.

Source: https://www.brightfutures.dcf.state.vt.us/vtcc/ — a Maximus STT
(Struts/JSP) application whose URLs are opaque per-navigation ``?<token>``
strings. See ``project_vermont_brightfutures_flow`` for the reverse-engineering
notes. The crawl has three phases:

1. **Bootstrap (needs a session):** GET ``/vtcc/public.jsp`` which 302-redirects
   (downgrading to ``http://…:80``, rewritten back to https) to the public Home
   page. The Home page carries an "advanced search" link to the "Search Provider
   Directory" form (``<form name='main'>``, submit ``eventSubmit_doSearch``).
   Submitting it with **every filter blank** returns the full directory
   (~1070 providers, 20 per page). The empty search occasionally bounces back to
   the form; we detect that (a results page has "Details" links, the form does
   not) and retry.

2. **Pagination:** each results page links the next one. These "next" links —
   and the per-provider "Details" links — are **fully self-contained**: a fresh,
   cookieless GET returns the page. So we walk the ~54 result pages harvesting
   Details links, then

3. **Details:** fan out to the detail pages concurrently. Detail requests carry
   ``dont_merge_cookies`` so they never share the bootstrap session cookie (each
   is independent — no server-side session serialization). Everything we emit is
   on the detail page.
"""

import re
from html import unescape

from scrapy import FormRequest, Request, Spider

from provider_scrape.items import InspectionItem, ProviderItem

BASE = "https://www.brightfutures.dcf.state.vt.us"
BOOTSTRAP_URL = f"{BASE}/vtcc/public.jsp"

# Realistic desktop UA — a bare scraper UA isn't needed here, but keeps us a
# good citizen and consistent with the other spiders.
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/151.0.0.0 Safari/537.36"
)


def _content_after(text, key):
    """Value of a labelled detail field.

    The detail page renders each field as ``<label for="field_XXX">Label</label>``
    followed (a couple of table cells later) by ``<span class="content1">VALUE
    </span>``. Return the text of the first ``content1`` span after the label,
    with ``<br>`` turned into newlines and HTML entities decoded. ``None`` when
    absent or empty.
    """
    m = re.search(r'<label for="%s"[^>]*>.*?</label>' % re.escape(key), text, re.S)
    if not m:
        return None
    v = re.search(r'<span class="content1">(.*?)</span>', text[m.end() : m.end() + 600], re.S)
    if not v:
        return None
    raw = re.sub(r"<br\s*/?>", "\n", v.group(1))
    raw = re.sub(r"<[^>]+>", "", raw)  # strip any stray inline tags
    return unescape(raw).strip() or None


def _hidden(text, name):
    """Value of a hidden ``<input name="..." value="...">`` (decoded)."""
    m = re.search(r'name="%s"\s+value="([^"]*)"' % re.escape(name), text)
    if not m:
        return None
    return unescape(m.group(1)).strip() or None


def _num(value):
    """Coerce a clean integer string to ``int``; pass anything else through."""
    if value is None:
        return None
    if re.fullmatch(r"\d+", value):
        return int(value)
    return value


class VermontSpider(Spider):
    name = "vermont"
    allowed_domains = ["brightfutures.dcf.state.vt.us"]

    # Bound the retry of a bounced (empty) search so a persistently-empty
    # response can't loop forever.
    MAX_SEARCH_ATTEMPTS = 4

    custom_settings = {
        "USER_AGENT": USER_AGENT,
        "ROBOTSTXT_OBEY": False,
        # Polite but efficient: ~1120 requests (54 pages + ~1070 details) against
        # a small state directory server.
        "CONCURRENT_REQUESTS": 6,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 6,
        "DOWNLOAD_DELAY": 0.3,
        "RETRY_TIMES": 5,
    }

    def start_requests(self):
        # Handle the public.jsp 302 ourselves: its Location downgrades to
        # http://…:80 (which then 301s back to https). Rewriting it directly is
        # deterministic and avoids the extra http->https hop.
        yield Request(
            BOOTSTRAP_URL,
            callback=self.parse_bootstrap,
            meta={"dont_redirect": True, "handle_httpstatus_list": [301, 302]},
            dont_filter=True,
        )

    def parse_bootstrap(self, response):
        location = response.headers.get("Location")
        if not location:
            self.logger.error(
                "VT bootstrap: no redirect from public.jsp (HTTP %s); aborting.",
                response.status,
            )
            return
        home = location.decode("latin-1")
        home = re.sub(r"^http://", "https://", home)
        home = re.sub(r":80(/|$)", r"\1", home)
        yield Request(home, callback=self.parse_home)

    def parse_home(self, response):
        adv = response.xpath('//a[contains(normalize-space(.), "advanced search")]/@href').get()
        if not adv:
            self.logger.error("VT: no 'advanced search' link on the Home page; aborting.")
            return
        yield response.follow(adv, callback=self.parse_search_form)

    def parse_search_form(self, response):
        # Submit the directory search with every filter left blank -> all
        # providers. from_response carries every hidden control field and clicks
        # the single submit button (eventSubmit_doSearch).
        yield FormRequest.from_response(
            response,
            formname="main",
            formdata={"eventSubmit_doSearch": "Search"},
            callback=self.parse_results,
            meta={"search_attempt": 1},
            dont_filter=True,
        )

    def parse_results(self, response):
        details = response.xpath('//a[normalize-space(text())="Details"]/@href').getall()

        if not details:
            attempt = response.meta.get("search_attempt", 1)
            # A bounce: the empty search re-rendered the search form (which still
            # carries eventSubmit_doSearch). Re-submit it, bounded.
            if "eventSubmit_doSearch" in response.text:
                if attempt < self.MAX_SEARCH_ATTEMPTS:
                    self.logger.warning(
                        "VT empty search bounced to the form (attempt %d/%d); retrying.",
                        attempt,
                        self.MAX_SEARCH_ATTEMPTS,
                    )
                    yield FormRequest.from_response(
                        response,
                        formname="main",
                        formdata={"eventSubmit_doSearch": "Search"},
                        callback=self.parse_results,
                        meta={"search_attempt": attempt + 1},
                        dont_filter=True,
                    )
                else:
                    self.logger.error(
                        "VT search still empty after %d attempts; giving up.",
                        self.MAX_SEARCH_ATTEMPTS,
                    )
            else:
                self.logger.warning("VT: results page with no Details links at %s", response.url)
            return

        # Log the declared total once, from the first results page.
        if response.meta.get("search_attempt"):
            plain = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", response.text))
            m = re.search(r"Displaying\s*[\d\-]+\s*of\s*(\d+)\s*Items", plain)
            if m:
                self.logger.info("VT directory declares %s providers.", m.group(1))

        for href in details:
            yield response.follow(
                href,
                callback=self.parse_detail,
                meta={"dont_merge_cookies": True},
            )

        next_href = response.xpath('//a[normalize-space(.)="next>"]/@href').get()
        if next_href:
            yield response.follow(next_href, callback=self.parse_results)

    def parse_detail(self, response):
        text = response.text
        item = ProviderItem()
        item["source_state"] = "Vermont"
        item["provider_url"] = re.sub(r";jsessionid=[^?]*", "", response.url)

        item["provider_name"] = _hidden(text, "field_name2")
        provider_id = _hidden(text, "field_pid2")
        item["vt_provider_id"] = provider_id
        # VT publishes no license number; the internal Provider ID is the closest
        # stable identifier (DC precedent).
        item["license_number"] = provider_id

        provider_type = _content_after(text, "field_ltype3")
        item["provider_type"] = provider_type
        # The directory only lists actively licensed/registered providers; derive
        # a status from the license type (both normalize to "active").
        if provider_type:
            item["status"] = "Registered" if "registered" in provider_type.lower() else "Licensed"

        # Site Address block: "street | Town, VT ZIP | City: <postal city>".
        # Keep the street + municipal line (geocodable); drop the postal "City:".
        addr_raw = _content_after(text, "field_addr3")
        if addr_raw:
            lines = [ln.strip() for ln in addr_raw.split("\n") if ln.strip()]
            lines = [ln for ln in lines if not ln.lower().startswith("city:")]
            item["address"] = ", ".join(lines) or None

        item["phone"] = _content_after(text, "field_pnum3")
        item["email"] = _content_after(text, "field_email3")
        item["provider_website"] = _content_after(text, "field_web3")
        item["license_begin_date"] = _content_after(text, "field_ldate3")
        item["license_holder"] = _content_after(text, "field_owner3")
        item["administrator"] = _content_after(text, "field_director3")

        # Per-age capacities (common fields) + per-age vacancies (vt_*).
        item["infant"] = _num(_content_after(text, "field_infant4"))
        item["toddler"] = _num(_content_after(text, "field_toddler4"))
        item["preschool"] = _num(_content_after(text, "field_pre4"))
        item["school"] = _num(_content_after(text, "field_school4"))
        item["capacity"] = _num(_content_after(text, "field_totcap4"))
        item["vt_infant_vacancies"] = _num(_content_after(text, "field_ivac4"))
        item["vt_toddler_vacancies"] = _num(_content_after(text, "field_tvac4"))
        item["vt_preschool_vacancies"] = _num(_content_after(text, "field_pvac4"))
        item["vt_school_age_vacancies"] = _num(_content_after(text, "field_svac4"))
        item["vt_current_vacancy"] = _num(_content_after(text, "field_numvac4"))
        item["vt_vacancy_as_of"] = _content_after(text, "field_curr_asof4")

        item["ages_served"] = _content_after(text, "field_ages6")
        item["languages"] = _content_after(text, "field_langs7")
        item["hours"] = _content_after(text, "field_usualhours7")
        item["transportation"] = _content_after(text, "field_trans7")
        item["school_district"] = _content_after(text, "field_schools7")
        item["meals"] = _content_after(text, "field_specmeals7")
        item["accreditation"] = _content_after(text, "field_caccred8")
        # Subsidy (VT Child Care Financial Assistance) acceptance.
        item["scholarships_accepted"] = _content_after(text, "field_subprov6")

        # STARS quality rating stays state-specific (field-mapping playbook).
        item["vt_star_level"] = _content_after(text, "field_stars8")
        item["vt_type_of_care"] = _content_after(text, "field_toc7")
        item["vt_days_of_operation"] = _content_after(text, "field_daysop7")
        item["vt_special_schedule"] = _content_after(text, "field_specsch7")
        item["vt_building_type"] = _content_after(text, "field_buildtype6")
        item["vt_area_description"] = _content_after(text, "field_areadesc6")
        item["vt_religious_activity"] = _content_after(text, "field_religious6")
        item["vt_sibling_discount"] = _content_after(text, "field_sibdis6")
        item["vt_special_services"] = _content_after(text, "field_pservice6")
        item["vt_program_participation"] = _content_after(text, "field_svccat6")
        item["vt_guidance"] = _content_after(text, "field_guid6")
        item["vt_program_description"] = _content_after(text, "field_prog6")
        item["vt_pets"] = _content_after(text, "field_pets6")

        item["inspections"] = self._parse_site_visits(text)
        yield item

    def _parse_site_visits(self, text):
        """Site-visit dates from the "Site Visits" section become inspections.

        Each visit renders as a date-bearing anchor (``>MM/DD/YYYY<``). The
        section is scoped between its heading and the "Return to Search Results"
        button so unrelated dates elsewhere on the page aren't picked up.
        """
        start = text.find("Site Visits")
        if start == -1:
            return []
        end = text.find("Return to Search Results", start)
        section = text[start : end if end != -1 else len(text)]
        inspections = []
        seen = set()
        for date in re.findall(r">\s*(\d{2}/\d{2}/\d{4})\s*<", section):
            if date in seen:
                continue
            seen.add(date)
            insp = InspectionItem()
            insp["date"] = date
            insp["type"] = "Site Visit"
            inspections.append(insp)
        return inspections

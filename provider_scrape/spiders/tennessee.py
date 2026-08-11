"""Tennessee child care provider spider.

Source: the TN DHS "OneDHS" provider map at
https://onedhs.tn.gov/csp?id=tn_cc_prv_maps -- a ServiceNow Service Portal
(JavaScript app). This spider talks to the portal's JSON API directly; no
browser is used.

Flow:
  0. Bootstrap: GET the portal page -> session cookies + a ``g_ck`` token (sent
     as the ``X-UserToken`` header on every API call). Also discover the map
     widget's ``rectangle_id`` from the page API (falls back to a constant).
  1. Enumerate: POST ``getcountyarr`` once per county (95 counties) ->
     ``result.data.locsCounty[]`` (name, address, lat/long, phone, rating,
     capacity, vacancy, and the detail ``sysid``).
  2. Detail: GET the SP page API per ``sysid`` for the full record (license #,
     type, status, hours, age-group rates, QRIS rating + scorecard, visits).

See tasks/tennessee_epic/tennessee_development_plan.md for the full write-up.
"""
import json
import re
from urllib.parse import urlencode

import scrapy

from provider_scrape.items import InspectionItem, ProviderItem

BASE = "https://onedhs.tn.gov"
LANDING_URL = f"{BASE}/csp?id=tn_cc_prv_maps"
PAGE_API = f"{BASE}/api/now/sp/page"
RECTANGLE_URL = f"{BASE}/api/now/sp/rectangle/{{}}?id=tn_cc_prv_maps"
# Observed map-widget instance id; used only if dynamic discovery fails.
RECTANGLE_ID_FALLBACK = "9d8c1f2e1bc6c9102c9ce3fb234bcbe9"
DETAIL_PAGE_ID = "cp_provider_details_maps"
MAP_PAGE_ID = "tn_cc_prv_maps"

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64; rv:153.0) Gecko/20100101 Firefox/153.0"
)

# Warn in closed() if we finish well below this. Calibrate after the first full
# run (set to ~80% of the observed statewide total).
EXPECTED_MIN_PROVIDERS = 4000

# The 95 Tennessee counties, spelled exactly as the API expects (verified live:
# "DeKalb" has no space; "Van Buren" has a space).
ALL_COUNTIES = [
    "Anderson", "Bedford", "Benton", "Bledsoe", "Blount", "Bradley", "Campbell",
    "Cannon", "Carroll", "Carter", "Cheatham", "Chester", "Claiborne", "Clay",
    "Cocke", "Coffee", "Crockett", "Cumberland", "Davidson", "Decatur", "DeKalb",
    "Dickson", "Dyer", "Fayette", "Fentress", "Franklin", "Gibson", "Giles",
    "Grainger", "Greene", "Grundy", "Hamblen", "Hamilton", "Hancock", "Hardeman",
    "Hardin", "Hawkins", "Haywood", "Henderson", "Henry", "Hickman", "Houston",
    "Humphreys", "Jackson", "Jefferson", "Johnson", "Knox", "Lake", "Lauderdale",
    "Lawrence", "Lewis", "Lincoln", "Loudon", "Macon", "Madison", "Marion",
    "Marshall", "Maury", "McMinn", "McNairy", "Meigs", "Monroe", "Montgomery",
    "Moore", "Morgan", "Obion", "Overton", "Perry", "Pickett", "Polk", "Putnam",
    "Rhea", "Roane", "Robertson", "Rutherford", "Scott", "Sequatchie", "Sevier",
    "Shelby", "Smith", "Stewart", "Sullivan", "Sumner", "Tipton", "Trousdale",
    "Unicoi", "Union", "Van Buren", "Warren", "Washington", "Wayne", "Weakley",
    "White", "Williamson", "Wilson",
]

NO_INFO = "no information available"


def clean(value):
    """Trim; treat empty and "No Information Available" as missing (None)."""
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() == NO_INFO:
        return None
    return s


def to_int(value):
    """int(value) when it's a clean whole number, else the cleaned string/None."""
    s = clean(value)
    if s is None:
        return None
    return int(s) if s.isdigit() else s


def put(item, key, value):
    """Set item[key] only when value is not None (keeps output clean)."""
    if value is not None:
        item[key] = value


def find_dict_with(node, key):
    """Depth-first: return the first dict in the tree that contains ``key``."""
    if isinstance(node, dict):
        if key in node:
            return node
        for v in node.values():
            hit = find_dict_with(v, key)
            if hit is not None:
                return hit
    elif isinstance(node, list):
        for v in node:
            hit = find_dict_with(v, key)
            if hit is not None:
                return hit
    return None


def find_rectangle_id(node):
    """Find the ``cp_provider_maps`` widget's ``rectangle_id`` in page JSON."""
    if isinstance(node, dict):
        if node.get("id") == "cp_provider_maps" and node.get("rectangle_id"):
            return node["rectangle_id"]
        for v in node.values():
            hit = find_rectangle_id(v)
            if hit:
                return hit
    elif isinstance(node, list):
        for v in node:
            hit = find_rectangle_id(v)
            if hit:
                return hit
    return None


def flatten_hours(hours):
    """Structured ``hours`` list -> string; skip days with no times. None if empty.

    Input: [{"name": "Monday", "hours": ["06:30 AM -- 05:30 PM"]}, ...].
    """
    if not isinstance(hours, list):
        return None
    parts = []
    for day in hours:
        name = (day or {}).get("name")
        times = (day or {}).get("hours") or []
        if name and times:
            parts.append(f"{name}: {', '.join(times)}")
    return "; ".join(parts) or None


def build_ages_served(young, old):
    """"6 Week(s)" + "12 Year(s)" -> "6 Week(s) to 12 Year(s)"."""
    young, old = clean(young), clean(old)
    if young and old:
        return f"{young} to {old}"
    return young or old


def build_rates(arr_aar):
    """Map ``arrAar`` rows to the tn_age_group_rates list."""
    rates = []
    for r in arr_aar or []:
        rates.append({
            "age_group": clean(r.get("ageGroup")),
            "weekly_rate": clean(r.get("fullTime")),
            "unit_of_care": clean(r.get("unitOfCare")),
            "vacancy": clean(r.get("vacancy")),
        })
    return rates


def build_scorecard(d):
    """Build tn_rating_scorecard from the QRIS sub-scores (when rated)."""
    fields = {
        "teacher_child_interactions": ("teachVal", "teachTotal"),
        "health_and_safety": ("healthVal", "healthTotal"),
        "critical_items": ("critical", "criticalVal"),
        "supervision": ("supervision", "superVal"),
        "record_keeping": ("reckeeping", "recVal"),
        "healthy_weight": ("healthy", "healthyVal"),
        "organizational_structure": ("org", "orgVal"),
    }
    card = {}
    for label, (score_key, pct_key) in fields.items():
        score, pct = clean(d.get(score_key)), clean(d.get(pct_key))
        if score or pct:
            card[label] = {"score": score, "percent": pct}
    return card


def build_inspections(arr_cd):
    """Map ``arrCd`` visits to InspectionItem objects."""
    inspections = []
    for v in arr_cd or []:
        insp = InspectionItem()
        insp["date"] = clean(v.get("visitDate"))
        insp["type"] = clean(v.get("VisitType"))
        insp["corrective_status"] = clean(v.get("CorrectiveActionTaken"))
        url = clean(v.get("visitUrl"))
        if url:
            insp["report_url"] = f"{BASE}/csp{url}" if url.startswith("?") else url
        inspections.append(insp)
    return inspections


class TennesseeSpider(scrapy.Spider):
    name = "tennessee"
    allowed_domains = ["onedhs.tn.gov"]

    custom_settings = {
        "DOWNLOAD_DELAY": 0.25,
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "RETRY_TIMES": 5,
        "DOWNLOAD_TIMEOUT": 60,
        "ROBOTSTXT_OBEY": False,
        "USER_AGENT": USER_AGENT,
    }

    def __init__(self, *args, counties=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.token = None
        self.rectangle_id = RECTANGLE_ID_FALLBACK
        self.seen = set()  # sysids already scheduled for detail (global dedupe)
        if counties:
            self.counties = [c.strip() for c in counties.split(",") if c.strip()]
        else:
            self.counties = list(ALL_COUNTIES)

    # --- headers / request builders ------------------------------------- #

    def api_headers(self):
        return {
            "X-UserToken": self.token or "",
            "Accept": "application/json",
            "Referer": LANDING_URL,
            "Origin": BASE,
        }

    def _county_request(self, county):
        body = {
            "act": "getcountyarr",
            "searchStrCount": county,
            "searchType": "Child Care",
            "sessionRotationTrigger": True,
        }
        headers = dict(self.api_headers())
        headers["Content-Type"] = "application/json;charset=utf-8"
        return scrapy.Request(
            RECTANGLE_URL.format(self.rectangle_id),
            method="POST",
            body=json.dumps(body),
            headers=headers,
            callback=self.parse_county,
            errback=self.on_error,
            meta={"county": county, "handle_httpstatus_list": [401]},
            dont_filter=True,
        )

    def _detail_request(self, rec):
        sysid = rec.get("sysidCounty")
        url = f"{PAGE_API}?" + urlencode({"id": DETAIL_PAGE_ID, "sysid": sysid})
        return scrapy.Request(
            url,
            headers=self.api_headers(),
            callback=self.parse_detail,
            errback=self.on_error,
            meta={
                "handle_httpstatus_list": [401],
                "sysid": sysid,
                "latitude": clean(rec.get("p1LatCounty")),
                "longitude": clean(rec.get("p1LongCounty")),
                "name": clean(rec.get("prnameCounty")),
                "phone": clean(rec.get("phnumCounty")),
                "email": clean(rec.get("emailCounty")),
            },
            dont_filter=True,
        )

    # --- phase 0: bootstrap --------------------------------------------- #

    def start_requests(self):
        yield scrapy.Request(LANDING_URL, callback=self.parse_landing,
                             dont_filter=True)

    def parse_landing(self, response):
        self.token = self._extract_token(response.text)
        if not self.token:
            self.logger.error("TN: no g_ck token on the landing page; aborting.")
            return
        yield scrapy.Request(
            f"{PAGE_API}?" + urlencode({"id": MAP_PAGE_ID}),
            headers=self.api_headers(),
            callback=self.parse_widget_meta,
            errback=self.on_error,
            dont_filter=True,
        )

    def parse_widget_meta(self, response):
        try:
            rid = find_rectangle_id(response.json())
        except ValueError:
            rid = None
        if rid:
            self.rectangle_id = rid
            self.logger.info("TN: using rectangle_id %s", rid)
        else:
            self.logger.warning(
                "TN: could not discover rectangle_id; using fallback %s",
                self.rectangle_id,
            )
        for county in self.counties:
            yield self._county_request(county)

    @staticmethod
    def _extract_token(text):
        m = re.search(r"window\.g_ck = '([^']+)'", text)
        return m.group(1) if m else None

    # --- phase 1: county enumeration ------------------------------------ #

    def parse_county(self, response):
        meta = response.meta
        county = meta["county"]
        stale = response.status == 401
        data = {}
        if not stale:
            try:
                data = response.json()
            except ValueError:
                stale = True
        locs = (((data.get("result") or {}).get("data") or {})
                .get("locsCounty")) or []

        # A stale session shows up as HTTP 401 or an empty list. Re-bootstrap
        # once, then retry this county.
        if (stale or not locs) and not meta.get("reauth"):
            self.logger.warning(
                "TN county %s: stale/empty (HTTP %s) -- re-bootstrapping.",
                county, response.status,
            )
            yield scrapy.Request(
                LANDING_URL, callback=self._refresh_then_retry_county,
                meta={"retry_county": county}, dont_filter=True,
            )
            return

        if not locs:
            self.logger.info("TN county %s: 0 providers.", county)
            return

        self.logger.info("TN county %s: %d providers.", county, len(locs))
        for rec in locs:
            sysid = rec.get("sysidCounty")
            color = rec.get("colorCounty")
            if color and color != "green":
                self.logger.warning(
                    "TN county %s: non-green marker (%s) for %s",
                    county, color, rec.get("prnameCounty"),
                )
            if not sysid or sysid in self.seen:
                continue
            self.seen.add(sysid)
            yield self._detail_request(rec)

    def _refresh_then_retry_county(self, response):
        self.token = self._extract_token(response.text) or self.token
        req = self._county_request(response.meta["retry_county"])
        req.meta["reauth"] = True
        yield req

    # --- phase 2: detail ------------------------------------------------- #

    def parse_detail(self, response):
        meta = response.meta
        if response.status == 401 and not meta.get("reauth"):
            self.logger.warning("TN detail %s: 401 -- re-bootstrapping.",
                                meta.get("sysid"))
            yield scrapy.Request(
                LANDING_URL, callback=self._refresh_then_retry_detail,
                meta={"retry_meta": dict(meta)}, dont_filter=True,
            )
            return

        try:
            page = response.json()
        except ValueError:
            self.logger.warning("TN detail %s: non-JSON response.",
                                meta.get("sysid"))
            return

        det = find_dict_with(page, "prvId")
        contact = find_dict_with(page, "ActsPresent") or {}
        if not det:
            self.logger.warning("TN detail %s: no provider data found.",
                                meta.get("sysid"))
            return

        yield self.build_item(det, contact, meta)

    def _refresh_then_retry_detail(self, response):
        self.token = self._extract_token(response.text) or self.token
        meta = dict(response.meta["retry_meta"])
        sysid = meta["sysid"]
        url = f"{PAGE_API}?" + urlencode({"id": DETAIL_PAGE_ID, "sysid": sysid})
        meta["reauth"] = True
        yield scrapy.Request(url, headers=self.api_headers(),
                             callback=self.parse_detail, errback=self.on_error,
                             meta=meta, dont_filter=True)

    def build_item(self, det, contact, meta):
        sysid = meta.get("sysid")
        item = ProviderItem()
        item["source_state"] = "Tennessee"
        item["provider_url"] = f"{BASE}/csp?id={DETAIL_PAGE_ID}&sysid={sysid}"

        put(item, "provider_name",
            clean(contact.get("name")) or meta.get("name"))
        put(item, "license_number", clean(det.get("prvLicNum")))
        put(item, "provider_type", clean(det.get("prvType")))
        put(item, "status", clean(det.get("PrvStatus")))
        put(item, "county", clean(det.get("PrvCounty")))
        put(item, "address", clean(contact.get("address")))
        put(item, "latitude", meta.get("latitude"))
        put(item, "longitude", meta.get("longitude"))
        put(item, "phone", clean(contact.get("phone")) or meta.get("phone"))
        put(item, "email", clean(contact.get("email")) or meta.get("email"))
        put(item, "capacity", to_int(det.get("capacity")))
        put(item, "hours", flatten_hours(det.get("hours")))
        put(item, "ages_served",
            build_ages_served(det.get("youngChildAge"), det.get("oldChildAge")))
        put(item, "transportation", clean(det.get("transport")))
        put(item, "scholarships_accepted", clean(det.get("ccAssist")))

        vacancy = to_int(det.get("vacancy"))
        if isinstance(vacancy, int):
            item["accepting_new_children"] = vacancy > 0

        # --- Tennessee-specific ---
        put(item, "tn_provider_id", clean(det.get("prvId")))
        put(item, "tn_regulatory_agency", clean(det.get("PrvRegAgen")))
        put(item, "tn_regulatory_individual", clean(det.get("prvRegInd")))
        put(item, "tn_vacancy", clean(det.get("vacancy")))
        put(item, "tn_wheelchair_accessible", clean(det.get("wheelChair")))

        participate = clean(det.get("participate"))
        if participate is not None:
            item["tn_participates_certificate"] = participate.lower() == "true"

        rates = build_rates(det.get("arrAar"))
        if rates:
            item["tn_age_group_rates"] = rates

        if det.get("displayRating"):
            put(item, "tn_quality_rating", clean(det.get("rating")))
            put(item, "tn_rating_effective_date", clean(det.get("effectDate")))
            put(item, "tn_rating_expiration", clean(det.get("expDate")))
            card = build_scorecard(det)
            if card:
                item["tn_rating_scorecard"] = card

        inspections = build_inspections(det.get("arrCd"))
        if inspections:
            item["inspections"] = inspections

        return item

    def on_error(self, failure):
        self.logger.error("TN request failed: %s", repr(failure))

    def closed(self, reason):
        self.logger.info("TN: finished (%s) -- %d unique providers.",
                         reason, len(self.seen))
        if len(self.seen) < EXPECTED_MIN_PROVIDERS:
            self.logger.warning(
                "TN: only %d providers (< %d baseline) -- possible incomplete "
                "crawl.", len(self.seen), EXPECTED_MIN_PROVIDERS,
            )

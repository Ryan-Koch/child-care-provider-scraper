"""North Dakota child care provider spider.

Source: the ND Early Childhood registry at https://search.ec.hhs.nd.gov, a
JSON API (no cookie/token/CAPTCHA required).

The roster endpoint (``publicSearch``) is hard-capped at 100 results with no
pagination. The only way past the cap is its distance search: supplying a
``location`` returns every provider within ~10 miles (capped at 100). We
therefore enumerate the whole state with an adaptive geographic grid:

  * a covering grid at 12-mile spacing (an unsaturated node returns everything
    in its cell), then
  * local densification wherever a node saturates (returns 100) and its
    100th-result radius doesn't reach the cell corner -- split into 4 and recurse.

Each unique provider id is then fetched from ``/api/programs/{id}`` for the full
record (capacity, ages, vacancies, license dates, ...).
"""
import json
import math

import scrapy

from provider_scrape.items import ProviderItem

SEARCH_URL = "https://search.ec.hhs.nd.gov/api/programs/search/publicSearch"
DETAIL_URL = "https://search.ec.hhs.nd.gov/api/programs/{}"
PROFILE_URL = "https://search.ec.hhs.nd.gov/search/(slide-full:{}/profile)"

# The 20-key filter object the API expects; all null == an unfiltered search.
# A ``location`` key is added per-request to switch on the distance search.
FILTER_TEMPLATE = {
    "name": None, "publicProgramType": None, "nonLicensedProgramTypes": None,
    "qualityRating": None, "accreditedOnly": None,
    "programAcceptsFinancialAssistance": None, "currentVacancies": None,
    "ageGroupsWillingToServe": None, "programSchedulesOffered": None,
    "supplementalCareTypes": None, "programEnrollmentDuringSummer": None,
    "programEnrollmentDuringSchoolYear": None, "adaCompliant": None,
    "wheelchairAccessible": None, "smokeFree": None,
    "breastfeedingFriendly": None, "noPets": None,
    "transportationProvided": None, "languages": None, "specialPopulations": None,
}

# ND bounding box (padded slightly beyond the state borders).
LAT_MIN, LAT_MAX = 45.90, 49.05
LON_MIN, LON_MAX = -104.10, -96.50
# Distance search returns providers within ~10 mi, capped at 100 (RESULT_CAP).
RESULT_CAP = 100
GRID_SPACING_MI = 12.0  # covering step: cell corner (8.49mi) < 10mi radius
MI_PER_DEG_LAT = 69.0
MI_PER_DEG_LON = 69.0 * math.cos(math.radians((LAT_MIN + LAT_MAX) / 2))
MAX_DENSIFY_DEPTH = 5
MIN_HALF_MI = 1.0
# Baseline unique count (calibrated); warn loudly if a run falls far short.
EXPECTED_MIN_PROVIDERS = 1000

# ageGroupsServed integer codes -> the ProviderItem boolean fields.
AGE_GROUP_TO_FIELD = {1: "infant", 2: "toddler", 3: "preschool", 4: "school"}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:152.0) Gecko/20100101 Firefox/152.0"
    ),
    "Accept": "application/json, text/plain, */*",
}


def iso_date(value):
    """Return the ``YYYY-MM-DD`` part of an ISO timestamp (e.g.
    ``2025-08-11T00:00:00Z`` -> ``2025-08-11``). Passes other values through."""
    if isinstance(value, str) and "T" in value:
        return value.split("T", 1)[0]
    return value


def join_labels(value):
    """Join a list of label strings with ``, ``; return None when empty."""
    if isinstance(value, list):
        parts = [str(v).strip() for v in value if v is not None and str(v).strip()]
        return ", ".join(parts) if parts else None
    return None


def age_range(age, unit_label):
    """Build a human age like ``0 Months`` / ``12 Years``; None when no value."""
    if age is None:
        return None
    unit = (unit_label or "").strip()
    return f"{age} {unit}".strip()


class NorthDakotaSpider(scrapy.Spider):
    name = "north_dakota"
    allowed_domains = ["search.ec.hhs.nd.gov"]

    custom_settings = {
        "DOWNLOAD_DELAY": 0.25,
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "RETRY_TIMES": 5,
        "ROBOTSTXT_OBEY": False,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen = set()  # provider ids already scheduled for detail
        self.node_count = 0  # search nodes processed

    # --- enumeration (search) ------------------------------------------- #

    def _search_request(self, lat, lon, half_mi, depth):
        filters = dict(FILTER_TEMPLATE)
        filters["location"] = {"latitude": lat, "longitude": lon}
        body = {"filters": filters, "sort": "distance"}
        return scrapy.Request(
            SEARCH_URL,
            method="POST",
            body=json.dumps(body),
            headers={
                **HEADERS,
                "Content-Type": "application/json",
                "Origin": "https://search.ec.hhs.nd.gov",
                "Referer": "https://search.ec.hhs.nd.gov/search",
            },
            callback=self.parse_search,
            meta={"lat": lat, "lon": lon, "half_mi": half_mi, "depth": depth},
            dont_filter=True,
        )

    def start_requests(self):
        half = GRID_SPACING_MI / 2.0
        dlat = GRID_SPACING_MI / MI_PER_DEG_LAT
        dlon = GRID_SPACING_MI / MI_PER_DEG_LON
        points = []
        lat = LAT_MIN
        while lat <= LAT_MAX + 1e-9:
            lon = LON_MIN
            while lon <= LON_MAX + 1e-9:
                points.append((lat, lon))
                lon += dlon
            lat += dlat
        self.logger.info(
            "North Dakota: seeding %d primary grid nodes at %.0f-mi spacing",
            len(points), GRID_SPACING_MI,
        )
        for lat, lon in points:
            yield self._search_request(lat, lon, half, 0)

    def parse_search(self, response):
        data = response.json()
        results = data.get("results", [])
        lat = response.meta["lat"]
        lon = response.meta["lon"]
        half_mi = response.meta["half_mi"]
        depth = response.meta["depth"]
        self.node_count += 1

        for r in results:
            pid = r.get("id")
            if not pid or pid in self.seen:
                continue
            self.seen.add(pid)
            yield scrapy.Request(
                DETAIL_URL.format(pid),
                headers=HEADERS,
                callback=self.parse_detail,
                meta={"id": pid},
            )

        # Densify on saturation: subdivide the cell into 4 finer nodes.
        if len(results) >= RESULT_CAP:
            far = max((r.get("locationFilterDistance") or 0) for r in results)
            corner = half_mi * math.sqrt(2)
            if far < corner and depth < MAX_DENSIFY_DEPTH and half_mi > MIN_HALF_MI:
                child_half = half_mi / 2.0
                off_lat = child_half / MI_PER_DEG_LAT
                off_lon = child_half / MI_PER_DEG_LON
                self.logger.info(
                    "North Dakota: SATURATED (%.4f,%.4f) depth=%d "
                    "radius=%.2fmi < corner=%.2fmi -> densify into 4 at %.1f-mi",
                    lat, lon, depth, far, corner, child_half,
                )
                for si in (-1, 1):
                    for sj in (-1, 1):
                        yield self._search_request(
                            lat + si * off_lat, lon + sj * off_lon,
                            child_half, depth + 1,
                        )
            elif far < corner:
                self.logger.warning(
                    "North Dakota: (%.4f,%.4f) still saturated at depth cap "
                    "(radius=%.2fmi < corner=%.2fmi) -- providers may be missed",
                    lat, lon, far, corner,
                )

        if self.node_count % 50 == 0:
            self.logger.info(
                "North Dakota: %d search nodes processed, %d unique providers",
                self.node_count, len(self.seen),
            )

    # --- detail (per provider) ------------------------------------------ #

    def parse_detail(self, response):
        d = response.json()
        pid = d.get("id") or response.meta.get("id")

        item = ProviderItem()
        item["source_state"] = "North Dakota"
        item["provider_url"] = PROFILE_URL.format(pid)

        def put(key, value):
            if isinstance(value, str):
                value = value.strip() or None
            elif isinstance(value, list):
                value = value or None
            if value is not None:
                item[key] = value

        # --- common fields ---
        put("provider_name", d.get("orgName"))
        put("license_number", d.get("formattedLicenseNumber"))
        put("provider_type", d.get("facilityTypeLabel"))
        item["status"] = "Closed" if d.get("deactivated") else "Active"

        addr = ", ".join(
            p.strip() for p in (d.get("address1"), d.get("address2"))
            if p and p.strip()
        )
        put("address", addr)
        put("city", d.get("addressCity"))
        put("state", d.get("addressState"))
        put("zip", d.get("addressZip"))
        put("county", d.get("addressCounty"))

        location = d.get("location") or {}
        if location.get("latitude") is not None:
            item["latitude"] = str(location["latitude"])
        if location.get("longitude") is not None:
            item["longitude"] = str(location["longitude"])

        put("phone", d.get("contactPhone"))
        put("email", d.get("contactEmailAddress"))
        put("provider_website", d.get("website"))
        # Best-effort: the registry's contact person (may be a referral contact
        # rather than the on-site director).
        put("administrator", d.get("contactName"))

        put("capacity", d.get("programCapacity"))
        put("hours", d.get("hoursOfOperation"))
        put("ages_served", join_labels(d.get("ageGroupsServedLabels")))

        ages = d.get("ageGroupsServed") or []
        if ages:
            served = set(ages)
            for code, field in AGE_GROUP_TO_FIELD.items():
                item[field] = code in served

        if d.get("acceptsFinancialAssistance") is not None:
            item["scholarships_accepted"] = d["acceptsFinancialAssistance"]

        put("license_begin_date", iso_date(d.get("licenseEffectiveBeginDate")))
        put("license_expiration", iso_date(d.get("licenseEffectiveEndDate")))
        put("languages", join_labels(d.get("languagesLabels")))
        put("transportation", join_labels(d.get("transportationProvidedLabels")))
        if d.get("accreditations"):
            item["accreditation"] = d["accreditations"]

        item["head_start"] = bool(d.get("headStartGranteeId")) or (
            d.get("facilityTypeLabel") == "Head Start Site"
        )
        total_vac = d.get("totalVacancies")
        if total_vac is not None:
            item["accepting_new_children"] = total_vac > 0

        # --- North Dakota specific ---
        put("nd_quality_rating", d.get("qualityRatingLabel"))
        if total_vac is not None:
            item["nd_total_vacancies"] = total_vac
        vba = d.get("vacanciesByAgeGroup") or []
        if vba:
            item["nd_vacancies_by_age"] = [
                {"age_group": x.get("ageGroupLabel"),
                 "vacancies": x.get("numberVacancies")}
                for x in vba
            ]
        put("nd_vacancies_details", d.get("vacanciesDetails"))
        put("nd_vacancies_updated", iso_date(d.get("vacanciesTimestamp")))
        put("nd_desired_capacity", d.get("desiredCapacity"))
        put("nd_total_enrollment", d.get("totalEnrollment"))
        put("nd_enrollment_schedule", d.get("programEnrollmentScheduleLabel"))
        put("nd_special_populations", d.get("specialPopulationsLabels"))
        put("nd_supplemental_care", d.get("supplementalCareTypesLabels"))
        put("nd_min_age",
            age_range(d.get("minimumAge"), d.get("minimumAgeMeasurementLabel")))
        put("nd_max_age",
            age_range(d.get("maximumAge"), d.get("maximumAgeMeasurementLabel")))
        put("nd_program_id", pid)
        put("nd_org_id", d.get("orgId"))
        put("nd_philosophy", d.get("philosophyStatement"))

        yield item

    def closed(self, reason):
        self.logger.info(
            "North Dakota: finished (%s) -- %d search nodes, %d unique providers",
            reason, self.node_count, len(self.seen),
        )
        if len(self.seen) < EXPECTED_MIN_PROVIDERS:
            self.logger.warning(
                "North Dakota: only %d providers found (< %d baseline) -- "
                "possible incomplete crawl",
                len(self.seen), EXPECTED_MIN_PROVIDERS,
            )

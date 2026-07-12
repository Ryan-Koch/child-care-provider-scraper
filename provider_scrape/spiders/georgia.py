import csv
import io
import json

import scrapy
from scrapy.http import JsonRequest

from provider_scrape.items import InspectionItem, ProviderItem

SITE = "https://families.decal.ga.gov"
API_BASE = "https://dcle2-decalapiprd.azurewebsites.net/api"
TOKEN_URL = f"{API_BASE}/Token"
EXPORT_URL = f"{API_BASE}/Provider/Export"
SEARCH_URL = f"{API_BASE}/provider/search"
VISITS_URL = f"{API_BASE}/visits/public"  # /{location_id}
COMPLIANCE_URL = f"{API_BASE}/PDGData/GetComplianceStatus"  # /{location_id}
DETAIL_PAGE_URL = f"{SITE}/ChildCare/detail"  # /{location_id}

# Public client credentials shipped in the site's JS bundle (assets/index-*.js).
# The React app exchanges these for a short-lived (1h) bearer token that every
# API call requires. They are not secret: anyone loading the site receives them.
CLIENT_ID = "9a16d2db-a557-40dd-b2ab-b55e3e6da721"
CLIENT_SECRET = "5a345818-f2f9-4523-bec9-e640e8898383"
AUDIENCE = "Families"

# Every licensed + non-licensed program-type id from the export form's option
# list. The export rejects an empty selection ("Please select at least one
# County or Program Type."), so we request all of them for the full roster.
PROGRAM_TYPE_IDS = [100, 102, 104, 110, 111, 112, 113, 115, 116]


def _clean(value):
    """Trim to a non-empty string, or None. Non-strings are stringified."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _split_multi(value, sep="|"):
    """Turn a delimited API string ("A|B|C") into "A; B; C"; empty -> None.

    The detail page used to render these as checkbox lists / <li> items joined
    with "; "; the API returns the same values delimited by ``sep`` instead.
    """
    text = _clean(value)
    if not text:
        return None
    parts = [p.strip() for p in text.split(sep) if p.strip()]
    return "; ".join(parts) if parts else None


def _yes_no(value):
    """Map an API boolean to the "Yes"/"No" strings the detail scrape produced."""
    if value is None:
        return None
    return "Yes" if value else "No"


def _format_fee(value):
    """Render a numeric fee (95.0) as the "$95.00" string the old page showed."""
    if value is None:
        return None
    try:
        return f"${float(value):.2f}"
    except (TypeError, ValueError):
        return _clean(value)


def parse_weekly_rates(value):
    """Parse "Under 1 year - $110.00|1 year - $95.00" into a list of dicts.

    The old detail-page table exposed nine rate columns per age group; the API
    now publishes only the full-day weekly rate, so that is the one column we
    can preserve (keyed the same way for output compatibility).
    """
    text = _clean(value)
    if not text:
        return []
    rates = []
    for chunk in text.split("|"):
        chunk = chunk.strip()
        if not chunk:
            continue
        age, sep, rate = chunk.rpartition(" - ")
        if sep:
            rates.append({"age": age.strip(), "weekly_full_day": rate.strip()})
        else:
            rates.append({"age": chunk, "weekly_full_day": None})
    return rates


def build_mailing_address(record):
    """Assemble "street, city, ST zip" from the search record's ml* fields."""
    street = _clean(record.get("mlAddress"))
    city = _clean(record.get("mlCity"))
    state = _clean(record.get("mlState"))
    zip_code = _clean(record.get("mlZip"))
    if street and city:
        return f"{street}, {city}, {state or 'GA'} {zip_code or ''}".strip()
    parts = [p for p in (street, city, state, zip_code) if p]
    return ", ".join(parts) if parts else None


class GeorgiaSpider(scrapy.Spider):
    """Georgia child care providers, read from the DECAL Family Portal JSON API.

    The public site (families.decal.ga.gov) used to be an ASP.NET WebForms app:
    a form POST downloaded a CSV roster and each provider had a server-rendered
    detail page. It is now a React SPA backed by a JSON API, so there is no form
    to submit and the detail pages return an empty app shell. We consume the same
    API the SPA does:

      * POST /Token                             -> short-lived bearer token
      * POST /Provider/Export                   -> CSV roster of every provider
      * POST /provider/search {ProviderNumber}  -> one provider's rich detail
      * GET  /visits/public/{id}                -> that provider's inspections
      * GET  /PDGData/GetComplianceStatus/{id}  -> compliance tooltip

    The token lasts one hour and a full run finishes comfortably inside that, so
    a single token is fetched at start and reused for every request.

    The compliance-status call is an extra GET per provider (the roster/search
    endpoints no longer expose ``ga_compliance_status``); it is on by default.
    Pass ``-a fetch_compliance=0`` to skip it and save that request per provider.
    """

    name = "georgia"
    allowed_domains = [
        "families.decal.ga.gov",
        "dcle2-decalapiprd.azurewebsites.net",
    ]

    custom_settings = {
        # Plain JSON/CSV over HTTP: use the standard download handler instead of
        # the project-wide Playwright one (no chromium, and POST bodies work).
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler",
            "https": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler",
        },
        # Three API calls per provider by default (~24k total; ~16k with
        # fetch_compliance=0). Keep the whole run well under the 1h token
        # lifetime while staying polite to the API — one token for every request.
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "DOWNLOAD_DELAY": 0.05,
        "RETRY_TIMES": 5,
        "RETRY_HTTP_CODES": [408, 429, 500, 502, 503, 504, 522, 524],
        "DOWNLOAD_MAXSIZE": 50 * 1024 * 1024,  # export CSV is several MB
    }

    def __init__(self, fetch_compliance=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # On by default; one extra GET per provider recovers ga_compliance_status.
        # Pass ``-a fetch_compliance=0`` (or false/no) to skip that call.
        self.fetch_compliance = str(fetch_compliance).lower() not in ("0", "false", "no")
        self.token = None

    # -- auth --------------------------------------------------------------

    def start_requests(self):
        yield JsonRequest(
            url=TOKEN_URL,
            data={
                "clientId": CLIENT_ID,
                "clientSecret": CLIENT_SECRET,
                "audience": AUDIENCE,
            },
            callback=self.parse_token,
            dont_filter=True,
        )

    def parse_token(self, response):
        self.token = json.loads(response.text)["access_token"]
        self.logger.info("Obtained API bearer token; requesting provider export.")
        yield self._export_request()

    def _auth_headers(self):
        return {"Authorization": f"Bearer {self.token}"}

    def _export_request(self):
        return JsonRequest(
            url=EXPORT_URL,
            data={
                "qualityRatedOnly": False,
                "preKOnly": False,
                "selectedProgramTypes": PROGRAM_TYPE_IDS,
                "selectedCountyId": None,
            },
            headers=self._auth_headers(),
            callback=self.parse_export,
            dont_filter=True,
        )

    # -- phase 1: roster CSV ----------------------------------------------

    def parse_export(self, response):
        """Parse the exported CSV and fan out one search request per provider."""
        text = self._decode_csv(response)
        if text is None:
            self.logger.error("Could not decode export CSV with any known encoding")
            return

        # csv module needs consistent line endings.
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        reader = csv.DictReader(io.StringIO(text))
        self.logger.info(f"Export CSV columns: {reader.fieldnames}")

        count = 0
        for row in reader:
            count += 1
            item = self._map_csv_row(row)
            provider_number = item.get("license_number")
            if provider_number:
                yield JsonRequest(
                    url=SEARCH_URL,
                    data={"ProviderNumber": provider_number},
                    headers=self._auth_headers(),
                    callback=self.parse_detail,
                    errback=self.errback_enrich,
                    cb_kwargs={"item": item, "provider_number": provider_number},
                    dont_filter=True,
                )
            else:
                # No provider number -> no detail lookup possible.
                yield item
        self.logger.info(f"Parsed {count} providers from export CSV.")

    @staticmethod
    def _decode_csv(response):
        """Decode the export bytes, honoring the header charset then falling back.

        The live export is UTF-8 with a BOM; older responses used UTF-16. We try
        the header-declared charset first, then the common encodings.
        """
        content_type = response.headers.get("Content-Type", b"").decode(
            "utf-8", "ignore"
        )
        charset = None
        if "charset=" in content_type.lower():
            charset = content_type.lower().split("charset=")[-1].split(";")[0].strip()

        encodings = []
        if charset:
            encodings.append(charset)
        encodings.extend(["utf-8-sig", "utf-16", "utf-8", "latin-1"])

        for encoding in encodings:
            try:
                return response.body.decode(encoding)
            except (UnicodeDecodeError, UnicodeError, LookupError):
                continue
        return None

    # -- phase 2: per-provider detail -------------------------------------

    def parse_detail(self, response, item, provider_number):
        """Enrich the CSV item with the provider's rich search record."""
        records = json.loads(response.text)
        record = None
        if isinstance(records, list) and records:
            # The ProviderNumber filter returns a single row; match defensively.
            record = next(
                (r for r in records if r.get("providerNumber") == provider_number),
                records[0],
            )

        if not record:
            self.logger.warning(
                f"No search record for {provider_number}; emitting CSV-only item."
            )
            yield item
            return

        self._enrich_from_search(item, record)

        location_id = record.get("id")
        if location_id is None:
            yield item
            return
        item["provider_url"] = f"{DETAIL_PAGE_URL}/{location_id}"

        yield scrapy.Request(
            url=f"{VISITS_URL}/{location_id}",
            headers=self._auth_headers(),
            callback=self.parse_visits,
            errback=self.errback_enrich,
            cb_kwargs={"item": item, "location_id": location_id},
            dont_filter=True,
        )

    def parse_visits(self, response, item, location_id):
        """Attach inspection history, then optionally fetch compliance status."""
        item["inspections"] = self._build_inspections(json.loads(response.text))

        if self.fetch_compliance:
            yield scrapy.Request(
                url=f"{COMPLIANCE_URL}/{location_id}",
                headers=self._auth_headers(),
                callback=self.parse_compliance,
                errback=self.errback_enrich,
                cb_kwargs={"item": item},
                dont_filter=True,
            )
        else:
            yield item

    def parse_compliance(self, response, item):
        data = json.loads(response.text)
        item["ga_compliance_status"] = _clean(data.get("tooltip"))
        yield item

    def errback_enrich(self, failure):
        """A detail/visits/compliance call failed after retries: emit what we have."""
        item = failure.request.cb_kwargs.get("item")
        self.logger.warning(
            f"Enrichment request failed ({failure.request.url}): "
            f"{failure.value!r}. Emitting item without the missing fields."
        )
        return [item] if item is not None else []

    # -- mapping helpers ---------------------------------------------------

    def _enrich_from_search(self, item, record):
        """Fill the detail-only fields from a /provider/search record.

        Only fields the export CSV cannot supply are set here (and a few the
        API supplies in a richer form); the CSV base values are otherwise left
        untouched so we do not downgrade nicely-formatted columns.
        """

        def r(key):
            return record.get(key)

        # Administrator (two separate labels on the old detail page).
        admin = " ".join(
            p
            for p in (_clean(r("adminFirstName")), _clean(r("adminLastName")))
            if p
        )
        if admin:
            item["administrator"] = admin

        if r("capacity") is not None:
            item["capacity"] = str(r("capacity"))
        if r("qualityRating") is not None:
            item["ga_quality_rated_level"] = str(r("qualityRating"))

        item["ga_liability_insurance"] = _yes_no(r("liabilityInsurance"))
        item["ga_accepting_new_children"] = _yes_no(r("isAcceptingNewChildren"))
        item["ga_registration_fee"] = _format_fee(r("rateRegistrationFee"))
        item["ga_activity_fee"] = _format_fee(r("rateActivityFee"))

        # Pipe-delimited multi-value fields. For services/transportation the CSV
        # already derives a value from boolean flags; keep it if the API is empty.
        item["ga_services"] = _split_multi(r("servicesProvided")) or item.get(
            "ga_services"
        )
        item["ga_transportation"] = _split_multi(r("transportation")) or item.get(
            "ga_transportation"
        )
        item["ga_meals"] = _split_multi(r("mealInfo"))
        item["ga_environment"] = _split_multi(r("environmentInfo"))
        item["ga_summer_camp"] = _split_multi(r("campCareInfo"))
        item["ga_accepts_children_type"] = _split_multi(r("acceptingChildrenTimeType"))
        item["ga_activities"] = _split_multi(r("activities"))
        item["ga_other_care_type"] = _split_multi(r("otherChildCareTypes"))
        item["ga_financial_info"] = _split_multi(r("financialInfo"))
        item["ga_special_hours"] = _split_multi(r("specialHourInfo"))
        item["ga_family_engagement"] = _split_multi(r("familyEngagement"))
        item["languages"] = _split_multi(r("languages"))

        # ages_served is comma-delimited on this endpoint (the labels contain no
        # commas). It is richer than the CSV's boolean-derived value, so prefer
        # it when present.
        ages = _split_multi(r("agesServed"), sep=",")
        if ages:
            item["ages_served"] = ages

        # Accreditation: keep the CSV value unless the API supplies a code.
        accreditation = _clean(r("accreditations"))
        if accreditation:
            item["ga_accreditation"] = accreditation

        item["ga_profit_status"] = _clean(r("profitStatus"))

        rates = parse_weekly_rates(r("weeklyFullDayRates"))
        if rates:
            item["ga_weekly_rates"] = rates

        if not item.get("ga_mailing_address"):
            mailing = build_mailing_address(record)
            if mailing:
                item["ga_mailing_address"] = mailing

        # Notes fields the new API rarely populates, mapped defensively.
        item["ga_transportation_notes"] = _clean(r("transportToFromSchool"))
        item["ga_school_break_notes"] = _clean(
            r("schoolCareBreakAdditionalSchedulingInfo")
        )

    @staticmethod
    def _build_inspections(visits):
        """Build InspectionItems from the /visits/public list."""
        inspections = []
        for visit in visits or []:
            insp = InspectionItem()
            date = _clean(visit.get("visitDate"))
            if date and "T" in date:
                date = date.split("T", 1)[0]  # ISO datetime -> YYYY-MM-DD
            insp["date"] = date
            insp["type"] = _clean(visit.get("visitType"))
            insp["original_status"] = _clean(visit.get("visitStatus"))
            if insp.get("date") or insp.get("type"):
                inspections.append(insp)
        return inspections

    def _map_csv_row(self, row):
        """Map a CSV row dict to a ProviderItem.

        CSV columns (from the /Provider/Export endpoint):
        Provider_Number, Location, County, Address, City, State, Zip,
        MailingAddress, MailingCity, MailingState, MailingZip, Email, Phone,
        LicenseCapacity, Operation_Months, Operation_Days, Hours_Open,
        Hours_Close, Infant_0_To_12mos, Toddler_13mos_To_2yrs,
        Preschool_3yrs_To_4yrs, Pre_K_Served, School_Age_5yrs_Plus,
        Ages_Other_Than_Pre_K_Served, CAPS_Enrolled, Has_Evening_Care,
        Has_Drop_In_Care, Has_School_Age_Summer_Care,
        Has_Transport_ToFrom_School, Has_Transport_ToFrom_Home, Has_Cacfp,
        Accreditation_Status, Program_Type, Provider_Type, Exemption_Category,
        Available_PreK_Slots, Funded_PreK_Slots, QR_Participant, QR_Rated,
        QR_Rating, Region, IsTemporarilyClosed, TemporaryClosure_StartDate,
        TemporaryClosure_EndDate, CurrentProgramStatus
        """
        item = ProviderItem()
        item["source_state"] = "Georgia"

        def g(key):
            """Get a trimmed value from the row, returning None for empty."""
            val = row.get(key, "")
            return val.strip() if val and val.strip() else None

        item["provider_name"] = g("Location")
        item["license_number"] = g("Provider_Number")
        item["provider_type"] = g("Program_Type") or g("Provider_Type")
        item["status"] = g("CurrentProgramStatus")
        item["email"] = g("Email")
        item["phone"] = g("Phone")
        item["capacity"] = g("LicenseCapacity")
        item["county"] = g("County")

        # Assemble full address from parts
        street = g("Address")
        city = g("City")
        state = g("State")
        zipcode = g("Zip")
        addr_parts = [p for p in [street, city, state, zipcode] if p]
        if addr_parts:
            # Format as "street, city, state zip"
            if street and city:
                item["address"] = f"{street}, {city}, {state or 'GA'} {zipcode or ''}".strip()
            else:
                item["address"] = ", ".join(addr_parts)

        # Mailing address
        mail_parts = [g("MailingAddress"), g("MailingCity"), g("MailingState"), g("MailingZip")]
        if any(mail_parts):
            mail_street = mail_parts[0]
            mail_city = mail_parts[1]
            mail_state = mail_parts[2]
            mail_zip = mail_parts[3]
            if mail_street and mail_city:
                item["ga_mailing_address"] = f"{mail_street}, {mail_city}, {mail_state or 'GA'} {mail_zip or ''}".strip()
            else:
                item["ga_mailing_address"] = ", ".join(p for p in mail_parts if p)

        # Hours
        hours_open = g("Hours_Open")
        hours_close = g("Hours_Close")
        if hours_open and hours_close:
            item["hours"] = f"{hours_open} - {hours_close}"
        elif hours_open:
            item["hours"] = hours_open

        # Operating schedule
        item["ga_operating_months"] = g("Operation_Months")
        item["ga_operating_days"] = g("Operation_Days")

        # Age groups served
        age_parts = []
        if g("Infant_0_To_12mos") and g("Infant_0_To_12mos").upper() == "TRUE":
            age_parts.append("Infant (0-12 months)")
            item["infant"] = "Yes"
        if g("Toddler_13mos_To_2yrs") and g("Toddler_13mos_To_2yrs").upper() == "TRUE":
            age_parts.append("Toddler (13 months - 2 years)")
            item["toddler"] = "Yes"
        if g("Preschool_3yrs_To_4yrs") and g("Preschool_3yrs_To_4yrs").upper() == "TRUE":
            age_parts.append("Preschool (3-4 years)")
            item["preschool"] = "Yes"
        if g("Pre_K_Served") and g("Pre_K_Served").upper() == "TRUE":
            age_parts.append("Pre-K (4 years)")
        if g("School_Age_5yrs_Plus") and g("School_Age_5yrs_Plus").upper() == "TRUE":
            age_parts.append("School Age (5+)")
            item["school"] = "Yes"
        if age_parts:
            item["ages_served"] = "; ".join(age_parts)

        # Services
        services = []
        if g("CAPS_Enrolled") and g("CAPS_Enrolled").upper() == "TRUE":
            services.append("CAPS Enrolled")
            item["scholarships_accepted"] = "Yes"
        if g("Has_Evening_Care") and g("Has_Evening_Care").upper() == "TRUE":
            services.append("Evening Care")
        if g("Has_Drop_In_Care") and g("Has_Drop_In_Care").upper() == "TRUE":
            services.append("Drop-In Care")
        if g("Has_School_Age_Summer_Care") and g("Has_School_Age_Summer_Care").upper() == "TRUE":
            services.append("School-age Summer Care")
        if g("Has_Cacfp") and g("Has_Cacfp").upper() == "TRUE":
            services.append("CACFP")
        if services:
            item["ga_services"] = "; ".join(services)

        # Transportation
        transport = []
        if g("Has_Transport_ToFrom_School") and g("Has_Transport_ToFrom_School").upper() == "TRUE":
            transport.append("To/From School")
        if g("Has_Transport_ToFrom_Home") and g("Has_Transport_ToFrom_Home").upper() == "TRUE":
            transport.append("To/From Home")
        if transport:
            item["ga_transportation"] = "; ".join(transport)

        # Accreditation
        item["ga_accreditation"] = g("Accreditation_Status")

        # Quality Rating
        item["ga_quality_rated_level"] = g("QR_Rating")

        # Program status
        item["ga_program_status"] = g("CurrentProgramStatus")

        return item

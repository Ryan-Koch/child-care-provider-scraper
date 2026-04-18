import csv
import io
import re

import scrapy

from provider_scrape.items import InspectionItem, ProviderItem

DATA_PAGE_URL = "https://families.decal.ga.gov/Provider/Data"
DETAIL_BASE_URL = "https://families.decal.ga.gov/ChildCare/detail/"

# The download button triggers an ASP.NET postback with this event target
DOWNLOAD_EVENT_TARGET = "ctl00$Content_Main$btnExportToExcel"


def extract_checked_labels(response, table_id):
    """Extract label text for all checked checkboxes in an ASP.NET CheckBoxList."""
    checked = []
    table = response.css(f"#{table_id}")
    if table:
        for inp in table.css("input[type=checkbox][checked]"):
            inp_id = inp.attrib.get("id", "")
            label = table.css(f'label[for="{inp_id}"]::text').get()
            if label:
                checked.append(label.strip())
    return "; ".join(checked) if checked else None


def extract_list_items(response, span_id):
    """Extract text from <li> elements inside a span, joined by semicolons."""
    items = response.css(f"#{span_id} li::text").getall()
    return "; ".join(i.strip() for i in items if i.strip()) if items else None


def extract_radio_checked(response, table_id):
    """Extract label text for the checked radio button in a RadioButtonList."""
    table = response.css(f"#{table_id}")
    if table:
        checked = table.css("input[type=radio][checked]")
        if checked:
            inp_id = checked.attrib.get("id", "")
            label = table.css(f'label[for="{inp_id}"]::text').get()
            if label:
                return label.strip()
    return None


def extract_numeric_id(license_number):
    """Extract the numeric portion from a license number like CCLC-38436 -> 38436."""
    if not license_number:
        return None
    match = re.search(r"(\d+)$", license_number)
    return match.group(1) if match else None


def parse_weekly_rates(response):
    """Parse the weekly rates table into a list of dicts."""
    rates = []
    table = response.css("#Content_Main_gvFacilityRates")
    if not table:
        return rates

    rows = table.css("tr")
    if len(rows) < 2:
        return rates

    for row in rows[1:]:
        cells = row.css("td")
        if len(cells) < 9:
            continue

        def cell_text(idx):
            # Get text from the div inside, or fall back to td text
            text = cells[idx].css("div::text").get()
            if text:
                return text.strip()
            # Fall back to direct text content (skip the visible-xs span)
            all_text = cells[idx].css("::text").getall()
            # Filter out the mobile label spans
            filtered = [
                t.strip()
                for t in all_text
                if t.strip() and not t.strip().endswith(":")
            ]
            return filtered[-1] if filtered else None

        rate = {
            "age": cell_text(0),
            "weekly_full_day": cell_text(1),
            "weekly_before_school": cell_text(2),
            "weekly_after_school": cell_text(3),
            "vacancies": cell_text(4),
            "num_rooms": cell_text(5),
            "staff_child_ratio": cell_text(6),
            "daily_drop_in": cell_text(7),
            "day_camp": cell_text(8),
        }
        # Only include if at least the age is present
        if rate["age"]:
            rates.append(rate)
    return rates


class GeorgiaSpider(scrapy.Spider):
    """Spider for Georgia child care provider data from families.decal.ga.gov.

    Phase 1: Downloads a CSV of all providers from the Provider Data page.
    Phase 2: Visits each provider's detail page for additional fields.
    """

    name = "georgia"
    allowed_domains = ["families.decal.ga.gov"]

    custom_settings = {
        "DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS": 4,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "RETRY_TIMES": 5,
        # CSV download can be large
        "DOWNLOAD_MAXSIZE": 50 * 1024 * 1024,  # 50MB
    }

    def start_requests(self):
        yield scrapy.Request(DATA_PAGE_URL, callback=self.parse_data_page)

    def parse_data_page(self, response):
        """Extract checkbox names from the Data page, then submit the form."""
        # Find all program type checkboxes and check them all
        checkboxes = response.css(
            "input[type=checkbox][name*='Content_Main']"
        )
        formdata = {}
        for cb in checkboxes:
            name = cb.attrib.get("name", "")
            value = cb.attrib.get("value", "on")
            if name:
                formdata[name] = value

        self.logger.info(
            f"Found {len(formdata)} program type checkboxes, submitting form..."
        )

        # Trigger the download button via __doPostBack
        formdata["__EVENTTARGET"] = DOWNLOAD_EVENT_TARGET
        formdata["__EVENTARGUMENT"] = ""

        yield scrapy.FormRequest.from_response(
            response,
            formdata=formdata,
            callback=self.parse_csv,
            dont_click=True,
            dont_filter=True,
        )

    def parse_csv(self, response):
        """Parse the downloaded CSV and yield detail page requests."""
        content_type = response.headers.get("Content-Type", b"").decode("utf-8", "ignore")
        self.logger.info(
            f"CSV response: {len(response.body)} bytes, "
            f"Content-Type: {content_type}"
        )

        # Detect encoding from Content-Type header (live site sends charset=utf-16).
        # Fall back to BOM detection and common encodings.
        charset = None
        if "charset=" in content_type.lower():
            charset = content_type.lower().split("charset=")[-1].strip().split(";")[0]

        # Build encoding priority: header charset first, then common fallbacks
        encodings = []
        if charset:
            encodings.append(charset)
        encodings.extend(["utf-16", "utf-8-sig", "utf-8", "latin-1"])

        text = None
        for encoding in encodings:
            try:
                text = response.body.decode(encoding)
                self.logger.info(f"Decoded CSV with encoding: {encoding}")
                break
            except (UnicodeDecodeError, UnicodeError):
                continue
        if text is None:
            self.logger.error("Could not decode CSV with any known encoding")
            return

        # csv module needs consistent line endings
        text = text.replace("\r\n", "\n").replace("\r", "\n")

        reader = csv.DictReader(io.StringIO(text))
        field_names = reader.fieldnames
        self.logger.info(f"CSV columns: {field_names}")

        count = 0
        for row in reader:
            count += 1
            item = self._map_csv_row(row)

            # Extract numeric ID for the detail page
            license_number = item.get("license_number")
            numeric_id = extract_numeric_id(license_number)

            if numeric_id:
                detail_url = f"{DETAIL_BASE_URL}{numeric_id}"
                yield scrapy.Request(
                    detail_url,
                    callback=self.parse_detail,
                    cb_kwargs={"item": item},
                    dont_filter=True,
                )
            else:
                # No detail page available, yield the CSV-only item
                yield item

        self.logger.info(f"Parsed {count} rows from CSV")

    def parse_detail(self, response, item):
        """Parse a provider detail page for additional fields."""
        # Verify we got a detail page
        if "panel-primary" not in response.text:
            self.logger.warning(
                f"Expected detail page but got unexpected content at {response.url}"
            )
            yield item
            return

        item["provider_url"] = response.url

        # Quality Rated level from the image alt text
        qr_img = response.css("#Content_Main_imgQRLevel")
        if qr_img:
            alt = qr_img.attrib.get("alt", "")
            level_match = re.search(r"(\d+)", alt)
            if level_match:
                item["ga_quality_rated_level"] = level_match.group(1)

        # Administrator
        admin = response.css("#Content_Main_lblAdmin::text").get()
        if admin:
            item["administrator"] = admin.strip()

        # Licensed capacity
        capacity = response.css("#Content_Main_lblCapacity::text").get()
        if capacity:
            item["capacity"] = capacity.strip()

        # Liability insurance
        insurance = response.css("#Content_Main_lblLiabilityInsurance::text").get()
        if insurance:
            item["ga_liability_insurance"] = insurance.strip()

        # Accepting new children
        accepting = response.css(
            "#Content_Main_chkIsAcceptingNewChildren[checked]"
        )
        item["ga_accepting_new_children"] = "Yes" if accepting else "No"

        # Mailing address
        mail_street = response.css("#Content_Main_lblMailStreet::text").get()
        mail_csz = response.css("#Content_Main_lblMailCityStateZip::text").get()
        if mail_street or mail_csz:
            parts = []
            if mail_street:
                parts.append(mail_street.strip())
            if mail_csz:
                parts.append(mail_csz.strip())
            item["ga_mailing_address"] = " ".join(parts)

        # Fees
        reg_fee = response.css("#Content_Main_lblRegistrationFee::text").get()
        if reg_fee:
            item["ga_registration_fee"] = reg_fee.strip()

        activity_fee = response.css("#Content_Main_lblActivityFee::text").get()
        if activity_fee:
            item["ga_activity_fee"] = activity_fee.strip()

        # Program status
        program_status = response.css(
            "#Content_Main_lblCurrentProgramStatus::text"
        ).get()
        if program_status:
            item["ga_program_status"] = program_status.strip()

        # Compliance status from image title
        compliance_img = response.css("#Content_Main_imgCompliance")
        if compliance_img:
            item["ga_compliance_status"] = compliance_img.attrib.get(
                "title", ""
            ).strip()

        # Operating schedule
        op_months = response.css(
            "#Content_Main_lblMonthsOfOperation::text"
        ).get()
        if op_months:
            item["ga_operating_months"] = op_months.strip()

        op_days = response.css("#Content_Main_lblDaysOfOperation::text").get()
        if op_days:
            item["ga_operating_days"] = op_days.strip()

        # Hours - may contain <br> tags
        hours_span = response.css("#Content_Main_lblHoursOfOperation")
        if hours_span:
            hours_texts = hours_span.css("::text").getall()
            hours = "; ".join(h.strip() for h in hours_texts if h.strip())
            if hours:
                item["hours"] = hours

        # Phone (may override CSV if present)
        phone = response.css("#Content_Main_lblPhone::text").get()
        if phone:
            item["phone"] = phone.strip()

        # Checkbox list fields
        item["ga_services"] = extract_checked_labels(
            response, "Content_Main_cblServicesProvided"
        )
        item["ages_served"] = extract_checked_labels(
            response, "Content_Main_cblAgesServed"
        )
        item["ga_transportation"] = extract_checked_labels(
            response, "Content_Main_cblTransportation"
        )
        item["ga_meals"] = extract_checked_labels(
            response, "Content_Main_cblMeals"
        )
        item["ga_environment"] = extract_checked_labels(
            response, "Content_Main_cblEnvironment"
        )
        item["ga_summer_camp"] = extract_checked_labels(
            response, "Content_Main_cblCampCare"
        )
        item["ga_accepts_children_type"] = extract_checked_labels(
            response, "Content_Main_cblAcceptingChildrenType"
        )

        # Accreditation
        accreditation = response.css(
            "#Content_Main_lblAccreditation::text"
        ).get()
        if accreditation:
            item["ga_accreditation"] = accreditation.strip()

        # Profit status
        item["ga_profit_status"] = extract_radio_checked(
            response, "Content_Main_rblForProfit"
        )

        # Weekly rates
        rates = parse_weekly_rates(response)
        if rates:
            item["ga_weekly_rates"] = rates

        # List-based fields
        item["ga_activities"] = extract_list_items(
            response, "Content_Main_lblActivities"
        )
        item["ga_other_care_type"] = extract_list_items(
            response, "Content_Main_lblOtherChildCareType"
        )
        item["ga_financial_info"] = (
            response.css("#Content_Main_lblFinancialInformation::text").get()
        )
        if item.get("ga_financial_info") == "N/A":
            item["ga_financial_info"] = None

        item["languages"] = extract_list_items(
            response, "Content_Main_lblLanguages"
        )
        item["ga_special_hours"] = extract_list_items(
            response, "Content_Main_lblSpecialHours"
        )
        item["ga_curriculum"] = extract_list_items(
            response, "Content_Main_lblCurriculum"
        )
        item["ga_family_engagement"] = extract_list_items(
            response, "Content_Main_lblFamilyEngagement"
        )

        # Transportation and scheduling notes
        transport_notes = response.css(
            "#Content_Main_pnlTransportationNotes span::text"
        ).get()
        if transport_notes:
            item["ga_transportation_notes"] = transport_notes.strip()

        school_break = response.css(
            "#Content_Main_pnlSchoolBreakNotes span::text"
        ).get()
        if school_break:
            item["ga_school_break_notes"] = school_break.strip()

        # Inspections
        item["inspections"] = self._extract_inspections(response)

        yield item

    def _extract_inspections(self, response):
        """Extract inspection report data from the detail page table."""
        inspections = []
        rows = response.css("#Content_Main_gvReports tr")

        for row in rows[1:]:  # skip header
            cells = row.css("td")
            if len(cells) < 5:
                continue

            insp = InspectionItem()

            # Report date
            date_text = cells[1].css("::text").getall()
            date_filtered = [
                t.strip()
                for t in date_text
                if t.strip() and not t.strip().startswith("Report Date")
            ]
            if date_filtered:
                insp["date"] = date_filtered[-1]

            # Visit status
            status_text = cells[3].css("::text").getall()
            status_filtered = [
                t.strip()
                for t in status_text
                if t.strip() and not t.strip().startswith("Visit Status")
            ]
            if status_filtered:
                insp["original_status"] = status_filtered[-1]

            # Report type
            type_text = cells[4].css("::text").getall()
            type_filtered = [
                t.strip()
                for t in type_text
                if t.strip() and not t.strip().startswith("Report Type")
            ]
            if type_filtered:
                insp["type"] = type_filtered[-1]

            if insp.get("date") or insp.get("type"):
                inspections.append(insp)

        return inspections

    def _map_csv_row(self, row):
        """Map a CSV row dict to a ProviderItem.

        CSV columns (discovered from live site):
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
        item["sutq_rating"] = g("QR_Rating")
        item["ga_quality_rated_level"] = g("QR_Rating")

        # Program status
        item["ga_program_status"] = g("CurrentProgramStatus")

        return item

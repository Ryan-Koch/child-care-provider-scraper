# items.py

import scrapy

class InspectionItem(scrapy.Item):
    date = scrapy.Field()
    type = scrapy.Field()
    original_status = scrapy.Field()
    corrective_status = scrapy.Field()
    status_updated = scrapy.Field()
    report_url = scrapy.Field()
    va_shsi = scrapy.Field()
    va_complaint_related = scrapy.Field()
    va_violations = scrapy.Field()


class ProviderItem(scrapy.Item):
    # This defines all the possible columns for your final CSV file.
    provider_name = scrapy.Field()
    license_number = scrapy.Field()
    provider_type = scrapy.Field()
    status = scrapy.Field()
    status_date = scrapy.Field()
    sutq_rating = scrapy.Field()
    address = scrapy.Field()
    phone = scrapy.Field()
    email = scrapy.Field()
    administrator = scrapy.Field()
    capacity = scrapy.Field()
    hours = scrapy.Field()
    ages_served = scrapy.Field()
    infant = scrapy.Field()
    toddler = scrapy.Field()
    preschool = scrapy.Field()
    school = scrapy.Field()
    county = scrapy.Field()
    scholarships_accepted = scrapy.Field()
    license_begin_date = scrapy.Field()
    license_expiration = scrapy.Field()
    deficiencies = scrapy.Field()
    # Virginia specific fields
    va_license_type = scrapy.Field()
    va_inspector = scrapy.Field()
    va_current_subsidy_provider = scrapy.Field()
    # Texas specific fields
    tx_rising_star = scrapy.Field()
    tx_operation_id = scrapy.Field()
    tx_agency_number = scrapy.Field()


    # These fields help with tracking and debugging.
    provider_url = scrapy.Field()
    source_state = scrapy.Field()

    # This will hold the list of inspections.
    inspections = scrapy.Field()

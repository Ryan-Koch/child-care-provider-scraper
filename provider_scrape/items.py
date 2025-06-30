# items.py

import scrapy

class InspectionItem(scrapy.Item):
    date = scrapy.Field()
    type = scrapy.Field()
    original_status = scrapy.Field()
    corrective_status = scrapy.Field()
    status_updated = scrapy.Field()
    report_url = scrapy.Field()

class ProviderItem(scrapy.Item):
    # This defines all the possible columns for your final CSV file.
    provider_name = scrapy.Field()
    license_number = scrapy.Field()
    provider_type = scrapy.Field()
    status = scrapy.Field()
    sutq_rating = scrapy.Field()
    address = scrapy.Field()
    phone = scrapy.Field()
    email = scrapy.Field()
    administrator = scrapy.Field()
    capacity = scrapy.Field()
    hours = scrapy.Field()
    ages_served = scrapy.Field()
    county = scrapy.Field()
    license_begin_date = scrapy.Field()
    license_expiration = scrapy.Field()

    # These fields help with tracking and debugging.
    provider_url = scrapy.Field()
    source_state = scrapy.Field()

    # This will hold the list of inspections.
    inspections = scrapy.Field()

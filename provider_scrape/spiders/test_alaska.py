import json
import unittest

from provider_scrape.spiders.alaska import (
    AlaskaSpider,
    _build_address,
    _expand_age_range,
    _format_date,
    _parse_capacity,
)


# A representative /api/Provider/{id} record (detail endpoint includes
# complianceEvents; the roster endpoint returns the same shape with an empty
# complianceEvents list).
DETAIL_RECORD = {
    "facilityId": 10000011,
    "facilityName": "LITTLE PEOPLES LEARNING WORLD",
    "facilityType": "LC",
    "licenseNumber": 11769,
    "licenseStatus": "BI",
    "acceptsCCAP": "Yes",
    "phoneNumber": "(907) 262-4113",
    "capacity": "60 Children",
    "firstName": "JESSICA",
    "lastName": "BAKER",
    "address": "35095 HUNTINGTON DRIVE",
    "city": "SOLDOTNA",
    "state": "AK",
    "zip": "99669",
    "effectiveDate": "20230301",
    "expirationDate": "20250228",
    "ageRange": "0W - 12 Y",
    "complianceEvents": [
        {
            "isn": 3177,
            "facilityId": 10000011,
            "complianceType": "INVESTIGATION/PRIORITY 2",
            "intakeDate": "20151222",
            "insInvDate": "20160127",
            "complianceDate": "20160719",
            "section": "10.1030 TOILET/SINK/SHOW/BATH FAC",
            "findings": "NON-COMPLIANCE",
            "violationDate": "20160127",
            "actionTaken": "PLAN OF CORRECTION",
            "statuteRegulation": "LICENSING REGS 7AAC 10",
            "comments": "7 AAC 10.1030(c): as evidenced by...",
        }
    ],
}


class AlaskaHelperTest(unittest.TestCase):
    def test_format_date(self):
        self.assertEqual(_format_date("20230301"), "2023-03-01")
        self.assertIsNone(_format_date(""))
        self.assertIsNone(_format_date(None))
        # The API's placeholder/sentinel/corrupt values map to None.
        self.assertIsNone(_format_date("0"))
        self.assertIsNone(_format_date("00000000"))
        self.assertIsNone(_format_date("99999999"))  # "no date" sentinel
        self.assertIsNone(_format_date("70506"))      # wrong-length / corrupt
        self.assertIsNone(_format_date("20250230"))   # invalid calendar date
        # A non-digit value is passed through for the pipeline (defensive).
        self.assertEqual(_format_date("2023-03-01"), "2023-03-01")

    def test_parse_capacity(self):
        self.assertEqual(_parse_capacity("60 Children"), 60)
        self.assertEqual(_parse_capacity("8 Children"), 8)
        self.assertIsNone(_parse_capacity(""))
        self.assertIsNone(_parse_capacity(None))

    def test_expand_age_range(self):
        self.assertEqual(_expand_age_range("0W - 12 Y"), "0 Weeks - 12 Years")
        self.assertEqual(_expand_age_range("2M - 7 Y"), "2 Months - 7 Years")
        self.assertIsNone(_expand_age_range(""))

    def test_build_address(self):
        self.assertEqual(
            _build_address("35095 HUNTINGTON DRIVE", "SOLDOTNA", "AK", "99669"),
            "35095 HUNTINGTON DRIVE SOLDOTNA, AK 99669",
        )
        # Tolerates missing pieces.
        self.assertEqual(_build_address("1 MAIN ST", None, "AK", None), "1 MAIN ST AK")
        self.assertIsNone(_build_address(None, None, None, None))


class AlaskaBuildItemTest(unittest.TestCase):
    def setUp(self):
        self.spider = AlaskaSpider()

    def test_build_item_maps_core_fields(self):
        item = self.spider.build_item(DETAIL_RECORD)
        self.assertEqual(item["source_state"], "Alaska")
        self.assertEqual(item["provider_name"], "LITTLE PEOPLES LEARNING WORLD")
        # license_number is the real license number, not the facility id.
        self.assertEqual(item["license_number"], "11769")
        self.assertEqual(
            item["provider_url"],
            "https://findccprovider.health.alaska.gov/ProviderInfo/10000011",
        )
        self.assertEqual(item["phone"], "(907) 262-4113")
        self.assertEqual(item["capacity"], 60)
        self.assertEqual(item["administrator"], "JESSICA BAKER")
        self.assertEqual(item["ages_served"], "0 Weeks - 12 Years")
        self.assertEqual(item["scholarships_accepted"], "Yes")
        self.assertEqual(item["status_date"], "2023-03-01")
        self.assertEqual(item["license_begin_date"], "2023-03-01")
        self.assertEqual(item["license_expiration"], "2025-02-28")
        self.assertEqual(item["address"], "35095 HUNTINGTON DRIVE SOLDOTNA, AK 99669")
        self.assertEqual(item["city"], "SOLDOTNA")
        self.assertEqual(item["state"], "AK")
        self.assertEqual(item["zip"], "99669")

    def test_build_item_maps_inspections(self):
        item = self.spider.build_item(DETAIL_RECORD)
        self.assertEqual(len(item["inspections"]), 1)
        insp = item["inspections"][0]
        self.assertEqual(insp["date"], "2016-01-27")  # insInvDate
        self.assertEqual(insp["type"], "INVESTIGATION/PRIORITY 2")
        self.assertEqual(insp["original_status"], "NON-COMPLIANCE")
        self.assertEqual(insp["corrective_status"], "PLAN OF CORRECTION")
        self.assertEqual(insp["status_updated"], "2016-07-19")  # complianceDate

    def test_inspection_date_falls_back_past_zero_placeholder(self):
        # insInvDate/violationDate/intakeDate are all the "0" placeholder;
        # the date should fall through to complianceDate, not become "0".
        events = [{
            "complianceType": "INSPECTION",
            "intakeDate": "0",
            "insInvDate": "0",
            "violationDate": "0",
            "complianceDate": "20170403",
            "findings": "NON-COMPLIANCE",
            "actionTaken": "PLAN OF CORRECTION",
        }]
        inspections = AlaskaSpider.build_inspections(events)
        self.assertEqual(len(inspections), 1)
        self.assertEqual(inspections[0]["date"], "2017-04-03")

    def test_duplicate_violation_rows_are_deduplicated(self):
        # The API emits one row per violated regulation; rows that are identical
        # once reduced to the generic fields collapse to a single inspection.
        event = {
            "complianceType": "INSPECTION",
            "insInvDate": "0",
            "complianceDate": "20170403",
            "findings": "NON-COMPLIANCE",
            "actionTaken": "PLAN OF CORRECTION",
        }
        inspections = AlaskaSpider.build_inspections([event, dict(event), dict(event)])
        self.assertEqual(len(inspections), 1)

    def test_roster_record_without_compliance_yields_empty_inspections(self):
        # A roster row (no complianceEvents key) still produces a valid item.
        roster = {k: v for k, v in DETAIL_RECORD.items() if k != "complianceEvents"}
        item = self.spider.build_item(roster)
        self.assertEqual(item["inspections"], [])
        self.assertEqual(item["provider_name"], "LITTLE PEOPLES LEARNING WORLD")

    def test_empty_optional_fields_become_none(self):
        sparse = {
            "facilityId": 999,
            "facilityName": "TEST",
            "licenseNumber": 1,
            "licenseStatus": "",
            "capacity": "",
            "effectiveDate": "",
            "expirationDate": "",
            "ageRange": "",
            "acceptsCCAP": "No",
            "complianceEvents": [],
        }
        item = self.spider.build_item(sparse)
        self.assertIsNone(item["capacity"])
        self.assertIsNone(item["status_date"])
        self.assertIsNone(item["license_expiration"])
        self.assertIsNone(item["ages_served"])
        self.assertEqual(item["scholarships_accepted"], "No")


class AlaskaParseListTest(unittest.TestCase):
    def setUp(self):
        self.spider = AlaskaSpider()

    def test_parse_list_yields_one_detail_request_per_provider(self):
        class FakeResponse:
            text = json.dumps([
                {"facilityId": 10000011, "facilityName": "A"},
                {"facilityId": 10000013, "facilityName": "B"},
                {"facilityName": "no id"},  # skipped: no facilityId
            ])

        requests = list(self.spider.parse_list(FakeResponse()))
        self.assertEqual(len(requests), 2)
        self.assertEqual(
            requests[0].url,
            "https://findccprovider.health.alaska.gov/api/Provider/10000011",
        )
        # Roster row is carried through for the errback fallback.
        self.assertEqual(requests[0].cb_kwargs["roster"]["facilityId"], 10000011)


if __name__ == "__main__":
    unittest.main()

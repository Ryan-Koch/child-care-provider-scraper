import unittest

from provider_scrape.items import ProviderItem
from provider_scrape.spiders.pennsylvania import (
    PennsylvaniaSpider,
    format_phone,
    join_labels,
)

# A representative getchildcareinfo `provider` record (trimmed to the fields the
# item builder reads), modeled on the live API response for a real provider.
SAMPLE_DETAIL = {
    "providerName": "Gettysburgs Growing Place",
    "addressLine1": "142 Constitution Ave",
    "addressLine2": None,
    "addressLine3": None,
    "city": "Gettysburg",
    "state": "PA",
    "zipCode": 17325,
    "phoneNumber": "7173378211",
    "providerType": "Center",
    "providerMaxCapacity": 138,
    "keystoneStars": "4",
    "referralStatus": "ACT",
    "schoolDistrict": ["Gettysburg Area  - Pick-Up service to/from school"],
    "mealOptions": ["Breakfast", "Lunch", "PM Snack"],
    "generalSchedule": ["School District Holidays", "After School", "Summer Only"],
    "careLevel": [
        {
            "careLevel": "Infant (6 Weeks-12 mos.)",
            "careLevelOpeningStatus": "C",
            "ftRate": "$60.00",
            "ptRate": "$60.00",
        },
        {
            "careLevel": "Preschool (37 mos.- Entering K)",
            "careLevelOpeningStatus": "E",
            "ftRate": "$48.80",
            "ptRate": "$48.80",
        },
    ],
}


class PennsylvaniaBuildItemTest(unittest.TestCase):
    def setUp(self):
        self.item = PennsylvaniaSpider.build_item(SAMPLE_DETAIL)

    def test_returns_provider_item(self):
        self.assertIsInstance(self.item, ProviderItem)
        self.assertEqual(self.item["source_state"], "Pennsylvania")

    def test_basic_fields(self):
        self.assertEqual(self.item["provider_name"], "Gettysburgs Growing Place")
        self.assertEqual(self.item["provider_type"], "Center")
        self.assertEqual(self.item["capacity"], 138)
        self.assertEqual(self.item["pa_stars_rating"], "4")

    def test_address_is_street_city_state_zip(self):
        # Format the normalization pipeline can parse into city/state/zip.
        self.assertEqual(self.item["address"], "142 Constitution Ave, Gettysburg, PA 17325")

    def test_phone_is_formatted(self):
        self.assertEqual(self.item["phone"], "(717) 337-8211")

    def test_certification_active_from_referral_status(self):
        self.assertEqual(self.item["pa_certificate_status"], "Active")

    def test_list_fields_join(self):
        self.assertIn("Breakfast", self.item["pa_meal_options"])
        self.assertIn("Lunch", self.item["pa_meal_options"])
        self.assertIn("After School", self.item["pa_schedule"])
        self.assertIn("Gettysburg Area", self.item["pa_school_district"])

    def test_cost_table_maps_openings_codes(self):
        cost = self.item["pa_cost_table"]
        self.assertEqual(len(cost), 2)
        self.assertEqual(cost[0]["age_group"], "Infant (6 Weeks-12 mos.)")
        self.assertEqual(cost[0]["full_time_rate"], "$60.00")
        self.assertEqual(cost[0]["part_time_rate"], "$60.00")
        self.assertEqual(cost[0]["openings"], "Call for Availability")  # "C"
        self.assertEqual(cost[1]["openings"], "Enrolling")  # "E"


class PennsylvaniaCertificationTest(unittest.TestCase):
    def test_inactive_referral_maps_to_inactive(self):
        item = PennsylvaniaSpider.build_item({**SAMPLE_DETAIL, "referralStatus": "INA"})
        self.assertEqual(item["pa_certificate_status"], "Inactive")

    def test_missing_referral_maps_to_none(self):
        item = PennsylvaniaSpider.build_item({**SAMPLE_DETAIL, "referralStatus": None})
        self.assertIsNone(item["pa_certificate_status"])


class PennsylvaniaHelpersTest(unittest.TestCase):
    def test_format_phone(self):
        self.assertEqual(format_phone("7173378211"), "(717) 337-8211")
        self.assertEqual(format_phone("717-337-8211"), "(717) 337-8211")
        self.assertIsNone(format_phone(None))
        # Non-10-digit input passes through untouched.
        self.assertEqual(format_phone("511"), "511")

    def test_join_labels(self):
        self.assertEqual(join_labels(["A", "B"]), "A, B")
        self.assertEqual(join_labels([]), "")
        self.assertEqual(join_labels(None), "")
        self.assertEqual(join_labels(["  X  ", None, ""]), "X")


if __name__ == "__main__":
    unittest.main()

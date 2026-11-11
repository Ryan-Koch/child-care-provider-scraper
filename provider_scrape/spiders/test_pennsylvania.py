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


class PennsylvaniaSearchPollingTest(unittest.TestCase):
    """The poll/re-POST/give-up state machine for an async county search.

    Regression: polling used to be bounded by a fast-fire poll count (15 polls
    at ~0.1s spacing), so under prod load a slow-to-serve search silently ended
    its county early -- whole counties (or their later pages) went missing and
    the run just "finished" with fewer items.
    """

    def setUp(self):
        from collections import Counter
        from types import SimpleNamespace

        self.stats = Counter()
        stats = SimpleNamespace(
            inc_value=lambda key, count=1: self.stats.update({key: count}),
            get_value=lambda key, default=None: self.stats.get(key, default),
        )
        self.spider = PennsylvaniaSpider()
        self.spider.crawler = SimpleNamespace(stats=stats)

    def _meta(self, **overrides):
        import time

        meta = {
            "county": "02",
            "sid": "sid-1",
            "group": 1,
            "search_id": 0,
            "accum": {},
            "prev_count": -1,
            "poll": 0,
            "attempt": 1,
            "poll_started": time.monotonic(),
        }
        meta.update(overrides)
        return meta

    def _expired(self):
        import time

        from provider_scrape.spiders import pennsylvania

        return time.monotonic() - pennsylvania.POLL_TIMEOUT - 1

    def test_unserved_within_budget_polls_again(self):
        reqs = list(self.spider._handle_unserved(self._meta(poll=40)))
        self.assertEqual(len(reqs), 1)
        self.assertEqual(reqs[0].method, "GET")
        self.assertEqual(reqs[0].meta["poll"], 41)
        self.assertEqual(self.spider.incomplete_counties, {})

    def test_unserved_past_budget_reposts_search(self):
        reqs = list(self.spider._handle_unserved(self._meta(poll_started=self._expired())))
        self.assertEqual(len(reqs), 1)
        self.assertEqual(reqs[0].method, "POST")
        self.assertEqual(reqs[0].meta["attempt"], 2)
        self.assertEqual(reqs[0].meta["group"], 1)
        self.assertEqual(self.spider.incomplete_counties, {})

    def test_unserved_after_last_attempt_is_flagged_incomplete(self):
        from provider_scrape.spiders import pennsylvania

        accum = {("P1", "L1"): {"providerIdentifier": "P1", "providerLocationIdentifier": "L1"}}
        meta = self._meta(
            group=3,
            accum=accum,
            attempt=pennsylvania.MAX_SEARCH_ATTEMPTS,
            poll_started=self._expired(),
        )
        self.spider.pending_counties = []
        reqs = list(self.spider._handle_unserved(meta))
        # What was already collected still gets its detail fetch...
        self.assertEqual([r.url for r in reqs], [pennsylvania.INFO_URL])
        # ...but the county is recorded as incomplete and counted.
        self.assertIn("02", self.spider.incomplete_counties)
        self.assertEqual(self.stats["pennsylvania/county_incomplete"], 1)

    def test_served_growth_paginates_and_resets_attempt(self):
        data = {
            "searchStatusServedByCallBack": True,
            "provider": [
                {"providerIdentifier": "P1", "providerLocationIdentifier": "L1", "searchIdentifier": 99},
                {"providerIdentifier": None},
            ],
        }
        reqs = list(self.spider._handle_served(self._meta(attempt=3), data))
        self.assertEqual(len(reqs), 1)
        self.assertEqual(reqs[0].meta["group"], 2)
        self.assertEqual(reqs[0].meta["attempt"], 1)
        self.assertEqual(reqs[0].meta["search_id"], 99)

    def test_failed_search_request_flags_county(self):
        from scrapy import Request
        from twisted.python.failure import Failure

        try:
            raise TimeoutError("boom")
        except TimeoutError:
            failure = Failure()
        failure.request = Request("https://example.com", meta={"county": "51", "group": 4})
        self.spider.pending_counties = ["52"]
        reqs = list(self.spider.on_error(failure))
        self.assertIn("51", self.spider.incomplete_counties)
        # The dead county frees its search slot for the next queued county.
        self.assertEqual([r.meta["county"] for r in reqs], ["52"])

    def test_closed_reports_incomplete_counties(self):
        self.spider.incomplete_counties["51"] = "group 4 never served"
        with self.assertLogs(self.spider.logger.logger, level="ERROR") as logs:
            self.spider.closed("finished")
        self.assertIn("51: group 4 never served", "\n".join(logs.output))


class PennsylvaniaCountyQueueTest(unittest.TestCase):
    """At most ``search_concurrency`` county searches are in flight at once.

    The server processes searches ~serially, so launching all 67 counties at
    once made the tail of the queue wait minutes per search.
    """

    def test_start_requests_launches_only_the_concurrency_cap(self):
        spider = PennsylvaniaSpider(counties="01,02,03,04,05,06", search_concurrency="2")
        reqs = list(spider.start_requests())
        self.assertEqual([r.meta["county"] for r in reqs], ["01", "02"])
        self.assertEqual(spider.pending_counties, ["03", "04", "05", "06"])

    def test_finished_county_starts_the_next_one(self):
        from provider_scrape.spiders import pennsylvania

        spider = PennsylvaniaSpider(counties="01,02,03", search_concurrency="1")
        list(spider.start_requests())
        meta = {"county": "01", "group": 2, "accum": {("P1", "L1"): {"providerIdentifier": "P1"}}}
        reqs = list(spider._finish_or_paginate(meta, grew=False))
        searches = [r for r in reqs if r.url == pennsylvania.SUMMARY_URL]
        details = [r for r in reqs if r.url == pennsylvania.INFO_URL]
        self.assertEqual([r.meta["county"] for r in searches], ["02"])
        self.assertEqual(len(details), 1)
        self.assertEqual(spider.pending_counties, ["03"])

    def test_paginating_county_keeps_its_slot(self):
        spider = PennsylvaniaSpider(counties="01,02", search_concurrency="1")
        list(spider.start_requests())
        meta = {"county": "01", "group": 1, "accum": {}, "poll": 0, "attempt": 1, "sid": "s", "search_id": 5}
        reqs = list(spider._finish_or_paginate(meta, grew=True))
        self.assertEqual([(r.meta["county"], r.meta["group"]) for r in reqs], [("01", 2)])
        self.assertEqual(spider.pending_counties, ["02"])

    def test_default_concurrency(self):
        from provider_scrape.spiders import pennsylvania

        spider = PennsylvaniaSpider()
        self.assertEqual(len(list(spider.start_requests())), pennsylvania.SEARCH_CONCURRENCY)

import os
import unittest

from scrapy import Selector
from scrapy.http import Request, TextResponse

from provider_scrape.items import ProviderItem
from provider_scrape.spiders.alabama import AlabamaSpider, _extract_br_separated_address

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def _response(name):
    with open(os.path.join(FIXTURES, name), "rb") as fh:
        body = fh.read()
    url = "https://apps.dhr.alabama.gov/daycare/daycare_results.aspx?ID=996"
    return TextResponse(url=url, body=body, encoding="utf-8", request=Request(url))


class AlabamaSpiderTest(unittest.TestCase):
    def setUp(self):
        self.spider = AlabamaSpider()

    def test_parse_results(self):
        html = """
        <html>
        <body>
            <form action="daycare_search" method="post">
            <table id="MainContent_GridView1">
                <tr><th>Header</th></tr>
                <tr>
                    <td>Star 2</td>
                    <td><a href="daycare_results.aspx?ID=128">DENNIS, CONSTANCE <br />GLOBAL LEARNING ACADEMY</a></td>
                    <td>MT. VERNON</td>
                    <td>(251) 635-3595</td>
                </tr>
                <tr>
                    <td>
                        <table>
                            <tr><td><span>1</span></td><td><a href="javascript:__doPostBack('ctl00$MainContent$GridView1','Page$2')">2</a></td></tr>
                        </table>
                    </td>
                </tr>
            </table>
            </form>
        </body>
        </html>
        """
        response = TextResponse(url="https://apps.dhr.alabama.gov/daycare/daycare_search", body=html, encoding="utf-8")

        results = list(self.spider.parse_results(response))

        # Expect 1 Request for detail and 1 Request for next page
        self.assertEqual(len(results), 2)

        # Check detail request
        req1 = results[0]
        self.assertIsInstance(req1, Request)
        self.assertIn("daycare_results.aspx?ID=128", req1.url)

        # Check pagination request
        req2 = results[1]
        self.assertIsInstance(req2, Request)
        # FormRequest is a subclass of Request, checks logic for pagination

    def test_parse_detail(self):
        # bug_09 regression: the mailing/street blocks below reproduce the
        # real page layout (label <span>, then street/city/state/zip <span>s
        # joined only by <br>s with no text of their own between street and
        # city). Before the fix, joining the container's full text ran
        # "1400 BRISBANE AVENUE" and "BIRMINGHAM" together with no separator;
        # these assertions are exact-equality so that regression can't hide
        # behind a substring check again.
        html = """
        <div id="MainContent_Label1">
            <div style='width:100%;clear:both;overflow:hidden;'>
                <div style='width:17%;float:left;margin-right:10px;'><span style='font-size:16px;font-weight:bold;'>Licensee:</span></div>
                <div style='float:left;'><span style='font-size:14px;font-weight:bold;'>JOHNSON, DARRYL </span></div>
            </div>
            <div style='width:100%;clear:both;overflow: hidden;'>
                <div style='width:17%;float:left;margin-right:10px;'><span style='font-size:16px;font-weight:bold;'>Facility:</span></div>
                <div style='float:left;'><span style='font-size:14px;font-weight:bold;'>GLAD TIDINGS DAY CARE</span></div>
            </div>
            <br /><b>Status:</b> Licensed<br />
            <b>JOHNSON, KATRINA M - Director</b><br />
            <b>Phone:</b> (205) 798-1248<br />
            <span style='font-size:12px;font-weight:bold;'>Alabama Quality Star Rating:   </span><span style='font-size:12px;'> &nbsp;&nbsp; 1 Star</span>
            <span style='font-size:12px;font-weight:bold; &nbsp;&nbsp;'>     Rating Expiration Date:    </span><span style='font-size:12px;'>&nbsp;&nbsp;&nbsp;</span>

            <table style='padding:1px 1px 5px 1px;border:1px solid #999999;width:100%;border-collapse:collapse;' border='1'>
                <tr><td width='50%'><b>Daytime Hours:</b> &nbsp;07:00 AM - 06:00 PM</td><td width='50%'><b>Nighttime Hours:</b> &nbsp;N/A - N/A</td></tr>
                <tr><td width='50%'><b>Daytime Ages:</b> &nbsp;2 Years To 14 Years</td><td width='50%'><b>Nighttime Ages:</b> &nbsp;N/A</td></tr>
            </table>

            <span style='font-size:12px;font-weight:bold;'>Mailing Address:</span><br /><span style='font-size:12px;'>1400 BRISBANE AVENUE</span><br /><span style='font-size:12px;'>BIRMINGHAM</span>, <span style='font-size:12px;'>AL</span> <span style='font-size:12px;'>35214</span><br /><br />
            <span style='font-size:12px;font-weight:bold;'>Street Address:</span><br /><span style='font-size:12px;'>1400 BRISBANE AVENUE</span><br /><span style='font-size:12px;'>BIRMINGHAM</span>, <span style='font-size:12px;'>AL</span> <span style='font-size:12px;'>35214</span>
            <br /><br /><a href='http://map.google.com/maps?q=1400+BRISBANE+AVENUE,BIRMINGHAM, AL 35214' target='_blank'>Click for Interactive Map</a>
        </div>

        <table id="MainContent_GridView1"><tr><td>No Accreditations</td></tr></table>
        <table id="MainContent_GridView3"><tr><td>No Adverse Actions</td></tr></table>
        <table id="MainContent_GridView2"><tr><td>No Substantiated Complaints</td></tr></table>
        <table id="MainContent_GridView4"><tr><td>No Evaluation/Deficiency Reports</td></tr></table>
        """

        response = TextResponse(
            url="https://apps.dhr.alabama.gov/daycare/daycare_results?ID=589", body=html, encoding="utf-8"
        )

        results = list(self.spider.parse_detail(response))
        item = results[0]

        self.assertIsInstance(item, ProviderItem)
        self.assertEqual(item["license_holder"], "JOHNSON, DARRYL ")
        self.assertEqual(item["provider_name"], "GLAD TIDINGS DAY CARE")
        self.assertEqual(item["status"], "Licensed")
        self.assertEqual(item["administrator"], "JOHNSON, KATRINA M")
        self.assertEqual(item["phone"], "(205) 798-1248")
        self.assertEqual(item["al_quality_rating"], "1 Star")
        self.assertEqual(item["hours"], "07:00 AM - 06:00 PM")
        self.assertEqual(item["ages_served"], "2 Years To 14 Years")
        self.assertEqual(item["al_mailing_address"], "1400 BRISBANE AVENUE, BIRMINGHAM, AL 35214")
        self.assertEqual(item["address"], "1400 BRISBANE AVENUE, BIRMINGHAM, AL 35214")
        self.assertEqual(item["al_accreditations"], [])

    def test_parse_detail_real_fixture_strips_zip_placeholder_and_joins_address(self):
        # bug_09: captured live from
        # https://apps.dhr.alabama.gov/daycare/daycare_results.aspx?ID=996
        # on 2026-10-15 (see fixtures/al_detail_zip_placeholder.html). This
        # provider has no ZIP+4 on file for its mailing address, which AL
        # renders as the literal placeholder "35023-____".
        response = _response("al_detail_zip_placeholder.html")

        results = list(self.spider.parse_detail(response))
        item = results[0]

        self.assertEqual(item["al_mailing_address"], "7547 15TH STREET ROAD, HUEYTOWN, AL 35023")
        self.assertEqual(item["address"], "2712 31ST STREET ENSLEY, BIRMINGHAM, AL 35228")
        # Sanity: the rest of parse_detail still works against this fixture.
        self.assertEqual(item["status"], "Licensed")
        self.assertEqual(item["phone"], "(205) 701-5437")


class ExtractBrSeparatedAddressTest(unittest.TestCase):
    """Direct unit tests of the address-joining helper (bug_09)."""

    @staticmethod
    def _label(html, label_text):
        sel = Selector(text=html)
        return sel.xpath(f'//span[contains(text(), "{label_text}")]')[0]

    def test_joins_street_and_city_with_separator(self):
        # The street/city spans are separated only by a <br> -- the exact
        # layout that silently ran them together pre-fix.
        html = (
            "<div><span>Mailing Address:</span><br />"
            "<span>1909 CENTRAL PARKWAY SW</span><br />"
            "<span>DECATUR</span>, <span>AL</span> <span>35601</span>"
            "<br /><br /></div>"
        )
        label = self._label(html, "Mailing Address:")
        self.assertEqual(
            _extract_br_separated_address(label),
            "1909 CENTRAL PARKWAY SW, DECATUR, AL 35601",
        )

    def test_strips_zip_plus_four_placeholder(self):
        html = (
            "<div><span>Mailing Address:</span><br />"
            "<span>1909 CENTRAL PARKWAY SW</span><br />"
            "<span>DECATUR</span>, <span>AL</span> <span>35601-____</span>"
            "<br /><br /></div>"
        )
        label = self._label(html, "Mailing Address:")
        self.assertEqual(
            _extract_br_separated_address(label),
            "1909 CENTRAL PARKWAY SW, DECATUR, AL 35601",
        )

    def test_preserves_real_zip_plus_four(self):
        # A real ZIP+4 (digits after the dash, not underscores) must survive.
        html = (
            "<div><span>Mailing Address:</span><br />"
            "<span>P. O. BOX 8782</span><br />"
            "<span>DOTHAN</span>, <span>AL</span> <span>36304-0782</span>"
            "<br /><br /></div>"
        )
        label = self._label(html, "Mailing Address:")
        self.assertEqual(
            _extract_br_separated_address(label),
            "P. O. BOX 8782, DOTHAN, AL 36304-0782",
        )

    def test_blank_address_returns_none(self):
        # bug_09: ~38% of records have no mailing address on file -- all four
        # spans are empty. The old regex produced a bare "," for these; the
        # fix must produce None instead.
        html = (
            "<div><span>Mailing Address:</span><br />"
            "<span></span><br />"
            "<span></span>, <span></span> <span></span>"
            "<br /><br /></div>"
        )
        label = self._label(html, "Mailing Address:")
        self.assertIsNone(_extract_br_separated_address(label))


if __name__ == "__main__":
    unittest.main()

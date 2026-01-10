import unittest
from scrapy.http import TextResponse
from provider_scrape.spiders.new_york import NewYorkSpider
from provider_scrape.items import ProviderItem

class NewYorkSpiderTest(unittest.TestCase):
    def setUp(self):
        self.spider = NewYorkSpider()

    def test_parse(self):
        csv_content = """"Facility ID","Program Type","Region Code","County","Facility Status","Facility Name","Facility Opened Date","License Issue Date","License Expiration Date","Address Omitted","Street Number","Street Name","Additional Address","Floor","Apartment","City","State","Zip Code","Phone Number Omitted","Phone Number","Phone Extension","Provider Name","School District Name","Capacity Description","Infant Capacity","Toddler Capacity","Preschool Capacity","School Age Capacity","Total Capacity","Program Profile","Latitude","Longitude","Georeference"
"39937","DCC","SRO","Oneida","License","Mohawk Valley Community Action Agency, Inc.","07/01/1990","06/01/2024","05/31/2028",,"132","Main St.",,,,"Camden","NY","133161138",,"(315)624-9930","2830","Joanne C. Powers","Camden","21 Preschoolers","0","0","21","0","21","https://hs.ocfs.ny.gov/dcfs/Profile/Index/39937","43.33949","-75.749264","POINT (-75.749264 43.33949)"
"40783","DCC","BRO","Erie","License","Brierwood Child Care Center","03/01/1992","03/01/2024","02/29/2028",,"5540","Southwestern Boulevard",,,,"Hamburg","NY","14075",,"(716)646-0233",,"Nicole M. Rhynes","Frontier","16 Infants, 12 Toddlers, 35 Preschoolers and 21 School-Aged Children","16","12","35","21","84","https://hs.ocfs.ny.gov/dcfs/Profile/Index/40783","42.739818","-78.878955","POINT (-78.878955 42.739818)"
"""

        response = TextResponse(url='https://data.ny.gov/api/v3/views/cb42-qumz/export.csv',
                                body=csv_content,
                                encoding='utf-8')

        results = list(self.spider.parse(response))

        self.assertEqual(len(results), 2)
        
        # Check first item
        item1 = results[0]
        self.assertIsInstance(item1, ProviderItem)
        self.assertEqual(item1['ny_facility_id'], '39937')
        self.assertEqual(item1['provider_name'], 'Mohawk Valley Community Action Agency, Inc.')
        self.assertEqual(item1['address'], '132 Main St., Camden, NY, 133161138')
        self.assertEqual(item1['phone'], '(315)624-9930')
        self.assertEqual(item1['ny_phone_extension'], '2830')
        self.assertEqual(item1['preschool'], '21')
        self.assertEqual(item1['capacity'], '21')
        self.assertEqual(item1['provider_url'], 'https://hs.ocfs.ny.gov/dcfs/Profile/Index/39937')
        self.assertEqual(item1['latitude'], '43.33949')
        self.assertEqual(item1['longitude'], '-75.749264')

        # Check second item
        item2 = results[1]
        self.assertEqual(item2['ny_facility_id'], '40783')
        self.assertEqual(item2['infant'], '16')
        self.assertEqual(item2['toddler'], '12')
        self.assertEqual(item2['preschool'], '35')
        self.assertEqual(item2['school'], '21')
        self.assertEqual(item2['capacity'], '84')
        self.assertEqual(item2['address'], '5540 Southwestern Boulevard, Hamburg, NY, 14075')
        self.assertEqual(item2['latitude'], '42.739818')
        self.assertEqual(item2['longitude'], '-78.878955')

if __name__ == '__main__':
    unittest.main()
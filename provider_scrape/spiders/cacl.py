import scrapy
import csv
from io import StringIO

class CaclSpider(scrapy.Spider):
    name = 'cacl'
    allowed_domains = ['www.ccld.dss.ca.gov']
    start_urls = [
        'https://www.ccld.dss.ca.gov/transparencyapi/api/DownloadStateData?id=ChildCareCenters&GUID=8cdb2366-1db9-4977-bf5a-06ae048b824d',
        'https://www.ccld.dss.ca.gov/transparencyapi/api/DownloadStateData?id=CHILDCAREHOMEmorethan8&GUID=8cdb2366-1db9-4977-bf5a-06ae048b824d'
    ]

    def start_requests(self):
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Referer': 'https://www.ccld.dss.ca.gov/carefacilitysearch/DownloadData',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0',
            'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Microsoft Edge";v="138"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
        }

        for url in self.start_urls:
            yield scrapy.Request(url, headers=headers, callback=self.parse)


    def parse(self, response):
        """
        Parses the CSV response and yields a dictionary for each row.
        """
        # The response body is a string, so we use StringIO to treat it like a file
        csv_file = StringIO(response.text)

        # Use DictReader to easily parse the CSV into dictionaries
        reader = csv.DictReader(csv_file)

        for row in reader:
            yield dict(row)

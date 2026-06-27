# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

from provider_scrape import normalization


class VaScrapePipeline:
    def process_item(self, item, spider):
        return item


class NormalizationPipeline:
    """Normalize each scraped item via the pure helpers in ``normalization``.

    Controlled by the ``NORMALIZE_ENABLED`` setting (default ``True``). When
    disabled the item is passed through untouched, which is how a
    non-normalized run is produced to recover raw values (decision D4).
    """

    def open_spider(self, spider):
        self.enabled = spider.settings.getbool("NORMALIZE_ENABLED", True)
        if not self.enabled:
            spider.logger.info(
                "NormalizationPipeline disabled (NORMALIZE_ENABLED=False); "
                "items pass through untouched."
            )

    def process_item(self, item, spider):
        if not self.enabled:
            return item
        adapter = ItemAdapter(item)
        data = adapter.asdict()
        data = normalization.normalize_item(data, spider.name)
        if data.get("inspections"):
            data["inspections"] = [
                normalization.normalize_inspection(i, spider.name)
                for i in data["inspections"]
            ]
        for key, value in data.items():
            adapter[key] = value
        return item

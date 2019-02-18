# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from pymongo import MongoClient
from brand_spider.items import GoodInfoItem, JDGoodInfoItem
from brand_spider.field_config import JDGoodInfoField as JGif, GoodInfoField as Gif


class BrandSpiderPipeline(object):

    goods_collect_info = 'kaola'
    jd_info_collect = 'jd'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DB', 'items')
        )

    def open_spider(self, spider):
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        if isinstance(item, GoodInfoItem):
            self.db[self.goods_collect_info].insert(dict(item))
        elif isinstance(item, JDGoodInfoItem):
            query = {JGif.SKUID: item[JGif.SKUID]}
            self.db[self.jd_info_collect].update(query, dict(item), upsert=True)
        return item


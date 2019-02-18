# coding: utf-8
# author: zjw

from scrapy.spiders import Spider
from scrapy import Request

from pymongo import MongoClient
from pymongo.errors import AutoReconnect
from datetime import datetime
from datetime import timedelta
from pprint import pprint
import traceback
import logging
import random

from brand_spider.field_config import JDGoodInfoField as GIF


class JDUpdater(Spider):

    name = 'jd_updater'

    def __init__(self, name=None, **kwargs):
        super(JDUpdater, self).__init__(name, **kwargs)
        self.client = MongoClient("10.214.224.142:20000")
        self.db = self.client['ebweb']
        self.col_jd_item = self.db['jd']
        self.update_time = datetime(2017, 6, 15)

    def start_requests(self):

        # for item in self.col_jd_item.find({"name" : {"$regex": "^\s*$"}}, {"_id": 0, GIF.URL: 1},
        #                                   no_cursor_timeout=True):
        #     req = Request(url=item[GIF.URL], dont_filter=True, callback=self.update_good_name)
        #     req.meta[GIF.URL] = item[GIF.URL]
        #     yield req
        for item in self.col_jd_item.find({GIF.UPDATE_TIME: {"$exists": False}}, {"_id": 0, GIF.URL: 1},
                                          no_cursor_timeout=True):
            req = Request(url=item[GIF.URL], dont_filter=True)
            req.meta[GIF.URL] = item[GIF.URL]
            yield req

    def update_good_name(self, response):
        query = {
            GIF.URL: response.meta[GIF.URL]
        }
        value = {
            GIF.NAME: ''.join(response.css('div.sku-name::text').extract()).strip()
        }
        pprint(value)
        pprint(response.meta[GIF.URL])
        try:
            self.col_jd_item.update(query, {"$set": value})
        except AutoReconnect as exp:
            logging.error(exp.message)
            logging.error(traceback.format_exc())
            wait_t = random.randint(5, 20) / 10.0
            logging.info("Pymongo auto reconnecting. Waiting for %.1f seconds", wait_t)
            self.col_jd_item.update(query, {"$set": value})

    def parse(self, response):
        query = {
            GIF.URL: response.meta[GIF.URL]
        }
        value = {
            GIF.DESC: self.get_good_desc(response),
            GIF.SPEC: self.get_good_spec(response),
            GIF.UPDATE_TIME: self.update_time
        }
        pprint(value)
        try:
            self.col_jd_item.update(query, {"$set": value})
        except AutoReconnect as exp:
            logging.error(exp.message)
            logging.error(traceback.format_exc())
            wait_t = random.randint(5, 20) / 10.0
            logging.info("Pymongo auto reconnecting. Waiting for %.1f seconds", wait_t)
            self.col_jd_item.update(query, {"$set": value})

    @staticmethod
    def get_good_desc(response):
        """
        解析商品详情页面的商品描述信息
        :param response: 
        :return: 
        """
        desc = dict()
        for li in response.css('ul.parameter2 li'):
            para = ''.join(li.xpath('.//text()').extract())
            arr = para.split(u'：', 1)
            if len(arr) == 2:
                # mongo does not support dot field
                # such as 'USB 2.0': '*2',  replace '.' with '-' 'USB 2.0' -> 'USB 2_0'
                arr[0] = arr[0].replace('.', '_')
                desc[arr[0]] = arr[1]
            else:
                logging.warning("Parsing product desc meets unexpected condition! URL: " + response.url)
        return desc

    @staticmethod
    def get_good_spec(response):
        """
        解析商品详情页面的规格信息
        :param response: 
            <div class="Ptable-item">
                <h3>特性</h3>
                <dl>
                    <dt>特性</dt> <dd>打破容量和性能界限，推动NVMe时代大众化的新一代SSD</dd>
                    <dt>尺寸</dt> <dd>80.15*22.15*2.38</dd>
                    <dt>工作温度</dt> <dd>0 - 70 ℃ Operating Temperature</dd>
                    <dt>TRIM</dt> <dd>支持</dd>
                </dl>
            </div>
        :return: 
        """
        spec = dict()
        for tb_item in response.css('div.Ptable div.Ptable-item'):
            head = tb_item.xpath('./h3/text()').extract_first()
            dt = tb_item.css('dl dt::text').extract()
            dd = tb_item.css('dl dd::text').extract()
            content = dict()
            for i, key in enumerate(dt):
                key = key.replace('.', '_')
                content[key] = dd[i]
            spec[head] = content
        return spec

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = cls(*args, **kwargs)
        spider._set_crawler(crawler)
        return spider

    def close(self, reason):
        try:
            self.client.close()
        except Exception as err:
            logging.error(err.message)

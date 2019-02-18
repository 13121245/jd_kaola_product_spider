# coding: utf-8
# @Author: zjw

from scrapy.spiders import Spider
from scrapy.spiders import Request
from scrapy.exceptions import CloseSpider

from datetime import datetime
import urlparse
import logging
import traceback
import random

from brand_spider.field_config import JDGoodInfoField as GIF
from brand_spider.items import JDGoodInfoItem as GItem
from brand_spider.price_helper import JDApiPriceTool, ManManBuyPriceTool


class JdSpider(Spider):
    """
    京东上商品信息的爬虫
    """

    name = 'jd_spider'
    start_urls = ['https://www.jd.com/allSort.aspx']

    # price_req_count = 0
    # price_req_index = 0
    # price_url_format = ['http://p.3.cn/prices/get?skuid=J_{0}', 'http://p.3.cn/prices/mgets?skuIds=J_{0}']
    ptIndex = 0
    ptCount = 0
    priceToolList = [ManManBuyPriceTool()]
    jdPriceTool = JDApiPriceTool()

    source = u'京东'

    excluded_cat_list = {u'图书、音像、电子书刊', u'彩票、旅行、充值、票务', u'整车'}

    def parse(self, response):
        """
        解析京东商品的分类页面
        """
        selectors = response.css('.category-items .col .category-item')
        logging.info(u'------------从主页上获取的一级类别数目为：{0}------------'.format(len(selectors)))
        url_count = 0
        for main_cat_sel in selectors:
            # 第一级类别名称
            first_cat = main_cat_sel.css('.mt .item-title span::text').extract_first()
            if first_cat in self.excluded_cat_list:
                continue
            # 找到二级类别名称，以及其下面的三级类别名称列表和对应的页面
            for items_sel in main_cat_sel.css('.mc div.items dl.clearfix'):
                # 二级类别名称
                second_cat = items_sel.css('dt a::text').extract_first()
                # 三级类别名称，技改类别下面商品列表的链接
                for item_sel in items_sel.css('dd a'):
                    url_count += 1
                    third_cat = item_sel.xpath('./text()').extract_first()
                    url = item_sel.xpath('./@href').extract_first()
                    req = Request(url=urlparse.urljoin(response.url, url), callback=self.parse_good_list,
                                  dont_filter=True)
                    req.meta[GIF.CATEGORY] = [first_cat, second_cat, third_cat]
                    yield req
        logging.info(u'------------从主页上获取的三级类别数目为：{0}------------'.format(url_count))

    def parse_good_list(self, response):
        """
        解析三级类别下的商品列表页面
        """
        for g_item in response.css('div#plist li.gl-item div.j-sku-item'):
            item = {
                GIF.SKUID: g_item.xpath('./@data-sku').extract_first(),
                GIF.NAME: g_item.css('div.p-name a em::text').extract_first(),
                GIF.URL: g_item.css('div.p-name a::attr(href)').extract_first(),
                GIF.CATEGORY: response.meta[GIF.CATEGORY]
            }
            item[GIF.URL] = urlparse.urljoin(response.url, item[GIF.URL])
            req = Request(item[GIF.URL], callback=self.parse_good_brand)
            req.meta['item'] = item
            yield req
        # 解析该商品类别页面的下一页
        next_page_url = response.css('#J_bottomPage span.p-num a.pn-next::attr(href)').extract_first()
        if next_page_url:
            req = Request(url=urlparse.urljoin(response.url, next_page_url), callback=self.parse_good_list,
                          dont_filter=True)
            req.meta[GIF.CATEGORY] = response.meta[GIF.CATEGORY]
            yield req

    def parse_good_brand(self, response):
        """
        解析商品的品牌名称
        """
        item = response.meta['item']
        brand = response.css('ul#parameter-brand li[title]::attr(title)').extract_first()
        item[GIF.BRAND] = brand
        # 发送请求, 从价格查询网站中选择一个
        pt_index = self.get_price_tool_index()
        req = Request(url=self.priceToolList[pt_index].get_price_url(item[GIF.URL]),
                      callback=self.parse_good_price)
        req.meta['item'] = item
        req.meta['is_jd_api'] = False
        req.meta['pt_index'] = pt_index
        yield req

    def parse_good_price(self, response):
        """
        解析每个商品的价格
        """
        try:
            pt_index = response.meta['pt_index']
            if response.meta['is_jd_api']:
                price = self.jdPriceTool.get_price_from_response(response)
            else:
                price = self.priceToolList[pt_index].get_price_from_response(response)
            item = response.meta['item']
            item[GIF.PRICE] = price
            item[GIF.UPDATE_TIME] = datetime.utcnow()
            item[GIF.SOURCE] = self.source
            good_item = GItem(item)
            yield good_item
        except Exception as e:
            # 返回的数据格式：[{"id":"J_4426168","p":"23.90","m":"32.01","op":"32.00"}]
            logging.error(u"解析价格错误，链接为： " + response.url)
            logging.error(e.message)
            logging.error(traceback.format_exc())
            if response.meta['is_jd_api']:
                raise CloseSpider(u'解析价格错误， 返回数据为： ' + response.body)
            else:
                # 使用京东API进行查询
                item = response.meta['item']
                req = Request(url=self.jdPriceTool.get_price_url(item[GIF.SKUID]), callback=self.parse_good_price)
                req.meta['item'] = item
                req.meta['is_jd_api'] = True
                yield req

    @classmethod
    def get_price_tool_index(cls):
        """
        返回price tool 的引用
        :return: 
        """
        if cls.ptCount <= 0:
            cls.ptCount = random.randint(2000, 3000)
            cls.ptIndex = (cls.ptIndex + 1) % len(cls.priceToolList)
        cls.ptCount -= 1
        return cls.ptIndex


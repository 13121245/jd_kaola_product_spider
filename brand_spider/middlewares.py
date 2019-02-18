# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from scrapy.downloadermiddlewares.retry import RetryMiddleware
import random
import logging

from brand_spider.user_agent import user_agents
from brand_spider.spiders.jd_spider import JdSpider


class UserAgentMiddleware(object):

    def process_request(self, request, spider):
        agent = random.choice(user_agents)
        request.headers['User-Agent'] = agent
        return None


class RetryPriceMiddleware(RetryMiddleware):

    price_url = 'http://p.3.cn/prices/'
    err_msg = '{"error":"pdos_captcha"}'
    reason = u'请求价格时遇到{"error": "pdos_capcha"}的情况'

    def __init__(self, crawler):
        super(RetryPriceMiddleware, self).__init__(crawler.settings)
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_response(self, request, response, spider):
        """
        处理请求价格时遇到{"error": "pdos_capcha"}的情况
        """
        if spider.name is not JdSpider.name or not response.url.startswith(self.price_url):
            return response
        if self.err_msg in response.body:
            self.crawler.engine.pause()
            logging.warning(u'爬虫暂停，原因：' + self.reason)
            self.crawler.engine.unpause()
            return self._retry(request, self.reason, spider) or response


class BrandSpiderSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)

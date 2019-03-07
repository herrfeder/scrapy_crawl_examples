from scrapy.spiders import CrawlSpider,Rule

import scrapy
from crawl_bw.spiders import libgenparser as gen
from IPython.core.debugger import Tracer
from functools import partial
from scrapy.linkextractors import LinkExtractor

#es_client = Elasticsearch(['http://127.0.0.1:9200'])

#drop_index = es_client.indices.create(index='blog-sysadmins', ignore=400)
#create_index = es_client.indices.delete(index='blog-sysadmins', ignore=[400, 404])


ALLOWED_DOMAINS = ["www.bundeswehr.de"]
START_URLS = ["https://www.bundeswehr.de"]


#"https://www.geopowers.com/",
#"https://www.bendler-blog.de/"
#]
digest_array = []

class CrawlbwSpider(CrawlSpider):
    name = "crawlbw"


    allowed_domains = ALLOWED_DOMAINS
    start_urls = START_URLS
    


    rules = (Rule(LinkExtractor(allow=()),callback="parse_item",follow=True),)


   
    def parse_item(self, response):
        if response.status==200:
            gen.general_parser(response.body,response.url,digest_array) 

        return 1

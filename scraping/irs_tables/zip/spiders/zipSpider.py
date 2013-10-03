from scrapy import log # This module is useful for printing out debug information
from scrapy.spider import BaseSpider

class zipSpider(BaseSpider):
    name = 'http://www.melissadata.com/'
    allowed_domains = ['http://www.melissadata.com/lookups/TaxZip.asp']
    f = open("zips.txt")
    myZips = [ url for url in f.readlines()]
    start_urls = [ 'http://www.melissadata.com/lookups/TaxZip.asp?Zip=' 
    + z.strip() +
    '&submit1=Submit' for z in myZips]
    
    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        items = hxs.select('//tr')
        for item in items:
            
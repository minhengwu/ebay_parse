import scrapy
import json
import boto3
import hashlib

class SmartSpider(scrapy.Spider):
    name = 'ebay'
    url_set = set()
    #ebay search page for cellphones
    start_urls = ['http://www.ebay.com/sch/i.html?_from=R40&_trksid=p2374313.m570.l1313.TR12.TRC2.A0.H0.Xsmartphone.TRS0&_nkw=smartphone&_sacat=9355&_fsrp=1']

    def parse(self, response):
        #parse out all the lising on the page and sent the links to parse_indi
        links = response.xpath('//a[contains(@href, "/itm/")]/@href').extract()
        #if the linked is visited, pass, if not, visit the link and parse out info using parse_indi.
        for link in links:
            url = hashlib.md5(str(link).encode('utf-8')).hexdigest()
            if url in SmartSpider.url_set:
                pass
            else:
                SmartSpider.url_set.add(url)
                yield scrapy.Request(response.urljoin(link), callback=self.parse_indi)
        #parse out the next page link and repeat the process
        next_page = response.xpath('//a[contains(@class, "gspr next")]/@href')
        next_page = response.urljoin(next_page.extract_first())
        yield scrapy.Request(next_page, callback=self.parse)

    def parse_indi(self, response):
        #parse out the condition, price and model, can add more field if needed
        condition = response.css('div[class="u-flL condText  "]::text').extract_first()
        price = response.css('span[itemprop="price"]::text').extract_first()
        model = response.css('h2[itemprop="model"]::text').extract_first()
        output = {'model':model,'condition':condition, 'price':price}
        yield self.file(output)

    def file(self, output):
        #send the output to s3 through kinesis firehose for later usage
        conn = boto3.client('firehose', region_name='us-east-1')
        conn.put_record(DeliveryStreamName = 'ebay',
                           Record ={'Data': json.dumps(output)+'\n'})

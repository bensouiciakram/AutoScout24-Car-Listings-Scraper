"""
AutoScout24 Make/Model URL Pattern Extractor

This script extracts make and model information from AutoScout24 and outputs it to a CSV file. Used for building the makes_and_models.csv input for the main scraper.
"""
import scrapy 
from scrapy.crawler import CrawlerProcess 
from itemloaders.processors import TakeFirst
from scrapy.loader import ItemLoader
from scrapy import Request 


    
class DetailsItem(scrapy.Item):
    """Scrapy Item for make/model details."""
    make_name: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    )
    make_id: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    )
    model: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    )

class ExtractorSpider(scrapy.Spider):
    """
    Scrapy Spider to extract make and model details from AutoScout24.
    """
    name = 'extractor'
    start_urls = [
        'https://www.autoscout24.com/'
    ]
    model_template = 'https://www.autoscout24.com/as24-home/api/taxonomy/cars/makes/{make_id}/models'
        
    def parse(self, response: scrapy.http.Response):
        """
        Parse the homepage to extract make options and request models for each make.
        """
        makes_sels = response.xpath('//select[@id="make"]//option')
        for block in makes_sels : 
            loader = ItemLoader(DetailsItem(),block)
            loader.add_xpath('make_name','./text()')
            loader.add_xpath('make_id','./@value')
            yield Request(
                self.model_template.format(make_id=loader._values['make_id'][0]),
                callback=self.parse_models,
                meta={
                    'loader':loader
                }
            )

    def parse_models(self, response: scrapy.http.Response):
        """
        Parse the models API response and yield make/model items.
        """
        loader = response.meta['loader']
        models_items = response.json()['models']['model']['values']
        for item in models_items :
            loader.replace_value('model',item['name'])
            yield loader.load_item()

process = CrawlerProcess({
    'FEED_URI':'makes_and_models.csv',
    'FEED_FORMAT':'csv',  
})

process.crawl(ExtractorSpider)
process.start() 
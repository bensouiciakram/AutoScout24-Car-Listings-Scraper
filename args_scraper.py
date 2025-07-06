"""
AutoScout24 Car Listings Scraper (Argument-based)

This Scrapy-based script extracts car listing data from AutoScout24 for a specific make/model/page/query combination, supporting command-line arguments for flexible scraping. Outputs data to MongoDB or CSV as configured.
"""
from cgi import print_exception
import scrapy 
from scrapy.crawler import CrawlerProcess 
from itemloaders.processors import TakeFirst
from scrapy.loader import ItemLoader
from scrapy import Request 
import pandas as pd 
from scrapy.shell import inspect_response
from scrapy.utils.response import open_in_browser
from urllib.parse import quote
import requests 
from nested_lookup import nested_lookup
import argparse
    
class DetailsItem(scrapy.Item):
    """Scrapy Item for car listing details."""
    url: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    )
    seller_address: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    )
    seller_contact: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    )
    vat_deductable: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    )
    model: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    make: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    price: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    images: scrapy.Field = scrapy.Field(
        #output_processor=TakeFirst()
    ) 
    mileage: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    gearbox: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    first_registration: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    fuel_type: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    )
    power: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    seller: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    body_type: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    car_type: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    drive_train: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    seats: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    doors: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    general_inspection: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    )
    previous_owner: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    )
    engine_size: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    gears: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    cylinders: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    )
    empty_weight: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    )
    fuel_type: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    )
    fuel_consumption: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    co2_emissions: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    emission_class: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    equipements: scrapy.Field = scrapy.Field()
    color: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    paint: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    upholstery_colour: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    )
    upholstry: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    contact: scrapy.Field = scrapy.Field(
        output_processor=TakeFirst()
    )
    similar_cars: scrapy.Field = scrapy.Field()

class ExtractorSpider(scrapy.Spider):
    """
    Scrapy Spider to extract car listing details from AutoScout24 for a specific make/model/page/query.
    """
    name = 'extractor'
    listing_template = 'https://www.autoscout24.com/lst/{make}/{model}?{query}&page={page}'

    def __init__(self, make: str, model: str, page: int, query: str) -> None:
        """
        Initialize the spider with make, model, page, and query.
        """
        self.make = make 
        self.model = model 
        self.page = page
        self.query = query

    def start_requests(self):
        """
        Start the scraping process for the specified make/model/page/query.
        """
        yield Request(
            self.listing_template.format(
                make=self.make.lower().replace(' ','-'),
                model=self.clean_model(self.model),
                page=self.page,
                query=self.query
            ),
            callback=self.parse_cars,
            meta={
                'make':self.make.lower().replace(' ','-'),
                'model':self.clean_model(self.model)
            },
            dont_filter=True
        )

    def parse_cars(self, response: scrapy.http.Response):
        """
        Parse car listing pages and follow car links.
        """
        yield from response.follow_all(
            response.xpath('//div[contains(@class,"ListItem_header")]/a/@href').getall(),
            callback=self.parse_car,
            meta=response.meta
        )

    def parse_car(self, response: scrapy.http.Response):
        """
        Parse individual car pages and extract details.
        """
        loader = ItemLoader(DetailsItem(),response)
        loader.add_value('url',response.url)
        loader.add_xpath('seller_address','string(//span[contains(text(),"Contact")]/ancestor::div[1]/preceding-sibling::div[1]/div[last()]//a/div)')
        loader.add_xpath('seller_contact','//span[contains(text(),"Contact")]/following-sibling::div/a/text()')
        loader.add_value('vat_deductable',self.get_vat(response))
        loader.add_value('model',response.meta['model']) 
        loader.add_value('make',response.meta['make']) 
        loader.add_xpath('price','//span[contains(text(),"\u20ac")]/text()',re='\d+\,\d+') 
        loader.add_xpath('images','//img/@src') 
        loader.add_xpath('mileage','//div[contains(text(),"Mileage")]/following-sibling::div[text()]/text()') 
        loader.add_xpath('gearbox','//div[contains(text(),"Gearbox")]/following-sibling::div[text()]/text()') 
        loader.add_xpath('first_registration','//div[contains(text(),"First registration")]/following-sibling::div[text()]/text()') 
        loader.add_xpath('fuel_type','//div[contains(text(),"Fuel type")]/following-sibling::div[text()]/text()')
        loader.add_xpath('power','//div[contains(text(),"Power")]/following-sibling::div[text()]/text()') 
        loader.add_xpath('seller','//div[contains(text(),"Seller")]/following-sibling::div[text()]/text()') 
        loader.add_xpath('body_type','string(//span[contains(text(),"Body type")]/../following-sibling::dd)') 
        loader.add_xpath('car_type','string(//span[contains(text(),"Type")]/../following-sibling::dd)') 
        loader.add_xpath('drive_train','string(//span[contains(text(),"Drivetrain")]/../following-sibling::dd)') 
        loader.add_xpath('seats','string(//span[contains(text(),"Seats")]/../following-sibling::dd)') 
        loader.add_xpath('doors','string(//span[contains(text(),"Doors")]/../following-sibling::dd)') 
        loader.add_xpath('general_inspection','string(//span[contains(text(),"General inspection")]/../following-sibling::dd)')
        loader.add_xpath('previous_owner','string(//span[contains(text(),"Previous owner")]/../following-sibling::dd)')
        loader.add_xpath('engine_size','string(//span[contains(text(),"Engine size")]/../following-sibling::dd)') 
        loader.add_xpath('gears','string(//span[contains(text(),"Gears")]/../following-sibling::dd)') 
        loader.add_xpath('cylinders','string(//span[contains(text(),"Cylinders")]/../following-sibling::dd)')
        loader.add_xpath('empty_weight','string(//span[contains(text(),"Empty weight")]/../following-sibling::dd)')
        loader.add_xpath('fuel_type','string(//span[contains(text(),"Fuel type")]/../following-sibling::dd)')
        loader.add_xpath('fuel_consumption','string(//span[contains(text(),"Fuel consumption")]/../following-sibling::dd)') 
        loader.add_xpath('co2_emissions','string(//span[contains(text(),"CO\u2082-emissions")]/../following-sibling::dd)') 
        loader.add_xpath('emission_class','string(//span[contains(text(),"Emission class")]/../following-sibling::dd)') 
        loader.add_value('equipements',self.get_equipements(response))
        loader.add_xpath('color','string(//span[contains(text(),"Colour")]/../following-sibling::dd)') 
        loader.add_xpath('paint','string(//span[contains(text(),"Paint")]/../following-sibling::dd)') 
        loader.add_xpath('upholstery_colour','string(//span[contains(text(),"Upholstery colour")]/../following-sibling::dd)')
        loader.add_xpath('upholstry','string(//span[contains(text(),"Upholstery")]/../following-sibling::dd)') 
        loader.add_xpath('contact','//span[contains(text(),"Contact")]/following-sibling::div/a/text()')
        loader.add_value('similar_cars',self.get_similar_cars(response.url))
        yield loader.load_item()

    def get_total_pages(self,response):
        try :
            return int(response.xpath('//nav[@aria-label="Pagination"]//li[position() = last() -2]//text()').get())
        except TypeError :
            return None

    def clean_model(self, model: str) -> str:
        """
        Clean and format the model string for use in URLs.
        """
        clean_list = {
            ' ': '-',
            '/':'%2F',
        }
        model = model.lower()
        for key,value in clean_list.items() :
            model = model.replace(key,value)
        return model

    def get_similar_cars(self, url: str):
        """
        Placeholder for logic to get similar cars (not implemented).
        """
        return []

    def get_equipements(self, response: scrapy.http.Response):
        """
        Placeholder for logic to get car equipment (not implemented).
        """
        return []

    def get_vat(self, response: scrapy.http.Response):
        """
        Placeholder for logic to get VAT information (not implemented).
        """
        return None

#---------------------------- args logic ---------------------------------------------#
parser = argparse.ArgumentParser(description='choosing the make ,model and page of the car')
parser.add_argument('-m','--make',help='the make of the car')
parser.add_argument('-d','--model',help='the model of the car')
parser.add_argument('-p','--page',help='the page of the cars listing',type=int)
parser.add_argument('-q','--query',help='choose the query criteria')

args = parser.parse_args()


process = CrawlerProcess({
        'FEED_URI':'partial.csv',
        'FEED_FORMAT':'csv', 
        'HTTPCACHE_ENABLED' : True,
        'ITEM_PIPELINES':{
            'pipeline.MongoDBPipeline': 500,
        },
        #'LOG_LEVEL':'ERROR',
        #'MONGODB_USERNAME' : 'user',
        #'MONGODB_PASSWORD' : 'password',
        # 'MONGODB_HOST' : 'localhost',
        # 'MONGODB_PORT' : 27017,
        # 'MONGODB_DATABASE' : 'test',
        # 'MONGODB_COLLECTION' : 'test_collection',
},
)



process.crawl(ExtractorSpider,args.make,args.model,args.page,args.query)
process.start() 
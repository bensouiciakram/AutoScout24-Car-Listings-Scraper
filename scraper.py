"""
AutoScout24 Car Listings Scraper

This Scrapy-based project extracts car listing data from AutoScout24, including details such as make, model, price, seller info, and technical attributes. It supports batch scraping from a CSV of makes and models, and exports data to MongoDB or CSV as configured.
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
    
class DetailsItem(scrapy.Item):
    url = scrapy.Field(
        output_processor=TakeFirst()
    )
    seller_address = scrapy.Field(
        output_processor=TakeFirst()
    )
    vat_deductable = scrapy.Field(
        output_processor=TakeFirst()
    )
    seller_contact = scrapy.Field(
        output_processor=TakeFirst()
    )   
    model = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    make = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    price = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    images = scrapy.Field(
        #output_processor=TakeFirst()
    ) 
    mileage = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    gearbox = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    first_registration = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    fuel_type = scrapy.Field(
        output_processor=TakeFirst()
    )
    power = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    seller = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    body_type = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    car_type = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    drive_train = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    seats = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    doors = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    general_inspection = scrapy.Field(
        output_processor=TakeFirst()
    )
    previous_owner = scrapy.Field(
        output_processor=TakeFirst()
    )
    engine_size = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    gears = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    cylinders = scrapy.Field(
        output_processor=TakeFirst()
    )
    empty_weight = scrapy.Field(
        output_processor=TakeFirst()
    )
    fuel_type = scrapy.Field(
        output_processor=TakeFirst()
    )
    fuel_consumption = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    co2_emissions = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    emission_class = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    # comfort_and_convenience = scrapy.Field(
    #     output_processor=TakeFirst()
    # )
    # entertainement_and_media = scrapy.Field(
    #     output_processor=TakeFirst()
    # ) 
    # safety_and_security = scrapy.Field(
    #     #output_processor=TakeFirst()
    # ) 
    equipements = scrapy.Field(

    )
    color = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    paint = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    upholstery_colour = scrapy.Field(
        output_processor=TakeFirst()
    )
    upholstry = scrapy.Field(
        output_processor=TakeFirst()
    ) 
    contact = scrapy.Field(
        output_processor=TakeFirst()
    )
    similar_cars = scrapy.Field()

class ExtractorSpider(scrapy.Spider):
    name = 'extractor'
    listing_template = 'https://www.autoscout24.com/lst/{make}/{model}?sort=standard&desc=0&cy=A&atype=C&ustate=N%2CU&powertype=kw&priceto=6000&page={page}'

    def __init__(self):
        self.model_and_make_iterator = pd.read_csv('makes_and_models.csv').iterrows()

    def start_requests(self):
        for _,row in self.model_and_make_iterator :
            # if not row['model'] == "city-coupé/city-cabrio":
            #     continue
            yield Request(
                self.listing_template.format(
                    make=row['make_name'].lower().replace(' ','-'),
                    model=self.clean_model(row['model']),
                    page=1
                ),
                callback=self.parse_listing,
                meta={
                    'make':row['make_name'].lower().replace(' ','-'),
                    'model':self.clean_model(row['model'])
                },
                dont_filter=True
            )
    
    def parse_listing(self,response):
        #inspect_response(response,self )
        total_pages = self.get_total_pages(response)
        if not total_pages :
            return
        meta = response.meta
        for page in range(1,total_pages+1):
            meta['page'] = page
            yield Request(
                self.listing_template.format(
                    make=response.meta['make'],
                    model=response.meta['model'],
                    page=page
                ),
                callback=self.parse_cars,
                #meta=response.meta
                meta = meta
            )

    def parse_cars(self,response):
        make = response.meta['make']
        model = response.meta['model']
        page = response.meta['page']
        with open(f'pages/{make}-{model}-{page}.html','w') as file :
            file.write(response.text)
        yield from response.follow_all(
            response.xpath('//div[contains(@class,"ListItem_header")]/a/@href').getall(),
            callback=self.parse_car,
            meta=response.meta
        )

    def parse_car(self,response):
        loader = ItemLoader(DetailsItem(),response)
        loader.add_value('url',response.url)
        loader.add_xpath('seller_address','string(//span[contains(text(),"Contact")]/ancestor::div[1]/preceding-sibling::div[1]/div[last()]//a/div)')
        loader.add_xpath('seller_contact','//span[contains(text(),"Contact")]/following-sibling::div/a/text()')
        loader.add_value('vat_deductable',self.get_vat(response))
        loader.add_value('model',response.meta['model']) 
        loader.add_value('make',response.meta['make']) 
        loader.add_xpath('price','//span[contains(text(),"€")]/text()',re='\d+\,\d+') 
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
        loader.add_xpath('co2_emissions','string(//span[contains(text(),"CO₂-emissions")]/../following-sibling::dd)') 
        loader.add_xpath('emission_class','string(//span[contains(text(),"Emission class")]/../following-sibling::dd)') 
        #loader.add_xpath('comfort_and_convenience','string(//span[contains(text(),"Comfort & Convenience")]/../following-sibling::dd)')
        #loader.add_xpath('entertainement_and_media','string(//span[contains(text(),"Entertainment & Media")]/../following-sibling::dd)') 
        #loader.add_xpath('safety_and_security','//span[contains(text(),"Safety & Security")]/../following-sibling::dd//text()') 
        loader.add_value('equipements',self.get_equipements(response))
        loader.add_xpath('color','string(//span[contains(text(),"Colour")]/../following-sibling::dd)') 
        loader.add_xpath('paint','string(//span[contains(text(),"Paint")]/../following-sibling::dd)') 
        loader.add_xpath('upholstery_colour','string(//span[contains(text(),"Upholstery colour")]/../following-sibling::dd)')
        loader.add_xpath('upholstry','string((//span[contains(text(),"Upholstery")])[2]/../following-sibling::dd)') 
        loader.add_xpath('contact','(//span[contains(text(),"Contact")]/following-sibling::div//text())[last()]')
        loader.add_value('similar_cars',self.get_similar_cars(response.url))
        yield loader.load_item()

    def get_total_pages(self,response):
        try :
            return int(response.xpath('//nav[@aria-label="Pagination"]//li[position() = last() -2]//text()').get())
        except TypeError :
            return None

    def clean_model(self,model):
        clean_list = {
            ' ': '-',
            '/':'%2F',
        }
        model = model.lower()
        for key,value in clean_list.items() :
            model = model.replace(key,value)
        return model

    def get_similar_cars(self,url):
        json_data = {
            'query': '\n  \n  \n  fragment ListingFields on Listing {\n    id\n    ocsInfo {\n      __typename\n      ... on OcsInfoResponse {\n        as24OriginalListingId\n      }\n    }\n    details {\n      webPage\n      prices {\n        public {\n          amountInEUR {\n            formatted\n            raw\n          }\n          evaluation {\n            category\n          }\n          taxDeductible\n        }\n        dealer {\n          amountInEUR {\n            formatted\n          }\n          evaluation {\n            category\n          }\n          taxDeductible\n        }\n      }\n      statistics {\n        leadsRange\n      }\n      media {\n        youtubeLink\n        images(with360Images: false, first: 1) {\n          __typename\n          ... on StandardImage {\n            formats {\n              webp {\n                size420x315\n              }\n            }\n            imageFeatures {\n              isPlaceholder\n            }\n          }\n        }\n      }\n      vehicle {\n        classification {\n          make {\n            formatted\n          }\n          model {\n            formatted\n          }\n          modelVersionInput\n          type\n        }\n        condition {\n          mileageInKm {\n            raw\n            formatted\n          }\n          numberOfPreviousOwnersExtended {\n            formatted\n          }\n          firstRegistrationDate {\n            raw\n            formatted\n          }\n        }\n        numberOfBeds {\n          formatted\n        }\n        numberOfAxles {\n          formatted\n        }\n        bodyType {\n          formatted\n        }\n        grossVehicleWeight {\n          formatted\n        }\n        engine {\n          power {\n            kw {\n              formatted\n            }\n            hp {\n              formatted\n            }\n          }\n          transmissionType {\n            formatted\n          }\n        }\n        fuels {\n          primary {\n            type {\n              raw\n              formatted\n            }\n            consumption {\n              combined {\n                raw\n                formatted\n              }\n            }\n            co2emissionInGramPerKm {\n              raw\n              formatted\n            }\n          }\n          additional {\n            type {\n              raw\n              formatted\n            }\n            consumption {\n              combined {\n                raw\n                formatted\n              }\n            }\n            co2emissionInGramPerKm {\n              raw\n              formatted\n            }\n          }\n          fuelCategory {\n            raw\n            formatted\n          }\n          allFuelTypes {\n            raw\n            formatted\n          }\n        }\n        legalCategories {\n          formatted\n        }\n      }\n      seller {\n        id\n        type\n      }\n      location {\n        city\n        zip\n        countryCode\n      }\n    }\n  }\n\n  fragment RecommendationListingsFields on RecommendedListing {\n    score\n    listing {\n      ...ListingFields\n    }\n  }\n\n  query FetchRecommendations($guid: String!, $locale: Locale_!) {\n    search {\n      listing(guid: $guid, locale: $locale) {\n        id\n        recommendations {\n          items(limit: 20) {\n            ...RecommendationListingsFields\n          }\n        }\n      }\n    }\n  }\n',
            'variables': {
                'guid': '-'.join(url.split('-')[-5:]),
                'locale': 'en_GB',
            },
            'operationName': 'FetchRecommendations',
        }
        headers = {
            'authority': 'listing-search.api.autoscout24.com',
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'authorization': 'Basic YXMyNC1zZWFyY2gtZnVubmVsOnZucmZiYkJqSTMyT2wxV2thNnVOSFJwM0VZbjRkag==',
            # Already added when you pass json=
            # 'content-type': 'application/json',
            'origin': 'https://www.autoscout24.com',
            'referer': 'https://www.autoscout24.com/',
            'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="102", "Google Chrome";v="102"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
        }
        response = requests.post('https://listing-search.api.autoscout24.com/graphql', headers=headers, json=json_data)
        return nested_lookup('items',response.json())

    def get_equipements(self,response):
        return {
            'Comfort & Convenience': response.xpath('//span[contains(text(),"Comfort & Convenience")]/../following-sibling::dd[1]//text()').getall(),
            'Entertainment & Media':response.xpath('//span[contains(text(),"Entertainment & Media")]/../following-sibling::dd[1]//text()').getall(),
            'Safety & Security':response.xpath('//span[contains(text(),"Safety & Security")]/../following-sibling::dd[1]//text()').getall(),
            'Extras':response.xpath('//span[contains(text(),"Extras")]/../following-sibling::dd[1]//text()').getall()
            }

    def get_vat(self,response):
        return bool(response.xpath('//span[contains(text(),"€")]/text()/following-sibling::sup/text()').get())


process = CrawlerProcess({
        'FEED_URI':'output.csv',
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
})



process.crawl(ExtractorSpider)
process.start() 
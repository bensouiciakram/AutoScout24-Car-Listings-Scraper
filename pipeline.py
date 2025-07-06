"""
MongoDB Pipeline for AutoScout24 Car Listings

Exports scraped car listing data to a MongoDB collection. Used as a Scrapy pipeline.
"""
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import os, os.path
from scrapy.pipelines.images import ImagesPipeline
from scrapy.exceptions import DropItem
from datetime import datetime
import pathlib
import shutil
import pymongo

class MongoDBPipeline(object):
    """
    Pipeline for exporting car listing data to MongoDB.
    """
    collection_name = 'cars_items'

    def __init__(self, mongo_uri: str) -> None:
        """
        Initialize the pipeline with the MongoDB URI.
        """
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler) -> 'MongoDBPipeline':
        """
        Create the pipeline from Scrapy crawler settings.
        """
        return cls(
            # mongo_uri=crawler.settings.get('MONGO_URI'), 
            mongo_uri= "mongodb://localhost:27017/test"
            #mongo_uri= "mongodb+srv://doadmin:16XycPA5aJV80943@mongodb-neat-scraper-18864cf8.mongo.ondigitalocean.com/admin"
        )    

    def open_spider(self, spider) -> None:
        """
        Open the MongoDB connection when the spider starts.
        """
        self.client = pymongo.MongoClient(
            self.mongo_uri,
            )
        self.db = self.client['test']

    def close_spider(self, spider) -> None:
        """
        Close the MongoDB connection when the spider finishes.
        """
        self.client.close()

    def process_item(self, item: dict, spider) -> dict:
        """
        Insert the item into the MongoDB collection.
        """
        self.db[self.collection_name].insert_one(dict(item))
        return item    
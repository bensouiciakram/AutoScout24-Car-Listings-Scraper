# AutoScout24 Car Listings Scraper

## Overview
This project is a web scraping toolkit built with Scrapy for extracting car listing data from AutoScout24 (https://www.autoscout24.com/). It supports batch scraping from a CSV of makes and models, as well as argument-based scraping for specific queries. Data can be exported to MongoDB or CSV as configured.

## Features
- Batch scraping of car listings by make/model from CSV
- Command-line argument support for targeted scraping
- Extracts detailed car info: make, model, price, seller, technical specs, images, and more
- Outputs data to MongoDB or CSV

## Requirements
- Python 3.7+
- Scrapy
- pandas
- pymongo
- nested-lookup

Install dependencies:
```bash
pip install scrapy pandas pymongo nested-lookup
```

## Setup & Usage
1. Place the project files in a directory.
2. Prepare `makes_and_models.csv` with the desired makes and models.
3. Run the batch scraper:
   ```bash
   python scraper.py
   ```
   Or run the argument-based scraper:
   ```bash
   python args_scraper.py --make "BMW" --model "320i" --page 1 --query "sort=standard"
   ```
4. Data will be exported to MongoDB or `output.csv` as configured.

## Output
- `output.csv`: Contains car listing details (if CSV output is enabled).
- MongoDB: Car listing data is stored in the configured collection.

## Notes
- The pipeline is defined in `pipeline.py` and is referenced in the Scrapy settings.
- The `url_pattern.py` script can be used to generate the makes_and_models.csv file. 
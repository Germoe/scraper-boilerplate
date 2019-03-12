# -*- coding: utf-8 -*-
import click
import logging
import pprint
import json
import os
import time
import random
import re
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
# Scraping and IP rotation
import requests as req
from lxml.html import fromstring
from itertools import cycle
import traceback
# Pandas
import pandas as pd
# Headless Scraper
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# Import the Scrape Function defined in scraper.py
from scraper import walmart_scraper
from models import ZipcodeScraper

## ----------------------- Utils ------------------------

def print_progress(counter, zip_code, total, interval=500):
    # Prints a progress statement for every 500 records that were processed
    if counter % interval == 0:
        print("Progress: {} \n ID: {} \n Records: {}".format(counter,zip_code,total))

# Set indentation level of pretty printer
pp = pprint.PrettyPrinter(indent=2)

@click.command()
@click.argument('target', type=str) # Target is the identifier for the scraped url, destination directory and name (e.g. walmart)
@click.argument('scrapetype', type=str) # Scraper Type defines the iteration unit or type of scraper that will be used (e.g. zipcode)
@click.option('--ip_territory',default=None,type=str)
@click.option('--ip_port',default=None,type=str) # This option is not tied to any action
@click.option('--scrape_speed',default='regular',type=str)
@click.option('--force',is_flag=True)
def main(target, scrapetype, ip_territory, ip_port, scrape_speed, force=False):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    if scrapetype == 'zipcode':
        scraper = ZipcodeScraper(target)
        radius = 100 # Set Scrape Radius
        scraper.set_radius(radius)
    # Read in proxies
    if ip_territory:
        ip_path = './data/proxies/proxies_' + ip_territory + '.csv'
    else:              
        ip_path = './data/proxies/proxies.csv'

    scrape_limit = scraper.init_proxies(ip_path, force)
    scraper.set_speed(scrape_speed)
    print('Scrape Limit: {} units'.format(scrape_limit))
    if scrapetype == 'zipcode':
        # Read in Zip Codes. Zipcodes csv need to have columns = ['zip','lat','lng','type']
        if radius:
            zip_codes_file = './data/zip_codes/zipcodes_' + str(radius) + '.csv'
        else:
            zip_codes_file = './data/zip_codes/zipcodes_100.csv'
        scraper.init_zipcodes(zip_codes_file)
        scraper.init_scraper(walmart_scraper)
        scraper.scrape()
    print('done')


    # -------- Paul Mitchell ------
    # session = req.Session()

    # # HEAD requests ask for *just* the headers, which is all you need to grab the
    # # session cookie
    # # NOTE: BEFORE SETTING THIS LIFE MAKE SURE TO ROTATE PROXIES
    # session.head('https://locator.paulmitchell.com/SalonLocator/')

    # response = session.post(
    #     url='https://locator.paulmitchell.com/SalonLocator/generateXML.php',
    #     data={
    #         'lat': 42.92,
    #         'lng': -78.88,
    #         'radius': 25
    #     },
    #     headers={
    #         'Referer': 'https://locator.paulmitchell.com/SalonLocator/locator.php?zip=14222'
    #     },
    #     proxies=proxies,
    #     timeout=10
    # )

    # pp.pprint(json.loads(response.text))

    # ------- Redken --------
    # HEAD requests ask for *just* the headers, which is all you need to grab the
    # session cookie
    # session.head('https://www.redken.com/salon-finder')

    # response = session.post(
    #     url='https://storelocator.api.lorealebusiness.com/api/SalonFinderservice/GetSalonFinderstores',
    #     data={
    #         'radius': 5,
    #         'storesperpage': 5,
    #         'pagenum': 1,
    #         'latitude': 29.9339046,
    #         'longitude': -90.03053899999998,
    #         'brand': 'Redken',
    #         'Nametype': 'N',
    #         'IsCurrentloc': False
    #     },
    #     headers={
    #         'Referer': 'https://www.redken.com/salon-finder?search=20010'
    #     }
    # )

    # pp.pprint(json.loads(response.text))

    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()

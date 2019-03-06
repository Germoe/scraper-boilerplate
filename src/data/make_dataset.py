# -*- coding: utf-8 -*-
import click
import logging
import pprint
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
# Scraping and IP rotation
import requests as req
from lxml.html import fromstring
from itertools import cycle
import traceback
# USzipcodes with geographic location
import zipcode
from uszipcode import SearchEngine, Zipcode
# Pandas
import pandas as pd
# Headless Scraper
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

## ----------------------- Utils ------------------------

def print_progress(counter, zipcode, total, interval=500):
    # Prints a progress statement for every 500 records that were processed
    if counter % interval == 0:
        print("Progress: {} \n ID: {} \n Records: {}".format(counter,zipcode,total))

# Set indentation level of pretty printer
pp = pprint.PrettyPrinter(indent=2)


@click.command()
# @click.argument('input_filepath', type=click.Path(exists=True))
# @click.argument('output_filepath', type=click.Path())
def main(input_filepath=None, output_filepath='./data/'):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """

    # Read in proxies
    ip_territory = 'US'
    if ip_territory:
        path = './data/interim/proxies_' + ip_territory + '.csv'
    else:              
        path = './data/interim/proxies.csv'
    proxies = pd.read_csv(path, sep='\t')['ip'].values.tolist()
    proxy_pool = cycle(proxies)
    # Read in Zip Codes
    radius = 100
    if radius:
        path = './data/interim/zipcodes_' + str(rad) + '.csv'
    else:
        path = './data/interim/zipcodes_100.csv'
    zip_codes = pd.read_csv()
    url = 'https://httpbin.org/ip'
    for i in range(1,len(proxies)):
        # Get a proxy from the pool
        proxy = next(proxy_pool)
        print("Request #%d"%i)
        try:
            response = req.get(url,proxies={"http": proxy, "https": proxy},timeout=10)
            print(response.json())
        except:
            '''
                Most free proxies will often get connection errors. You will have retry the entire request using another proxy to work.
                We will just skip retries as its beyond the scope of this tutorial and we are only downloading a single url
            '''
            print("Skipping. Connnection error")
    


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

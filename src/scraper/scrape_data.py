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
from scraper import scrape_func

from models import ZipcodeScraper

## ----------------------- Utils ------------------------

def print_progress(counter, zip_code, total, interval=500):
    # Prints a progress statement for every 500 records that were processed
    if counter % interval == 0:
        print("Progress: {} \n ID: {} \n Records: {}".format(counter,zip_code,total))

# Set indentation level of pretty printer
pp = pprint.PrettyPrinter(indent=2)

def check_proxy(proxy, proxy_pool, num_proxies, timeout, counter=0):
    '''
        Make sure to input proxies in the following format
        proxies = {
            "http": 'http://104.199.166.104:8800',
            "https": 'http://104.199.166.104:8800'
        }
    '''
    proxies_settings = {"http": proxy, "https": proxy}
    url = 'https://httpbin.org/ip'
    try:
        response = req.get(url,proxies=proxies_settings, timeout=timeout)
        print(response.json())
        return proxies_settings, timeout
    except:
        print('proxy unresponsive trying next')
        counter += 1
        if counter >= num_proxies:
            print('Timeout extended as Proxies are slow to respond.')
            timeout += 15
            counter = 0
        return check_proxy(proxy=next(proxy_pool), proxy_pool=proxy_pool, num_proxies=num_proxies, counter=counter, timeout=timeout)

@click.command()
@click.option('--ip_territory',default=None,type=str)
@click.option('--ip_port',default=None,type=str) # This option is not tied to any action
@click.option('--scrape_speed',default='regular',type=str)
@click.option('--force',is_flag=True)
def main(ip_territory, ip_port, scrape_speed, force=False):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    target = 'walmart' # Set Target
    scrape_type = 'zipcode' # Set Scraper Type
    if scrape_type == 'zipcode':
        scraper = ZipcodeScraper()
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
    if scrape_type == 'zipcode':
        # Read in Zip Codes. Zipcodes csv need to have columns = ['zip','lat','lng','type']
        if radius:
            zip_codes_path = './data/zip_codes/zipcodes_' + str(radius) + '.csv'
        else:
            zip_codes_path = './data/zip_codes/zipcodes_100.csv'
        scraper.init_zipcodes(zip_codes_path)
        # Shuffle the zip codes to lower probability of pattern recognition
        zip_codes_shuffled = zip_codes.sample(frac=1)
        counter = 0
        timeout = 3
        for row in zip_codes_shuffled.iterrows():
            row_values = row[1]
            zip_code = row_values['zip']
            print("Request for zip code {}".format(zip_code))
            num_retries = 0
            max_retries = 3
            for retry in range(0,max_retries):
                # Get a proxy from the pool
                proxy = next(proxy_pool)
                print("with proxy {}".format(proxy))
                proxies_settings, timeout = check_proxy(proxy=proxy, proxy_pool=proxy_pool, num_proxies=len(proxies), timeout=timeout)
                try:
                    scrape_timeout = timeout + 5
                    path = dir_path + '/' + target + '_' + zip_code + '.csv'
                    success = scrape_func(zip_code, path=path, radius=radius, proxies=proxies_settings, timeout=scrape_timeout)
                    break
                except req.exceptions.ConnectionError:
                    print("xxxxxxxxxxxx  Connection refused  xxxxxxxxxxxx")
                    seconds = getRandomArbitrary(min_wait_failed,max_wait_failed)
                    print('wait for {}'.format(seconds))
                    time.sleep(seconds)
                    num_retries += 1
                    print("Retry #{}".format(num_retries))
                except:
                    '''
                        Most free proxies will often get connection errors. You will have retry the entire request using another proxy to work.
                        We will just skip retries as its beyond the scope of this tutorial and we are only downloading a single url
                    '''
                    print(traceback.format_exc())
                    seconds = getRandomArbitrary(min_wait_failed,max_wait_failed)
                    print('Failed: wait for {}'.format(seconds))
                    time.sleep(seconds)
                    num_retries += 1
                    print("Retry #{}".format(num_retries))
            # wait until you get next Zip Code
            if counter > scrape_limit:
                break
            else:
                counter += 1
            if success:
                seconds = getRandomArbitrary(min_wait,max_wait)
                print('Success: Wait for {}'.format(seconds))
                time.sleep(seconds)
            else:
                print('No Success: Continue to next with no wait.')
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

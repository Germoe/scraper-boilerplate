# -*- coding: utf-8 -*-
import click
import logging
import pprint
import json
import os
import time
import random
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

def print_progress(counter, zip_code, total, interval=500):
    # Prints a progress statement for every 500 records that were processed
    if counter % interval == 0:
        print("Progress: {} \n ID: {} \n Records: {}".format(counter,zip_code,total))

def getRandomArbitrary(min, max):
  return random.random() * (max - min) + min

# Set indentation level of pretty printer
pp = pprint.PrettyPrinter(indent=2)

def check_proxy(proxy, proxy_pool, timeout=1):
    '''
        Make sure to input proxies in the following format
        proxies = {
            "http": 'http://104.199.166.104:8800',
            "https": 'http://104.199.166.104:8800'
        }
    '''
    proxies = {"http": proxy, "https": proxy}
    url = 'https://httpbin.org/ip'
    try:
        response = req.get(url,proxies=proxies, timeout=timeout)
        print(response.json())
        return proxies
    except:
        print('proxy broken')
        check_proxy(next(proxy_pool), proxy_pool)

def scrape_walmart_stores(zip_code, proxies, radius=100, method='GET'):
    stores = []
    # Create dir if not exists
    dir_path = './data/interim/walmart'
    try:
        Path(dir_path).mkdir(parents=True)
    except:
        pass
    path = dir_path + '/walmart_' + zip_code + '.csv'
    this = Path(path)
    if this.is_file():
        print('this zip code was already scraped')
        return stores
    if method == 'GET':
        # Make sure the headers are still correct
        url="https://www.walmart.com/store/finder/electrode/api/stores?singleLineAddr={}&distance={}".format(zip_code, radius)
        headers={ 'accept':'*/*',
                'accept-encoding':'gzip, deflate, br',
                'accept-language':'en-GB,en;q=0.9,en-US;q=0.8,ml;q=0.7',
                'cache-control':'max-age=0, no-cache, no-store',
                'upgrade-insecure-requests':'1',
                'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36'
        }
        get_store = req.get(url, 
            headers=headers, 
            proxies=proxies, 
            verify=False,
            timeout=3
        )
        store_response = get_store.json()
        stores_data = store_response.get('payload',{}).get("storesData",{}).get("stores",[])
        if not stores_data:
            print('no stores found near %s'%(zip_code))
            stores = [{
                    'name':'',
                    'distance':'',
                    'address':'',
                    'zip_code':'',
                    'city':'',
                    'store_id':'',
                    'phone':'',
            }]
        else:
            print('processing store details')
            #iterating through all stores
            for store in stores_data:
                store_id = store.get('id')
                display_name = store.get('displayName')
                address = store.get('address').get('address')
                postal_code = store.get('address').get('postalCode')
                city = store.get('address').get('city')
                phone = store.get('phone')
                distance = store.get('distance')

                data = {
                        'name':display_name,
                        'distance':distance,
                        'address':address,
                        'zip_code':postal_code,
                        'city':city,
                        'store_id':store_id,
                        'phone':phone,
                }
                stores.append(data)
        stores = pd.DataFrame(stores)
        stores.to_csv(path, sep='\t', encoding='utf-8')
        return stores

@click.command()
# @click.argument('input_filepath', type=click.Path(exists=True))
# @click.argument('output_filepath', type=click.Path())
def main(input_filepath=None, output_filepath='./data/'):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    # Set a limit of scrapes - Batching
    scrape_limit = int(getRandomArbitrary(100,500))

    # Read in proxies
    ip_territory = 'US'
    if ip_territory:
        ip_path = './data/interim/proxies_' + ip_territory + '.csv'
    else:              
        ip_path = './data/interim/proxies.csv'
    proxies = pd.read_csv(ip_path, sep='\t')['ip'].values.tolist()
    random.shuffle(proxies)
    proxy_pool = cycle(proxies)
    # Read in Zip Codes
    radius = 100
    if radius:
        zip_codes_path = './data/interim/zipcodes_' + str(radius) + '.csv'
    else:
        zip_codes_path = './data/interim/zipcodes_100.csv'
    zip_codes = pd.read_csv(zip_codes_path,
                            dtype={'zip': object, 'lat': float, 'lng': float, 'type': object})
    # Shuffle the zip codes to lower probability of pattern recognition
    zip_codes_shuffled = zip_codes.sample(frac=1)
    counter = 0
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
            proxies_settings = check_proxy(proxy, proxy_pool)
            try:
                scrape_walmart_stores(zip_code, radius=radius, proxies=proxies_settings, method='GET')
                break
            except req.exceptions.ConnectionError:
                print("xxxxxxxxxxxx  Connection refused  xxxxxxxxxxxx")
                seconds = getRandomArbitrary(10,30)
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
                seconds = getRandomArbitrary(3,20)
                print('wait for {}'.format(seconds))
                time.sleep(seconds)
                num_retries += 1
                print("Retry #{}".format(num_retries))
        # wait until you get next Zip Code
        if counter > scrape_limit:
            break
        else:
            counter += 1
        time.sleep(getRandomArbitrary(1,10))
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

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

# Set a special ip and port for scraping of proxies
ip_port = None

def scrape_proxies(url, xpath_tbody_tr, xpath_scrape_condition, xpath_ip, xpath_port, xpath_next_disable_condition=None, xpath_next_a=None):
    # create Proxy Set
    proxies = set()
    # initialize webdriver
    options = Options()
    if ip_port:
        # Checks if a specific ip and port were set to fetch proxies otherwise uses computer credentials
        options.add_argument('--proxy-server=' + ip_port)
    driver = webdriver.Chrome(chrome_options=options)
    driver.get(url)
    try:
        next_page = True
        counter = 0
        while next_page:
            element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, xpath_tbody_tr))
            )
            for i in element.find_elements_by_xpath(xpath_tbody_tr):
                # condition to scrape
                try:
                    i.find_element_by_xpath(xpath_scrape_condition)
                except:
                    continue
                #Grabbing IP and corresponding PORT
                proxy = ":".join([i.find_element_by_xpath(xpath_ip).text, i.find_element_by_xpath(xpath_port).text])
                print(proxy)
                proxies.add(proxy)
            try:
                if element.find_element_by_xpath(xpath_next_disable_condition):
                    next_page = True
            except:
                next_page = False
            print(next_page)
            if next_page:
                if driver and element:
                    print('element exists')
                    driver.find_element_by_xpath(xpath_next_a).click()
                else:
                    print('element does not exist')
                print(element)
    finally:
        driver.quit()
    return proxies

# Utility for Random Proxies
def get_proxies(territory=None):
    proxies = set()
    # free-proxy-list
    if not territory:
        url = 'https://free-proxy-list.net/'
    elif territory == 'US':
        url = 'https://www.us-proxy.org/'
    proxies_free_proxy_set = scrape_proxies(url, xpath_tbody_tr='//tbody/tr', xpath_scrape_condition='.//td[7][contains(text(),"yes")]', xpath_ip='.//td[1]', xpath_port='.//td[2]', xpath_next_disable_condition="//li[@id='proxylisttable_next'][not(contains(@class, 'disabled'))]", xpath_next_a="//li[@id='proxylisttable_next']/a")
    proxies.update(proxies_free_proxy_set)
    # proxynove.com
    if not territory:
        url = 'https://www.proxynova.com/proxy-server-list/'
        proxies_hidemyname = scrape_proxies(url, xpath_tbody_tr='//tbody/tr', xpath_scrape_condition='.//td[7]/span[contains(text(),"Elite")]', xpath_ip='.//td[1]', xpath_port='.//td[2]')
        proxies.update(proxies_hidemyname)

    # proxy.rudnkh.me/txt
    if not territory:
        url = 'https://proxy.rudnkh.me/txt'
        response = req.get(url)
        proxies_rudnkh = set(response.text.split('\n'))
        proxies.update(proxies_rudnkh)

    # remove empty values
    proxies.discard('')
    print(proxies)

    return proxies


@click.command()
# @click.argument('input_filepath', type=click.Path(exists=True))
# @click.argument('output_filepath', type=click.Path())
def main(input_filepath=None, output_filepath='./data/'):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """

    # Round Robin proxy rotation
    territory = 'US'
    if territory:
        path = './data/interim/proxies_' + territory + '.csv'
    else:
        path = './data/interim/proxies.csv'
    # Check date of csv creation or modification
    try:
        new_proxies = datetime.fromtimestamp(os.stat(path)[8]) > datetime.now() - timedelta(minutes=30)
    except:
        new_proxies = False
    if not new_proxies:
        proxies = get_proxies(territory=territory)
        proxy_df = pd.DataFrame(list(proxies), columns=['ip'])
        proxy_df.to_csv(path, sep='\t', encoding='utf-8')

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

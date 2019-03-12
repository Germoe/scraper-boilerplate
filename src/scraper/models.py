import pandas as pd
from uszipcode import SearchEngine, Zipcode
import random
from itertools import cycle
import os
import re
import requests as req
import traceback
import time
from pathlib import Path

# ------ Utils ------

def getRandomArbitrary(min, max):
  return random.random() * (max - min) + min

# ------ Zipcode Scraper Classes ------

class Zipcodes():
    #constructor
    def __init__(self, zipcodes):
        if isinstance(zipcodes, list):
            if len(zipcodes) <= 0:
                raise ValueError("arg 'zipcodes' must contain at least one item")
            # Take Zipcode Objects and return a dataframe with unique zipcodes and columns for lat and lng
            self.zipcodes = pd.DataFrame([{ 'zip': i.zipcode, 'type': i.zipcode_type, 'lat': i.lat, 'lng': i.lng } for i in zipcodes]) \
                                .drop_duplicates(subset='zip', keep='first') \
                                .dropna()
            # Only allow Zipcode Type Standard as UNIQUE, PO BOX and MILITARY aren't relevant to this scraper and 
            # the uszipcodes package doesn't list non-standard zipcodes with the by_coordinates method.
            self.zipcodes = self.zipcodes[self.zipcodes['type'] == 'STANDARD']
        else:
            raise ValueError("arg 'zipcodes' must be of type list")

    def _in_radius(self, row, rad):
        z = row
        try:
            search = SearchEngine(simple_zipcode=True)
        except NameError as error:
            # Output expected ImportErrors.
            print('NameError: {} module from {} package not found'.format('SearchEngine', 'uszipcode'))
        except Exception as exception:
            # Output unexpected Exceptions.
            print('Exception: {}'.format(exception))
        zipcodes = [i.zipcode for i in search.by_coordinates(lat=z.loc['lat'], lng=z.loc['lng'], radius=rad, zipcode_type='Standard', returns=100000)]
        zipcodes = set(zipcodes)
        try:
            zipcodes.remove(z.loc['zip'])
            other = zipcodes
            unique = z
        except:
            # If the zip code is of type STANDARD they'll be covered by a different zip code. 
            # The output provides indicators to the population of the zip code in question.
            faulty_zip = search.by_zipcode(z.zip)
            if faulty_zip.population or faulty_zip.land_area_in_sqmi:
                print('We couldn\'t remove {}. The zip code is of the type {} and the Population or Land Area is not None. Make sure this is ok. The zip code is kept.'.format(z.zip, z.type))
                print('** Population: {} \n Land Area (in sqmi): {}'.format(faulty_zip.population, faulty_zip.land_area_in_sqmi)) 
                other = set()
                unique = z
            else:
                other = set(z.zip)
                unique = pd.Series()
        return (unique, other)

    def filter_by_rad(self, rad):
        """
            This method is to filter out redundant zip codes for a specific radius. 
            The result will still overlap but significantly reduce the amount of zip 
            codes necessary to be scraped.
        """
        if isinstance(rad, int):
            half_rad = rad/2
        else:
            raise ValueError("arg 'rad' must be of type int")
        unique_zipcodes = []
        other_zipcodes = set()
        total_zipcodes = len(self.zipcodes)
        counter = 0
        for index, row in self.zipcodes.iterrows():
            if row.zip in other_zipcodes:
                continue
            unique, other = self._in_radius(row=row, rad=half_rad)
            if not unique.empty:
                unique_zipcodes.append(unique)
            for i in other:
                other_zipcodes.add(i)
            print_progress(counter, row.zip, total_zipcodes, interval=1000)
            counter += 1
        new_df = pd.DataFrame(unique_zipcodes)
        return new_df

class Scraper():
    # constructor 
    def __init__(self):
        print('Scraper')

    def init_proxies(self, proxies_csv, force=False):
        # Read in proxies
        try:
            proxies = pd.read_csv(proxies_csv, sep='\t')['ip'].values.tolist()
        except:
            raise ValueError('No Proxies found! Please generate a proxies csv using `scraper/get_proxies.py` or pass a path to a csv.')
        if not force and len(proxies) < 5:
            raise Exception('You have less than 5 valid proxies this could create issues. Consider not limiting your proxies to a territory. You can ignore this Exception by using `--force`')
        random.shuffle(proxies)
        self.num_proxies = len(proxies)
        self.proxy_pool = cycle(proxies)

        lower_limit_per_proxy = 50
        upper_limit_per_proxy = 150
        # Set a limit of scrapes - Batching
        scrape_limit_min = lower_limit_per_proxy * len(proxies)
        scrape_limit_max = upper_limit_per_proxy * len(proxies)
        self.scrape_limit = int(getRandomArbitrary(scrape_limit_min,scrape_limit_max))
        return self.scrape_limit

    def set_speed(self, speed='regular'):
        if speed == 'extreme':
            self.min_wait = 1
            self.max_wait = 3
            self.min_wait_failed = 1
            self.max_wait_failed = 3
        elif speed == 'fast':
            self.min_wait = 5
            self.max_wait = 10
            self.min_wait_failed = 3
            self.max_wait_failed = 5
        elif speed == 'regular':
            self.min_wait = 10
            self.max_wait = 20
            self.min_wait_failed = 20
            self.max_wait_failed = 30
        elif speed == 'slow':
            self.min_wait = 20
            self.max_wait = 30
            self.min_wait_failed = 30
            self.max_wait_failed = 60
    
    def init_scraper(self,scrape_func):
        self.scrape_func = scrape_func

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
    print('checking proxy: ', proxy)
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


class ZipcodeScraper(Scraper):
    #constructor
    def __init__(self, target):
        self.target = target

    def set_radius(self, radius):
        self.radius = radius

    def init_zipcodes(self, zipcode_csv):
        # This function reads in the zipcodes and removes those that have already been scraped.
        zip_codes = pd.read_csv(zipcode_csv,
                                dtype={'zip': object, 'lat': float, 'lng': float, 'type': object})
        # Create dir if not exists
        self.dir_path = './data/interim/' + self.target
        try:
            Path(self.dir_path).mkdir(parents=True)
        except:
            pass
        # Get filenames of already scraped zipcodes and filter the dataframe
        target_filenames = os.listdir(self.dir_path)
        scraped_zip_codes = [extract_zip_code(filename, self.target) for filename in target_filenames]
        self.zip_codes = zip_codes.loc[~zip_codes['zip'].isin(scraped_zip_codes),:]
        return self.zip_codes

    def wait_seconds(self):
        seconds = getRandomArbitrary(self.min_wait,self.max_wait)
        print('Success: Wait for {}'.format(seconds))
        time.sleep(seconds)

    def failed_wait_seconds(self):
        seconds = getRandomArbitrary(self.min_wait_failed,self.max_wait_failed)
        print('Failed: wait for {}'.format(seconds))
        time.sleep(seconds)
    
    def scrape(self, timeout=3, max_retries=10, limit_per_proxy=10):
        # Shuffle the zip codes to lower probability of pattern recognition
        zip_codes_shuffled = self.zip_codes.sample(frac=1)
        counter = 0
        iteration = 0
        self.proxy = next(self.proxy_pool)
        for row in zip_codes_shuffled.iterrows():
            row_values = row[1]
            zip_code = row_values['zip']
            print("Request for zip code {}".format(zip_code))
            num_retries = 0
            for retry in range(0,max_retries):
                # Get a proxy from the pool
                print("with proxy {}".format(self.proxy))
                proxies_settings, timeout = check_proxy(proxy=self.proxy, proxy_pool=self.proxy_pool, num_proxies=self.num_proxies, timeout=timeout)
                try:
                    scrape_timeout = timeout + 5
                    path = self.dir_path + '/' + self.target + '_' + zip_code + '.csv'
                    success = self.scrape_func(row_values, path=path, radius=self.radius, proxies=proxies_settings, timeout=scrape_timeout)
                    break
                except req.exceptions.ConnectionError:
                    print("xxxxxxxxxxxx  Connection refused  xxxxxxxxxxxx")
                    self.failed_wait_seconds()
                    num_retries += 1
                    # Get new proxy
                    print('new proxy')
                    self.proxy = next(self.proxy_pool)
                    print("Retry #{}".format(num_retries))
                except:
                    '''
                        Most free proxies will often get connection errors. You will have retry the entire request using another proxy to work.
                        We will just skip retries as its beyond the scope of this tutorial and we are only downloading a single url
                    '''
                    print(traceback.format_exc())
                    self.failed_wait_seconds()
                    num_retries += 1
                    print("Retry #{}".format(num_retries))
            # wait until you get next Zip Code
            if counter > self.scrape_limit:
                break
            else:
                counter += 1
            if iteration > limit_per_proxy:
                print('next proxy')
                self.proxy = next(self.proxy_pool)
                iteration = 0
            else:
                iteration += 1
            if success:
                self.wait_seconds()
            else:
                print(success)
                print('No Success for ZIP {}: Continue to next with no wait.'.format(zip_code))

def extract_zip_code(filename, target):
    zip_code = re.search('(?<=' + target + '_)' + '[0-9]*' + '(?!>.csv)', filename)
    if zip_code:
        return zip_code.group(0)
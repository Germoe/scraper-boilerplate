import pandas as pd
from uszipcode import SearchEngine, Zipcode
import random
from itertools import cycle
import os

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
        self.proxy_pool = cycle(proxies)

        lower_limit_per_proxy = 50
        upper_limit_per_proxy = 150
        # Set a limit of scrapes - Batching
        scrape_limit_min = lower_limit_per_proxy * len(proxies)
        scrape_limit_max = upper_limit_per_proxy * len(proxies)
        self.scrape_limit = int(getRandomArbitrary(scrape_limit_min,scrape_limit_max))
        return self.scrape_limit

    def set_speed(self, speed='regular'):
        if speed == 'fast':
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

class ZipcodeScraper(Scraper):
    #constructor
    def __init__(self, target):
        self.target = target

    def set_radius(self, radius):
        self.radius = radius

    def init_zipcodes(self, zipcode_csv):
        # This function reads in the zipcodes and removes those that have already been scraped.
        self.zipcodes = pd.read_csv(zipcode_csv,
                                dtype={'zip': object, 'lat': float, 'lng': float, 'type': object})
        # Create dir if not exists
        dir_path = './data/interim/' + target
        try:
            Path(dir_path).mkdir(parents=True)
        except:
            pass
        # Get filenames of already scraped zipcodes and filter the dataframe
        target_filenames = os.listdir(dir_path)
        scraped_zip_codes = [extract_zip_code(filename, target) for filename in target_filenames]
        zip_codes_filtered = zip_codes.loc[~zip_codes['zip'].isin(scraped_zip_codes),:]
        return zip_codes_filtered

def extract_zip_code(filename, target):
    zip_code = re.search('(?<=' + target + '_)' + '[0-9]*' + '(?!>.csv)', filename)
    if zip_code:
        return zip_code.group(0)
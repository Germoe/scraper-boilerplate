# -*- coding: utf-8 -*-
import click
import logging
import pprint
import json
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

## ----------------------- Utils ------------------------

def print_progress(counter, zipcode, total, interval=500):
    # Prints a progress statement for every 500 records that were processed
    if counter % interval == 0:
        print("Progress: {} \n ID: {} \n Records: {}".format(counter,zipcode,total))


# Taken this function from https://github.com/ChrisMuir/Zillow/blob/master/zillow_functions.py
def zipcodes_list(st_items, res_type='strings'):
    # If st_items is a single zipcode string.
    if isinstance(st_items, str):
        # If st_items is the special keyword 'all' fetch all US zipcodes
        if st_items == 'all':
            st_items = ['0','1','2','3','4','5','6','7','8','9']
            zc_objects = [n for i in st_items for n in zipcode.islike(str(i))]
            print(len(zc_objects))
        else:
            zc_objects = zipcode.islike(st_items)
    # If st_items is a list of zipcode strings.
    elif isinstance(st_items, list):
        zc_objects = [n for i in st_items for n in zipcode.islike(str(i))]
    else:
        raise ValueError("arg 'st_items' must be of type str or list")
    
    if res_type == 'objects':
        output = zc_objects
    else:
        output = [str(i).split(" ", 1)[1].split(">")[0] for i in zipcode_objects]
    return(output)

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
        print(len(other_zipcodes))
        new_df = pd.DataFrame(unique_zipcodes)
        return new_df

# Set indentation level of pretty printer
pp = pprint.PrettyPrinter(indent=2)

@click.command()
# @click.argument('input_filepath', type=click.Path(exists=True))
# @click.argument('output_filepath', type=click.Path())
def main(input_filepath=None, output_filepath='./data/'):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """

    us = Zipcodes(zipcodes_list('all', res_type='objects'))
    rad = 100
    if rad:
        path = './data/interim/zipcodes_' + str(rad) + '.csv'
    else:
        path = './data/interim/zipcodes_100.csv'
    # Check date of csv creation or modification
    try:
        new_zip_codes = datetime.fromtimestamp(os.stat(path)[8]) > datetime.now() - timedelta(days=180)
    except:
        new_zip_codes = False

    if not new_zip_codes:
        us_zip_filtered = us.filter_by_rad(rad=rad)
        us_zip_filtered = us_zip_filtered.set_index('zip')
        us_zip_filtered.to_csv(output_filepath + 'interim/zipcodes_' + str(rad) + '.csv', encoding='utf-8')

    # To combine multiple radi zipcode maps
    # us_zip_combined = pd.concat([us_zip_filtered_100, us_zip_filtered_50]) \
    #                     .reset_index() \
    #                     .drop_duplicates(subset='zip', keep='first') \
    #                     .set_index('zip')
    # us_zip_combined.to_csv(output_filepath + 'interim/zipcodes_combined.csv', encoding='utf-8')

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

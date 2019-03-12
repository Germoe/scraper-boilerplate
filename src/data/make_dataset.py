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
# Pandas
import pandas as pd
# Zipcode Operations
import zipcode

## ----------------------- Utils ------------------------

def print_progress(counter, zip_code, total, interval=500):
    # Prints a progress statement for every 500 records that were processed
    if counter % interval == 0:
        print("Progress: {} \n ID: {} \n Records: {}".format(counter,zip_code,total))

def getRandomArbitrary(min, max):
  return random.random() * (max - min) + min

# Set indentation level of pretty printer
pp = pprint.PrettyPrinter(indent=2)

@click.command()
# @click.argument('input_filepath', type=click.Path(exists=True))
# @click.argument('output_filepath', type=click.Path())
def main(input_filepath=None, output_filepath='./data/'):
    """
        Runs data processing scripts to turn raw data from (../interim) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    
    # Get list of directories in ./data/interim/*
    p = Path('./data/interim')
    interim_dirs = [x for x in p.iterdir() if x.is_dir()]

    out_p = Path('./data/processed')

    results = []
    # define columns that should be imported
    usecols = ['address','city','distance','name','phone','store_id','zip_code']
    # Set dtypes
    dtype = {'address': object,
            'city': object,
            'distance': float,
            'name': object,
            'phone': object,
            'store_id': object,
            'zip_code': object}
    # choose an index column
    index_col = 'store_id'
    # set the separator - by default all files are separated using \t
    sep = '\t'
    # Combine files by directory
    for d in interim_dirs:
        dfs = []
        path_elements = str(d.absolute()).split('/')
        target_name = path_elements[len(path_elements) - 1]
        for file in d.iterdir():
            try:
                df = pd.read_csv(file.absolute(),
                                usecols=usecols,
                                sep=sep,
                                dtype=dtype).set_index(index_col)
                dfs.append(df)
            except:
                print('Path: {} couldn\'t be read. Skipped.'.format(file.absolute()))
                continue
        combined_df = pd.concat(dfs).reset_index().drop_duplicates(subset='store_id').set_index('store_id').dropna()
        combined_df.to_csv(str(out_p.absolute()) + '/' + target_name + '.csv', sep=sep, encoding='utf-8')
        combined_df['type'] = combined_df.apply(define_type, axis=1)
        combined_df['state'] = combined_df.apply(define_state, axis=1)
        print(combined_df.head())
        by_state_df = combined_df.groupby(['state']).size().to_frame().rename(columns={0:'count'})
        by_state_df.to_csv(str(out_p.absolute()) + '/' + target_name + '_by_state.csv', sep=sep, encoding='utf-8')
        by_type_df = combined_df.groupby(['type']).size().to_frame().rename(columns={0:'count'})
        by_type_df.to_csv(str(out_p.absolute()) + '/' + target_name + '_by_type.csv', sep=sep, encoding='utf-8')
        by_state_type_df = combined_df.groupby(['state','type']).size().to_frame().rename(columns={0:'count'})
        # state_total_df = combined_df
        # state_total_df['type'] = 'total'
        # print(state_total_df.head(10))
        # state_total_df = state_total_df.groupby(['state','type']).size().to_frame().rename(columns={0:'count'})
        # pd.concat([state_total_df,by_state_type_df], sort=True).sort_values(['state','type']).to_csv(str(out_p.absolute()) + '/' + target_name + '_by_state_type.csv', sep=sep, encoding='utf-8')
        
        # Which state has the most, which one has the least
        # Find out walmarts by population_density
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')

def define_type(store):
    predefined_types = ['Supercenter','Store','Pharmacy','Pickup only','Neighborhood Market','Amigo','Gas Station']
    store_type = re.search('(' + '|'.join(predefined_types) + ')$',store['name'])
    if store_type:
        return store_type.group(0)
    return str(store['name'])

def define_state(store):
    return zipcode.isequal(store['zip_code']).state

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()

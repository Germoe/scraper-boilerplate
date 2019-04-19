# -*- coding: utf-8 -*-
import os
import click
import logging
from datetime import datetime
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
# Iterator
from dateutil import rrule
from datetime import datetime, timedelta
# Pandas
import pandas as pd

@click.command()
# @click.argument('input_filepath', type=click.Path(exists=True))
# @click.argument('output_filepath', type=click.Path())
@click.option('--force',is_flag=True)
def main(input_filepath=None, output_filepath='./data/', force=False):
    """ 
    Create your own custom iterators in this function and then run `make iterator` in your console
    """
    subdir = 'iterators'
    filename = 'weeks.csv' # Define the filename to the csv file that contains the Zip codes that need to be scraped

    first_week = datetime(1958, 8, 4)
    this_week = datetime(2019, 4, 13)
    weeks_iter = []
    for dt in rrule.rrule(rrule.WEEKLY, dtstart=first_week, until=this_week):
        weeks_iter.append(dt)

    df = pd.DataFrame({'iterator':weeks_iter})

    if not os.path.exists(output_filepath + subdir):
        os.mkdir(output_filepath + subdir)
    df.to_csv(output_filepath + subdir + '/' + filename, encoding='utf-8')

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

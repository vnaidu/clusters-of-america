# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv

from bs4 import BeautifulSoup

import pandas as pd
import numpy as np

def make_dataset():
    table_elm
    return table_cols

def city_populations(fpath):
    df = pd.read_csv(fpath)
    df[['lat', 'long']] = df.pop('location').str.extract(r'([-+]?\d{0,3}\.\d{1,4})\; ([-+]?\d{0,3}\.\d{1,4})', expand=True)
    df.pop_rank = df.pop_rank.astype(int)
    df.city = df.city.str.replace(r'\[.+\]', '')
    df.state = df.state.str.replace(r'\[.+\]', '')
    df.pop_estimate = df.pop_estimate.str.replace(r'\,', '').astype(int)
    df.census = df.census.str.replace(r'\,', '').astype(int)
    df.pop_delta = df.pop_delta.str.extract(r'([+-].+)%', expand=False).str.replace('+', '').astype(float)/100
    df.pop_density__mi = df.pop_density__mi.str.replace(',', '').str.extract(r'(\d+\.\d+)', expand=False).astype(float)
    df.pop_density__km2 = df.pop_density__km2.str.replace(',', '').str.extract(r'(\d+\.\d+)', expand=False).astype(float)
    df.land_area__mi = df.land_area__mi.str.replace(',', '').str.extract(r'(\d+)\/', expand=False).astype(int)
    df.land_area__km2 = df.land_area__km2.str.replace(',', '').str.extract(r'(\d+)\/', expand=False).astype(int)
    return df

@click.command()
@click.argument(
    'input_html',
    default='../../data/raw/List_of_United_States_cities_by_population.html',
    type=click.Path(exists=True))
def main(input_html):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')
    
    soup = BeautifulSoup(input_html, 'html5lib')
    csv_output = parse_wiki(soup).to_csv()
    click.echo(csv_output)

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()

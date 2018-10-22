# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv

from bs4 import BeautifulSoup

import pandas as pd
import numpy as np

def parse_wiki(soup):
    table_elm = soup.find('table', {'class': 'wikitable sortable'})

    df = None
    for r in table_elm.tbody.find_all('tr'):
        if r.th is not None and df is None:
            print('ok')
            col_names = [c.text.replace('\n', '') for c in r.findAll('th')]
            df = list()
            df.append(col_names)
        if r.td is not None and df is not None:
            df.append([c.text.replace('\n', '') for c in r.findAll('td')])

    table_cols = ['pop_rank', 'city', 'state', 'pop_estimate', 'census', 
                  'pop_delta', 'pop_density__mi', 'pop_density__km2', 
                  'land_area__mi', 'land_area__km2', 'location']
    return pd.DataFrame(df[1:], columns=table_cols)


@click.command()
@click.argument(
    'input_html',
    default='../../data/raw/List_of_United_States_cities_by_population.html',
    type=click.Path(exists=True), required=False)
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
    
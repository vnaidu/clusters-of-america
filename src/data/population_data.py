# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv

import requests
from bs4 import BeautifulSoup

import pandas as pd


def fetch_wiki(page):
    url = 'https://en.wikipedia.org/wiki/' + page
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html5lib')
    table_elm = soup.find('table', {'class': 'wikitable sortable'})

    df = None
    for r in table_elm.tbody.find_all('tr'):
        if r.th is not None and df is None:
            col_names = [c.text.replace('\n', '') for c in r.findAll('th')]
            df = list()
            df.append(col_names)
        if r.td is not None and df is not None:
            df.append([c.text.replace('\n', '') for c in r.findAll('td')])

    table_cols = ['pop_rank', 'city', 'state', 'pop_estimate', 'census',
                  'pop_delta', 'pop_density__mi', 'pop_density__km2',
                  'land_area__mi', 'land_area__km2', 'location']
    return pd.DataFrame(df[1:], columns=table_cols)


def clean(df):
    df[['latitude', 'longitude']] = df.pop('location').str.extract(r'([-+]?\d{0,3}\.\d{1,4})\; ([-+]?\d{0,3}\.\d{1,4})', expand=True)
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


def add_features(df):
    df['location'] = df.city + df.state
    return df


@click.command()
@click.argument('output_fpath', required=True)
def main(output_fpath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)

    wiki_page = 'List_of_United_States_cities_by_population'
    logger.info('Downloading Wikipedia Article: ' + wiki_page)

    (fetch_wiki(wiki_page)
     .pipe(clean)
     .pipe(add_features)
     .to_csv(output_fpath, index=False))


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()

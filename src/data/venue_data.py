# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
from bs4 import BeautifulSoup

import pandas as pd
import numpy as np

import json
import requests
##
# your Foursquare ID
# your Foursquare Secret
CLIENT_ID = 'HNF5QHXIWOKHP4FBJ0A4W32RNEQWY13ZADPNVN1ODETECMCU'
CLIENT_SECRET = 'G4EMPUSWZTQKC4S2LGYO0VLYQTBBWYQDOZVFSA3JRGWILHQZ'
VERSION = '20180605' # Foursquare API Version
RADIUS = 500         # Query Raidus
SECTION = 'topPicks' # Return List
LIMIT = 50
##

def top_venue_picks(lat, lng):
    return requests.get(
        f'https://api.foursquare.com/v2/venues/explore?' +
        f'&client_id={CLIENT_ID}' +
        f'&client_secret={CLIENT_SECRET}' +
        f'&v={VERSION}' +
        f'&ll={lat},{lng}' +
        f'&radius={RADIUS}' +
        f'&section={SECTION}' +
        f'&limit={LIMIT}'
    ).json()

#item['venue'] for result in
    #(v['id'], v['name'])
    v['venue']
    for v['items']
venue = (
    (('id', result['venue']['id']),
     ('name', result['venue']['name']),
     ('city', result['venue']['location']['city']),
        #('zipcode', result['venue']['location']['postalCode']),
     ('lat', result['venue']['location']['lat']),
     ('lng', result['venue']['location']['lng']))
    for result in top_venue_picks(
        37.910076, -122.065186
    )['response']['groups'][0]['items'])
list(venue)


def parse_wiki(soup):
    table_elm = soup.find('table', {'class': 'wikitable sortable'})

    df = None
    for r in table_elm.tbody.find_all('tr'):
        if r.th is not None and df is None:
            #print('ok')
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

    with open(input_html, 'r') as f:
        html = f.read()
    soup = BeautifulSoup(html, 'html5lib')
    csv_output = parse_wiki(soup).to_csv(sep=',', index=False)
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

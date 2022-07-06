import argparse
import logging
import json
import os
import requests

from datetime import datetime, timedelta
from time import sleep


DIR_DATA = os.environ.get(
    'DIR_DATA', 
    '/app/data'
)

DIR_DICTIONARIES = os.path.join(
    DIR_DATA,
    'dictionaries'
)

OPAC_ENDPOINT = os.environ.get(
    'OPAC_ENDPOINT', 
    'https://scielo.br/api/v1/counter_dict'
)

OPAC_DICTIONARY_PREFIX = os.environ.get(
    'OPAC_DICTIONARY_PREFIX', 
    'opac-counter-dict'
)

MAX_RETRIES = int(os.environ.get(
    'OPAC_COLLECT_MAX_RETRIES',
    5
))

SLEEP_TIME = int(os.environ.get(
    'OPAC_COLLECT_URL_SLEEP_TIME',
    30
))


def collect(begin_date, end_date, page=1):
    for t in range(1, MAX_RETRIES + 1):
        params = {
            'begin_date': begin_date, 
            'end_date': end_date, 
            'page': page
        }

        response = requests.get(url=OPAC_ENDPOINT, params=params, verify=False)

        try:
            response.raise_for_status()
            logging.debug(response.url)

        except requests.exceptions.HTTPError:
            logging.warning('Não foi possível coletar dados de %s. Aguardando %d segundos para tentativa %d de %d' % (response.url,SLEEP_TIME, t, MAX_RETRIES))
            sleep(SLEEP_TIME)

        else:
            return response.json()


def save(response, filename):
    filepath = os.path.join(DIR_DICTIONARIES, filename)

    if not os.path.exists(DIR_DICTIONARIES):
        os.makedirs(DIR_DICTIONARIES)

    try:
        json.dump(response, open(filepath, 'w'))
    except FileExistsError:
        logging.error('Arquivo %s já existe' % filepath)


def _generate_filename(prefix, from_date, until_date, page):
    return f'{prefix}-{from_date}-{until_date}-page-{str(page)}.json'


def _collect_and_save(from_date, until_date, page, prefix):
    logging.info('Obtendo dados de OPAC para (%s, %s) e página %d' % (from_date, until_date, page))
    content = collect(from_date, until_date)
    
    output_filename = _generate_filename(prefix, from_date, until_date, page)
    save(content, output_filename)

    if page == 1:
        return content


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--until_date', required=True)
    parser.add_argument('-f', '--from_date')

    params = parser.parse_args()

    from_date = params.from_date or (datetime.strptime(params.until_date, '%Y-%m-%d') - timedelta(30)).strftime('%Y-%m-%d')

    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s] %(levelname)s %(message)s',
                        datefmt='%d/%b/%Y %H:%M:%S')

    content = _collect_and_save(from_date, params.until_date, 1, OPAC_DICTIONARY_PREFIX)

    pages = int(content.get('pages', '1'))
    for i in range(2, pages + 1):
        _collect_and_save(from_date, params.until_date, i, OPAC_DICTIONARY_PREFIX)

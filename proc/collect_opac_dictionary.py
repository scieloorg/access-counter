import argparse
import datetime
import logging
import json
import os
import requests


COLLECTION = os.environ.get('COLLECTION', 'scl')
DIR_DATA = os.environ.get('DIR_DATA', '/app/data')
DIR_DICTIONARIES = os.path.join(DIR_DATA, 'dictionaries')
OPAC_ENDPOINT = os.environ.get('OPAC_ENDPOINT', 'https://new.scielo.br/api/v1/counter_dict')
OPAC_DICTIONARY_PREFIX = os.environ.get('OPAC_DICTIONARY_PREFIX', 'opac-counter-dict-')


def collect(date: str):
    try:
        until_date = datetime.datetime.strptime(date, '%Y-%m-%d')

        response = requests.get(url=OPAC_ENDPOINT, params={'end_date': until_date.strftime('%Y-%m-%d')}, verify=False)
        if response.status_code == 200:
            return response.json()

    except ValueError:
        logging.error('Data inválida %s' % date)


def save(response, filename):
    filepath = os.path.join(DIR_DICTIONARIES, filename)

    if not os.path.exists(DIR_DICTIONARIES):
        os.makedirs(DIR_DICTIONARIES)

    try:
        json.dump(response, open(filepath, 'w'))
    except FileExistsError:
        logging.error('Arquivo %s já existe' % filepath)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--date', required=True)
    params = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s] %(levelname)s %(message)s',
                        datefmt='%d/%b/%Y %H:%M:%S')

    logging.info('Obtendo dados do OPAC para date=%s' % params.date)
    data = collect(params.date)

    logging.info('Armazenando...')
    filename = ''.join([OPAC_DICTIONARY_PREFIX, COLLECTION, '-', params.date, '.json'])
    save(data, filename)

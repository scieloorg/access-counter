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


def collect(date: str, page=1):
    try:
        until_date = datetime.datetime.strptime(date, '%Y-%m-%d')

        response = requests.get(url=OPAC_ENDPOINT, params={'end_date': until_date.strftime('%Y-%m-%d'), 'page': page}, verify=False)
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

    logging.info('Obtendo dados do OPAC para date=%s e página=%d' % (params.date, 1))
    filename = ''.join([OPAC_DICTIONARY_PREFIX, COLLECTION, '-', params.date, '-p', '1', '.json'])
    data = collect(params.date, page=1)
    save(data, filename)

    pages = int(data.get('pages', '1'))
    for i in range(2, pages + 1):
        logging.info('Obtendo dados do OPAC para date=%s e página=%d de %d' % (params.date, i, pages))
        filename = ''.join([OPAC_DICTIONARY_PREFIX, COLLECTION, '-', params.date, '-p', str(i), '.json'])
        data_i = collect(params.date, page=i)
        save(data_i, filename)

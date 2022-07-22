import argparse
import logging
import json
import os
import requests

from datetime import datetime
from time import sleep


DIR_DATA = os.environ.get(
    'DIR_DATA', 
    '/app/data'
)

DIR_DICTIONARIES = os.path.join(
    DIR_DATA, 
    'dictionaries'
)

ARTICLEMETA_ENDPOINT = os.environ.get(
    'ARTICLEMETA_COLLECT_URL', 
    'http://articlemeta.scielo.org/api/v1/article/counter_dict/'
)

ARTICLEMETA_DICTIONARY_PREFIX = os.environ.get(
    'ARTICLEMETA_COLLECT_DICTIONARY_PREFIX', 
    'am-counter-dict'
)

LOGGING_LEVEL = os.environ.get(
    'ARTICLEMETA_COLLECT_LOGGING_LEVEL',
    'INFO'
)

MAX_RETRIES = int(os.environ.get(
    'ARTICLEMETA_COLLECT_MAX_RETRIES',
    5
))

SLEEP_TIME = int(os.environ.get(
    'ARTICLEMETA_COLLECT_URL_SLEEP_TIME',
    30
))


class ArticlemetaPaginationWasNotDetected(Exception):
    ...


def collect(from_date, until_date, offset=0):
    for t in range(1, MAX_RETRIES + 1):
        params = {'from': from_date, 'offset': offset}

        if until_date:
            params.update({'until': until_date})

        response = requests.get(ARTICLEMETA_ENDPOINT, params=params)

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


def _extract_total_and_limit(response):
    try:
        total = response['meta']['total']
        limit = response['meta']['limit']
    except KeyError:
        raise ArticlemetaPaginationWasNotDetected('Não foi possível extrair parâmetros limit e total')

    return total, limit


def _generate_filename(prefix, from_date, until_date, offset):
    return f'{prefix}-{from_date}-{until_date}-offset-{str(offset)}.json'


def _collect_and_save(from_date, until_date, offset, prefix):
    logging.info('Obtendo dados de Articlemeta para (%s, %s) e offset %d' % (from_date, until_date, offset))
    content = collect(from_date, until_date, offset)
    
    output_filename = _generate_filename(prefix, from_date, until_date, offset)
    save(content, output_filename)

    if offset == 0:
        return content


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--from_date', required=True)
    parser.add_argument('-u', '--until_date', default=datetime.now().strftime('%Y-%m-%d'))

    params = parser.parse_args()

    logging.basicConfig(level=LOGGING_LEVEL,
                        format='[%(asctime)s] %(levelname)s %(message)s',
                        datefmt='%d/%b/%Y %H:%M:%S')

    content = _collect_and_save(params.from_date, params.until_date, 0, ARTICLEMETA_DICTIONARY_PREFIX)
    total, limit = _extract_total_and_limit(content)

    for i_offset in range(limit, total, limit):
        _collect_and_save(params.from_date, params.until_date, i_offset, ARTICLEMETA_DICTIONARY_PREFIX)

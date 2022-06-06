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


def _extract_total_and_limit(response):
    try:
        total = response['meta']['total']
        limit = response['meta']['limit']
    except KeyError:
        raise ArticlemetaPaginationWasNotDetected('Não foi possível extrair parâmetros limit e total')

    return total, limit


def _generate_filename(prefix, from_date, until_date, offset):
    return f'{prefix}-{from_date}-{until_date}-offset-{str(offset)}.json'


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




def _extract_total_and_limit(response):
    try:
        total = response['meta']['total']
        limit = response['meta']['limit']
    except KeyError:
        raise ArticlemetaPaginationWasNotDetected('Não foi possível extrair parâmetros limit e total')

    return total, limit

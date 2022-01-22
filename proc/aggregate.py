import argparse
from genericpath import exists
import logging
import os
import time

from datetime import datetime, timedelta
from libs import lib_database, lib_status
from sqlalchemy import create_engine, table
from sqlalchemy.orm import sessionmaker


STR_CONNECTION = os.environ.get('STR_CONNECTION', 'mysql://user:pass@localhost:3306/matomo')
COLLECTION = os.environ.get('COLLECTION', 'scl')
LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', 'INFO')
ENGINE = create_engine(STR_CONNECTION, pool_recycle=1800)
SESSION_FACTORY = sessionmaker(bind=ENGINE)
SESSION_BULK_LIMIT = int(os.environ.get('SESSION_BULK_LIMIT', '500'))
TABLES_TO_UPDATE = os.environ.get('TABLES', 'aggr_article_language_year_month,aggr_journal_language_year_month')


def _extrac_dates_from_period(period: str):
    dates = []

    try:
        start, end = period.split(',')

        start_date = datetime.strptime(start, '%Y-%m-%d')
        end_date = datetime.strptime(end, '%Y-%m-%d')

        current_date = start_date
        while current_date <= end_date:
            dates.append(current_date)
            current_date = current_date + timedelta(days=1)

        return [d.strftime('%Y-%m-%d') for d in dates]

    except:
        return []


def _extract_tables_to_update(tables: str):
    return [t.strip() for t in tables.split(',')]


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c',
        '--collection',
        default=COLLECTION,
        help='Acrônimo de coleção'
    )

    parser.add_argument(
        '-p',
        '--period',
        help='Período de datas a serem agregadas (YYYY-MM-DD,YYYY-MM-DD)'
    )

    parser.add_argument(
        '-t',
        '--tables',
        choices=['aggr_article_language_year_month_metric', 'aggr_journal_language_year_month_metric'],
        default='aggr_article_language_year_month_metric,aggr_journal_language_year_month_metric',
        help='Tabelas a serem povoadas'
    )

    params = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s] %(levelname)s %(message)s',
                        datefmt='%d/%b/%Y %H:%M:%S')

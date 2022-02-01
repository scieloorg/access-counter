import argparse
import logging
import os
import time
import reverse_geocode

from datetime import datetime, timedelta
from libs import lib_database, lib_status
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.cursor import LegacyCursorResult


STR_CONNECTION = os.environ.get('STR_CONNECTION', 'mysql://user:pass@localhost:3306/matomo')
COLLECTION = os.environ.get('COLLECTION', 'scl')
LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', 'INFO')
ENGINE = create_engine(STR_CONNECTION, pool_recycle=1800)
SESSION_FACTORY = sessionmaker(bind=ENGINE)
SESSION_BULK_LIMIT = int(os.environ.get('SESSION_BULK_LIMIT', '500'))
TABLES_TO_UPDATE = os.environ.get('TABLES', 'aggr_article_language_year_month_metric,aggr_journal_language_year_month_metric,aggr_journal_geolocation_year_month_metric')


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

    except Exception as e:
        logging.error(e)
        return []


def _extract_tables_to_update(tables: str):
    return [t.strip() for t in tables.split(',')]


def _translate_geolocation_to_country(data):
    translated_data = {}

    for d in data:
        geo_reversed = reverse_geocode.search([[d.latitude, d.longitude]])
        if geo_reversed:
            country_code = geo_reversed.pop().get('country_code', '')

            key = (d.collection, d.id, d.ym, country_code)

            if key not in translated_data:
                translated_data[key] = [0, 0, 0, 0]

            translated_data[key][0] += d.tir
            translated_data[key][1] += d.tii
            translated_data[key][2] += d.uir
            translated_data[key][3] += d.uii

    return translated_data
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
        default=TABLES_TO_UPDATE,
        help='Tabelas a serem preenchidas'
    )

    params = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s] %(levelname)s %(message)s',
                        datefmt='%d/%b/%Y %H:%M:%S')

    dates = _extrac_dates_from_period(params.period)
    logging.info('Há %d datas...' % len(dates))

    tables = _extract_tables_to_update(params.tables)
    logging.info('Há %d tabelas de agregação' % len(tables))

    for date in dates:
        for table_name in tables:
            status_column_name = 'status_' + table_name

            try:
                current_date_aggr_status_table = lib_database.get_aggr_status(SESSION_FACTORY(), params.collection, date, status_column_name)

                if current_date_aggr_status_table == lib_status.AGGR_STATUS_QUEUE:
                    logging.info('Adicionando métricas agregadas para tabela %s e data (%s)...' % (table_name, date))

                    time_start = time.time()

                    if table_name == 'aggr_article_language_year_month_metric':
                        status = lib_database.extract_aggregated_data_for_article_language_year_month(STR_CONNECTION, params.collection, date)

                    elif table_name == 'aggr_journal_language_year_month_metric':
                        status = lib_database.extract_aggregated_data_for_journal_language_year_month(STR_CONNECTION, params.collection, date)

                    else:
                        status = None

                    if status is not None and status._generate_rows:
                        lib_database.update_aggr_status_for_table(SESSION_FACTORY(), params.collection, date, lib_status.AGGR_STATUS_DONE, status_column_name)

                    logging.info('Tempo total: %.2f segundos' % (time.time() - time_start))

                elif current_date_aggr_status_table is None:
                    logging.info('Data %s da coleção %s não está pronta para agregação' % (date, params.collection))
                    break

                else:
                    logging.info('Data %s da coleção %s já foi agregada para tabela %s' % (date, params.collection, table_name))

            except Exception as e:
                logging.error(e)

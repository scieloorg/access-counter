import logging
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from libs.lib_database import extract_pretable, get_dates_able_to_extract, update_date_status
from libs.lib_status import DATE_STATUS_PRETABLE, DATE_STATUS_EXTRACTING_PRETABLE

LOG_FILE_DATABASE_STRING = os.environ.get('LOG_FILE_DATABASE_STRING', 'mysql://user:pass@localhost:3306/matomo')
DIR_DATA = os.environ.get('DIR_DATA', '/app/data')
COLLECTION = os.environ.get('COLLECTION', 'scl')
MATOMO_ID_SITE = int(os.environ.get('MATOMO_ID_SITE', '1'))
PRETABLE_DAYS_N = int(os.environ.get('PRETABLE_DAYS_N', '5'))
LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', 'INFO')

DIR_PRETABLES = os.environ.get('DIR_PRETABLES', os.path.join(DIR_DATA, 'pretables'))

ENGINE = create_engine(LOG_FILE_DATABASE_STRING, pool_recycle=1800)
SESSION_FACTORY = sessionmaker(bind=ENGINE)


def save_pretable(str_date, query_result_data):
    str_date_with_extension = str_date + '.tsv'
    pretable_full_path = os.path.join(DIR_PRETABLES, str_date_with_extension)
    try:
        with open(pretable_full_path, 'w') as f:
            f.write('\t'.join(['serverTime',
                               'browserName',
                               'browserVersion',
                               'ip',
                               'latitude',
                               'longitude',
                               'actionName']) + '\n')

            for i in query_result_data:
                timestamp = i.serverTime.strftime("%Y-%m-%d %H:%M:%S")
                browser_name = i.browserName if i.browserName else 'NULL'
                browser_version = i.browserVersion if i.browserVersion else 'NULL'
                ip = i.ip if i.ip else 'NULL'
                latitude = str(i.latitude) if i.latitude else 'NULL'
                longitude = str(i.longitude) if i.longitude else 'NULL'
                action_name = i.actionName if i.actionName else 'NULL'

                line = '\t'.join([timestamp, browser_name, browser_version, ip, latitude, longitude, action_name])
                f.write(line + '\n')
    except IOError:
        pass


def main():
    logging.basicConfig(level=LOGGING_LEVEL,
                        format='[%(asctime)s] %(levelname)s %(message)s',
                        datefmt='%d/%b/%Y %H:%M:%S')

    if not os.path.exists(DIR_PRETABLES):
        os.makedirs(DIR_PRETABLES)

    dates = get_dates_able_to_extract(SESSION_FACTORY(), COLLECTION, PRETABLE_DAYS_N)

    logging.info('There are %d dates to be extracted', len(dates))

    for d in dates:
        str_date = d.strftime('%Y-%m-%d')

        logging.info('Extracting pretable of %s' % str_date)

        update_date_status(SESSION_FACTORY(), COLLECTION, d, DATE_STATUS_EXTRACTING_PRETABLE)
        query_result_data = extract_pretable(LOG_FILE_DATABASE_STRING, d, MATOMO_ID_SITE)
        save_pretable(str_date, query_result_data)

        update_date_status(SESSION_FACTORY(), COLLECTION, d, DATE_STATUS_PRETABLE)

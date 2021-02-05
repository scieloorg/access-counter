import logging
import os

from libs.lib_database import extract_pretable, get_dates_able_to_extract, update_date_status, get_db_session
from libs.lib_status import DATE_STATUS_PRETABLE


DIR_DATA = os.environ.get('DIR_DATA', '/app/data')
DIR_WORKING_LOGS = os.path.join(DIR_DATA, 'working')
DIR_PRETABLES = os.path.join(DIR_DATA, 'pretables')
LOG_FILE_DATABASE_STRING = os.environ.get('LOG_FILE_DATABASE_STRING', 'mysql://user:pass@localhost:3306/matomo')

COLLECTION = os.environ.get('COLLECTION', 'scl')
MAX_PRETABLE_DAYS = int(os.environ.get('MAX_PRETABLE_DAYS', '10'))

LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', 'INFO')


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
                actionName = i.actionName if i.actionName else 'NULL'

                line = '\t'.join([timestamp, browser_name, browser_version, ip, latitude, longitude, actionName])
                f.write(line + '\n')
    except IOError:
        pass


def main():
    logging.basicConfig(level=LOGGING_LEVEL,
                        format='[%(asctime)s] %(levelname)s %(message)s',
                        datefmt='%d/%b/%Y %H:%M:%S')

    if not os.path.exists(DIR_PRETABLES):
        os.makedirs(DIR_PRETABLES)

    dates = get_dates_able_to_extract(LOG_FILE_DATABASE_STRING, COLLECTION, MAX_PRETABLE_DAYS)

    logging.info('There are %d dates to be extracted', len(dates))

    for d in dates:
        str_date = d.strftime('%Y-%m-%d')

        logging.info('Extracting pretable of %s' % str_date)

        query_result_data = extract_pretable(LOG_FILE_DATABASE_STRING, d)
        save_pretable(str_date, query_result_data)

        db_session = get_db_session(LOG_FILE_DATABASE_STRING)
        update_date_status(db_session, d, DATE_STATUS_PRETABLE)
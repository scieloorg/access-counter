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



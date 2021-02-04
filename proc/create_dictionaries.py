import argparse
import datetime
import logging
import pickle
import os

from pymongo import MongoClient, uri_parser
from urllib.parse import urlparse


ARTICLEMETA_DATABASE_STRING = os.environ.get('ARTICLEMETA_DATABASE_STRING', 'mongodb://username:password@host:port/database.collection')
DIR_DATA = os.environ.get('DIR_DATA', '/app/data')
LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', 'INFO')

DIR_DICTIONARIES = os.path.join(DIR_DATA, 'dictionaries')
DOMAINS = set()
FULLTEXT_MODES = ['pdf', 'html']


def generate_file_path(dir_dictionaries, dict_name, version, extension):
    if version:
        file_full_name = dict_name + '-' + version
    else:
        file_full_name = dict_name
    file_full_name += extension

    return os.path.join(dir_dictionaries, file_full_name)


def get_all_dates(article):
    """
    Obtém todas as datas relacionadas a um artigo
    """
    dates = {
        'created_at': str(article.get('created_at', '')),
        'processing_date': str(article.get('processing_date', '')),
        'publication_year': article.get('publication_year', ''),
        'updated_at': str(article.get('updated_at', '')),
        'publication_date': article.get('publication_date', '')
    }
    return dates


def get_default_lang(article: dict):
    """
    Obtém o idioma padrão do artigo com base no campo ``v40``

    :param article: artigo em formato de dicionário
    :return: o idioma do artigo, se encontrado, ou string vazia, caso contrário
    """
    return article.get('article', {}).get('v40', [{}])[0].get('_', '').lower()


def fix_issn_acronym_errors(issn_acronym: dict):
    """
    Corrige problemas originários dos dados coletados do ArticleMeta

    :param issn_acronym: dicionário a ser corrigido
    """
    try:
        issn_acronym['bol']['2077-3323'] = 'rcc'
        issn_acronym['bol']['1817-7433'] = 'rccm'
        issn_acronym['bol']['2220-2234'] = 'rccm'
    except KeyError:
        pass


def get_journal_acronym(article: dict):
    """
    Obtém o acrônimo do periódico em que o artigo foi publicado, com base no campo ``v68``

    :param article: artigo em formato de dicionário
    :return: o acrônimo do periódico, ou string vazia, caso contrário
    """
    return article.get('title', {}).get('v68', [{}])[0].get('_', '').lower()


def get_fulltext_langs(article: dict):
    ftls = {'pdf': [], 'html': []}

    for mode in FULLTEXT_MODES:
        for lang, url in article.get('fulltexts', {}).get(mode, {}).items():
            url_parsed = urlparse(url.lower())
            url_host = url_parsed.hostname
            url_path = url_parsed.path

            ftls[mode].append([lang.lower(), url_host, url_path])
            DOMAINS.add(url_host)

    return ftls


def save_csv(dictionary, csv_file_name):
    if 'pid-dates' in csv_file_name:
        _save_csv_pid_dates(dictionary, csv_file_name)

    if 'issn-acronym' in csv_file_name:
        _save_csv_issn_acronym(dictionary, csv_file_name)

    if 'pdf-pid' in csv_file_name:
        _save_csv_pdf_pid(dictionary, csv_file_name)

    if 'pid-format-lang' in csv_file_name:
        _save_csv_pid_format_lang(dictionary, csv_file_name)

    if 'pid-issn' in csv_file_name:
        _save_csv_pid_issns(dictionary, csv_file_name)


def _save_csv_pid_dates(pid_dates: dict, filename: str):
    with open(filename, 'w') as f:
        keys = ['created_at', 'processing_date', 'publication_year', 'updated_at', 'publication_date']

        f.write('collection_acronym,pid,' + ','.join(keys) + '\n')

        for col, pid_values in pid_dates.items():
            for pid, values in pid_values.items():
                date_values = [values[k] for k in keys]

                f.write(','.join([col, pid, ','.join(date_values)]) + '\n')


def _save_csv_pid_issns(pid_issns: dict, filename: str):
    with open(filename, 'w') as f:
        f.write('collection_acronym,pid,issn\n')

        for col, values in pid_issns.items():
            for k, issns in values.items():
                for i in issns:
                    f.write(','.join([col, k, i]) + '\n')


def _save_csv_issn_acronym(issn_acronym: dict, filename: str):
    with open(filename, 'w') as f:
        f.write('collection_acronym,issn,journal_acronym\n')

        for col, values in issn_acronym.items():
            for issn, acronym in values.items():
                f.write(','.join([col, issn, acronym]) + '\n')


def _save_csv_pdf_pid(pdf_pid: dict, filename: str):
    with open(filename, 'w') as f:
        f.write('collection_acronym\tpid\tpdf\n')

        for col, values in pdf_pid.items():
            for pdf, v in values.items():
                for vi in v:
                    f.write('\t'.join([col, vi, pdf]) + '\n')


def _save_csv_pid_format_lang(pid_format_lang: dict, filename: str):
    with open(filename, 'w') as f:
        f.write('collection_acronym,pid,format,language,is_default\n')

        for col, values in pid_format_lang.items():
            for pid, format_langs in values.items():
                default_value = format_langs['default']

                for ki, vi in format_langs.items():
                    if ki != 'default':
                        for vii in vi:
                            line = ','.join([col, pid, ki, vii])

                            if vii == default_value:
                                line += ',1'
                            else:
                                line += ',0'

                            f.write(line + '\n')


def main():
    usage = 'Cria dicionários para serem utilizados no processo de cálculo de métricas COUNTER'
    parser = argparse.ArgumentParser(usage)

    parser.add_argument(
        '-u', '--articlemeta_db_uri',
        default=ARTICLEMETA_DATABASE_STRING,
        dest='mongo_uri',
        help='String de conexão a base MongoDB do ArticleMeta no formato '
             'mongodb://username:password@host:port/database.collection'
    )

    parser.add_argument(
        '-c', '--generate_csv',
        default=False,
        dest='generate_csv',
        action='store_true',
        help='Booleano que indica para também gerar a versão CSV dos dicionários'
    )

    parser.add_argument(
        '-v',
        default=datetime.datetime.now().strftime('%Y-%m-%d'),
        dest='version',
        help='String que representa a versão dos dicionários gerados (usar uma data no formato YYYY-MM-DD)'
    )

    parser.add_argument(
        '--logging_level',
        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET'],
        dest='logging_level',
        default=LOGGING_LEVEL
    )

    params = parser.parse_args()

    logging.basicConfig(level=LOGGING_LEVEL,
                        format='[%(asctime)s] %(levelname)s %(message)s',
                        datefmt='%d/%b/%Y %H:%M:%S')

    try:
        logging.info('Conectando ao banco de dados ArticleMeta...')
        dbname = uri_parser.parse_uri(params.mongo_uri).get('database')
        colname = uri_parser.parse_uri(params.mongo_uri).get('collection')
        articlemeta = MongoClient(params.mongo_uri)[dbname].get_collection(colname)
    except ConnectionError as e:
        logging.error(e)
        exit(1)

    logging.info('Testando conexão...')
    is_conection_ok = True if 'code' in articlemeta.find_one().keys() else False
    if is_conection_ok:
        logging.info('Sucesso')
    else:
        logging.error('Não foi possível coletar dados da base informada %s' % params.mongo_uri)
        exit(1)

    if not os.path.exists(DIR_DICTIONARIES):
        os.makedirs(DIR_DICTIONARIES)

    version = params.version

    # PID de artigo -> lista de ISSNs
    pid_issns = {}

    # PID de artigo para lista de idiomas disponíveis (para PDF e para HTML)
    pid_format_lang = {}

    # Caminho de arquivo PDF -> PID de artigo
    pdf_pid = {}

    # ISSN -> acrônimo
    issn_acronym = {}

    # PID de artigo para lista de datas
    pid_dates = {}

    total_extracted = 0
    logging.info('Coletando dados...')
    for article in articlemeta.find({}):
        total_extracted += 1
        if total_extracted % 1000 == 0:
            logging.info('Extraídos: %d' % total_extracted)

        pid = article.get('code')
        issns = [i for i in article.get('code_title') if i is not None]
        collection = article.get('collection')
        if not collection:
            logging.warning('Coleção indefinida para pid (%s) e issns (%s)' % (pid, issns))

        default_lang = get_default_lang(article)
        acronym = get_journal_acronym(article)

        if collection not in issn_acronym:
            issn_acronym[collection] = {}

        if acronym:
            for i in issns:
                if i:
                    if i not in issn_acronym[collection]:
                        issn_acronym[collection][i] = acronym
                    else:
                        if acronym != issn_acronym[collection][i]:
                            logging.warning('(%s, %s, %s, %s) já está na lista e é diferente de %s' % (collection, pid, i, acronym, issn_acronym[collection][i]))
        else:
            logging.info('%s, %s não possui acrônimo' % (collection, pid))

        if collection not in pid_issns:
            pid_issns[collection] = {}

        if collection not in pid_format_lang:
            pid_format_lang[collection] = {}

        if collection not in pdf_pid:
            pdf_pid[collection] = {}

        if collection not in pid_dates:
            pid_dates[collection] = {}

        if pid not in pid_issns[collection]:
            pid_issns[collection][pid] = []
        pid_issns[collection][pid].extend(issns)

        pid_dates[collection][pid] = get_all_dates(article)

        ftls = get_fulltext_langs(article)

        for fl_mode, fl_els in ftls.items():
            for i in fl_els:
                lang, url_host, url_path = i

                if pid not in pid_format_lang[collection]:
                    pid_format_lang[collection][pid] = {'default': default_lang}

                if fl_mode == 'pdf':
                    if url_path not in pdf_pid[collection]:
                        pdf_pid[collection][url_path] = set()
                    pdf_pid[collection][url_path].add(pid)

                    if len(pdf_pid[collection][url_path]) > 1:
                        logging.warning('Há mais de um PID em path_pdf_to_pid[%s][%s] = %s' % (collection, url_path, pdf_pid[collection][url_path]))

                    if 'pdf' not in pid_format_lang[collection][pid]:
                        pid_format_lang[collection][pid]['pdf'] = set()
                    pid_format_lang[collection][pid]['pdf'].add(lang)

                else:
                    if 'html' not in pid_format_lang[collection][pid]:
                        pid_format_lang[collection][pid]['html'] = set()
                    pid_format_lang[collection][pid]['html'].add(lang)

    fix_issn_acronym_errors(issn_acronym)

    for d in [(pid_issns, 'pid-issn'),
              (pid_format_lang, 'pid-format-lang'),
              (pdf_pid, 'pdf-pid'),
              (issn_acronym, 'issn-acronym'),
              (pid_dates, 'pid-dates')]:

        d_data, d_name = d

        d_file_full_path = generate_file_path(DIR_DICTIONARIES, d_name, version, '.data')
        try:
            pickle.dump(d_data, open(d_file_full_path, 'wb'))
        except FileExistsError:
            pickle.dump(d_data, open(d_file_full_path + datetime.datetime.now().timestamp().__str__(), 'wb'))

        if params.generate_csv:
            d_csv_name = generate_file_path(DIR_DICTIONARIES, d_name, version, '.csv')
            save_csv(d_data, d_csv_name)

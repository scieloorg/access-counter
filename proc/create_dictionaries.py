import argparse
import datetime
import json
import logging
import pickle
import os
import re

from dateutil import parser as date_parser
from pymongo import MongoClient, uri_parser
from urllib.parse import urlparse
from utils.regular_expressions import REGEX_OPAC_DICTIONARY, REGEX_PREPRINT_DICTIONARY, REGEX_YEAR


ARTICLEMETA_DATABASE_STRING = os.environ.get('ARTICLEMETA_DATABASE_STRING', 'mongodb://username:password@host:port/database.collection')
DIR_DATA = os.environ.get('DIR_DATA', '/app/data')
LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', 'INFO')

DIR_DICTIONARIES = os.path.join(DIR_DATA, 'dictionaries')
DOMAINS = set()
FULLTEXT_MODES = ['pdf', 'html']


def _get_json_files(dir_dictionaries):
    json_files = [f for f in os.listdir(dir_dictionaries) if f.endswith('.json')]

    opac_files = {}
    preprint_files = []

    for jf in json_files:
        jf_match_opac = re.match(REGEX_OPAC_DICTIONARY, jf)

        if jf_match_opac:
            jf_collection = jf_match_opac.group(1)

            if jf_collection not in opac_files:
                opac_files[jf_collection] = []

            opac_files[jf_collection].append(os.path.join(dir_dictionaries, jf))

        jf_match_preprint = re.match(REGEX_PREPRINT_DICTIONARY, jf)
        if jf_match_preprint:
            preprint_files.append(os.path.join(dir_dictionaries, jf))

    return opac_files, preprint_files


def _put_date(date_str, date_name, data):
    try:
        date_value = date_parser.parse(date_str)
        if date_value:
            data.update({date_name: date_value.strftime('%Y-%m-%d %H:%M:%S')})
    except ValueError:
        logging.warning('Data %s é inválida' % date_str)


def _extract_year(date_str):
    try:
        return date_parser.parse(date_str).strftime('%Y')

    except date_parser.ParserError:
        matches = {''}

        for m in re.findall(REGEX_YEAR, date_str):
            matches.add(m)

        return max(matches)

    except ValueError:
        return ''


def load_other_dictionaries(dir_dictionaries):
    opac_files, preprint_files = _get_json_files(dir_dictionaries)
    opac_dict = {}
    preprint_dict = {'pre': {}}

    for collection, files in opac_files.items():
        if collection not in opac_dict:
            opac_dict[collection] = {}

        for f in files:
            with open(f) as fin:
                fj = json.load(fin)

                pid_to_values = fj.get('documents', {})

                for pid, values in pid_to_values.items():
                    if pid not in opac_dict[collection]:
                        opac_dict[collection][pid] = values
                    else:
                        existing_created_date = date_parser.parse(opac_dict[collection][pid]['create'])
                        new_created_date = date_parser.parse(values['create'])

                        if new_created_date > existing_created_date:
                            opac_dict[collection][pid] = values

    for file in preprint_files:
        with open(file) as fin:
            preprint_metadata = json.load(fin)

            for pid, values in preprint_metadata.items():
                if pid not in preprint_dict['pre']:
                    preprint_dict['pre'][pid] = values

    return opac_dict, preprint_dict


def add_info_to_dates_dict(data, dates_dict):
    for collection, pids in data.items():
        if collection not in dates_dict:
            dates_dict[collection] = {}

        for pid, values in data[collection].items():
            if pid not in dates_dict[collection]:
                dates_dict[collection][pid] = {}

            publication_date = values.get('publication_date')

            if publication_date:
                dates_dict[collection][pid]['publication_date'] = publication_date
                year = _extract_year(publication_date)

                if year:
                    dates_dict[collection][pid].update({'publication_year': year})

            if collection != 'pre':
                _put_date(values.get('create'), 'created_at', dates_dict[collection][pid])
                _put_date(values.get('update'), 'updated_at', dates_dict[collection][pid])


def add_info_to_pid_format_lang(data, pid_format_lang_dict):
    for collection, pids in data.items():
        if collection not in pid_format_lang_dict:
            pid_format_lang_dict[collection] = {}

        for pid, values in pids.items():
            if pid not in pid_format_lang_dict[collection]:
                pid_format_lang_dict[collection][pid] = {}

            default_lang = values.get('default_language', '')

            if default_lang:
                pid_format_lang_dict[collection][pid].update({'default': default_lang})


def load_old_dictionaries(dir_dictionaries, version):
    old_dictionaries = {
        'pid-dates': {},
        'issn-acronym': {},
        'pdf-pid': {},
        'pid-format-lang': {},
        'pid-issn': {}
    }

    old_dicts_names = os.listdir(dir_dictionaries)

    for d in old_dicts_names:
        for name in old_dictionaries.keys():
            if name in d and version in d:
                logging.info('Loading old dictionary %s' % d)
                old_dictionaries[name] = pickle.load(open(os.path.join(dir_dictionaries, d), 'rb'))

    return old_dictionaries


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
        '--until_date',
        required=True,
        help='Documentos até uma data (indicar uma data no formato YYYY-MM-DD)'
    )

    parser.add_argument(
        '--from_date',
        required=True,
        help='Documentos a partir de uma data (indicar uma data no formato YYYY-MM-DD)'
    )

    parser.add_argument(
        '--old_dictionaries_version',
        dest='old_version'
    )

    parser.add_argument(
        '-v',
        default=datetime.datetime.now().strftime('%Y-%m-%d'),
        dest='version',
        help='String que representa a versão dos dicionários gerados (indicar uma data no formato YYYY-MM-DD)'
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

    old_dicts = load_old_dictionaries(DIR_DICTIONARIES, params.old_version)

    # PID de artigo -> lista de ISSNs
    pid_issns = old_dicts['pid-issn']

    # PID de artigo para lista de idiomas disponíveis (para PDF e para HTML)
    pid_format_lang = old_dicts['pid-format-lang']

    # Caminho de arquivo PDF -> PID de artigo
    pdf_pid = old_dicts['pdf-pid']

    # ISSN -> acrônimo
    issn_acronym = old_dicts['issn-acronym']

    # PID de artigo para lista de datas
    pid_dates = old_dicts['pid-dates']

    total_extracted = 0
    logging.info('Coletando dados...')
    for article in articlemeta.find({'$or': [
        {'updated_at': {'$gte': datetime.datetime.strptime(params.from_date, '%Y-%m-%d'), '$lte': datetime.datetime.strptime(params.until_date, '%Y-%m-%d')}},
        {'created_at': {'$gte': datetime.datetime.strptime(params.from_date, '%Y-%m-%d'), '$lte': datetime.datetime.strptime(params.until_date, '%Y-%m-%d')}},
    ]}):
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

        if pid not in pid_dates[collection]:
            pid_dates[collection][pid] = {}
        pid_dates[collection][pid].update(get_all_dates(article))

        if pid not in pid_format_lang[collection]:
            pid_format_lang[collection][pid] = {}

        if default_lang:
            pid_format_lang[collection][pid].update({'default': default_lang})

        ftls = get_fulltext_langs(article)

        for fl_mode, fl_els in ftls.items():
            for i in fl_els:
                lang, url_host, url_path = i

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

    logging.info('Carregando dados de OPAC e de Preprints...')
    opac_dict, preprint_dict = load_other_dictionaries(DIR_DICTIONARIES)

    logging.info('Atualizando dicionário de datas...')
    add_info_to_dates_dict(opac_dict, pid_dates)
    add_info_to_dates_dict(preprint_dict, pid_dates)

    logging.info('Atualizando dicionário de idiomas...')
    add_info_to_pid_format_lang(opac_dict, pid_format_lang)
    add_info_to_pid_format_lang(preprint_dict, pid_format_lang)

    logging.info('Gravando dicionários...')
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

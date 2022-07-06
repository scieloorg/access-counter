import argparse
import datetime
import logging
import os

from utils import dict_utils, dict_validator, file_utils


DIR_DATA = os.environ.get(
    'DIR_DATA', 
    '/app/data'
)

LOGGING_LEVEL = os.environ.get(
    'LOGGING_LEVEL', 
    'INFO'
)

DIR_DICTIONARIES = os.path.join(
    DIR_DATA, 
    'dictionaries'
)


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
        pid_issns[collection][pid] = sorted(set(pid_issns[collection][pid]))

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

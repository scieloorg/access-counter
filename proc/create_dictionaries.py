import argparse
import pickle

from pymongo import MongoClient
from urllib.parse import urlparse


MONGO_DATABASE = 'articlemeta'
MONGO_COLLECTION = 'articles'

DOMAINS = set()

FULLTEXT_MODES = ['pdf', 'html']


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


def fix_issn_to_acronym_errors(issn_to_acronym: dict):
    """
    Corrige problemas originários dos dados coletados do ArticleMeta

    :param issn_to_acronym: dicionário a ser corrigido
    """
    issn_to_acronym['bol']['2077-3323'] = 'rcc'
    issn_to_acronym['bol']['1817-7433'] = 'rccm'
    issn_to_acronym['bol']['2220-2234'] = 'rccm'


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


def export_pid_to_dates(pid_to_dates: dict, filename: str):
    with open(filename, 'w') as f:
        keys = ['created_at', 'processing_date', 'publication_year', 'updated_at', 'publication_date']

        f.write('collection_acronym,pid,' + ','.join(keys) + '\n')

        for col, pid_values in pid_to_dates.items():
            for pid, values in pid_values.items():
                date_values = [values[k] for k in keys]

                f.write(','.join([col, pid, ','.join(date_values)]) + '\n')


def export_pid_to_issn(pid_to_issns: dict, filename: str):
    with open(filename, 'w') as f:
        f.write('collection_acronym,pid,issn\n')

        for col, values in pid_to_issns.items():
            for k, issns in values.items():
                for i in issns:
                    f.write(','.join([col, k, i]) + '\n')


def export_issn_to_acronym(issn_to_acronyms: dict, filename: str):
    with open(filename, 'w') as f:
        f.write('collection_acronym,issn,journal_acronym\n')

        for col, values in issn_to_acronyms.items():
            for issn, acronym in values.items():
                f.write(','.join([col, issn, acronym]) + '\n')


def export_path_pdf_to_pid(path_pdf_to_pid: dict, filename: str):
    with open(filename, 'w') as f:
        f.write('collection_acronym\tpid\tpdf\n')

        for col, values in path_pdf_to_pid.items():
            for pdf, v in values.items():
                for vi in v:
                    f.write('\t'.join([col, vi, pdf]) + '\n')


def export_lang_format_to_pid(pid_to_langs: dict, filename: str):
    with open(filename, 'w') as f:
        f.write('collection_acronym,pid,format,language,is_default\n')

        for col, values in pid_to_langs.items():
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
        required=True,
        dest='mongo_uri',
        help='String de conexão a base MongoDB do ArticleMeta no formato '
             'mongodb://username:password@host:port/database.collection'
    )

    params = parser.parse_args()

    try:
        mc = MongoClient(params.mongo_uri)
    except ConnectionError as e:
        print('\nNão foi possível conectar a base de dados do ArticleMeta')
        print(e)
        exit(1)

    # PID de artigo -> lista de ISSNs
    pid_to_issns = {}

    # PID de artigo para lista de idiomas disponíveis (para PDF e para HTML)
    pid_to_langs = {}

    # Caminho de arquivo PDF -> PID de artigo
    path_pdf_to_pid = {}

    # ISSN -> acrônimo
    issn_to_acronym = {}

    # PID de artigo para lista de datas
    pid_to_dates = {}

    mongo_client = mc[MONGO_DATABASE][MONGO_COLLECTION]

    print('Testando conexão... ', end='')
    is_conection_ok = True if 'code' in mongo_client.find_one().keys() else False
    if is_conection_ok:
        print('Sucesso')
    else:
        print('Falhou')
        exit(1)

    total_extracted = 0
    print('Coletando dados...')
    for article in mongo_client.find({}):
        total_extracted += 1
        print('\rExtraídos: %d' % total_extracted, end='')

        pid = article.get('code')
        issns = [i for i in article.get('code_title') if i is not None]
        collection = article.get('collection')
        if not collection:
            print('\n[WARNING] Coleção indefinida para pid (%s) e issns (%s)' % (pid, issns))

        default_lang = get_default_lang(article)
        acronym = get_journal_acronym(article)

        if collection not in issn_to_acronym:
            issn_to_acronym[collection] = {}

        if acronym:
            for i in issns:
                if i:
                    if i not in issn_to_acronym[collection]:
                        issn_to_acronym[collection][i] = acronym
                    else:
                        if acronym != issn_to_acronym[collection][i]:
                            print('\n[WARNING] (%s, %s, %s, %s) já está na lista e é diferente de %s' % (collection, pid, i, acronym, issn_to_acronym[collection][i]))
        else:
            print('\n[WARNING] %s, %s não possui acrônimo' % (collection, pid))

        if collection not in pid_to_issns:
            pid_to_issns[collection] = {}

        if collection not in pid_to_langs:
            pid_to_langs[collection] = {}

        if collection not in path_pdf_to_pid:
            path_pdf_to_pid[collection] = {}

        if collection not in pid_to_dates:
            pid_to_dates[collection] = {}

        if pid not in pid_to_issns[collection]:
            pid_to_issns[collection][pid] = []
        pid_to_issns[collection][pid].extend(issns)

        pid_to_dates[collection][pid] = get_all_dates(article)

        ftls = get_fulltext_langs(article)

        for fl_mode, fl_els in ftls.items():
            for i in fl_els:
                lang, url_host, url_path = i

                if pid not in pid_to_langs[collection]:
                    pid_to_langs[collection][pid] = {'default': default_lang}

                if fl_mode == 'pdf':
                    if url_path not in path_pdf_to_pid[collection]:
                        path_pdf_to_pid[collection][url_path] = set()
                    path_pdf_to_pid[collection][url_path].add(pid)

                    if len(path_pdf_to_pid[collection][url_path]) > 1:
                        print('\n[WARNING] Há mais de um PID em path_pdf_to_pid[%s][%s] = %s' % (collection, url_path, path_pdf_to_pid[collection][url_path]))

                    if 'pdf' not in pid_to_langs[collection][pid]:
                        pid_to_langs[collection][pid]['pdf'] = set()
                    pid_to_langs[collection][pid]['pdf'].add(lang)

                else:
                    if 'html' not in pid_to_langs[collection][pid]:
                        pid_to_langs[collection][pid]['html'] = set()
                    pid_to_langs[collection][pid]['html'].add(lang)

    fix_issn_to_acronym_errors(issn_to_acronym)

    pickle.dump(pid_to_issns, open('pid_to_issns.data', 'wb'))
    export_pid_to_issn(pid_to_issns, 'pid_to_issns.csv')

    pickle.dump(pid_to_langs, open('pid_to_format_lang.data', 'wb'))
    export_lang_format_to_pid(pid_to_langs, 'pid_to_format_lang.csv')

    pickle.dump(path_pdf_to_pid, open('path_pdf_to_pid.data', 'wb'))
    export_path_pdf_to_pid(path_pdf_to_pid, 'path_pdf_to_pid.csv')

    pickle.dump(issn_to_acronym, open('issn_to_acronym.data', 'wb'))
    export_issn_to_acronym(issn_to_acronym, 'issn_to_acronym.csv')

    pickle.dump(pid_to_dates, open('pid_to_dates.data', 'wb'))
    export_pid_to_dates(pid_to_dates, 'pid_to_dates.csv')


if __name__ == '__main__':
    main()

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
    return article.get('article', {}).get('v40', [{}])[0].get('_', '')


def fix_issn_to_acronym_errors(issn_to_acronym: dict):
    """
    Corrige problemas originários dos dados coletados do ArticleMeta

    :param issn_to_acronym: dicionário a ser corrigido
    """
    issn_to_acronym['bol']['2077-3323'] = ['rcc']
    issn_to_acronym['bol']['1817-7433'] = ['rccm']
    issn_to_acronym['bol']['2220-2234'] = ['rccm']


def get_journal_acronym(article: dict):
    """
    Obtém o acrônimo do periódico em que o artigo foi publicado, com base no campo ``v68``

    :param article: artigo em formato de dicionário
    :return: o acrônimo do periódico, ou string vazia, caso contrário
    """
    return article.get('title', {}).get('v68', [{}])[0].get('_', '')


def get_fulltext_langs(article: dict):
    ftls = {'pdf': [], 'html': []}

    for mode in FULLTEXT_MODES:
        for lang, url in article.get('fulltexts', {}).get(mode, {}).items():
            url_parsed = urlparse(url)
            url_host = url_parsed.hostname
            url_path = url_parsed.path

            ftls[mode].append([lang, url_host, url_path])
            DOMAINS.add(url_host)

    return ftls


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
        print('Não foi possível conectar a base de dados do ArticleMeta')
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

    print('Coletando dados...')
    for article in mc[MONGO_DATABASE][MONGO_COLLECTION].find({}):
        pid = article.get('code')
        issns = article.get('code_title')
        collection = article.get('collection')
        if not collection:
            print('WARNING:coleção indefinida para pid (%s) e issns (%s)' % (pid, issns))

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
                            print('WARNING:(%s, %s, %s, %s) já está na lista e é diferente de %s' % (collection, pid, i, acronym, issn_to_acronym[collection][i]))
        else:
            print('WARNING:%s, %s não possui acrônimo' % (collection, pid))

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
                        print('WARNING:há mais de um PID em path_pdf_to_pid[%s][%s] = %s' % (collection, url_path, path_pdf_to_pid[collection][url_path]))

                    if 'pdf' not in pid_to_langs[collection][pid]:
                        pid_to_langs[collection][pid]['pdf'] = set()
                    pid_to_langs[collection][pid]['pdf'].add(lang)

                else:
                    if 'html' not in pid_to_langs[collection][pid]:
                        pid_to_langs[collection][pid]['html'] = set()
                    pid_to_langs[collection][pid]['html'].add(lang)

    fix_issn_to_acronym_errors(issn_to_acronym)

    pickle.dump(pid_to_issns, open('pid_to_issns.data', 'wb'))
    pickle.dump(pid_to_langs, open('pid_to_format_lang.data', 'wb'))
    pickle.dump(path_pdf_to_pid, open('path_pdf_to_pid.data', 'wb'))
    pickle.dump(issn_to_acronym, open('issn_to_acronym.data', 'wb'))
    pickle.dump(pid_to_dates, open('pid_to_dates.data', 'wb'))


if __name__ == '__main__':
    main()

import argparse
import csv
import datetime
import logging
import os
import re
import time

from decimal import Decimal
from utils.regular_expressions import REGEX_ISSN, REGEX_ARTICLE_PID
from utils import values
from libs import lib_database
from models.declarative import (
    Journal,
    JournalCollection,
    Localization,
    ArticleFormat,
    ArticleLanguage,
    Article,
    ArticleMetric,
    JournalMetric,
    SushiJournalMetric,
    SushiJournalYOPMetric
)


COLLECTION = os.environ.get('COLLECTION', 'scl')
DIR_R5_METRICS = os.environ.get('DIR_R5_METRICS', '/app/data/r5/metrics')
MATOMO_DATABASE_STRING = os.environ.get('MATOMO_DATABASE_STRING', 'mysql://user:pass@localhost:3306/matomo')
LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', 'INFO')
MIN_YEAR = int(os.environ.get('MIN_YEAR', '1900'))
MAX_YEAR = datetime.datetime.now().year + 5


class R5Metrics:
    def __init__(self, **kargs):
        self.pid = kargs['pid']
        self.format_name = kargs['format_name']
        self.language_name = kargs['language_name']
        self.latitude = Decimal(kargs['latitude']) if kargs['latitude'] != 'NULL' else kargs['latitude']
        self.longitude = Decimal(kargs['longitude']) if kargs['longitude'] != 'NULL' else kargs['longitude']
        self.year_of_publication = int(kargs['year_of_publication']) if kargs['year_of_publication'].isdigit() else kargs['year_of_publication']
        self.issn = kargs['issn']
        self.year_month_day = kargs['year_month_day']
        self.total_item_investigations = int(kargs['total_item_investigations'])
        self.total_item_requests = int(kargs['total_item_requests'])
        self.unique_item_investigations = int(kargs['unique_item_investigations'])
        self.unique_item_requests = int(kargs['unique_item_requests'])

    def is_valid_metric(self):
        # Ignora métrica que latitude é nula
        if self.latitude == 'NULL':
            return False

        # Ignora métrica que longitude é nula
        if self.longitude == 'NULL':
            return False

        # Ignora métrica que PID é mal-formado
        if not re.match(REGEX_ARTICLE_PID, self.pid):
            return False

        # Ignora métricas cujo artigo possui ano de publicação inválido
        if not isinstance(self.year_of_publication, int):
            return False
        elif self.year_of_publication < MIN_YEAR or self.year_of_publication > MAX_YEAR:
            return False

        return True

    def __str__(self):
        return '|'.join([str(getattr(self, k)) for k in self.__dict__ if not k.startswith('_')])


def sum_metrics(m1, m2):
    """
    Retorna uma nova lista com a soma dos elementos de duas listas de métricas
    :param m1: Lista de métricas 1
    :param m2: Lista de métricas 2
    :return: Lista em que cada elemento é a soma dos elementos das listas m1 e m2
    """
    return [m1[x] + m2[x] for x in range(4)]


def mount_issn_map(session):
    """
    Cria mapa de ISSN chave para ISSN valor, com base na banco de dados COUNTER
    :param session: Sessão de conexão com banco de dados COUNTER
    :return: Um dicionário que mapeia ISSN-Valor a ISSN-Chave da tabela counter_journal
    """
    issn_map = {}

    for j in session.query(Journal):
        issns = [i for i in [j.pid_issn,
                 j.online_issn,
                 j.print_issn] if i != '']
        for i in issns:
            if i not in issn_map:
                issn_map[i] = j.id
            else:
                if issn_map[i] != j.id:
                    logging.error('Base de periódicos está inconsistente (%s -> %s,  %s -> %s)'
                                  % (i, j.id, i, issn_map[i]))

    return issn_map


def mount_localization_map(session):
    """
    Cria mapa de (latitude, longitude) para ID de localização na base de dados
    :param session: Sessão de conexão com banco de dados COUNTER
    :return: Um dicionário que mapeia (latitude, longitude) a ID de localização
    """
    localization_map = {}

    for m in session.query(Localization):
        localization_map[(m.latitude, m.longitude)] = m.id
    return localization_map


def mount_format_map(session):
    """
    Cria mapa de nome de formato para ID de formato na base de dados
    :param session: Sessão de conexão com banco de dados COUNTER
    :return: Um dicionário que mapeia nome de formato a ID de formato
    """
    format_map = {}

    for m in session.query(ArticleFormat):
        format_code = m.id
        format_name = m.format
        format_map[format_name] = format_code

    return format_map


def mount_pid_map(session):
    """
    Cria mapa de PID e COLLECTION ACRONYM a ID na base de dados
    :param session: Sessão de conexão com banco de dados COUNTER
    :return: Um dicionário que mapeia PID e COLLECTION a ID
    """
    pid_map = {}

    for m in session.query(Article):
        article_id = m.id
        article_pid = m.pid
        article_collection = m.collection
        pid_map[(article_pid, article_collection)] = article_id

    return pid_map


def mount_language_map(session):
    """
    Cria mapa de nome de idioma para ID de idioma na base de dados
    :param session: Sessão de conexão com banco de dados COUNTER
    :return: Um dicionário que mapeia nome de idioma a ID de idioma
    """
    language_map = {}

    for m in session.query(ArticleLanguage):
        language_id = m.id
        language_name = m.language
        language_map[language_name] = language_id

    return language_map


def read_r5_metrics(path_file_r5_metrics):
    """
    Processa arquivo r5_metrics para uma lista de instâncias R5Metrics
    :param path_file_r5_metrics: Caminho de arquivo r5_metrics
    :return: Lista de instâncias R5Metrics
    """
    r5_metrics = []
    with open(path_file_r5_metrics) as fi:
        csv_reader = csv.DictReader(fi, delimiter='|', fieldnames=['pid',
                                                                   'format_name',
                                                                   'language_name',
                                                                   'latitude',
                                                                   'longitude',
                                                                   'year_of_publication',
                                                                   'issn',
                                                                   'year_month_day',
                                                                   'total_item_investigations',
                                                                   'total_item_requests',
                                                                   'unique_item_investigations',
                                                                   'unique_item_requests'])
        for row in csv_reader:
            r5 = R5Metrics(**row)
            if r5.is_valid_metric():
                r5_metrics.append(R5Metrics(**row))
            else:
                logging.debug('Métrica ignorada: %s' % r5.__str__())

    return r5_metrics


def update_issn_map(r5_metrics, issn_map):
    """
    Obtém ISSNs da lista de métricas calculadas
    :param r5_metrics: lista de instâncias R5Metric
    :param issn_map: Dicionário que mapeia ISSNs
    :return: Um dicionário de ISSNs faltantes a serem inseridos na base de dados
    """
    new_issns = set()
    for r in r5_metrics:
        issn = re.match(REGEX_ISSN, r.issn).string
        if issn and issn not in issn_map:
            issn_map[issn] = issn
            new_issns.add(issn)
    return new_issns


def update_issn_table(issns, db_session):
    """
    Atualiza banco de dados COUNTER com novos periódicos
    :param issns: Lista de ISSNs que não constam no banco de dados
    :param db_session: Sessão de conexão com banco de dados
    """
    logging.info('Há %d periódico(s) a ser(em) adicionado(s) no banco de dados' % len(issns))
    for issn in issns:
        new_journal = Journal()
        new_journal.pid_issn = issn
        new_journal.print_issn = ''
        new_journal.online_issn = ''

        db_session.add(new_journal)
        db_session.flush()

        new_journal_collection = JournalCollection()
        new_journal_collection.idjournal_jc = new_journal.id
        new_journal_collection.collection = values.DEFAULT_COLLECTION
        new_journal_collection.title = ''

        db_session.add(new_journal_collection)
        db_session.flush()

        logging.info('Adicionado periódico %s' % issn)
    db_session.commit()


def update_localization_table(r5_metrics, db_session, localization_map):
    """
    Atualiza banco de dados com novas localizações, isto é, pares (latitude, longitude)
    :param r5_metrics: lista de instâncias R5Metric
    :param db_session: Sessão de conexão com banco de dados
    :param localization_map: Dicionário que mapeia (latitude, longitude) a seu respectivo código no banco de dados
    """
    all_localizations = set([(r.latitude, r.longitude) for r in r5_metrics])
    new_localizations = all_localizations.difference(set(localization_map.keys()))

    objects = []
    for ngeo in new_localizations:
        latitude, longitude = ngeo

        new_localization = Localization()
        new_localization.latitude = latitude
        new_localization.longitude = longitude

        localization_map[(latitude, longitude)] = None
        objects.append(new_localization)

    db_session.bulk_save_objects(objects)
    db_session.commit()

    # Recarrega mapa de localização
    if new_localizations:
        return True


def update_format_table(r5_metrics, db_session, format_map):
    """
    Atualiza banco de dados com novos formatos
    :param r5_metrics: lista de instâncias R5Metric
    :param db_session: Sessão de conexão com banco de dados
    :param format_map: Dicionário que mapeia formato a seu respectivo código no banco de dados
    """
    all_formats = set([r.format_name for r in r5_metrics])
    new_formats = all_formats.difference(set(format_map.keys()))

    for nfmt in new_formats:
        new_fmt = ArticleFormat()
        new_fmt.format = nfmt

        logging.info('Adicionado formato %s' % nfmt)
        db_session.add(new_fmt)
        db_session.commit()

        format_map[new_fmt.format] = new_fmt.id


def update_language_table(r5_metrics, db_session, language_map):
    """
    Atualiza banco de dados com novos idiomas
    :param r5_metrics: lista de instâncias R5Metric
    :param db_session: Sessão de conexão com banco de dados
    :param language_map: Dicionário que mapeia idioma a seu respectivo código no banco de dados
    """
    all_languages = set([r.language_name for r in r5_metrics])
    new_languages = all_languages.difference(set(language_map.keys()))

    for nlang in new_languages:
        new_language = ArticleLanguage()
        new_language.language = nlang

        logging.info('Adicionado idioma %s' % nlang)
        db_session.add(new_language)
        db_session.commit()

        language_map[new_language.language] = new_language.id


def update_article_table(r5_metrics, db_session, issn_map, pid_map):
    """
    Atualiza banco de dados com novos artigos
    :param r5_metrics: lista de instâncias R5Metric
    :param db_session: Sessão de conexão com banco de dados
    :param issn_map: Dicionário que mapeia ISSNs
    :param pid_map: Dicionário que mapeia PID e COLLECTION a código no banco de dados
    """
    all_pids = set([(r.pid, COLLECTION) for r in r5_metrics])
    new_pids = all_pids.difference(set(pid_map.keys()))

    pid_to_r5 = {}
    for r in r5_metrics:
        if (r.pid, COLLECTION) in new_pids:
            pid_to_r5[(r.pid, COLLECTION)] = r

    objects = []
    for k, v in pid_to_r5.items():
        pid, col = k
        if (pid, col) not in pid_map:
            new_article = Article()
            new_article.collection = COLLECTION
            new_article.idjournal_a = issn_map[v.issn]
            new_article.pid = v.pid
            new_article.yop = v.year_of_publication

            pid_map[(pid, col)] = None
            objects.append(new_article)

    db_session.bulk_save_objects(objects)
    db_session.commit()

    # Recarrega mapa de PID e COLLECTION
    if new_pids:
        return True


def persist_metrics(r5_metrics, db_session, maps, key_list, table_class):
    """
    Adiciona métricas no banco de dados
    :param r5_metrics: lista de instâncias R5Metric
    :param db_session: Sessão de conexão com banco de dados
    :param maps: Dicionários que mapeiam insumos a seus respectivos IDs no banco de dados
    :param key_list: Lista de chaves
    :param table_class: Classe que representa a tabela a ser persistida
    """
    objects = []

    # Obtém um dicionário de métricas agregadas pelos valores associados a chave de key_list
    aggregated_metrics = _aggregate_by_keylist(r5_metrics, key_list, maps)

    # Obtém último ID
    last_id = lib_database.get_last_id(db_session, table_class)
    next_id = 1 + last_id if last_id else 1

    # Transforma dicionário de métricas em itens persistíveis no banco de dados
    for k, v in aggregated_metrics.items():
        row = {}
        tii, tir, uii, uir = v
        row.update({'id': next_id,
                    'total_item_investigations': tii,
                    'total_item_requests': tir,
                    'unique_item_investigations': uii,
                    'unique_item_requests': uir})

        # É métrica agregada para artigo
        if table_class.__tablename__ == 'counter_article_metric':
            idarticle, idlanguage, idformat, idlocalization, year_month_day = k
            row.update({'idarticle': idarticle,
                        'idlanguage': idlanguage,
                        'idformat': idformat,
                        'idlocalization': idlocalization,
                        'year_month_day': year_month_day})

        # É métrica agregada para periódico
        elif table_class.__tablename__ == 'counter_journal_metric':
            idjournal_cjm, idlanguage_cjm, idformat_cjm, yop, year_month_day = k
            row.update({'idjournal_cjm': idjournal_cjm,
                        'idlanguage_cjm': idlanguage_cjm,
                        'idformat_cjm': idformat_cjm,
                        'yop': yop,
                        'year_month_day': year_month_day})

        # É métrica agregada para periódico e ano de publicação na tabela Sushi
        elif table_class.__tablename__ == 'sushi_journal_yop_metric':
            idjournal_sjym, yop, year_month_day = k
            row.update({'idjournal_sjym': idjournal_sjym,
                        'yop': yop,
                        'year_month_day': year_month_day})

        # É métricas agregada para periódico na tabela SUSHI
        elif table_class.__tablename__ == 'sushi_journal_metric':
            idjournal_sjm, year_month_day = k
            row.update({'idjournal_sjm': idjournal_sjm,
                        'year_month_day': year_month_day})

        objects.append(row)
        next_id += 1

    db_session.bulk_insert_mappings(table_class, objects)
    db_session.commit()


def _aggregate_by_keylist(r5_metrics, key_list, maps):
    """
    Agrega métricas de acordo com uma lista de chaves de agregação

    :param r5_metrics: Lista de métricas do tipo R5Metric
    :param key_list:  Lista de chaves agregadoras
    :param maps: Dicionários que mapeiam insumos a seus respectivos IDs no banco de dados
    :return: Um dicionário que mapeia as métricas a suas respectivas chaves
    """
    aggregated_metrics = {}

    for r in r5_metrics:
        attrs = {'idjournal_cjm': maps['issn'][r.issn],
                 'idjournal_sjm': maps['issn'][r.issn],
                 'idjournal_sjym': maps['issn'][r.issn],
                 'idarticle': maps['pid'][(r.pid, COLLECTION)],
                 'idlanguage': maps['language'][r.language_name],
                 'idlanguage_cjm': maps['language'][r.language_name],
                 'idformat': maps['format'][r.format_name],
                 'idformat_cjm': maps['format'][r.format_name],
                 'idlocalization': maps['localization'][(r.latitude, r.longitude)],
                 'yop': r.year_of_publication,
                 'year_month_day': r.year_month_day}

        # Monta chave de agregação de métricas
        key = tuple(attrs[k] for k in key_list)

        # Adiciona métricas no dicionário
        if key not in aggregated_metrics:
            aggregated_metrics[key] = [r.total_item_investigations,
                                       r.total_item_requests,
                                       r.unique_item_investigations,
                                       r.unique_item_requests]
        else:
            # Caso key já esteja no dicionário de métricas, faz a somatória dos valores
            aggregated_metrics[key] = sum_metrics(aggregated_metrics[key], [r.total_item_investigations,
                                                                            r.total_item_requests,
                                                                            r.unique_item_investigations,
                                                                            r.unique_item_requests])
    return aggregated_metrics


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-u', '--matomo_db_uri',
        default=MATOMO_DATABASE_STRING,
        dest='matomo_db_uri',
        help='String de conexão a base SQL Matomo no formato mysql://username:password@host1:port/database'
    )

    parser.add_argument(
        '-d', '--dir_r5_metrics',
        default=DIR_R5_METRICS,
        dest='dir_r5_metrics',
        help='Diretório com arquivos r5_metrics'
    )

    parser.add_argument(
        '-t', '--tables',
        dest='tables',
        default='counter_foreign,sushi_journal_metric,sushi_journal_yop_metric',
        type=str,
        help='Lista de tabelas a serem persistidas (indicar os nomes das tabelas separadaos por vírgula)'
    )

    params = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s] %(levelname)s %(message)s',
                        datefmt='%d/%b/%Y %H:%M:%S')

    # Obtém sessão de conexão com banco de dados COUNTER/Matomo
    db_session = lib_database.get_db_session(params.matomo_db_uri)

    # Obtém os nomes das tabelas a serem persistidas
    target_tables = params.tables.split(',')
    logging.info('Tabelas a serem persistidas: (%s)' % ','.join(target_tables))

    # Obtém dicionários que mapeia ISSN a ISSN-Chave, Idioma a ID e Formato a ID
    issn_map = mount_issn_map(db_session)
    localization_map = mount_localization_map(db_session)
    language_map = mount_language_map(db_session)
    format_map = mount_format_map(db_session)
    pid_map = mount_pid_map(db_session)

    # Obtém lista de arquivos r5_metrics a serem lidos
    files_r5 = sorted([os.path.join(params.dir_r5_metrics, f) for f in os.listdir(params.dir_r5_metrics) if 'r5_metrics' in f])
    logging.info('Há %d arquivo(s) para ser(em) processado(s)' % len(files_r5))

    for f in files_r5:
        time_start = time.time()
        logging.info('Processando arquivo %s' % f)

        # Lê arquivo r5
        logging.info('Convertendo arquivo para R5Metric...')
        r5_metrics = read_r5_metrics(f)

        # Obtém lista de ISSNs que não existem no banco de dados
        if 'counter_foreign' in target_tables:
            logging.info('Obtendo ISSNs...')
            new_issns = update_issn_map(r5_metrics, issn_map)

            # Atualiza banco de dados com ISSNs não encontrados
            if new_issns:
                logging.info('Atualizando lista de ISSNs...')
                update_issn_table(new_issns, db_session)
                issn_map = mount_issn_map(db_session)

            # Atualiza lista de pares (Latitude, Longitude) no banco de dados
            logging.info('Atualizando lista de pares (latitude, longitude)...')
            exist_new_localizations = update_localization_table(r5_metrics, db_session, localization_map)

            if exist_new_localizations and ('counter_article_metric' in target_tables or 'counter_journal_metric' in target_tables or 'sushi_journal_metric' in target_tables):
                logging.info('Recarregando dados de localização')
                localization_map = mount_localization_map(db_session)

            # Atualiza formatos de artigo no banco de dados
            logging.info('Atualizando formatos...')
            update_format_table(r5_metrics, db_session, format_map)

            # Atualiza idiomas de artigo no banco de dados
            logging.info('Atualizando idiomas...')
            update_language_table(r5_metrics, db_session, language_map)

            # Atualiza artigos no banco de dados
            logging.info('Atualizando artigos...')
            exist_new_pids = update_article_table(r5_metrics, db_session, issn_map, pid_map)

            if exist_new_pids and ('counter_article_metric' in target_tables or 'counter_journal_metric' in target_tables or 'sushi_journal_metric' in target_tables):
                logging.info('Recarregando dados de PID e COLLECTION')
                pid_map = mount_pid_map(db_session)

        maps = {'pid': pid_map, 'language': language_map, 'format': format_map, 'localization': localization_map, 'issn': issn_map}

        if 'counter_article_metric' in target_tables:
            logging.info('Adicionando métricas agregadas para counter_article...')
            keys_counter_article = ['idarticle', 'idlanguage', 'idformat', 'idlocalization', 'year_month_day']
            persist_metrics(r5_metrics, db_session, maps, keys_counter_article, ArticleMetric)

        if 'counter_journal_metric' in target_tables:
            logging.info('Adicionando métricas agregadas para counter_journal...')
            keys_counter_journal = ['idjournal_cjm', 'idlanguage_cjm', 'idformat_cjm', 'yop', 'year_month_day']
            persist_metrics(r5_metrics, db_session, maps, keys_counter_journal, JournalMetric)

        if 'sushi_journal_yop_metric' in target_tables:
            logging.info('Adicionando métricas agregadas para sushi_journal_yop...')
            keys_sushi_journal_yop = ['idjournal_sjym', 'yop', 'year_month_day']
            persist_metrics(r5_metrics, db_session, maps, keys_sushi_journal_yop, SushiJournalYOPMetric)

        if 'sushi_journal_metric' in target_tables:
            logging.info('Adicionando métricas agregadas para sushi_journal...')
            keys_sushi_journal = ['idjournal_sjm', 'year_month_day']
            persist_metrics(r5_metrics, db_session, maps, keys_sushi_journal, SushiJournalMetric)

        logging.info('Tempo total: %.2f segundos' % (time.time() - time_start))


if __name__ == '__main__':
    main()
import argparse
import csv
import datetime
import logging
import os
import pickle
import re

from counter import CounterStat
from hit import HitManager
from socket import inet_ntoa
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from time import time
from utils import db_tools, dicts, values, hit_tools as ht
from utils.sql_declarative import (
    Article,
    ArticleMetric,
    Localization,
    ArticleFormat,
    ArticleLanguage,
    Journal,
    JournalCollection,
)

MATOMO_DB_IP_COUNTER_LIMIT = int(os.environ.get('MATOMO_DB_IP_COUNTER_LIMIT', '250000'))


def get_dates(date: str):
    """
    Obtém lista de dias para extrair informações do Matomo

    @param date: uma data em formato de YYYY-MM-DD ou um período no formato YYYY-MM-DD,YYYY-MM-DD
    @return: lista de data(s) em formato de objeto datetime
    """
    if ',' not in date:
        return [datetime.datetime.strptime(date, '%Y-%m-%d')]
    else:
        dates = date.split(',')
        if len(dates) == 2:
            start = datetime.datetime.strptime(dates[0], '%Y-%m-%d')
            end = datetime.datetime.strptime(dates[1], '%Y-%m-%d') + datetime.timedelta(days=1)
            return [start + datetime.timedelta(days=x) for x in range(0, (end - start).days)]


def get_pretables(path_pretables: str):
    """
    Obtém lista de caminhos de arquivos log com dados previamente extraídos do Matomo

    @param path_pretables: arquivo de log ou pasta com arquivos de log
    @return: lista de caminhos de arquivo(s) log
    """
    pretables = []
    if os.path.isfile(path_pretables):
        pretables.append(path_pretables)
    elif os.path.isdir(path_pretables):
        pretables.extend([os.path.join(os.path.abspath(path_pretables), f) for f in os.listdir(path_pretables)])
    return pretables


def get_pretable_date_value(path_pretable: str):
    """
    Obtém uma data a partir de nome de arquivo de pré-tabelas. Esse valor é utilizado para gravar os valores de métricas em disco.
    :param path_pretable: caminho completo do arquivo de pré-tabelas
    :return: uma data
    """
    pattern_date = r'(\d{4}-\d{2}-\d{2})\.'
    matched_date = re.search(pattern_date, path_pretable)
    if matched_date:
        return matched_date.groups()[0].replace('-', '')
    else:
        logging.error('Não foi possível detectar data válida em nome de arquivo: %s' % path_pretable)
        exit(1)


def update_metrics(metric, data):
    """
    Atualiza valores de métricas COUNTER para um registro.
    Caso registro seja novo, considera os valores no objeto data.
    Caso registro já existe, faz soma valores atuais com valores novos

    @param metric: registro de Metric
    @param data: dados a serem adicionados ou atribuídos ao registro
    """
    keys = ['total_item_investigations',
            'total_item_requests',
            'unique_item_investigations',
            'unique_item_requests']

    for k in keys:
        current_value = metric.__getattribute__(k)
        if current_value:
            metric.__setattr__(k, current_value + data[k])
        else:
            metric.__setattr__(k, data[k])


def export_article_hits_to_csv(hits: dict, file_prefix: str):
    with open('r5_hits_' + file_prefix + '.csv', 'a') as f:
        for session, hits_data in hits.items():
            for key, hits_list in hits_data.items():
                pid, fmt, lang, lat, long, yop = key
                issn = ht.article_pid_to_journal_issn(pid)

                line_data = [pid, fmt, lang, lat, long, yop, issn]

                for hit in hits_list:
                    year = hit.server_time.year
                    month = hit.server_time.month
                    day = hit.server_time.day
                    hour =hit.server_time.hour
                    minute = hit.server_time.minute
                    second = hit.server_time.second

                    ymdhms = '-'.join([str(year),
                                       str(month).zfill(2),
                                       str(day).zfill(2),
                                       str(hour).zfill(2),
                                       str(minute).zfill(2),
                                       str(second).zfill(2)])

                    f.write('|'.join(line_data + [hit.session_id,
                                                  ymdhms,
                                                  hit.action_name]) + '\n')


def export_article_metrics_to_csv(metrics: dict, file_prefix: str):
    with open('r5_metrics_' + file_prefix + '.csv', 'a') as f:
        for key, article_data in metrics.items():
            pid, fmt, lang, lat, long, yop = key
            issn = ht.article_pid_to_journal_issn(pid)

            for ymd in article_data:
                line_data = [pid, fmt, lang, lat, long, yop, issn, ymd]
                line_data.append(article_data[ymd]['total_item_investigations'])
                line_data.append(article_data[ymd]['total_item_requests'])
                line_data.append(article_data[ymd]['unique_item_investigations'])
                line_data.append(article_data[ymd]['unique_item_requests'])
                f.write('|'.join([str(i) for i in line_data]) + '\n')


def export_metrics_to_matomo(metrics: dict, db_session, collection: str):
    """
    Exporta métricas para banco de dados

    @param metrics: conjunto de métricas COUNTER
    @param db_session: sessão com banco de dados
    @param collection: acrônimo de coleção
    """
    for group in metrics.keys():
        if group == 'article':
            _export_article(metrics[group], db_session, collection)

        elif group == 'issue':
            _export_issue(metrics[group], db_session, collection)

        elif group == 'journal':
            _export_journal(metrics[group], db_session, collection)

        elif group == 'platform':
            _export_platform(metrics[group], db_session, collection)

        elif group == 'others':
            _export_others(metrics[group], db_session, collection)


def _export_article(metrics, db_session, collection):
    for key, article_data in metrics.items():
        pid, data_format, lang, latitude, longitude, yop = key

        article_format_id = dicts.format_to_code.get(data_format, -1)

        # Caso formato de artigo não esteja no dicionário padrão, tenta obter dado da tabela counter_article_format
        if article_format_id == -1:
            # Procura formato de artigo na base dados
            try:
                existing_article_format = db_tools.get_article_format(db_session=db_session, format_name=data_format)
                article_format_id = existing_article_format.format_id
            # Cria formato caso não exista na base de dados
            except NoResultFound:
                new_article_format = ArticleFormat()
                new_article_format.name = data_format

                db_session.add(new_article_format)
                db_session.flush()

                article_format_id = new_article_format.format_id

                logging.debug('Adicionado formato (ID: %s, NAME: %s)' % (new_article_format.format_id,
                                                                         new_article_format.name))

        article_language_id = dicts.language_to_code.get(lang, -1)

        # Caso idioma de artigo não esteja no dicionário padrão, tenta obter dado da tabela counter_article_language
        if article_language_id == -1:
            # Procura idioma na base de dados
            try:
                existing_article_language = db_tools.get_article_language(db_session=db_session,
                                                                          language_name=lang)
                article_language_id = existing_article_language.language_id
            # Cria um novo idioma na base de dados, caso não exista
            except NoResultFound:
                new_article_language = ArticleLanguage()
                new_article_language.name = lang

                db_session.add(new_article_language)
                db_session.flush()

                article_language_id = new_article_language.language_id

                logging.debug('Adicionado idioma (ID: %s, NAME: %s)' % (new_article_language.language_id,
                                                                        new_article_language.name))

        issn = ht.article_pid_to_journal_issn(pid)

        for ymd in article_data:
            # Procura periódico na base de dados
            try:
                existing_journal = db_tools.get_journal(db_session=db_session, issn=issn)
            # Cria um novo periódico caso não exista na base de dados
            except NoResultFound:
                new_journal = Journal()
                new_journal.pid_issn = issn
                new_journal.print_issn = ''
                new_journal.online_issn = ''

                db_session.add(new_journal)
                db_session.flush()

                existing_journal = new_journal

                new_journal_collection = JournalCollection()
                new_journal_collection.fk_col_journal_id = existing_journal.journal_id
                new_journal_collection.name = values.DEFAULT_COLLECTION
                new_journal_collection.title = ''

                db_session.add(new_journal_collection)
                db_session.flush()

                logging.debug('Adicionado periódico (ISSN: %s)' % (new_journal.pid_issn))

            # Procura artigo na base de dados
            try:
                existing_article = db_tools.get_article(db_session=db_session,
                                                        pid=pid,
                                                        collection=collection)
            # Cria um novo artigo caso artigo não exista na base de dados
            except NoResultFound:
                new_article = Article()
                new_article.collection_acronym = collection
                new_article.fk_art_journal_id = existing_journal.journal_id
                new_article.pid = pid
                new_article.yop = yop

                db_session.add(new_article)
                db_session.flush()
                existing_article = new_article

            # Procura origem do acesso (latitude e longitude) na base de dados
            try:
                existing_localization = db_tools.get_localization(db_session=db_session,
                                                                  latitude=latitude,
                                                                  longitude=longitude)
            # Cria uma nova localização
            except NoResultFound:
                new_localization = Localization()
                new_localization.latitude = latitude
                new_localization.longitude = longitude

                db_session.add(new_localization)
                db_session.flush()

                existing_localization = new_localization

            # Procura métrica na base de dados para atualizá-la
            try:
                existing_article_metric = db_tools.get_article_metric(db_session=db_session,
                                                                      year_month_day=ymd,
                                                                      article_id=existing_article.article_id,
                                                                      article_format_id=article_format_id,
                                                                      article_language_id=article_language_id,
                                                                      localization_id=existing_localization.localization_id
                                                                      )

                update_metrics(existing_article_metric, article_data[ymd])
                logging.debug('Atualizada métrica (ART_ID: %s, FMT_ID: %s, LANG_ID: %s, GEO_ID: %s, YMD: %s)' % (existing_article_metric.fk_article_id,
                                                                                                                 existing_article_metric.fk_article_format_id,
                                                                                                                 existing_article_metric.fk_article_language_id,
                                                                                                                 existing_article_metric.fk_localization_id,
                                                                                                                 existing_article_metric.year_month_day))
            # Cria um novo registro de métrica, caso não exista na base de dados
            except NoResultFound:
                new_metric_article = ArticleMetric()
                new_metric_article.fk_article_id = existing_article.article_id
                new_metric_article.fk_article_format_id = article_format_id
                new_metric_article.fk_article_language_id = article_language_id
                new_metric_article.fk_localization_id = existing_localization.localization_id
                new_metric_article.year_month_day = ymd

                update_metrics(new_metric_article, article_data[ymd])
                db_session.add(new_metric_article)
                logging.debug('Adicionada métrica (ART_ID: %s, FMT_ID: %s, LANG_ID: %s, GEO_ID: %s, YMD: %s)' % (new_metric_article.fk_article_id,
                                                                                                                 new_metric_article.fk_article_format_id,
                                                                                                                 new_metric_article.fk_article_language_id,
                                                                                                                 new_metric_article.fk_localization_id,
                                                                                                                 new_metric_article.year_month_day))
    db_session.commit()


def _export_issue(metrics, db_session, collection):
    for key, issue_data in metrics.items():
        # ToDo
        pass


def _export_journal(metrics, db_session, collection):
    for key, journal_data in metrics.items():
        # ToDo
        pass


def _export_platform(metrics, db_session, collection):
    for key, platform_data in metrics.items():
        # ToDo
        pass


def _export_others(metrics, db_session, collection):
    for key, others_data in metrics.items():
        # ToDo
        pass


def run(data, mode, hit_manager: HitManager, db_session, collection, result_file_prefix):
    """
    Cria objetos Hit e chama rotinas COUNTER a cada 50 mil (valor definido em BUCKET_LIMIT) iterações.
    Por questões de limitação de memória, o método trabalha por IP.
    Para cada IP, são obtidos os registros a ele relacionados, de tabela pré-extraída da base de dados Matomo

    @param data: arquivo de pré-tabela ou result query
    @param mode: modo de execução (via pretables ou database)
    @param hit_manager: gerenciador de objetos Hit
    @param db_session: sessão com banco de dados
    @param collection: acrônimo de coleção
    @param result_file_prefix: um prefixo para ser usado no nome do arquivo com as métricas e hits
    """
    # IP atual a ser contabilizado
    past_ip = ''

    # Contador para rodar rotinas counter a cada IP_LIMIT
    ip_counter = 0

    # Verificador da primeira linha
    line_counter = 0

    for d in data:
        line_counter += 1
        ip_counter += 1

        if mode == 'pretable':
            current_ip = d.get('ip', '')
        else:
            current_ip = inet_ntoa(d.visit.location_ip)

        if line_counter == 1:
            past_ip = current_ip

        if past_ip != current_ip:
            ip_counter += 1

            if ip_counter >= MATOMO_DB_IP_COUNTER_LIMIT:
                run_counter_routines(hit_manager=hit_manager,
                                     db_session=db_session,
                                     collection=collection,
                                     file_prefix=result_file_prefix)
                ip_counter = 0

            past_ip = current_ip

        hit = hit_manager.create_hit(d, mode)

        if hit:
            hit_manager.add_hit(hit)

    run_counter_routines(hit_manager=hit_manager,
                         db_session=db_session,
                         collection=collection,
                         file_prefix=result_file_prefix)


def run_counter_routines(hit_manager: HitManager, db_session, collection, file_prefix):
    """
    Executa métodos COUNTER para remover cliques-duplos, contar acessos por PID e extrair métricas.
    Ao final, salva resultados (métricas) em base de dados

    @param hit_manager: gerenciador de objetos Hit
    @param db_session: sessão com banco de dados
    @param collection: acrônimo de coleção
    @param file_prefix: um prefixo para ser usado no nome do arquivo com as métricas e hits
    """
    hit_manager.remove_double_clicks()

    cs = CounterStat()
    cs.calculate_metrics(hit_manager.hits)
    export_metrics_to_matomo(metrics=cs.metrics, db_session=db_session, collection=collection)

    if hit_manager.debug:
        logging.info('Salvando hits em disco...')
        export_article_hits_to_csv(hit_manager.hits['article'], file_prefix)

        logging.info('Salvando métricas em disco...')
        export_article_metrics_to_csv(cs.metrics['article'], file_prefix)

    hit_manager.reset()


def main():
    usage = 'Calcula métricas COUNTER R5 usando dados de acesso SciELO'
    parser = argparse.ArgumentParser(usage)

    parser.add_argument(
        '-c', '--collection',
        dest='collection',
        default='scl',
        help='Acrônimo da coleção objeto de extração de métricas'
    )

    parser.add_argument(
        '-i', '--idsite',
        dest='idsite',
        required=True,
        help='Identificador Matomo do site do qual as informações serão extraídas'
    )

    parser.add_argument(
        '--dict_pdf',
        dest='pdf_to_pid',
        required=True,
        help='Dicionário que mapeia caminho de PDF a PID'
    )

    parser.add_argument(
        '--dict_acronym',
        dest='issn_to_acronym',
        required=True,
        help='Dicionário que mapeia ISSN a acrônimo'
    )

    parser.add_argument(
        '--dict_language',
        dest='pid_to_format_lang',
        required=True,
        help='Dicionário que mapeia PID a formato e idioma'
    )

    parser.add_argument(
        '--dict_yop',
        dest='pid_to_yop',
        required=True,
        help='Dicionário que mapeia PID a ano de publicação'
    )

    parser.add_argument(
        '-u', '--matomo_db_uri',
        required=True,
        dest='matomo_db_uri',
        help='String de conexão a base SQL Matomo no formato mysql://username:password@host1:port/database'
    )

    parser.add_argument(
        '-o', '--include_other_hit_types',
        dest='include_other_hit_types',
        action='store_true',
        default=False,
        help='Inclui na contagem Hits dos tipos HIT_TYPE_ISSUE, HIT_TYPE_JOURNAL e HIT_TYPE_PLATFORM'
    )

    parser.add_argument(
        '--logging_level',
        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET'],
        dest='logging_level',
        default='INFO',
        help='Nivel de log'
    )

    parser.add_argument(
        '--debug',
        dest='debug',
        default=False,
        action='store_true',
        help='Possibilita verificar corretude da extração de Hits e do cálculo de métricas.'
             ' Salva em disco dois arquivos: (1) r5_hits_yyyymmdd e (2) r5_metrics_yyyymmdd.'
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--period',
        dest='period',
        help='Periódo da qual os dados serão extraídos do Matomo. Caso esteja no formato YYYY-MM-DD,YYYY-MM-DD, '
             'o período entre a primeira e segunda datas será considerado. Caso esteja no formato YYYY-MM-DD, '
             'apenas os dados do dia indicado serão extraídos'
    )
    group.add_argument(
        '--pretables',
        dest='pretables',
        default=[],
        help='Carrega dados a partir de uma pasta com arquivos de pré-tabela ou um único arquivo de pré-tabela '
             'previamente extraído(s) do Matomo'
    )

    params = parser.parse_args()

    fileLog = logging.FileHandler('r5_' + time().__str__() + '.log')
    fileLog.setLevel(params.logging_level)

    consoleLog = logging.StreamHandler()
    consoleLog.setLevel(logging.INFO)

    logging.basicConfig(level=params.logging_level,
                        format='[%(asctime)s] %(levelname)s %(message)s',
                        datefmt='%d/%b/%Y %H:%M:%S',
                        handlers=[fileLog, consoleLog])

    logging.info('Carregando dicionário PDF-PID')
    pdf_to_pid = pickle.load(open(params.pdf_to_pid, 'rb'))

    logging.info('Carregando dicionário ISSN-Acrônimo')
    issn_to_acronym = pickle.load(open(params.issn_to_acronym, 'rb'))

    logging.info('Carregando dicionário PID-Formato-Idioma')
    pid_to_format_lang = pickle.load(open(params.pid_to_format_lang, 'rb'))

    logging.info('Carregando dicionário PID-Datas')
    pid_to_yop = pickle.load(open(params.pid_to_yop, 'rb'))

    db_session = db_tools.get_db_session(params.matomo_db_uri)
    hit_manager = HitManager(path_pdf_to_pid=pdf_to_pid,
                             issn_to_acronym=issn_to_acronym,
                             pid_to_format_lang=pid_to_format_lang,
                             pid_to_yop=pid_to_yop,
                             flag_include_other_hit_types=params.include_other_hit_types,
                             debug=params.debug)

    if params.pretables:
        logging.info('Iniciado em modo pré-tabelas')
        pretables = get_pretables(params.pretables)

        for pt in pretables:
            time_start = time()

            logging.info('Extraindo dados do arquivo {}...'.format(pt))
            hit_manager.reset()

            pretable_date_value = get_pretable_date_value(pt)

            with open(pt) as data:
                csv_data = csv.DictReader(data, delimiter='\t')
                run(data=csv_data,
                    mode='pretable',
                    hit_manager=hit_manager,
                    db_session=db_session,
                    collection=params.collection,
                    result_file_prefix=pretable_date_value)

            time_end = time()
            logging.info('Durou %.2f segundos' % (time_end - time_start))

    else:
        logging.info('Iniciado em modo de banco de dados')
        dates = get_dates(date=params.period)

        for date in dates:
            time_start = time()

            logging.info('Extraindo dados para data {}...'.format(date.strftime('%Y-%m-%d')))
            hit_manager.reset()

            db_data = db_tools.get_matomo_logs_for_date(db_session=db_session,
                                                        idsite=params.idsite,
                                                        date=date)

            run(data=db_data,
                mode='database',
                hit_manager=hit_manager,
                db_session=db_session,
                collection=params.collection,
                result_file_prefix=date.strftime('%Y%m%d'))

            time_end = time()
            logging.info('Durou %.2f segundos' % (time_end - time_start))


if __name__ == '__main__':
    main()

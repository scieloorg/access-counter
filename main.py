import argparse
import csv
import datetime
import logging
import os
import pickle

from counter import CounterStat
from hit import HitManager
from socket import inet_ntoa
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from time import time
from utils import db_tools, pid_tools
from utils.map_helper import FORMAT_TO_CODE, LANG_TO_CODE
from utils.sql_declarative import Article, ArticleMetric


MATOMO_DB_SESSION_BUCKET_LIMIT = int(os.environ.get('MATOMO_DB_SESSION_BUCKET_LIMIT', '50000'))


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


def get_log_files(path_log_files: str):
    """
    Obtém lista de caminhos de arquivos log com dados previamente extraídos do Matomo

    @param path_log_files: arquivo de log ou pasta com arquivos de log
    @return: lista de caminhos de arquivo(s) log
    """
    log_files = []
    if os.path.isfile(path_log_files):
        log_files.append(path_log_files)
    elif os.path.isdir(path_log_files):
        log_files.extend([os.path.join(os.path.abspath(path_log_files), f) for f in os.listdir(path_log_files)])
    return log_files


def update_metrics(article_metric, data):
    """
    Atualiza valores de métricas COUNTER para um registro.
    Caso registro seja novo, considera os valores em data.
    Caso registro já existe, faz soma valores atuais com valores novos

    @param article_metric: registro de ArticleMetric
    @param data: dados a serem adicionados ou atribuídos ao registro
    """
    keys = ['total_item_investigations',
            'total_item_requests',
            'unique_item_investigations',
            'unique_item_requests']

    for k in keys:
        current_value = article_metric.__getattribute__(k)
        if current_value:
            article_metric.__setattr__(k, current_value + data[k])
        else:
            article_metric.__setattr__(k, data[k])


def export_metrics_to_matomo(metrics: dict, db_session, collection: str):
    """
    Exporta métricas para banco de dados

    @param metrics: conjunto de métricas COUNTER
    @param db_session: sessão com banco de dados
    @param collection: acrônimo de coleção
    """
    for pfl_key, pid_data in metrics.items():
        pid, data_format, lang = pfl_key

        article_format_id = FORMAT_TO_CODE.get(data_format, 1)
        article_language_id = LANG_TO_CODE.get(lang, 1)

        issn = pid_tools.article_to_journal(pid)

        for ymd in pid_data:
            try:
                # Procura periódico na base de dados
                existing_journal = db_tools.get_journal(db_session=db_session, issn=issn)

                try:
                    # Procura artigo na base de dados
                    existing_article = db_tools.get_article(db_session=db_session,
                                                            pid=pid,
                                                            collection=collection)

                except NoResultFound:
                    # Cria um novo artigo caso artigo não exista na base de dados
                    new_article = Article()
                    new_article.collection_acronym = collection
                    new_article.fk_art_journal_id = existing_journal.journal_id
                    new_article.pid = pid

                    db_session.add(new_article)
                    db_session.flush()

                    existing_article = new_article

                try:
                    existing_article_metric = db_tools.get_article_metric(db_session=db_session,
                                                                          year_month_day=ymd,
                                                                          article_id=existing_article.article_id,
                                                                          article_format_id=article_format_id,
                                                                          article_language_id=article_language_id)
                    update_metrics(existing_article_metric, pid_data[ymd])
                    logging.info('Atualizada métrica {}-{}-{}-{}'.format(existing_article_metric.fk_article_id,
                                                                         existing_article_metric.fk_article_format_id,
                                                                         existing_article_metric.fk_article_language_id,
                                                                         existing_article_metric.year_month_day))

                except NoResultFound:
                    # Cria um novo registro de métrica, caso não exista na base de dados
                    new_metric_article = ArticleMetric()
                    new_metric_article.fk_article_id = existing_article.article_id
                    new_metric_article.fk_article_format_id = article_format_id
                    new_metric_article.fk_article_language_id = article_language_id
                    new_metric_article.year_month_day = ymd

                    update_metrics(new_metric_article, pid_data[ymd])

                    db_session.add(new_metric_article)
                    logging.info('Adicionada métrica {}-{}-{}-{}'.format(new_metric_article.fk_article_id,
                                                                         new_metric_article.fk_article_format_id,
                                                                         new_metric_article.fk_article_language_id,
                                                                         new_metric_article.year_month_day))

                db_session.flush()

            except NoResultFound:
                logging.error('Nenhum periódico foi localizado na base de dados: {}'.format(issn))
            except MultipleResultsFound:
                logging.error('Mais de um periódico foi localizado na base de dados: {}'.format(issn))
            except IntegrityError as e:
                db_session.rollback()
                logging.error('Artigo já está na base: {}'.format(e))


def run_for_log_file(log_file: str, hit_manager: HitManager, db_session, collection):
    """
    Cria objetos Hit e chama rotinas COUNTER a cada 50 mil (valor definido em BUCKET_LIMIT) iterações.
    Por questões de limitação de memória, o método trabalha por IP.
    Para cada IP, são obtidos os registros a ele relacionados, do arquivo de log pré-extraído da base de dados Matomo

    @param log_file: arquivo a ser lido
    @param hit_manager: gerenciador de objetos Hit
    @param db_session: sessão com banco de dados
    @param collection: acrônimo de coleção
    """
    with open(log_file) as f:
        csv_file = csv.DictReader(f, delimiter='\t')

        # IP atual a ser contabilizado
        past_ip = ''

        # Contador para enviar commits a base de dados Matomo a cada BUCKET_LIMIT
        bucket_counter = 0

        # Verificador da primeira linha
        line_counter = 0

        for log_row in csv_file:
            bucket_counter += 1
            line_counter += 1

            current_ip = log_row.get('ip', '')
            if line_counter == 1:
                past_ip = current_ip

            if past_ip != current_ip:
                run_counter_routines(hit_manager=hit_manager,
                                     db_session=db_session,
                                     collection=collection)
                past_ip = current_ip

            hit = hit_manager.create_hit_from_log_line(**log_row)
            if hit:
                hit_manager.add_hit(hit)

            # Se atingiu o limit do bucket, faz commit
            if bucket_counter >= MATOMO_DB_SESSION_BUCKET_LIMIT:
                db_session.commit()
                bucket_counter = 0

    db_session.commit()


def run_for_matomo_db(date, hit_manager, idsite, db_session, collection):
    """
    Cria objetos Hit e chama rotinas COUNTER a cada 50 mil (valor definido em BUCKET_LIMIT) iterações.
    Por questões de limitação de memória, o método trabalha por IP.
    Para cada IP, são obtidos os registros a ele relacionados, diretamente da base de dados Matomo

    @param date: data em format YYYY-MM-DD cujos dados serão extraídos
    @param hit_manager: gerenciador de objetos Hit
    @param idsite: id de site na base de dados
    @param db_session: sessão com banco de dados
    @param collection: acrônimo de coleção
    """
    results = db_tools.get_matomo_logs_for_date(db_session=db_session,
                                                idsite=idsite,
                                                date=date)

    # IP atual a ser contabilizado
    past_ip = ''

    # Contador para enviar commits a base de dados Matomo a cada BUCKET_LIMIT
    bucket_counter = 0

    # Verificador da primeira linha
    line_counter = 0
    for row in results:
        bucket_counter += 1
        line_counter += 1

        current_ip = inet_ntoa(row.visit.location_ip)
        if line_counter == 1:
            past_ip = current_ip

        if past_ip != current_ip:
            run_counter_routines(hit_manager=hit_manager,
                                 db_session=db_session,
                                 collection=collection)
            past_ip = current_ip

        hit = hit_manager.create_hit_from_sql_data(row)
        if hit:
            hit_manager.add_hit(hit)

        # Se atingiu o limit do bucket, faz commit
        if bucket_counter >= MATOMO_DB_SESSION_BUCKET_LIMIT:
            db_session.commit()
            bucket_counter = 0

    db_session.commit()


def run_counter_routines(hit_manager: HitManager, db_session, collection):
    """
    Executa métodos COUNTER para remover cliques-duplos, contar acessos por PID e extrair métricas.
    Ao final, salva resultados (métricas) em base de dados

    @param hit_manager: gerenciador de objetos Hit
    @param db_session: sessão com banco de dados
    @param collection: acrônimo de coleção
    """
    hit_manager.remove_double_clicks()
    hit_manager.group_by_pid_format_lang()

    cs = CounterStat()
    cs.calculate_metrics(hit_manager.pid_format_lang_to_hits)
    export_metrics_to_matomo(metrics=cs.metrics, db_session=db_session, collection=collection)
    hit_manager.reset()


def main():
    usage = 'Extrai informações de log no formato COUNTER R5'
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
        '-p', '--pdf_paths',
        dest='pdf_to_pid',
        required=True,
        help='Dicionário que mapeia caminho de PDF a PID'
    )

    parser.add_argument(
        '-a', '--acronyms',
        dest='issn_to_acronym',
        required=True,
        help='Dicionário que mapeia ISSN a acrônimo'
    )

    parser.add_argument(
        '--langs',
        dest='pid_to_format_lang',
        required=True,
        help='Dicionário que mapeia PID a formato e idioma'
    )

    parser.add_argument(
        '-u', '--matomo_db_uri',
        required=True,
        dest='matomo_db_uri',
        help='String de conexão a base SQL Matomo no formato mysql://username:password@host1:port/database'
    )

    parser.add_argument(
        '--logging_level',
        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET'],
        dest='logging_level',
        default='WARNING',
        help='Nível de log'
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
        '--logs',
        dest='logs',
        default=[],
        help='Carrega dados a partir de uma pasta com arquivos de log ou um único arquivo de log '
             'prevhit_managerente extraído(s) do Matomo'
    )

    params = parser.parse_args()

    logging.basicConfig(level=params.logging_level)

    time_start = time()

    logging.info('Carregando dicionário de caminhos de PDF --> PID')
    pdf_to_pid = pickle.load(open(params.pdf_to_pid, 'rb'))

    logging.info('Carregando dicionário de ISSN --> acrônimo de periódico')
    issn_to_acronym = pickle.load(open(params.issn_to_acronym, 'rb'))

    logging.info('Carregando dicionário de PID --> formato --> idioma')
    pid_to_format_lang = pickle.load(open(params.pid_to_format_lang, 'rb'))

    db_session = db_tools.get_db_session(params.matomo_db_uri)
    hit_manager = HitManager(path_pdf_to_pid=pdf_to_pid,
                             issn_to_acronym=issn_to_acronym,
                             pid_to_format_lang=pid_to_format_lang)

    if params.logs:
        logging.info('Iniciado para usar arquivos de log')
        log_files = get_log_files(params.logs)
        for lf in log_files:
            logging.info('Extraindo dados do arquivo {}'.format(lf))
            hit_manager.reset()

            run_for_log_file(log_file=lf,
                             hit_manager=hit_manager,
                             db_session=db_session,
                             collection=params.collection)

    else:
        logging.info('Iniciado para coletar dados diretamente de banco de dados Matomo')
        dates = get_dates(date=params.period)
        for date in dates:
            logging.info('Extraindo dados para data {}'.format(date.strftime('%Y-%m-%d')))
            hit_manager.reset()

            run_for_matomo_db(date=date,
                              hit_manager=hit_manager,
                              idsite=params.idsite,
                              db_session=db_session,
                              collection=params.collection)

    time_end = time()
    logging.info('Durou %.2f segundos' % (time_end - time_start))


if __name__ == '__main__':
    main()

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
from utils import db_tools, dicts, hit_tools as ht
from utils.sql_declarative import Article, ArticleMetric, Localization, ArticleFormat, ArticleLanguage


MATOMO_DB_IP_COUNTER_LIMIT = int(os.environ.get('MATOMO_DB_IP_COUNTER_LIMIT', '100000'))


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


def export_metrics_to_matomo(metrics: dict, db_session, collection: str):
    """
    Exporta métricas para banco de dados

    @param metrics: conjunto de métricas COUNTER
    @param db_session: sessão com banco de dados
    @param collection: acrônimo de coleção
    """
    for pfllly_key, pid_data in metrics.items():
        pid, data_format, lang, latitude, longitude, yop = pfllly_key

        article_format_id = FORMAT_TO_CODE.get(data_format, -1)

        # Caso artigo não esteja no dicionário padrão, tenta obter dado da tabela counter_article_language
        if article_format_id == -1:
            try:
                existing_article_format = db_tools.get_article_format(db_session=db_session,
                                                                      format_name=data_format)
                article_format_id = existing_article_format.format_id

            except NoResultFound:
                new_article_format = ArticleFormat()
                new_article_format.name = data_format

                db_session.add(new_article_format)
                db_session.flush()

                article_format_id = new_article_format.format_id

                logging.info('Adicionado formato {}-{}'.format(new_article_format.format_id,
                                                               new_article_format.name))

        article_language_id = LANG_TO_CODE.get(lang, -1)
        if article_language_id == -1:
            try:
                existing_article_language = db_tools.get_article_language(db_session=db_session,
                                                                          language_name=lang)
                article_language_id = existing_article_language.language_id

            except NoResultFound:
                new_article_language = ArticleLanguage()
                new_article_language.name = lang

                db_session.add(new_article_language)
                db_session.flush()

                article_language_id = new_article_language.language_id

                logging.info('Adicionado idioma {}-{}'.format(new_article_language.language_id,
                                                              new_article_language.name))

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
                    new_article.yop = yop

                    db_session.add(new_article)
                    db_session.flush()

                    existing_article = new_article

                try:
                    # Procura origem do acesso (latitude e longitude) na base de dados
                    existing_localization = db_tools.get_localization(db_session=db_session,
                                                                      latitude=latitude,
                                                                      longitude=longitude)
                except NoResultFound:
                    # Cria uma nova localização
                    new_localization = Localization()
                    new_localization.latitude = latitude
                    new_localization.longitude = longitude

                    db_session.add(new_localization)
                    db_session.flush()

                    existing_localization = new_localization

                try:
                    existing_article_metric = db_tools.get_article_metric(db_session=db_session,
                                                                          year_month_day=ymd,
                                                                          article_id=existing_article.article_id,
                                                                          article_format_id=article_format_id,
                                                                          article_language_id=article_language_id,
                                                                          localization_id=existing_localization.localization_id
                                                                          )

                    update_metrics(existing_article_metric, pid_data[ymd])
                    logging.info('Atualizada métrica {}-{}-{}-{}-{}'.format(existing_article_metric.fk_article_id,
                                                                            existing_article_metric.fk_article_format_id,
                                                                            existing_article_metric.fk_article_language_id,
                                                                            existing_article_metric.fk_localization_id,
                                                                            existing_article_metric.year_month_day))

                except NoResultFound:
                    # Cria um novo registro de métrica, caso não exista na base de dados
                    new_metric_article = ArticleMetric()
                    new_metric_article.fk_article_id = existing_article.article_id
                    new_metric_article.fk_article_format_id = article_format_id
                    new_metric_article.fk_article_language_id = article_language_id
                    new_metric_article.fk_localization_id = existing_localization.localization_id
                    new_metric_article.year_month_day = ymd

                    update_metrics(new_metric_article, pid_data[ymd])

                    db_session.add(new_metric_article)
                    logging.info('Adicionada métrica {}-{}-{}-{}-{}'.format(new_metric_article.fk_article_id,
                                                                            new_metric_article.fk_article_format_id,
                                                                            new_metric_article.fk_article_language_id,
                                                                            new_metric_article.fk_localization_id,
                                                                            new_metric_article.year_month_day))

                db_session.flush()

            except NoResultFound:
                logging.error('Nenhum periódico foi localizado na base de dados: {}'.format(issn))
            except MultipleResultsFound:
                logging.error('Mais de um periódico foi localizado na base de dados: {}'.format(issn))
            except IntegrityError as e:
                db_session.rollback()
                logging.error('Artigo já está na base: {}'.format(e))


def run(data, mode, hit_manager: HitManager, db_session, collection):
    """
    Cria objetos Hit e chama rotinas COUNTER a cada 50 mil (valor definido em BUCKET_LIMIT) iterações.
    Por questões de limitação de memória, o método trabalha por IP.
    Para cada IP, são obtidos os registros a ele relacionados, de tabela pré-extraída da base de dados Matomo

    @param data: arquivo de pré-tabela ou result query
    @param mode: modo de execução (via pretables ou database)
    @param hit_manager: gerenciador de objetos Hit
    @param db_session: sessão com banco de dados
    @param collection: acrônimo de coleção
    """
    # IP atual a ser contabilizado
    past_ip = ''

    # Contador para rodar rotinas counter a cada IP_LIMIT
    ip_counter = 0

    # Contador para enviar commits a base de dados Matomo a cada BUCKET_LIMIT
    bucket_counter = 0

    # Verificador da primeira linha
    line_counter = 0

    for d in data:
        bucket_counter += 1
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
                                     collection=collection)
                ip_counter = 0

            past_ip = current_ip

        if mode == 'pretable':
            hit = hit_manager.create_hit_from_log_line(**d)
        else:
            hit = hit_manager.create_hit_from_sql_data(d)

        if hit:
            hit_manager.add_hit(hit)

        # Se atingiu o limit do bucket, faz commit
        if bucket_counter >= MATOMO_DB_SESSION_BUCKET_LIMIT:
            db_session.commit()
            bucket_counter = 0

    run_counter_routines(hit_manager=hit_manager,
                         db_session=db_session,
                         collection=collection)
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
    hit_manager.group_by_pid_format_lang_localization_yop()

    cs = CounterStat()
    cs.calculate_metrics(hit_manager.pid_format_lang_localization_to_hits)
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
        '--pretables',
        dest='pretables',
        default=[],
        help='Carrega dados a partir de uma pasta com arquivos de pré-tabela ou um único arquivo de pré-tabela '
             'previamente extraído(s) do Matomo'
    )

    params = parser.parse_args()

    logging.basicConfig(filename='r5_' + time().__str__() + '.log', level=params.logging_level)

    time_start = time()

    logging.info('Carregando dicionário de caminhos de PDF --> PID')
    pdf_to_pid = pickle.load(open(params.pdf_to_pid, 'rb'))

    logging.info('Carregando dicionário de ISSN --> acrônimo de periódico')
    issn_to_acronym = pickle.load(open(params.issn_to_acronym, 'rb'))

    logging.info('Carregando dicionário de PID --> formato --> idioma')
    pid_to_format_lang = pickle.load(open(params.pid_to_format_lang, 'rb'))

    logging.info('Carregando dicinoário de PID --> ano de publicação')
    pid_to_yop = pickle.load(open(params.pid_to_yop, 'rb'))

    db_session = db_tools.get_db_session(params.matomo_db_uri)
    hit_manager = HitManager(path_pdf_to_pid=pdf_to_pid,
                             issn_to_acronym=issn_to_acronym,
                             pid_to_format_lang=pid_to_format_lang,
                             pid_to_yop=pid_to_yop)

    if params.pretables:
        logging.info('Iniciado para usar arquivos de pré-tabelas extraídas do Matomo')
        pretables = get_pretables(params.pretables)

        for pt in pretables:
            logging.info('Extraindo dados do arquivo {}'.format(pt))
            hit_manager.reset()

            with open(pt) as data:
                csv_data = csv.DictReader(data, delimiter='\t')
                run(data=csv_data,
                    mode='pretable',
                    hit_manager=hit_manager,
                    db_session=db_session,
                    collection=params.collection)

    else:
        logging.info('Iniciado para coletar dados diretamente de banco de dados Matomo')
        dates = get_dates(date=params.period)

        for date in dates:
            logging.info('Extraindo dados para data {}'.format(date.strftime('%Y-%m-%d')))
            hit_manager.reset()

            db_data = db_tools.get_matomo_logs_for_date(db_session=db_session,
                                                        idsite=params.idsite,
                                                        date=date)

            run(data=db_data,
                mode='database',
                hit_manager=hit_manager,
                db_session=db_session,
                collection=params.collection)

    time_end = time()
    logging.info('Durou %.2f segundos' % (time_end - time_start))


if __name__ == '__main__':
    main()

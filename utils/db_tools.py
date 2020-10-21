import datetime
import logging

from sqlalchemy import create_engine, and_, or_
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from utils.sql_declarative import Base, LogLinkVisitAction, LogAction, LogVisit, Journal, Article, MetricArticle


def create_tables(matomo_db_uri):
    """
    Cria tabelas na base de dados MariaDB

    @param matomo_db_uri: string de conexão à base do Matomo
    """
    engine = create_engine(matomo_db_uri)
    Base.metadata.create_all(engine)


def create_index_ip_on_table_matomo_log_visit(matomo_db_uri):
    """
    Cria índice index_ip para facilitar recuperação de dados baseado em endereço IP, no Matomo

    @param matomo_db_uri: string de conexão à base do Matomo
    """
    engine = create_engine(matomo_db_uri)

    sql_create_index_location_ip = 'CREATE INDEX index_location_ip ON matomo_log_visit (location_ip);'
    try:
        engine.execute(sql_create_index_location_ip)
    except OperationalError:
        logging.warning('Índice já existe: %s' % sql_create_index_location_ip)


def fix_fields_interactions(matomo_db_uri):
    """
    Altera campos visit_total_interactions (matamo_log_visit) e interaction_possition (matomo_log_link_visit_action)
    para MEDIUMINT(5)

    @param matomo_db_uri: string de conexão à base do Matomo
    """
    engine = create_engine(matomo_db_uri)

    sql_alter_column_visit_total_interactions = 'ALTER TABLE matomo_log_visit CHANGE visit_total_interactions visit_total_interactions MEDIUMINT(5) UNSIGNED NULL DEFAULT 0;'
    try:
        engine.execute(sql_alter_column_visit_total_interactions)
    except OperationalError:
        logging.warning('Não foi possível executar: %s' % sql_alter_column_visit_total_interactions)

    sql_alter_column_interaction_position = 'ALTER TABLE matomo_log_link_visit_action CHANGE interaction_position interaction_position MEDIUMINT(5) UNSIGNED NULL DEFAULT NULL;'
    try:
        engine.execute(sql_alter_column_interaction_position)
    except OperationalError:
        logging.warning('Não foi possível executar: %s' % sql_alter_column_interaction_position)


def add_foreign_keys_to_table_matomo_log_link_action(matomo_db_uri):
    """
    Adiciona chaves estrangeiras na tabela matomo_log_link_action já existente do Matomo
     
    @param matomo_db_uri: string de conexão à base do Matomo 
    """
    engine = create_engine(matomo_db_uri)

    sql_foreign_key_idaction = 'ALTER TABLE matomo_log_link_visit_action ADD CONSTRAINT idaction_url FOREIGN KEY(idaction_url) REFERENCES matomo_log_action (idaction);'
    try:
        engine.execute(sql_foreign_key_idaction)
    except OperationalError:
        logging.warning('Chave estrangeira já existe: %s' % sql_foreign_key_idaction)

    sql_foreign_key_idvisit = 'ALTER TABLE matomo_log_link_visit_action ADD CONSTRAINT idvisit FOREIGN KEY(idvisit) REFERENCES matomo_log_visit (idvisit);'
    try:
        engine.execute(sql_foreign_key_idvisit)
    except OperationalError:
        logging.warning('Chave estrangeira já existe: %s' % sql_foreign_key_idvisit)


def get_db_session(matomo_db_uri):
    """
    Obtém uma sessão de conexão com base de dados do Matomo

    @param matomo_db_uri: string de conexão à base do Matomo
    @return: uma sessão de conexão com a base do Matomo
    """
    engine = create_engine(matomo_db_uri)
    Base.metadata.bind = engine
    db_session = sessionmaker(bind=engine)
    return db_session()


def get_matomo_logs_for_date(db_session, idsite: int, date: datetime.datetime):
    """
    Obtém resultados das tabelas originais de log do Matomo para posterior extração de métricas.
    Ordena os resultados por IP

    @param db_session: um objeto Session SQLAlchemy com a base de dados do Matomo
    @param idsite: um inteiro que representa o identificador do site do qual as informações serão extraídas
    @param date: data a ser utilizada como filtro para obtenção dos dados
    @return: resultados em forma de Query
    """
    return db_session \
        .query(LogLinkVisitAction) \
        .filter(and_(LogLinkVisitAction.server_time >= date,
                     LogLinkVisitAction.server_time < date + datetime.timedelta(days=1),
                     LogLinkVisitAction.idsite == idsite)
                ) \
        .join(LogAction, LogAction.idaction == LogLinkVisitAction.idaction_url) \
        .join(LogVisit, LogVisit.idvisit == LogLinkVisitAction.idvisit) \
        .order_by('location_ip')


def get_journal(db_session, issn, collection):
    """
    Obtém periódico a partir de ISSN e coleção

    @param db_session: sessão de conexão com banco Matomo
    @param issn: ISSN do periódico
    @param collection: coleção do periódico
    @return: um resultado do tipo Journal
    """
    return db_session.query(Journal).filter(
        or_(and_(Journal.print_issn == issn, Journal.collection_acronym == collection),
            and_(Journal.online_issn == issn, Journal.collection_acronym == collection))).one()


def get_article(db_session, pid, collection):
    """
    Obtém artigo a partir de PID e coleção

    @param db_session: sessão de conexão com banco Matomo
    @param pid: PID do artigo
    @param collection: coleção do artigo
    @return: um resultado do tipo Article
    """
    return db_session.query(Article).filter(
        and_(Article.collection_acronym == collection,
             Article.pid == pid)).one()


def get_metric_article(db_session, year_month_day, article_id):
    """
    Obtém métrica de artigo a partir de data e id de artigo

    @param db_session: sessão de conexão com banco Matomo
    @param year_month_day: uma data em formato de String
    @param article_id: id de artigo
    @return: um resultado do tipo MetricArticle
    """
    return db_session.query(MetricArticle).filter(
        and_(MetricArticle.year_month_day == year_month_day,
             MetricArticle.fk_article_id == article_id)).one()

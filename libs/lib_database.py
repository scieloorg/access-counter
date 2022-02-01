import datetime
import logging

from libs import lib_status
from sqlalchemy import create_engine, and_, or_
from sqlalchemy.exc import OperationalError, IntegrityError
from sqlalchemy.sql import func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from models.declarative import (
    Base,
    Journal,
    Article,
    ArticleMetric,
    ArticleLanguage,
    ArticleFormat,
    Localization,
    JournalCollection,
    DateStatus,
    AggrStatus,
    AggrJournalGeolocationYearMonthMetric,
)


def create_tables(matomo_db_uri):
    """
    Cria tabelas na base de dados MariaDB

    @param matomo_db_uri: string de conexão à base do Matomo
    """
    engine = create_engine(matomo_db_uri)
    Base.metadata.create_all(engine)

    db_session = sessionmaker(bind=engine)

    _add_basic_article_languages(db_session())
    _add_basic_article_formats(db_session())


def _add_basic_article_languages(db_session):
    for ind, l in enumerate(['pt', 'es', 'en', 'fr', 'de', 'it']):
        art_lang = ArticleLanguage()
        art_lang.language = l
        db_session.add(art_lang)

    try:
        db_session.commit()
    except IntegrityError as ie:
        logging.warning('Idioma já existe: %s' % ie)


def _add_basic_article_formats(db_session):
    for ind, fmt in enumerate(['html', 'pdf']):
        art_fmt = ArticleFormat()
        art_fmt.format = fmt
        db_session.add(art_fmt)

    try:
        db_session.commit()
    except IntegrityError as ie:
        logging.warning('Formato já existe: %s' % ie)


def get_journal_from_issns(db_session, issns):
    """
    Obtém periódico a partir de ISSN e coleção

    @param db_session: sessão de conexão com banco Matomo
    @param issns: lista de ISSNs do periódico
    @return: um resultado do tipo Journal
    """
    for i in issns:
        try:
            journal = get_journal(db_session, i)
            if journal:
                return journal
        except NoResultFound:
            pass
    raise NoResultFound()


def get_journal_collection(db_session, collection, journal_id):
    """
    Obtém um registro de coleção de periódico a partir de id de periódico e coleção
    :param db_session: sessão de conexão com banco Matomo
    :param collection: coleção do periódico
    :param journal_id: ID de um periódico
    :return: um resultado do tipo JournalCollection
    """
    return db_session.query(JournalCollection).filter(and_(JournalCollection.collection == collection,
                                                           JournalCollection.idjournal_jc == journal_id)).one()


def get_journal(db_session, issn):
    """
    Obtém periódico a partir de ISSN e coleção

    @param db_session: sessão de conexão com banco Matomo
    @param issn: ISSN do periódico
    @return: um resultado do tipo Journal
    """
    return db_session.query(Journal).filter(or_(Journal.print_issn == issn,
                                                Journal.online_issn == issn,
                                                Journal.pid_issn == issn)).one()


def get_article(db_session, pid, collection):
    """
    Obtém artigo a partir de PID e coleção

    @param db_session: sessão de conexão com banco Matomo
    @param pid: PID do artigo
    @param collection: coleção do artigo
    @return: um resultado do tipo Article
    """
    return db_session.query(Article).filter(
        and_(Article.collection == collection,
             Article.pid == pid)).one()


def get_article_metric(db_session, year_month_day, article_id, article_format_id, article_language_id, localization_id):
    """
    Obtém métrica de artigo a partir de data, id de artigo, id de formato e id de idioma

    @param db_session: sessão de conexão com banco Matomo
    @param year_month_day: uma data em formato de String
    @param article_id: id de artigo
    @param article_format_id: id de formato do artigo
    @param article_language_id: id de idioma do artigo
    @param localization_id: latitude|longitude de origem do acesso
    @return: um resultado do tipo MetricArticle
    """
    return db_session.query(ArticleMetric).filter(
        and_(ArticleMetric.year_month_day == year_month_day,
             ArticleMetric.idarticle == article_id,
             ArticleMetric.idformat == article_format_id,
             ArticleMetric.idlanguage == article_language_id,
             ArticleMetric.idlocalization == localization_id)).one()


def get_localization(db_session, latitude, longitude):
    """
    Obtém um par latitude longitude a partir de latitude e longitude

    @param db_session: sessão de conexão com banco Matomo
    @param latitude: decimal representante da latitude
    @param longitude: decimal representante da longitude
    @return: um resultado do tipo ArticleLocalization
    """
    return db_session.query(Localization).filter(and_(
        Localization.latitude == latitude,
        Localization.longitude == longitude)).one()


def get_article_format(db_session, format_name):
    """
    Obtém um formato

    @param db_session: sessão de conexão com banco Matomo
    @param format_name: nome do formato
    @return: um resultado do tipo ArticleFormat
    """
    return db_session.query(ArticleFormat).filter(ArticleFormat.format == format_name).one()


def get_article_language(db_session, language_name):
    """
    Obtém um par latitude longitude a partir de latitude e longitude

    @param db_session: sessão de conexão com banco Matomo
    @param language_name: um nome abreviado de idioma
    @return: um resultado do tipo ArticleLanguage
    """
    return db_session.query(ArticleLanguage).filter(ArticleLanguage.language == language_name).one()


def get_last_id(db_session, table_class):
    """
    Obtém o maior valor de ID de uma tabela
    :param db_session:  sessão de conexão com banco de dados
    :param table_class: uma classe que representa a tabela
    :return: um inteiro que representa o último ID associado à tabela
    """
    return db_session.query(func.max(table_class.id)).scalar()


def get_date_status(db_session, collection, date):
    try:
        existing_date = db_session.query(DateStatus).filter(and_(DateStatus.collection == collection,
                                                                 DateStatus.date == date)).one()
        return existing_date.status
    except NoResultFound:
        return ''


def get_missing_aggregations(db_session, collection, date):
    try:
        missing_aggregations = []

        existing_date = db_session.query(DateStatus).filter(and_(DateStatus.collection == collection,
                                                                 DateStatus.date == date)).one()

        for i_name in ['status_counter_article_metric',
                       'status_counter_journal_metric',
                       'status_sushi_article_metric',
                       'status_sushi_journal_metric',
                       'status_sushi_journal_yop_metric']:
            i_value = getattr(existing_date, i_name)
            if i_value == 0:
                missing_aggregations.append(i_name.replace('status_', ''))

        return missing_aggregations
    except NoResultFound:
        return []
    except OperationalError:
        return []


def compute_date_metric_status(db_session, collection, date):
    try:
        existing_date = db_session.query(DateStatus).filter(and_(DateStatus.collection == collection,
                                                                 DateStatus.date == date)).one()

        return sum([existing_date.status_counter_article_metric,
                    existing_date.status_counter_journal_metric,
                    existing_date.status_sushi_article_metric,
                    existing_date.status_sushi_journal_metric,
                    existing_date.status_sushi_journal_yop_metric])
    except NoResultFound:
        return
    except OperationalError:
        return


def update_date_metric_status(db_session, collection, date, metric, status):
    try:
        existing_date_status = db_session.query(DateStatus).filter(and_(DateStatus.collection == collection,
                                                                        DateStatus.date == date)).one()

        current_metric_status = getattr(existing_date_status, metric)
        if current_metric_status != status:
            logging.info('Changing status of control_date_status.%s=%s from %s to %s' % (metric,
                                                                                         date,
                                                                                         current_metric_status,
                                                                                         status))
            setattr(existing_date_status, metric, status)

        db_session.commit()
    except NoResultFound:
        pass
    except OperationalError:
        logging.error('Error while trying to change metric status')


def update_date_status(db_session, collection, date, status):
    try:
        existing_date_status = db_session.query(DateStatus).filter(and_(DateStatus.collection == collection,
                                                                        DateStatus.date == date)).one()
        if existing_date_status.status != status:
            logging.info('Changing status of control_date_status.date=%s from %s to %s' % (date, existing_date_status.status, status))
            existing_date_status.status = status

        db_session.commit()
    except NoResultFound:
        pass
    except OperationalError:
        logging.error('Error while trying to update date status')


def get_aggr_status(db_session, collection, date, status_column_name):
    try:
        date_status = get_date_status(db_session, collection, date)

        if date_status == lib_status.DATE_STATUS_COMPLETED:
            try:
                object_aggr_status = db_session.query(AggrStatus).filter(and_(AggrStatus.collection == collection, AggrStatus.date == date)).one()
                return getattr(object_aggr_status, status_column_name)

            except NoResultFound as e:
                obj_aggr_status = AggrStatus()
                obj_aggr_status.collection = collection
                obj_aggr_status.date = date

                db_session.add(obj_aggr_status)
                db_session.commit()
                db_session.flush()

                return lib_status.AGGR_STATUS_QUEUE

        else:
            ...

    except OperationalError as e:
        raise e


def update_aggr_status_for_table(db_session, collection, date, status, table_name):
    try:
        existing_aggr_status = db_session.query(AggrStatus).filter(and_(AggrStatus.collection == collection,
                                                                        AggrStatus.date == date)).one()

        setattr(existing_aggr_status, table_name, status)

        db_session.commit()
        db_session.flush()
    except NoResultFound:
        pass
    except OperationalError:
        logging.error('Error while trying to update aggr status')


def extract_aggregated_data_for_article_language_year_month(database_uri, collection, date):
    raw_query = '''
    INSERT INTO
        aggr_article_language_year_month_metric (
            collection,
            article_id,
            language_id,
            `year_month`,
            total_item_requests,
            total_item_investigations,
            unique_item_requests,
            unique_item_investigations
        )
        SELECT
            ca.collection,
            cam.idarticle,
            cam.idlanguage,
            substr(cam.year_month_day, 1, 7) AS ym,
            sum(cam.total_item_requests) AS tir,
            sum(cam.total_item_investigations) AS tii,
            sum(cam.unique_item_requests) AS uir,
            sum(cam.unique_item_investigations) AS uii
        FROM
            counter_article_metric cam
        LEFT JOIN
            counter_article ca ON ca.id = cam.idarticle
        LEFT JOIN
            counter_journal cj ON cj.id = ca.idjournal_a
        LEFT JOIN
            counter_journal_collection cjc ON cjc.idjournal_jc = cj.id
        WHERE
            year_month_day = '{0}' AND
            cjc.collection = '{1}'
        GROUP BY
            ca.collection,
            cam.idarticle,
            cam.idlanguage,
            ym
    ON DUPLICATE KEY UPDATE
        total_item_requests = total_item_requests + VALUES(total_item_requests),
        total_item_investigations = total_item_investigations + VALUES(total_item_investigations),
        unique_item_requests = unique_item_requests + VALUES(unique_item_requests),
        unique_item_investigations = unique_item_investigations + VALUES(unique_item_investigations)
    ;
    '''.format(date, collection)
    engine = create_engine(database_uri)
    return engine.execute(raw_query)


def extract_aggregated_data_for_journal_language_year_month(database_uri, collection, date):
    raw_query = '''
    INSERT INTO
        aggr_journal_language_year_month_metric (
            collection,
            journal_id,
            language_id,
            `year_month`,
            total_item_requests,
            total_item_investigations,
            unique_item_requests,
            unique_item_investigations
        )
        SELECT
            cjc.collection,
            cj.id,
            cam.idlanguage,
            substr(cam.year_month_day, 1, 7) AS ym,
            sum(cam.total_item_requests) AS tir,
            sum(cam.total_item_investigations) AS tii,
            sum(cam.unique_item_requests) AS uir,
            sum(cam.unique_item_investigations) AS uii
        FROM
            counter_article_metric cam
        LEFT JOIN
            counter_article ca ON ca.id = cam.idarticle
        LEFT JOIN
            counter_journal cj ON cj.id = ca.idjournal_a
        LEFT JOIN
            counter_journal_collection cjc ON cjc.idjournal_jc = cj.id
        WHERE
            year_month_day = '{0}' AND
            cjc.collection = '{1}'
        GROUP BY
            cjc.collection,
            cj.id,
            cam.idlanguage,
            ym
    ON DUPLICATE KEY UPDATE
        total_item_requests = total_item_requests + VALUES(total_item_requests),
        total_item_investigations = total_item_investigations + VALUES(total_item_investigations),
        unique_item_requests = unique_item_requests + VALUES(unique_item_requests),
        unique_item_investigations = unique_item_investigations + VALUES(unique_item_investigations)
    ;
    '''.format(date, collection)
    engine = create_engine(database_uri)
    return engine.execute(raw_query)


def get_aggregated_data_for_journal_geolocation_year_month(database_uri, collection, date):
    raw_query = '''
    SELECT
        cjc.collection,
        cj.id,
        cl.latitude,
        cl.longitude,
        substr(cam.year_month_day, 1, 7) AS ym,
        sum(cam.total_item_requests) AS tir,
        sum(cam.total_item_investigations) AS tii,
        sum(cam.unique_item_requests) AS uir,
        sum(cam.unique_item_investigations) AS uii
    FROM
        counter_article_metric cam
    LEFT JOIN
        counter_article ca ON ca.id = cam.idarticle
    LEFT JOIN
        counter_journal cj ON cj.id = ca.idjournal_a
    LEFT JOIN
        counter_journal_collection cjc ON cjc.idjournal_jc = cj.id
    LEFT JOIN
        counter_localization cl ON cam.idlocalization = cl.id 
    WHERE
        year_month_day = '{0}' AND
        cjc.collection = '{1}'
    GROUP BY
        cjc.collection,
        cj.id,
        cl.id,
        ym
    ;
    '''.format(date, collection)
    engine = create_engine(database_uri)

    return engine.execute(raw_query)
def get_dates_able_to_extract(db_session, collection, number_of_days):
    dates = []

    try:
        ds_results = db_session.query(DateStatus).filter(and_(DateStatus.collection == collection,
                                                              DateStatus.status >= lib_status.DATE_STATUS_LOADED)).order_by(DateStatus.date.desc())

        date_to_status = {}
        for r in ds_results:
            date_to_status[r.date] = r.status

        days_counter = 0
        for date, status in date_to_status.items():
            arround_days = [date + datetime.timedelta(days=-2),
                            date + datetime.timedelta(days=-1),
                            date + datetime.timedelta(days=1)]

            is_valid_for_extracting = True
            for ad in arround_days:
                if ad not in date_to_status:
                    is_valid_for_extracting = False
                    break

            if status == lib_status.DATE_STATUS_LOADED and is_valid_for_extracting:
                dates.append(date)
                days_counter += 1

                if days_counter >= number_of_days:
                    break

    except NoResultFound:
        logging.info('There are no dates to be extracted')
        dates = []

    except OperationalError:
        logging.error('Error while trying to obtain date to be extracted')
        dates = []

    return sorted(dates, reverse=True)

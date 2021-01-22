from sqlalchemy import Column, ForeignKey, Integer, UniqueConstraint, Index, Date, DateTime, DECIMAL
from sqlalchemy.dialects.mysql import BIGINT, BINARY, INTEGER, TINYINT, VARBINARY, VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


Base = declarative_base()


class Journal(Base):
    __tablename__ = 'counter_journal'

    __table_args__ = (UniqueConstraint('print_issn', 'online_issn', 'pid_issn', name='uni_issn'),)
    __table_args__ += (Index('idx_print_issn', 'print_issn'),)
    __table_args__ += (Index('idx_online_issn', 'online_issn'),)
    __table_args__ += (Index('idx_pid_issn', 'pid_issn'),)

    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    print_issn = Column(VARCHAR(9), nullable=False)
    online_issn = Column(VARCHAR(9), nullable=False)
    pid_issn = Column(VARCHAR(9), nullable=False)


class JournalCollection(Base):
    __tablename__ = 'counter_journal_collection'
    __table_args__ = (UniqueConstraint('collection', 'idjournal_jc', name='uni_col_jou'),)

    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    collection = Column(VARCHAR(3), nullable=False)
    title = Column(VARCHAR(255), nullable=False)
    uri = Column(VARCHAR(255))
    publisher_name = Column(VARCHAR(255))
    idjournal_jc = Column(INTEGER(unsigned=True), ForeignKey('counter_journal.id', name='idjournal_jc'))


class ArticleLanguage(Base):
    __tablename__ = 'counter_article_language'
    __table_args__ = (UniqueConstraint('language', name='uni_lang'),)

    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    language = Column(VARCHAR(10), nullable=False)


class ArticleFormat(Base):
    __tablename__ = 'counter_article_format'
    __table_args__ = (UniqueConstraint('format', name='uni_fmt'),)

    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    format = Column(VARCHAR(10), nullable=False)


class Article(Base):
    __tablename__ = 'counter_article'
    __table_args__ = (UniqueConstraint('collection', 'pid', name='uni_col_pid'),)
    __table_args__ += (Index('idx_col_pid_jou_yop', 'collection', 'pid', 'idjournal_a', 'yop'),)

    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    collection = Column(VARCHAR(3), nullable=False)
    pid = Column(VARCHAR(23), nullable=False)
    yop = Column(INTEGER(4))

    idjournal_a = Column(INTEGER(unsigned=True), ForeignKey('counter_journal.id', name='idjournal_a'))
    journal = relationship(Journal)


class ArticleMetric(Base):
    __tablename__ = 'counter_article_metric'
    __table_args__ = (UniqueConstraint('year_month_day', 'idarticle', 'idformat', 'idlanguage', 'idlocalization', name='uni_date_art_all'),)
    __table_args__ += (Index('idx_date_art_all', 'year_month_day', 'idarticle', 'idformat', 'idlanguage', 'idlocalization'),)

    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)

    idarticle = Column(INTEGER(unsigned=True), ForeignKey('counter_article.id', name='idarticle'))
    article = relationship(Article)

    idformat = Column(INTEGER(unsigned=True), ForeignKey('counter_article_format.id', name='idformat'))
    idlanguage = Column(INTEGER(unsigned=True), ForeignKey('counter_article_language.id', name='idlanguage'))
    idlocalization = Column(INTEGER(unsigned=True), ForeignKey('counter_localization.id', name='idlocalization'))

    year_month_day = Column(Date, nullable=False)
    total_item_requests = Column(Integer, nullable=False)
    total_item_investigations = Column(Integer, nullable=False)
    unique_item_requests = Column(Integer, nullable=False)
    unique_item_investigations = Column(Integer, nullable=False)


class Localization(Base):
    __tablename__ = 'counter_localization'
    __table_args__ = (UniqueConstraint('latitude', 'longitude', name='uni_loc'),)
    __table_args__ += (Index('idx_loc', 'latitude', 'longitude'),)

    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    latitude = Column(DECIMAL(9, 6))
    longitude = Column(DECIMAL(9, 6))


class JournalMetric(Base):
    __tablename__ = 'counter_journal_metric'
    __table_args__ = (UniqueConstraint('year_month_day', 'idformat_cjm', 'idlanguage_cjm', 'idjournal_cjm', 'yop', name='uni_date_all_cjm'),)
    __table_args__ += (Index('idx_date_all_cjm', 'year_month_day', 'idformat_cjm', 'idlanguage_cjm', 'yop', 'idjournal_cjm'),)

    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)

    idformat_cjm = Column(INTEGER(unsigned=True), ForeignKey('counter_article_format.id', name='idformat_cjm'))
    idlanguage_cjm = Column(INTEGER(unsigned=True), ForeignKey('counter_article_language.id', name='idlanguage_cjm'))
    idjournal_cjm = Column(INTEGER(unsigned=True), ForeignKey('counter_journal.id', name='idjournal_cjm'))

    yop = Column(INTEGER(4))
    year_month_day = Column(Date, nullable=False)

    total_item_requests = Column(Integer, nullable=False)
    total_item_investigations = Column(Integer, nullable=False)
    unique_item_requests = Column(Integer, nullable=False)
    unique_item_investigations = Column(Integer, nullable=False)


class SushiJournalYOPMetric(Base):
    __tablename__ = 'sushi_journal_yop_metric'
    __table_args__ = (UniqueConstraint('year_month_day', 'yop', 'idjournal_sjym', name='uni_date_yop_jou_sjym'),)
    __table_args__ += (Index('idx_date_yop_sjym', 'year_month_day', 'yop', 'idjournal_sjym'),)

    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)

    idjournal_sjym = Column(INTEGER(unsigned=True), ForeignKey('counter_journal.id', name='idjournal_sjym'))
    yop = Column(INTEGER(4))
    year_month_day = Column(Date, nullable=False)

    total_item_requests = Column(Integer, nullable=False)
    total_item_investigations = Column(Integer, nullable=False)
    unique_item_requests = Column(Integer, nullable=False)
    unique_item_investigations = Column(Integer, nullable=False)


class SushiJournalMetric(Base):
    __tablename__ = 'sushi_journal_metric'
    __table_args__ = (UniqueConstraint('year_month_day', 'idjournal_sjm', name='uni_date_jou_sjm'),)
    __table_args__ += (Index('idx_date_sjm', 'year_month_day', 'idjournal_sjm'),)

    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)

    idjournal_sjm = Column(INTEGER(unsigned=True), ForeignKey('counter_journal.id', name='idjournal_sjm'))
    year_month_day = Column(Date, nullable=False)

    total_item_requests = Column(Integer, nullable=False)
    total_item_investigations = Column(Integer, nullable=False)
    unique_item_requests = Column(Integer, nullable=False)
    unique_item_investigations = Column(Integer, nullable=False)


class LogAction(Base):
    __tablename__ = 'matomo_log_action'

    __table_args__ = (Index('index_type_hash',
                            'type',
                            'hash'),)

    idaction = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    name = Column(VARCHAR(4096))
    hash = Column(INTEGER(unsigned=True), nullable=False)
    type = Column(TINYINT(unsigned=True))
    url_prefix = Column(TINYINT(2))


class LogVisit(Base):
    __tablename__ = 'matomo_log_visit'

    __table_args__ = (Index('index_idsite_idvisitor',
                            'idsite',
                            'idvisitor'),)

    idvisit = Column(BIGINT(10, unsigned=True), primary_key=True, autoincrement=True)
    idsite = Column(INTEGER(10, unsigned=True))
    idvisitor = Column(BINARY(8), nullable=False)
    config_browser_name = Column(VARCHAR(10))
    config_browser_version = Column(VARCHAR(20))
    location_ip = Column(VARBINARY(16), nullable=False)
    location_latitude = Column(DECIMAL(9, 6))
    location_longitude = Column(DECIMAL(9, 6))


class LogLinkVisitAction(Base):
    __tablename__ = 'matomo_log_link_visit_action'

    __table_args__ = (Index('index_idsite_servertime',
                            'idsite',
                            'server_time'),)

    __table_args__ += (Index('index_idvisit',
                             'idvisit'),)

    idlink_va = Column(BIGINT(10, unsigned=True), primary_key=True, autoincrement=True)
    idsite = Column(INTEGER(unsigned=True), nullable=False)
    idvisitor = Column(BINARY(8), nullable=False)
    server_time = Column(DateTime, nullable=False)

    idaction_url = Column(INTEGER(unsigned=True), ForeignKey('matomo_log_action.idaction', name='idaction_url'))
    action = relationship(LogAction)

    idvisit = Column(BIGINT(10, unsigned=True), ForeignKey('matomo_log_visit.idvisit', name='idvisit'))
    visit = relationship(LogVisit)

from sqlalchemy import Column, ForeignKey, Integer, UniqueConstraint, Index, Date, DECIMAL
from sqlalchemy.dialects.mysql import  BOOLEAN, INTEGER, VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


Base = declarative_base()


class DateStatus(Base):
    __tablename__ = 'control_date_status'
    __table_args__ = (UniqueConstraint('collection', 'date', name='uni_collection_date'), )

    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)

    date = Column(Date, nullable=False, index=True)
    collection = Column(VARCHAR(3), nullable=False)
    status = Column(INTEGER, default=0)

    status_counter_article_metric = Column(BOOLEAN, default=False)
    status_counter_journal_metric = Column(BOOLEAN, default=False)
    status_sushi_article_metric = Column(BOOLEAN, default=False)
    status_sushi_journal_metric = Column(BOOLEAN, default=False)
    status_sushi_journal_yop_metric = Column(BOOLEAN, default=False)


class AggrStatus(Base):
    __tablename__ = 'aggr_status'
    __table_args__ = (UniqueConstraint('collection', 'date', name='uni_collection_date'), )
    collection = Column(VARCHAR(3), nullable=False, primary_key=True)
    date = Column(Date, nullable=False, primary_key=True)

    status_aggr_article_language_year_month_metric = Column(BOOLEAN, default=False)
    status_aggr_journal_language_year_month_metric = Column(BOOLEAN, default=False)
    status_aggr_journal_geolocation_year_month_metric = Column(BOOLEAN, default=False)


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
    pid = Column(VARCHAR(128), nullable=False)
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
    __table_args__ = (UniqueConstraint('year_month_day', 'collection', 'idformat_cjm', 'idlanguage_cjm', 'idjournal_cjm', 'yop', name='uni_col_date_all_cjm'),)
    __table_args__ += (Index('idx_col_date_all_cjm', 'collection', 'year_month_day', 'idformat_cjm', 'idlanguage_cjm', 'yop', 'idjournal_cjm'),)

    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)

    collection = Column(VARCHAR(3), nullable=False)

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
    __table_args__ = (UniqueConstraint('year_month_day', 'collection', 'yop', 'idjournal_sjym', name='uni_col_date_yop_jou_sjym'),)
    __table_args__ += (Index('idx_col_date_yop_sjym', 'collection', 'year_month_day', 'yop', 'idjournal_sjym'),)

    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)

    collection = Column(VARCHAR(3), nullable=False)

    idjournal_sjym = Column(INTEGER(unsigned=True), ForeignKey('counter_journal.id', name='idjournal_sjym'))
    yop = Column(INTEGER(4))
    year_month_day = Column(Date, nullable=False)

    total_item_requests = Column(Integer, nullable=False)
    total_item_investigations = Column(Integer, nullable=False)
    unique_item_requests = Column(Integer, nullable=False)
    unique_item_investigations = Column(Integer, nullable=False)


class SushiJournalMetric(Base):
    __tablename__ = 'sushi_journal_metric'
    __table_args__ = (UniqueConstraint('year_month_day', 'collection', 'idjournal_sjm', name='uni_col_date_jou_sjm'),)
    __table_args__ += (Index('idx_col_date_sjm', 'collection', 'year_month_day', 'idjournal_sjm'),)

    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)

    collection = Column(VARCHAR(3), nullable=False)

    idjournal_sjm = Column(INTEGER(unsigned=True), ForeignKey('counter_journal.id', name='idjournal_sjm'))
    year_month_day = Column(Date, nullable=False)

    total_item_requests = Column(Integer, nullable=False)
    total_item_investigations = Column(Integer, nullable=False)
    unique_item_requests = Column(Integer, nullable=False)
    unique_item_investigations = Column(Integer, nullable=False)


class SushiArticleMetric(Base):
    __tablename__ = 'sushi_article_metric'
    __table_args__ = (UniqueConstraint('year_month_day', 'idarticle_sam', name='uni_date_art_sam'),)
    __table_args__ += (Index('idx_date_sam', 'year_month_day', 'idarticle_sam'),)

    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)

    idarticle_sam = Column(INTEGER(unsigned=True), ForeignKey('counter_article.id', name='idarticle_sam'))
    year_month_day = Column(Date, nullable=False)

    total_item_requests = Column(Integer, nullable=False)
    total_item_investigations = Column(Integer, nullable=False)
    unique_item_requests = Column(Integer, nullable=False)
    unique_item_investigations = Column(Integer, nullable=False)


class AggrArticleLanguageYearMonthMetric(Base):
    __tablename__ = 'aggr_article_language_year_month_metric'
    __table_args__ = (UniqueConstraint('year_month', 'article_id', 'language_id', name='uni_art_lan_aalymm'),)
    __table_args__ += (Index('idx_ym_id', 'year_month', 'article_id'),)

    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)

    collection = Column(VARCHAR(3), nullable=False, primary_key=True)
    article_id = Column(INTEGER(unsigned=True), ForeignKey('counter_article.id', name='idarticle_aalymm'))
    language_id = Column(INTEGER(unsigned=True), ForeignKey('counter_article_language.id', name='idlanguage_aalymm'))
    year_month = Column(VARCHAR(7), nullable=False)

    total_item_requests = Column(Integer, nullable=False)
    total_item_investigations = Column(Integer, nullable=False)
    unique_item_requests = Column(Integer, nullable=False)
    unique_item_investigations = Column(Integer, nullable=False)


class AggrJournalLanguageYearMonthMetric(Base):
    __tablename__ = 'aggr_journal_language_year_month_metric'
    __table_args__ = (UniqueConstraint('year_month', 'journal_id', 'language_id', name='uni_jou_lan_ajlymm'),)
    __table_args__ += (Index('idx_ym_id', 'year_month', 'journal_id'),)

    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)

    collection = Column(VARCHAR(3), nullable=False, primary_key=True)
    journal_id = Column(INTEGER(unsigned=True), ForeignKey('counter_journal.id', name='idjournal_ajlymm'))
    language_id = Column(INTEGER(unsigned=True), ForeignKey('counter_article_language.id', name='idlanguage_ajlymm'))
    year_month = Column(VARCHAR(7), nullable=False)

    total_item_requests = Column(Integer, nullable=False)
    total_item_investigations = Column(Integer, nullable=False)
    unique_item_requests = Column(Integer, nullable=False)
    unique_item_investigations = Column(Integer, nullable=False)


class AggrJournalGeolocationYearMonthMetric(Base):
    __tablename__ = 'aggr_journal_geolocation_year_month_metric'
    __table_args__ = (UniqueConstraint('year_month', 'journal_id', 'country_code', name='uni_jou_geo_ajlymm'),)
    __table_args__ += (Index('idx_ym_id', 'year_month', 'journal_id'),)

    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)

    collection = Column(VARCHAR(3), nullable=False, primary_key=True)
    journal_id = Column(INTEGER(unsigned=True), ForeignKey('counter_journal.id', name='idjournal_ajlymm'))
    country_code = Column(VARCHAR(4), nullable=False)
    year_month = Column(VARCHAR(7), nullable=False)

    total_item_requests = Column(Integer, nullable=False)
    total_item_investigations = Column(Integer, nullable=False)
    unique_item_requests = Column(Integer, nullable=False)
    unique_item_investigations = Column(Integer, nullable=False)

from sqlalchemy import Column, ForeignKey, Integer, String, UniqueConstraint, Index, Date, SmallInteger, BigInteger, DateTime, Binary
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


Base = declarative_base()


class Journal(Base):
    __tablename__ = 'counter_journal'
    __table_args__ = (UniqueConstraint('collection_acronym', 'print_issn', 'online_issn', 
                                       name='uni_col_print_issn_online_issn'),)
    __table_args__ += (Index('index_print_issn', 'print_issn'),)
    __table_args__ += (Index('index_print_online', 'online_issn'),)

    journal_id = Column(Integer, primary_key=True, autoincrement=True)
    collection_acronym = Column(String(3), nullable=False)
    title = Column(String(255), nullable=False)
    print_issn = Column(String(9))
    online_issn = Column(String(9))
    uri = Column(String(255))
    publisher_name = Column(String(255))


class Article(Base):
    __tablename__ = 'counter_article'
    __table_args__ = (UniqueConstraint('collection_acronym', 'pid', name='uni_col_pid'),)
    __table_args__ += (Index('index_col_pid', 'collection_acronym', 'pid'),)

    article_id = Column(Integer, primary_key=True, autoincrement=True)
    collection_acronym = Column(String(3), nullable=False)
    pid = Column(String(23), nullable=False)
    fk_journal_id = Column(Integer, ForeignKey('counter_journal.journal_id', name='fk_journal_id'))
    journal = relationship(Journal)


class MetricArticle(Base):
    __tablename__ = 'counter_metric_article'
    __table_args__ = (UniqueConstraint('fk_article_id', 'year_month_day'),)
    __table_args__ += (Index('index_year_month_day', 'year_month_day'),)

    metric_id = Column(Integer, primary_key=True, autoincrement=True)
    fk_article_id = Column(Integer, ForeignKey('counter_article.article_id', name='fk_article_id'))
    article = relationship(Article)
    year_month_day = Column(Date, nullable=False)
    total_item_requests = Column(Integer, nullable=False)
    total_item_investigations = Column(Integer, nullable=False)
    unique_item_requests = Column(Integer, nullable=False)
    unique_item_investigations = Column(Integer, nullable=False)


class LogAction(Base):
    __tablename__ = 'matomo_log_action'
    __table_args__ = (Index('index_type_hash', 'type', 'hash'),)

    idaction = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(4096))
    hash = Column(Integer, nullable=False)
    type = Column(SmallInteger)
    url_prefix = Column(SmallInteger)


class LogVisit(Base):
    __tablename__ = 'matomo_log_visit'

    idvisit = Column(BigInteger, primary_key=True, autoincrement=True)
    idvisitor = Column(Binary)
    config_browser_name = Column(String(10))
    config_browser_version = Column(String(20))
    location_ip = Column(Binary)


class LogLinkVisitAction(Base):
    __tablename__ = 'matomo_log_link_visit_action'
    __table_args__ = (Index('index_visit','idvisit'),)
    __table_args__ += (Index('index_idsite_servertime', 'idsite', 'server_time'),)

    idlink_va = Column(BigInteger, primary_key=True, autoincrement=True)
    idsite = Column(Integer)
    server_time = Column(DateTime)

    idaction_url = Column(Integer, ForeignKey('matomo_log_action.idaction', name='idaction_url'))
    action = relationship(LogAction)

    idvisit = Column(Integer, ForeignKey('matomo_log_visit.idvisit', name='idvisit'))
    visit = relationship(LogVisit)

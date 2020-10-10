import logging

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from utils.sql_declarative import Base


def create_tables(matomo_db_uri):
    """
    Cria tabelas na base de dados MariaDB

    @param matomo_db_uri: string de conexão à base do Matomo
    """
    engine = create_engine(matomo_db_uri)
    Base.metadata.create_all(engine)
    

def add_foreign_keys_to_table_matomo_log_link_action(matomo_db_uri):
    """
    Adiciona chaves estrangeiras na tabela matomo_log_link_actino já existente do Matomo
     
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

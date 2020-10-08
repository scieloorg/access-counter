from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from utils.sql_declarative import Base


def create_tables(mariadb_uri):
    """
    Cria tabelas na base de dados MariaDB

    @param mariadb_uri: string de conexão à base MariaDB
    """
    engine = create_engine(mariadb_uri)
    Base.metadata.create_all(engine)


def get_db_session(mariadb_uri):
    """
    Obtém uma sessão de conexão com base de dados MariaDB

    @param mariadb_uri: string de conexão à base MariaDB
    @return: uma sessão de conexão com a base MariaDB
    """
    engine = create_engine(mariadb_uri)
    Base.metadata.bind = engine
    db_session = sessionmaker(bind=engine)
    return db_session()

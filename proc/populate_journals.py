import argparse
import logging
import os
import re

from articlemeta.client import RestfulClient, ThriftClient
from sqlalchemy.sql import null
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from urllib import parse
from utils import dicts
from libs import lib_database
from utils.regular_expressions import REGEX_ISSN
from models.declarative import Journal, JournalCollection


MATOMO_DATABASE_STRING = os.environ.get('MATOMO_DATABASE_STRING', 'mysql://user:pass@localhost:3306/matomo')
LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', 'INFO')
ENGINE = create_engine(MATOMO_DATABASE_STRING, pool_recycle=1800)
SESSION_FACTORY = sessionmaker(bind=ENGINE)


def format_publisher_names(publisher_names: list):
    """
    Caso um periódico tenha mais de um publisher a ele associado, separa os seus nomes por ponto e vírgula

    @param publisher_names: lista de nomes dos publishers associados a um periódico
    @return: string contendo os nomes dos publishers separados por ponto e vírgula
    """
    if publisher_names:
        names_without_semicolon = [n.replace(';', ',') for n in publisher_names]
        return '; '.join(names_without_semicolon)
    return ''


def format_issn(issn: str):
    """
    Procura por padrão correto de ISSN e retorna o primeiro match encontrado.
    Caso não encontre ISSN, retorna string vazia

    @param issn: ISSN a ser tratado
    @return: ISSN tratado ou string vazia
    """
    if issn:
        search_results = re.search(REGEX_ISSN, issn)
        if search_results:
            return issn[search_results.start():search_results.end()]
        else:
            if len(issn) == 8:
                return ''.join([issn[:4], '-', issn[4:]])
    return ''


def extract_issn_from_url(journal):
    """
    Extrai issn de periódico com base na URL e do parâmetro pid

    @param journal: objeto Journal
    @return: issn para usar em pid_issn
    """
    parsed_url = dict(parse.parse_qsl(parse.urlsplit(journal.url()).query))
    pid_issn = parsed_url.get('pid', '')
    return format_issn(pid_issn)


def extract_url(journal):
    """
    Extrai url de periódico. Caso não encontra nenhuma válida, retorna sqlalchemy.sql.null()

    @param journal: objeto Journal obtido do RestfulClient
    @return: url
    """
    if journal.institutional_url:
        return journal.institutional_url
    if journal.url():
        return journal.url()
    return null()


def update_journal_issn(journal, issn_type, new_value):
    """
    Atualiza informações de periódico

    @param journal: um objeto Journal
    @param issn_type: tipo de ISSN
    @param new_value: novo valor do ISSN
    """
    current_value = getattr(journal, issn_type)
    if new_value:
        if current_value:
            if new_value != current_value:
                logging.error('Houston, we have a problem (%s) %s != %s' % (issn_type, new_value, current_value))
        else:
            setattr(journal, issn_type, new_value)
            logging.info('Atualizado periódico {} ({}), de {} para {}'.format(journal.id,
                                                                              issn_type,
                                                                              current_value,
                                                                              new_value))


def populate(articlemeta, db_session):
    """
    Povoa tabela journal com os dados dos periódicos extraídos do ArticleMeta

    @param articlemeta: cliente Thrift ou Restful da API ArticleMeta
    @param db_session: sessão de conexão com banco de dados Matomo
    """
    counter = 0
    for col in dicts.collection_to_code.keys():
        logging.info('Collecting journals from %s' % col)

        for journal in articlemeta.journals(col):
            counter += 1
            logging.info('Collecting journal %d' % counter)

            online_issn = format_issn(journal.electronic_issn)
            print_issn = format_issn(journal.print_issn)
            pid_issn = extract_issn_from_url(journal)

            possible_issns = [i for i in [online_issn, print_issn, pid_issn] if i != '']

            # Tenta localizar registro de Periódico com base nos ISSNs
            try:
                existing_journal = lib_database.get_journal_from_issns(db_session, possible_issns)

                # Atualiza registros. Caso sejam diferentes, há algo muito errado
                update_journal_issn(existing_journal, 'online_issn', online_issn)
                update_journal_issn(existing_journal, 'print_issn', print_issn)
                update_journal_issn(existing_journal, 'pid_issn', pid_issn)

                db_session.commit()

            # Caso não exista registro de periódico, cria um novo
            except NoResultFound:
                new_journal = Journal()
                new_journal.online_issn = online_issn
                new_journal.print_issn = print_issn
                new_journal.pid_issn = pid_issn

                db_session.add(new_journal)
                db_session.flush()
                logging.info('Adicionado periódico {}, {}, {}'.format(new_journal.print_issn,
                                                                      new_journal.online_issn,
                                                                      new_journal.pid_issn))

                existing_journal = new_journal

            except MultipleResultsFound:
                logging.critical('Há mais de um registro para um mesmo ISSN: {}'.format(online_issn,
                                                                                        print_issn,
                                                                                        pid_issn))
                db_session.rollback()
                exit(1)

            try:
                existing_journal_collection = lib_database.get_journal_collection(db_session,
                                                                                  col,
                                                                                  existing_journal.id)
            except NoResultFound:
                # Cria novo registro de JournalCollection. Assume que nenhum periódico é repetido para uma mesma coleção
                new_journal_collection = JournalCollection()

                new_journal_collection.idjournal_jc = existing_journal.id

                # O título e demais atributos estão no contexto da coleção associada ao periódico
                new_journal_collection.title = journal.title
                new_journal_collection.publisher_name = format_publisher_names(journal.publisher_name)
                new_journal_collection.uri = extract_url(journal)
                new_journal_collection.collection = col

                db_session.add(new_journal_collection)
                db_session.commit()
                logging.info('Adicionado informações da coleção {} para o periódico {}'.format(col, existing_journal.id))


def main():
    usage = """Povoa tabela de periódicos para uso da API SUSHI em relatórios COUNTER"""
    parser = argparse.ArgumentParser(usage)

    parser.add_argument(
        '-u', '--matomo_db_uri',
        default=MATOMO_DATABASE_STRING,
        dest='matomodb_uri',
        help='String de conexão a base SQL no formato mysql://username:password@host1:port/database'
    )

    parser.add_argument(
        '-t', '--use_thrift',
        dest='use_thrift',
        default=False,
        action='store_true',
        help='Usar ArticleMeta Thrift Client ao invés de RestfulClient'
    )

    parser.add_argument(
        '--logging_level',
        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET'],
        dest='logging_level',
        default=LOGGING_LEVEL,
        help='Nível de log'
    )

    params = parser.parse_args()

    logging.basicConfig(level=params.logging_level)

    if not params.use_thrift:
        articlemeta = RestfulClient()
    else:
        articlemeta = ThriftClient()

    populate(articlemeta=articlemeta, db_session=SESSION_FACTORY())

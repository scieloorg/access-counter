import argparse
import logging
import re
import sys
sys.path.append('..')

from articlemeta.client import RestfulClient, ThriftClient
from sqlalchemy.sql import null
from sqlalchemy.exc import IntegrityError
from urllib import parse
from utils import db_tools
from utils.sql_declarative import Journal


COLLECTIONS = ['arg', 'bol', 'chl', 'cic', 'col', 'cri', 'cub', 'esp', 'mex', 'per', 'prt', 'pry', 'psi', 'rve', 'rvt',
               'scl', 'spa', 'sss', 'sza', 'ury', 'ven', 'wid']

REGEX_ISSN = re.compile(r'[0-9]{4}-[0-9]{3}[0-9xX]')


def format_publisher_names(publisher_names: list):
    """
    Caso um periódico tenha mais de um publisher a ele associado, separa os seus nomes por ponto e vírgula

    @param publisher_names: lista de nomes dos publishers associados a um periódico
    @return: string contendo os nomes dos publishers separados por ponto e vírgula
    """
    names_without_semicolon = [n.replace(';', ',') for n in publisher_names]
    return '; '.join(names_without_semicolon)


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


def populate(articlemeta, db_session):
    """
    Povoa tabela journal com os dados dos periódicos extraídos do ArticleMeta

    @param articlemeta: cliente Thrift ou Restful da API ArticleMeta
    @param db_session: sessão de conexão com banco de dados Matomo
    """
    for col in COLLECTIONS:
        for journal in articlemeta.journals(col):
            new_journal = Journal()
            new_journal.collection_acronym = col

            new_journal.title = journal.title

            new_journal.online_issn = format_issn(journal.electronic_issn)
            new_journal.print_issn = format_issn(journal.print_issn)
            new_journal.pid_issn = extract_issn_from_url(journal)

            new_journal.publisher_name = format_publisher_names(journal.publisher_name)
            new_journal.uri = extract_url(journal)

            logging.info('Adicionado periódico ISSN_PRINT {}, ISSN_ONLINE {}, TITLE {}'.format(new_journal.print_issn,
                                                                                               new_journal.online_issn,
                                                                                               new_journal.title))

            if new_journal.online_issn or new_journal.print_issn:
                try:
                    db_session.add(new_journal)
                    db_session.commit()
                except IntegrityError as e:
                    db_session.rollback()
                    logging.error('Erro ao adicionar periódico: {}'.format(e))
            else:
                logging.warning('Periódico (%s, %s) não contém ISSN' % (new_journal.collection_acronym,
                                                                        new_journal.title))


def main():
    usage = """Povoa tabela de periódicos para uso da API SUSHI em relatórios COUNTER"""
    parser = argparse.ArgumentParser(usage)

    parser.add_argument(
        '-d',
        required=True,
        dest='matomodb_uri',
        help='String de conexão a base SQL no formato mysql://username:password@host1:port/database'
    )

    parser.add_argument(
        '-t',
        dest='use_thrift',
        default=False,
        action='store_true',
        help='Usar ArticleMeta Thrift Client ao invés de RestfulClient'
    )

    parser.add_argument(
        '--logging_level',
        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET'],
        dest='logging_level',
        default='INFO',
        help='Nível de log'
    )

    params = parser.parse_args()

    logging.basicConfig(level=params.logging_level)

    if not params.use_thrift:
        articlemeta = RestfulClient()
    else:
        articlemeta = ThriftClient()

    db_session = db_tools.get_db_session(params.matomodb_uri)
    populate(articlemeta=articlemeta, db_session=db_session)


if __name__ == '__main__':
    main()

import argparse
import logging
import os

from libs import lib_database


STR_CONNNECTION = os.environ.get('STR_CONNNECTION', 'mysql://user:pass@localhost:3306/matomo')
LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', 'INFO')


def main():
    usage = """Cria tabelas para uso da API SUSHI em relatórios COUNTER."""
    parser = argparse.ArgumentParser(usage)

    parser.add_argument(
        '-u', '--str_connection',
        default=STR_CONNNECTION,
        help='String de conexão com banco de dados (mysql://username:password@host:port/database)'
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

    logging.info('Criando tabelas COUNTER')
    lib_database.create_tables(params.str_connection)


if __name__ == '__main__':
    main()

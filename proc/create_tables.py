import argparse
import logging
import sys
sys.path.append('..')

from utils import db_tools


def main():
    usage = """Cria tabelas para uso da API SUSHI em relatórios COUNTER."""
    parser = argparse.ArgumentParser(usage)

    parser.add_argument(
        '-d',
        required=True,
        dest='matomo_db_uri',
        help='String de conexão a base SQL no formato mysql://username:password@host1:port/database'
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

    logging.info('Criando tabelas COUNTER')
    db_tools.create_tables(params.matomo_db_uri)

    logging.info('Alterando tabelas Matomo')
    db_tools.add_foreign_keys_to_table_matomo_log_link_action(params.matomo_db_uri)


if __name__ == '__main__':
    main()

import argparse
import logging
import sys
sys.path.append('..')

from utils import db_tools


def main():
    usage = """Cria tabelas para uso da API SUSHI em relatórios COUNTER."""
    parser = argparse.ArgumentParser(usage)

    parser.add_argument(
        '-u', '--matomo_db_uri',
        required=True,
        dest='matomo_db_uri',
        help='String de conexão a base SQL no formato mysql://username:password@host1:port/database'
    )

    parser.add_argument(
        '-f', '--fix_matomo_db',
        default=False,
        action='store_true',
        help='1) Cria índice index_ip na tabela matomo_log_link_visit_action.'
             '2) Altera coluna visit_total_interactions (matomo_log_visit) para MEDIUMINT(5).'
             '3) Altera coluna interaction_position (matomo_log_link_visit_action) para MEDIUMINT(5).'
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

    if params.fix_matomo_db:
        logging.info('Criando índice index_ip na tabela matomo_log_visit')
        db_tools.create_index_ip_on_table_matomo_log_visit(params.matomo_db_uri)

        logging.info('Corrigindo colunas visit_total_interactions e interaction_position')
        db_tools.fix_fields_interactions(params.matomo_db_uri)


if __name__ == '__main__':
    main()

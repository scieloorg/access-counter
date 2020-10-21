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
        '--create_index_ip_on_matomo_log_visit',
        default=False,
        action='store_true',
        help='Cria índice index_ip na tabela matomo_log_link_visit_action'
    )

    parser.add_argument(
        '--fix_fields_interactions',
        default=False,
        action='store_true',
        help='Corrige colunas visit_total_interactions (matomo_log_visit) e '
             'interaction_position (matomo_log_link_visit_action) para MEDIUMINT(5)'
    )

    parser.add_argument(
        '--add_foreign_keys_on_matomo_log_visit_action',
        default=False,
        action='store_true',
        help='Adiciona chaves estrangeiras na tabela matomo_log_link_visit_action'
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

    if params.add_foreign_keys_on_matomo_log_visit_action:
        logging.info('Adicionando chaves estrangeiras na tabela matomo_log_link_visit_action')
        db_tools.add_foreign_keys_to_table_matomo_log_link_action(params.matomo_db_uri)

    if params.create_index_ip_on_matomo_log_visit:
        logging.info('Criando índice index_ip na tabela matomo_log_visit')
        db_tools.create_index_ip_on_table_matomo_log_visit(params.matomo_db_uri)

    if params.fix_fields_interactions:
        logging.info('Corrigindo colunas visit_total_interactions e interaction_position')
        db_tools.fix_fields_interactions(params.matomo_db_uri)


if __name__ == '__main__':
    main()

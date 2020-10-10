import argparse
import datetime
import logging
import sys
sys.path.append('..')

from utils import db_tools


logging.basicConfig(filename='create_tables_' + datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S') + '.log',
                    level=logging.DEBUG)


def main():
    usage = """Cria tabelas para uso da API SUSHI em relatórios COUNTER."""
    parser = argparse.ArgumentParser(usage)

    parser.add_argument(
        '-d',
        required=True,
        dest='matomodb_uri',
        help='String de conexão a base SQL no formato mysql://username:password@host1:port/database'
    )

    params = parser.parse_args()

    db_tools.create_tables(params.matomodb_uri)


if __name__ == '__main__':
    main()

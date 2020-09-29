import argparse
import json
import os
import pickle

from counter import CounterStat
from hit import HitManager
from time import time


def main():
    usage = 'Extrai informações de log no formato COUNTER R5.'
    parser = argparse.ArgumentParser(usage)

    parser.add_argument(
        '--raw',
        required=True,
        dest='raw',
        help='log em formato matomo'
    )

    parser.add_argument(
        '--pdf_paths',
        dest='pdf_to_pid',
        help='dicionário que mapeia caminho de PDF a PID'
    )

    parser.add_argument(
        '--acronyms',
        dest='issn_to_acronym',
        help='dicionário que mapeia ISSN a acrônimo'
    )

    parser.add_argument(
        '--out',
        action='store_true',
        help='salva resultados em arquivo JSON'
    )

    params = parser.parse_args()

    files_queue = []
    if os.path.isfile(params.raw):
        files_queue.append(params.raw)
    elif os.path.isdir(params.raw):
        files_queue.extend([os.path.join(os.path.abspath(params.raw), f) for f in os.listdir(params.raw)])

    time_start = time()

    pdf_to_pid = pickle.load(open(params.pdf_to_pid, 'rb'))
    issn_to_acronym = pickle.load(open(params.issn_to_acronym, 'rb'))

    iam = HitManager(pdf_to_pid, issn_to_acronym)

    for f in files_queue:
        iam.set_hits(f)

    print('Removendo cliques-duplos')
    iam.remove_double_clicks(iam.session_to_actions)

    print('Contando acessos por PID')
    iam.count_hits_by_pid()

    print('Extraindo métricas COUNTER R5')
    cs = CounterStat()
    cs.populate_counter(iam.pid_to_hits)

    if params.out:
        json.dump(cs.articles, open('results/articles.json', 'w'), indent=True)
        json.dump(cs.journals, open('results/journals.json', 'w'), indent=True)

    time_end = time()
    print('Durou %.2f segundos' % (time_end - time_start))


if __name__ == '__main__':
    main()

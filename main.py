import argparse
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
        help='log em formato raw'
    )

    parser.add_argument(
        '--pdf_paths',
        dest='pdf_to_pid',
        help='dicionário que mapeia caminho de PDF a PID'
    )

    params = parser.parse_args()

    time_start = time()

    pdf_to_pid = pickle.load(open(params.pdf_to_pid, 'rb'))

    iam = HitManager(pdf_to_pid)
    iam.set_hits(params.raw)

    print('Removendo cliques-duplos')
    iam.clean_double_clicks(iam.session_to_actions)

    print('Contando acessos por PID')
    iam.count_hits_by_pid()

    print('Extraindo métricas COUNTER R5')
    cs = CounterStat()
    cs.populate_counter(iam.pid_to_hits)

    for pid in cs.articles:
        print(pid, cs.articles[pid])

    time_end = time()
    print('Durou %.2f segundos' % (time_end - time_start))


if __name__ == '__main__':
    main()

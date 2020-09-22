import argparse

from counter import CounterStat
from hit import HitManager
from time import time


def main():
    usage = 'Extrai informações de log no formato COUNTER R5.'
    parser = argparse.ArgumentParser(usage)

    parser.add_argument(
        '--raw',
        default=None,
        dest='raw',
        help='log em formato raw'
    )

    params = parser.parse_args()

    time_start = time()

    iam = HitManager()
    iam.set_items(params.raw)

    print('Removendo cliques-duplos')
    iam.clean_double_clicks(iam.session_to_actions)

    print('Contando acessos por PID')
    iam.count_hits_by_pid()

    print('Extrai métricas COUNTER R5')
    cs = CounterStat()
    cs.populate_reports(iam.pid_to_hits)

    for pid in cs.articles:
        print(pid, cs.articles[pid])

    time_end = time()
    print('Durou %.2f segundos' % (time_end - time_start))

if __name__ == '__main__':
    main()

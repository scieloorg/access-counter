import argparse
import os
import re

from multiprocessing import Pool


use_database_data = False
issns_keys = []
result_type = 'geral'


def get_date_from_path(path_file: str):
    pattern_date = r'(\d{4})(\d{2})(\d{2})\.'
    matched_date = re.search(pattern_date, path_file)
    if matched_date:
        raw_date = matched_date.groups()
        if len(raw_date) == 3:
            return raw_date[0] + '-' + raw_date[1] + '-' + raw_date[2]
    else:
        print('Não foi possível detectar data válida em nome de arquivo: %s' % path_file)
        exit(1)


def get_result_by_pid_fmt(data):
    results = {} 
    for d in data:
        pid = d[0]
        fmt = d[1]
        k = '-'.join([pid, fmt])
        if k not in results: 
            results[k] = [0, 0, 0, 0] 
        results[k] = sum_lists(results[k], d[8:]) 
    return results


def get_results_by_pid_fmt_lang(data):
    results = {}
    for d in data:
        pid = d[0]
        fmt = d[1]
        lang = d[2]
        k = '-'.join([pid, fmt, lang])
        if k not in results:
            results[k] = [0, 0, 0, 0]
        results[k] = sum_lists(results[k], d[8:])
    return results


def sum_lists(l1, l2):
    s = len(l1) 
    if len(l1) == len(l2): 
        l1i = [int(v) for v in l1] 
        l2i = [int(v) for v in l2] 
        return [l1i[x] + l2i[x] for x in range(s)]


def get_result_by_fmt(data): 
    results = {'html': [0, 0, 0, 0], 'pdf': [0, 0, 0, 0]}
    for d in data:
        fmt = d[1] 
        results[fmt] = sum_lists(results[fmt], d[8:])
    results['total'] = sum_lists(results['pdf'], results['html'])
    return results


def get_results_by_issn(data):
    results = {}

    for d in data[1:]:
        fmt = d[1]
        issn = d[6]
        ymd = d[7]
        values = d[8:]

        if issn not in results:
            results[issn] = {}

        if ymd not in results[issn]:
            results[issn][ymd] = {'total': [0, 0, 0, 0],
                                  'html': [0, 0, 0, 0],
                                  'pdf': [0, 0, 0, 0]}

        results[issn][ymd][fmt] = sum_lists(results[issn][ymd][fmt], values)
        results[issn][ymd]['total'] = sum_lists(results[issn][ymd]['total'], values)
    return results


def get_results_by_issns_list(data, issns):
    results = [] 
     
    for i in data[1:]: 
        issn = i[6] 
         
        if issn in issns: 
            results.append(i) 
    return results


def get_issn(issns):
    global issns_keys

    for i in issns:
        if i in issns_keys:
            return i


def process(path_file: str):
    date = get_date_from_path(path_file)

    raw_data = [k.strip().split('|') for k in open(path_file)]

    global use_database_data
    if use_database_data:
        new_raw_data = []
        for r in raw_data:
            online_issn = r[6]
            print_issn = r[7]
            pid_issn = r[8]
            issn = get_issn([i for i in [online_issn, print_issn, pid_issn] if i])

            new_row = r[:6] + [issn] + ['2020-02-18'] + r[9:]

            new_raw_data.append(new_row)
        raw_data = new_raw_data
    else:
        raw_data = [['pid',
                     'fmt',
                     'lang',
                     'lat',
                     'long',
                     'yop',
                     'issn',
                     'year_month_day',
                     'total_item_investigations',
                     'total_item_requests',
                     'unique_item_investigations',
                     'unique_item_requests']] + raw_data

    global result_type

    if result_type == 'geral':
        results = get_result_by_fmt(raw_data[1:])
        a = {k: v for k, v in sorted(results.items(), key=lambda item: item[0])}
        for k, v in a.items():
            print(','.join([str(vi) for vi in v] + [date, k]))
    elif result_type == 'pf':
        results = get_result_by_pid_fmt(raw_data[1:])
        a = {k: v for k, v in sorted(results.items(), key=lambda item: item[0])}
        for k, v in a.items():
            print(k, ','.join([str(vi) for vi in v]))
    elif result_type == 'pfl':
        results = get_results_by_pid_fmt_lang(raw_data[1:])
        a = {k: v for k, v in sorted(results.items(), key=lambda item: item[0])}
        for k, v in a.items():
            print(k, ','.join([str(vi) for vi in v]))
    elif result_type == 'issns':
        results = get_results_by_issn(raw_data[1:])
        for issn, ymd_formats in results.items():
            for ymd, formats in ymd_formats.items():
                for fmt in formats:
                    print(issn, ymd, fmt, '\t'.join([str(i) for i in formats[fmt]]), sep='\t')
    elif result_type == 'issn':
        results = get_results_by_issns_list(raw_data[1:], info_issns_list)
        for r in results:
            print(r)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-d',
        required=True,
        help='Diretório com arquivos de métricas COUNTER R5',
        dest='dir_metrics'
    )

    parser.add_argument(
        '-u', '--matomo_db_uri',
        dest='matomo_db_uri',
        help='String de conexão a base SQL Matomo no formato mysql://username:password@host1:port/database'
    )

    parser.add_argument(
        '-m',
        dest='use_database_mode',
        action='store_true',
        help='Marca aplicação para usar modo de base de dados'
    )

    parser.add_argument(
        '-i',
        dest='file_issns_keys',
        help='Arquivo contendo chaves de ISSNs'
    )

    parser.add_argument(
        '-r',
        choices=['geral', 'pf', 'pfl', 'issn', 'issns'],
        dest='result_type',
        default='geral',
        help='Modo de saída dos resultados'
    )

    parser.add_argument(
        '-p',
        dest='info_issns_list',
        default=[],
        help='Lista de ISSNs de um periódico'
    )

    params = parser.parse_args()
    files = [params.dir_metrics + '/' + f for f in os.listdir(params.dir_metrics) if 'r5_metrics' in f]

    global use_database_data
    use_database_data = params.use_database_mode

    if params.file_issns_keys:
        global issns_keys
        issns_keys = [i.strip() for i in open(params.file_issns_keys)]

    global result_type
    result_type = params.result_type

    global info_issns_list
    info_issns_list = [i.strip().split(',') for i in params.info_issns_list]

    with Pool(os.cpu_count()) as p:
        p.map(process, files)


if __name__ == '__main__':
    main()

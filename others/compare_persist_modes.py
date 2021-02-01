import argparse
import csv


DEFAULT_KEYS = ('year_month_day', 'pid', 'fmt', 'lang', 'lat', 'long')
MIN_KEYS = ('year_month_day', 'pid')


def extract_key(data, keyset):
    return '|'.join([data[k] for k in keyset])


def extract_metrics(data):
    metrics = {}
    for k, v in data.items():
        if k.startswith('total_') or k.startswith('unique_'):
            metrics[k] = int(v)
    return metrics


def sum(l1, l2):
    if sorted(l1.keys()) != sorted(l2.keys()):
        print('ERRO: Dicionários de métricas possuem chaves distintas')
        exit(1)

    common_keys = set(l1.keys()).union(set(l2.keys()))
    new_dict = {}
    for ck in common_keys:
        new_dict[ck] = l1[ck] + l2[ck]
    return new_dict


def extract_attrs_dict(path_file, use_key_min_set, delimiter='\t'):
    d = {}
    print('Extraindo atributos de %s...' % path_file)
    csv_reader = csv.DictReader(open(path_file), delimiter=delimiter)
    for i in csv_reader:
        if use_key_min_set:
            key = extract_key(i, MIN_KEYS)
        else:
            key = extract_key(i, DEFAULT_KEYS)

        metrics = extract_metrics(i)
        if key not in d:
            d[key] = {'total_item_requests': 0,
                      'total_item_investigations': 0,
                      'unique_item_requests': 0,
                      'unique_item_investigations': 0}
        d[key] = sum(d[key], metrics)
    return d


parser = argparse.ArgumentParser()
parser.add_argument('-1', dest='f1')
parser.add_argument('-2', dest='f2')
parser.add_argument('-m', '--use_min_keyset', dest='use_min_keyset', default=False, action='store_true')
params = parser.parse_args()

d1 = extract_attrs_dict(params.f1, params.use_min_keyset, delimiter='|')
d2 = extract_attrs_dict(params.f2, params.use_min_keyset, delimiter='|')

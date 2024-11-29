import argparse
from argparse import ArgumentParser
from operator import itemgetter
from os import getcwd
from os.path import join

import urllib3

from api.get_data_from_bfg import GetDataFromBFG
from utils.yml_config import read_config


def update_data(ia, table, key_field):
    data = ia.get_from_rest_collection(table, active_progress=False)
    if not data:
        return
    names_list = list(map(itemgetter(key_field), data[table]))
    print('\n'.join(names_list))


if __name__ == '__main__':
    parser = ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description='Strategic planning'
    )

    parser.add_argument('-c', '--config', required=False,
                        default=join(getcwd(), 'config.yml'))
    parser.add_argument('-t', '--table', required=True)
    parser.add_argument('-k', '--key', required=False, default='name')

    args = parser.parse_args()
    config = read_config(args.config)

    urllib3.disable_warnings()
    with GetDataFromBFG.from_config(config) as session:
        update_data(session, args.table, args.key)

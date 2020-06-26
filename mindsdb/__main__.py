import argparse
import atexit
import traceback
import sys
import os

import torch.multiprocessing as mp
from torch.multiprocessing import Process

from mindsdb.utilities.config import Config
from mindsdb.interfaces.native.mindsdb import MindsdbNative
from mindsdb.api.http.start import start as start_http
from mindsdb.api.mysql.start import start as start_mysql
from mindsdb.utilities.fs import get_or_create_dir_struct


def close_api_gracefully(p_arr):
    for p in p_arr:
        sys.stdout.flush()
        p.terminate()
        p.join()
        sys.stdout.flush()

if __name__ == '__main__':
    mp.freeze_support()

    parser = argparse.ArgumentParser(description='CL argument for mindsdb server')
    parser.add_argument('--api', type=str, default=None)
    parser.add_argument('--config', type=str, default=None)

    args = parser.parse_args()

    config_path = args.config
    if config_path is None:
        config_dir, _, _ = get_or_create_dir_struct()
        config_path = os.path.join(config_dir,'config.json')

    print(f'Using configuration file: {config_path}')
    config = Config(config_path)

    if args.api is None:
        api_arr = [api for api in config['api']]
    else:
        api_arr = args.api.split(',')

    start_functions = {
        'http': start_http,
        'mysql': start_mysql
    }

    if len(api_arr) > 0:
        mdb = MindsdbNative(config)
        models_data = [mdb.get_model_data(x['name']) for x in mdb.get_models()]

        try:
            clickhouse_enabled = config['integrations']['default_clickhouse']['enabled']
        except Exception:
            clickhouse_enabled = False

        if clickhouse_enabled:
            from mindsdb.interfaces.clickhouse.clickhouse import Clickhouse
            clickhouse = Clickhouse(config)
            if clickhouse.check_connection() is False:
                print('ERROR: can`t connect to Clickhouse')
                sys.exit(1)
            clickhouse.setup_clickhouse(models_data=models_data)

        try:
            mariadb_enabled = config['integrations']['default_mariadb']['enabled']
        except Exception:
            mariadb_enabled = False

        if mariadb_enabled:
            from mindsdb.interfaces.mariadb.mariadb import Mariadb
            mariadb = Mariadb(config)
            if mariadb.check_connection() is False:
                print('ERROR: can`t connect to MariaBD')
                sys.exit(1)
            mariadb.setup_mariadb(models_data=models_data)

    p_arr = []
    ctx = mp.get_context('spawn')
    for api in api_arr:
        print(f'Starting Mindsdb {api} API !')
        try:
            p = ctx.Process(target=start_functions[api], args=(config_path,True,))
            p.start()
            p_arr.append(p)
            print(f'Started Mindsdb {api} API !')
        except Exception as e:
            close_api_gracefully(p_arr)
            print(f'Failed to start {api} API with exception {e}')
            print(traceback.format_exc())
            raise

    atexit.register(close_api_gracefully, p_arr=p_arr)

    for p in p_arr:
        p.join()

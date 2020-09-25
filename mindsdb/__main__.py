import atexit
import traceback
import sys
import os
import time

import torch.multiprocessing as mp

from mindsdb.utilities.config import Config
from mindsdb.interfaces.native.mindsdb import MindsdbNative
from mindsdb.interfaces.custom.custom_models import CustomModels
from mindsdb.api.http.start import start as start_http
from mindsdb.api.mysql.start import start as start_mysql
from mindsdb.api.mongo.start import start as start_mongo
from mindsdb.utilities.fs import get_or_create_dir_struct, update_versions_file
from mindsdb.utilities.ps import is_port_in_use
from mindsdb.interfaces.database.database import DatabaseWrapper
from mindsdb.utilities.functions import args_parse


def close_api_gracefully(p_arr):
    for p in p_arr:
        sys.stdout.flush()
        p.terminate()
        p.join()
        sys.stdout.flush()


if __name__ == '__main__':
    mp.freeze_support()

    args = args_parse()

    config_path = args.config
    if config_path is None:
        config_dir, _ = get_or_create_dir_struct()
        config_path = os.path.join(config_dir, 'config.json')

    config = Config(config_path)

    from lightwood.__about__ import __version__ as lightwood_version
    from mindsdb.__about__ import __version__ as mindsdb_version
    from mindsdb_native.__about__ import __version__ as mindsdb_native_version

    if args.version:
        print(f'MindsDB {mindsdb_version}')
        sys.exit(0)

    if args.verbose:
        config['log']['level']['console'] = 'INFO'

    print(f'Configuration file:\n   {config_path}')
    print(f"Storage path:\n   {config.paths['root']}")

    print('Versions:')
    print(f' - lightwood {lightwood_version}')
    print(f' - MindsDB_native {mindsdb_native_version}')
    print(f' - MindsDB {mindsdb_version}')

    os.environ['MINDSDB_STORAGE_PATH'] = config.paths['predictors']
    if args.verbose is True:
        os.environ['DEFAULT_LOG_LEVEL'] = 'INFO'
        os.environ['LIGHTWOOD_LOG_LEVEL'] = 'INFO'
    else:
        os.environ['DEFAULT_LOG_LEVEL'] = 'ERROR'
        os.environ['LIGHTWOOD_LOG_LEVEL'] = 'ERROR'

    update_versions_file(
        config,
        {
            'lightwood': lightwood_version,
            'mindsdb_native': mindsdb_native_version,
            'mindsdb': mindsdb_version,
            'python': sys.version.replace('\n', '')
        }
    )

    if args.api is None:
        api_arr = ['http', 'mysql']
    else:
        api_arr = args.api.split(',')

    api_arr = [{
        'name': api,
        'port': config['api'][api]['port'],
        'started': False
    } for api in api_arr]

    for api in api_arr:
        api_name = api['name']
        if api_name not in config['api']:
            print(f"Trying run '{api_name}' API, but is no config for this api.")
            print(f"Please, fill config['api']['{api_name}']")
            sys.exit(0)

    start_functions = {
        'http': start_http,
        'mysql': start_mysql,
        'mongodb': start_mongo
    }

    mdb = MindsdbNative(config)
    cst = CustomModels(config)
    # @TODO Maybe just use `get_model_data` directly here ? Seems like a useless abstraction
    model_data_arr = [
        {
            'name': x['name'],
            'predict': x['predict'],
            'data_analysis': mdb.get_model_data(x['name'])['data_analysis_v2']
        } for x in mdb.get_models()
    ]

    for m in model_data_arr:
        if 'columns_to_ignore' in m['data_analysis']:
            del m['data_analysis']['columns_to_ignore']
        if 'train_std_dev' in m['data_analysis']:
            del m['data_analysis']['train_std_dev']

    model_data_arr.extend(cst.get_models())

    dbw = DatabaseWrapper(config)
    dbw.register_predictors(model_data_arr)

    for broken_name in [name for name, connected in dbw.check_connections().items() if connected is False]:
        print(f'Error failed to integrate with database aliased: {broken_name}')

    p_arr = []
    ctx = mp.get_context('spawn')

    for api in api_arr:
        api_name = api['name']
        print(f'{api_name} API: starting...')
        try:
            p = ctx.Process(target=start_functions[api_name], args=(config_path, args.verbose))
            p.start()
            p_arr.append(p)
        except Exception as e:
            close_api_gracefully(p_arr)
            print(f'Failed to start {api_name} API with exception {e}')
            print(traceback.format_exc())
            raise

    atexit.register(close_api_gracefully, p_arr=p_arr)

    timeout = 15
    start_time = time.time()
    all_started = False
    while (time.time() - start_time) < timeout and all_started is False:
        all_started = True
        for i, api in enumerate(api_arr):
            in_use = api['started'] or is_port_in_use(api['port'])
            if in_use and api['started'] != in_use:
                api['started'] = in_use
                print(f"{api['name']} API: started on {api['port']}")
            all_started = all_started and in_use
        time.sleep(0.5)

    for p in p_arr:
        p.join()

import atexit
import traceback
import sys
import os
import time

from pkg_resources import get_distribution
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

    from mindsdb.__about__ import __version__ as mindsdb_version

    if args.version:
        print(f'MindsDB {mindsdb_version}')
        sys.exit(0)

    try:
        lightwood_version = get_distribution('lightwood').version
    except Exception:
        from lightwood.__about__ import __version__ as lightwood_version

    try:
        mindsdb_native_version = get_distribution('mindsdb_native').version
    except Exception:
        from mindsdb_native.__about__ import __version__ as mindsdb_native_version

    if args.verbose:
        config['log']['level']['console'] = 'INFO'

    # Print Welcome message
    print("Welcome to MindsDB, you have the following versions installed:\n")
    print(f' - lightwood {lightwood_version}')
    print(f' - MindsDB_native {mindsdb_native_version}')
    print(f' - MindsDB {mindsdb_version}')

    print("\n\nNOTE: These paths may come handy if you want to make general config updates or see the data that is being saved.\n")
    print(f'Using configuration file: {config_path}')
    print(f"MindsDB storage directory: {config.paths['root']}")
    print('\n' + '='*7 + '\n') # print divider

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
        print(f'Starting Mindsdb {api_name} API ... OK')
        try:
            p = ctx.Process(target=start_functions[api_name], args=(config_path, args.verbose))
            p.start()
            p_arr.append(p)
        except Exception as e:
            close_api_gracefully(p_arr)
            print(f'Failed to start {api_name} API with exception {e}')
            print(traceback.format_exc())
            raise

    os.environ['MINDSDB_STORAGE_PATH'] = config.paths['predictors']
    if args.verbose is True:
        os.environ['DEFAULT_LOG_LEVEL'] = 'INFO'
        os.environ['LIGHTWOOD_LOG_LEVEL'] = 'INFO'
    else:
        os.environ['DEFAULT_LOG_LEVEL'] = 'ERROR'
        os.environ['LIGHTWOOD_LOG_LEVEL'] = 'ERROR'
        print("\nNOTE: Verbose is OFF [see --help for options]")
    print('\n' + '='*7 + '\n') # print divider

    atexit.register(close_api_gracefully, p_arr=p_arr)

    timeout = 15
    start_time = time.time()
    all_started = False
    while (time.time() - start_time) < timeout and all_started is False:
        all_started = True
        for i, api in enumerate(api_arr):
            try:
                in_use = api['started'] or is_port_in_use(api['port'])
            except Exception:
                # NOTE that hotfix for OSX: is_port_in_use will raise AccessDenied error if it runned not as sudo
                in_use = True
            if in_use and api['started'] != in_use:
                api['started'] = in_use
                if api['name'] == 'mysql':
                    print("You can now connect to MindsDB Server on any Mysql client as:")
                    print(f"\ttcp://127.0.0.1:{api['port']}")
                else: # api['name'] == http
                    print("You can now work on MindsDB in your browser at: ")
                    print(f"\thttp://localhost:{api['port']}")
            all_started = all_started and in_use
        time.sleep(0.5)

    print("\nHappy Predicting!")
    print("\nMore information @ https://docs.mindsdb.com")

    for p in p_arr:
        p.join()

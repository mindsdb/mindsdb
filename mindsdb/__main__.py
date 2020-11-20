import atexit
import traceback
import sys
import os
import time
import asyncio
import logging

from pkg_resources import get_distribution
import torch.multiprocessing as mp

from mindsdb.utilities.config import Config
from mindsdb.interfaces.native.mindsdb import MindsdbNative
from mindsdb.interfaces.custom.custom_models import CustomModels
from mindsdb.api.http.start import start as start_http
from mindsdb.api.mysql.start import start as start_mysql
from mindsdb.api.mongo.start import start as start_mongo
from mindsdb.utilities.fs import (
    get_or_create_dir_struct,
    update_versions_file,
    archive_obsolete_predictors,
    remove_corrupted_predictors
)
from mindsdb.utilities.ps import is_pid_listen_port
from mindsdb.interfaces.database.database import DatabaseWrapper
from mindsdb.utilities.functions import args_parse, get_all_models_meta_data
from mindsdb.utilities.log import initialize_log


def close_api_gracefully(apis):
    try: 
        for api in apis.values():
            process = api['process']
            sys.stdout.flush()
            process.terminate()
            process.join()
            sys.stdout.flush()
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == '__main__':
    version_error_msg = """
MindsDB server requires Python >= 3.6 to run

Once you have Python 3.6 installed you can tun mindsdb as follows:

1. create and activate venv:
python3.6 -m venv venv
source venv/bin/activate

2. install MindsDB:
pip3 install mindsdb

3. Run MindsDB
python3.6 -m mindsdb

More instructions in https://docs.mindsdb.com
    """

    if not (sys.version_info[0] >= 3 and sys.version_info[1] >= 6):
        print(version_error_msg)
        exit(1)

    mp.freeze_support()

    args = args_parse()

    from mindsdb.__about__ import __version__ as mindsdb_version

    if args.version:
        print(f'MindsDB {mindsdb_version}')
        sys.exit(0)

    config_path = args.config
    if config_path is None:
        config_dir, _ = get_or_create_dir_struct()
        config_path = os.path.join(config_dir, 'config.json')

    config = Config(config_path)

    if args.verbose is True:
        config['log']['level']['console'] = 'DEBUG'
    os.environ['DEFAULT_LOG_LEVEL'] = config['log']['level']['console']
    os.environ['LIGHTWOOD_LOG_LEVEL'] = config['log']['level']['console']

    initialize_log(config)
    log = logging.getLogger('mindsdb.main')

    try:
        lightwood_version = get_distribution('lightwood').version
    except Exception:
        from lightwood.__about__ import __version__ as lightwood_version

    try:
        mindsdb_native_version = get_distribution('mindsdb_native').version
    except Exception:
        from mindsdb_native.__about__ import __version__ as mindsdb_native_version

    print(f'Configuration file:\n   {config_path}')
    print(f"Storage path:\n   {config.paths['root']}")

    print('Versions:')
    print(f' - lightwood {lightwood_version}')
    print(f' - MindsDB_native {mindsdb_native_version}')
    print(f' - MindsDB {mindsdb_version}')

    os.environ['MINDSDB_STORAGE_PATH'] = config.paths['predictors']

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

    apis = {
        api: {
            'port': config['api'][api]['port'],
            'process': None,
            'started': False
        } for api in api_arr
    }

    for api_name in apis.keys():
        if api_name not in config['api']:
            print(f"Trying run '{api_name}' API, but is no config for this api.")
            print(f"Please, fill config['api']['{api_name}']")
            sys.exit(0)

    start_functions = {
        'http': start_http,
        'mysql': start_mysql,
        'mongodb': start_mongo
    }

    archive_obsolete_predictors(config, '2.11.0')

    mdb = MindsdbNative(config)
    cst = CustomModels(config)

    remove_corrupted_predictors(config, mdb)

    model_data_arr = get_all_models_meta_data(mdb, cst)

    dbw = DatabaseWrapper(config)
    dbw.register_predictors(model_data_arr)

    for broken_name in [name for name, connected in dbw.check_connections().items() if connected is False]:
        log.error(f'Error failed to integrate with database aliased: {broken_name}')

    ctx = mp.get_context('spawn')

    for api_name, api_data in apis.items():
        print(f'{api_name} API: starting...')
        try:
            p = ctx.Process(target=start_functions[api_name], args=(config_path, args.verbose))
            p.start()
            api_data['process'] = p
        except Exception as e:
            close_api_gracefully(apis)
            log.error(f'Failed to start {api_name} API with exception {e}\n{traceback.format_exc()}')
            raise

    atexit.register(close_api_gracefully, apis=apis)

    async def wait_api_start(api_name, pid, port):
        timeout = 60
        start_time = time.time()
        started = is_pid_listen_port(pid, port)
        while (time.time() - start_time) < timeout and started is False:
            await asyncio.sleep(0.5)
            started = is_pid_listen_port(pid, port)
        return api_name, port, started

    async def wait_apis_start():
        futures = [
            wait_api_start(api_name, api_data['process'].pid, api_data['port'])
            for api_name, api_data in apis.items()
        ]
        for i, future in enumerate(asyncio.as_completed(futures)):
            api_name, port, started = await future
            if started:
                print(f"{api_name} API: started on {port}")
            else:
                log.error(f"ERROR: {api_name} API cant start on {port}")

    ioloop = asyncio.get_event_loop()
    ioloop.run_until_complete(wait_apis_start())
    ioloop.close()

    try:
        for api_data in apis.values():
            api_data['process'].join()
    except KeyboardInterrupt:
        print('Closing app...')

import time
from pathlib import Path
import json
import docker
import requests
import subprocess
import atexit
import os
import socket
from contextlib import closing
import asyncio

from mindsdb.utilities.fs import create_dirs_recursive
from mindsdb.utilities.config import Config
from mindsdb.interfaces.native.mindsdb import MindsdbNative
from mindsdb.interfaces.datastore.datastore import DataStore
from mindsdb.interfaces.database.database import DatabaseWrapper
from mindsdb.utilities.ps import wait_port, is_port_in_use
from mindsdb_native import CONFIG

# USE_EXTERNAL_DB_SERVER = os.getenv('EXTERNAL_DB_SERVER')
USE_EXTERNAL_DB_SERVER = True

# EXTERNAL_DB_CREDENTIALS = str(Path.home().joinpath('.mindsdb_credentials.json'))
EXTERNAL_DB_CREDENTIALS = '/home/maxs/dev/mdb/venv_new/sources/mindsdb_server_enterprise/database_testing_env/credentials.json'

MINDSDB_DATABASE = f'mindsdb_{int(time.time()*1000)}' if USE_EXTERNAL_DB_SERVER else 'mindsdb'

dir_path = os.path.dirname(os.path.realpath(__file__))

TEST_CONFIG = dir_path + '/config/config.json'

TESTS_ROOT = Path(__file__).parent.absolute().joinpath('../../').resolve()

START_TIMEOUT = 15

OUTPUT = None  # [None|subprocess.DEVNULL]

# import time
# TEMP_DIR = Path(__file__).parent.absolute().joinpath(f'../../temp_{int(time.time()*1000)}/').resolve()
TEMP_DIR = Path(__file__).parent.absolute().joinpath('../../temp/').resolve()
TEMP_DIR.mkdir(parents=True, exist_ok=True)


def prepare_config(config, enable_dbs=[], mindsdb_database='mindsdb', override_integration_config={}):
    if isinstance(enable_dbs, list) is False:
        enable_dbs = [enable_dbs]
    for key in config._config['integrations'].keys():
        config._config['integrations'][key]['enabled'] = key in enable_dbs

    if USE_EXTERNAL_DB_SERVER:
        with open(EXTERNAL_DB_CREDENTIALS, 'rt') as f:
            cred = json.loads(f.read())
            for key in cred:
                if f'default_{key}' in config._config['integrations']:
                    config._config['integrations'][f'default_{key}'].update(cred[key])

    for integration in override_integration_config.keys():
        config._config['integrations'][integration].update(override_integration_config[integration])

    config['api']['mysql']['database'] = mindsdb_database
    config['api']['mongodb']['database'] = mindsdb_database

    storage_dir = TEMP_DIR.joinpath('storage')
    config._config['storage_dir'] = str(storage_dir)

    create_dirs_recursive(config.paths)

    temp_config_path = str(TEMP_DIR.joinpath('config.json').resolve())
    with open(temp_config_path, 'wt') as f:
        json.dump(config._config, f, indent=4, sort_keys=True)

    return temp_config_path


if USE_EXTERNAL_DB_SERVER:
    config = Config(TEST_CONFIG)
    # r = requests.get('http://127.0.0.1:5005/port')
    # if r.status_code != 200:
    #     raise Exception('Cant get port to run mindsdb')
    # mindsdb_port = r.content.decode()
    # print(f'use mindsdb port={mindsdb_port}')
    # config._config['api']['mysql']['port'] = mindsdb_port

    with open(EXTERNAL_DB_CREDENTIALS, 'rt') as f:
        credentials = json.loads(f.read())
    override = {}
    for key, value in credentials.items():
        override[f'default_{key}'] = value
    TEST_CONFIG = prepare_config(config, override_integration_config=override)


def is_port_opened(port, host='127.0.0.1'):
    ''' try connect to host:port, to check it open or not
    '''
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        is_open = sock.connect_ex((host, int(port))) == 0
    return is_open


def wait_api_ready(config, api='mysql'):
    port_num = config['api'][api]['port']
    api_ready = wait_port(port_num, START_TIMEOUT)
    return api_ready


def wait_db(config, db_name):
    m = DatabaseWrapper(config)

    start_time = time.time()

    connected = m.check_connections()[db_name]

    while not connected and (time.time() - start_time) < START_TIMEOUT:
        time.sleep(2)
        connected = m.check_connections()[db_name]

    return connected


def is_container_run(name):
    docker_client = docker.from_env()
    try:
        containers = docker_client.containers.list()
    except Exception:
        # In case docker is running for sudo or another user
        return True
    containers = [x.name for x in containers if x.status == 'running']
    return name in containers


def get_test_csv(name, url, lines_count=None, rewrite=False):
    test_csv_path = TESTS_ROOT.joinpath('temp/', name).resolve()
    if not test_csv_path.is_file() or rewrite:
        r = requests.get(url)
        with open(test_csv_path, 'wb') as f:
            f.write(r.content)
        if lines_count is not None:
            fp = str(test_csv_path)
            p = subprocess.Popen(
                f"mv {fp} {fp}_2; sed -n '1,{lines_count}p' {fp}_2 >> {fp}; rm {fp}_2",
                cwd=TESTS_ROOT.resolve(),
                stdout=OUTPUT,
                stderr=OUTPUT,
                shell=True
            )
            p.wait()
    return str(test_csv_path)


def run_container(name):
    env = os.environ.copy()
    env['UID'] = str(os.getuid())
    env['GID'] = str(os.getgid())
    subprocess.Popen(
        ['./cli.sh', name],
        cwd=TESTS_ROOT.joinpath('docker/').resolve(),
        stdout=OUTPUT,
        stderr=OUTPUT,
        env=env
    )
    atexit.register(stop_container, name=name)


def stop_container(name):
    sp = subprocess.Popen(
        ['./cli.sh', f'{name}-stop'],
        cwd=TESTS_ROOT.joinpath('docker/').resolve(),
        stdout=OUTPUT,
        stderr=OUTPUT
    )
    sp.wait()


def stop_mindsdb(sp):
    sp.kill()
    sp = subprocess.Popen('kill -9 $(lsof -t -i:47334)', shell=True)
    sp.wait()
    sp = subprocess.Popen('kill -9 $(lsof -t -i:47335)', shell=True)
    sp.wait()
    sp = subprocess.Popen('kill -9 $(lsof -t -i:47336)', shell=True)
    sp.wait()


def run_environment(config, apis=['mysql'], run_docker_db=[], override_integration_config={}, mindsdb_database='mindsdb'):
    ''' services = [mindsdb|]
    '''

    default_databases = [f'default_{db}' for db in run_docker_db]
    temp_config_path = prepare_config(config, default_databases, mindsdb_database, override_integration_config)

    db_ready = True
    for db in run_docker_db:
        if is_container_run(f'{db}-test') is False:
            run_container(db)
        db_ready = db_ready and wait_db(config, f'default_{db}')

    if db_ready is False:
        print('Cant start databases.')
        raise Exception()

    api_str = ','.join(apis)
    sp = subprocess.Popen(
        ['python3', '-m', 'mindsdb', '--api', api_str, '--config', temp_config_path],
        stdout=OUTPUT,
        stderr=OUTPUT
    )
    atexit.register(stop_mindsdb, sp=sp)

    async def wait_port_async(port, timeout):
        start_time = time.time()
        started = is_port_in_use(port)
        while (time.time() - start_time) < timeout and started is False:
            await asyncio.sleep(1)
            started = is_port_in_use(port)
        return started

    async def wait_apis_start(ports):
        futures = [wait_port_async(port, 60) for port in ports]
        success = True
        for i, future in enumerate(asyncio.as_completed(futures)):
            success = success and await future
        return success

    ports_to_wait = [config['api'][api]['port'] for api in apis]

    ioloop = asyncio.get_event_loop()
    success = ioloop.run_until_complete(wait_apis_start(ports_to_wait))
    ioloop.close()
    if not success:
        raise Exception('Cant start mindsdb apis')

    CONFIG.MINDSDB_STORAGE_PATH = config.paths['predictors']
    mdb = MindsdbNative(config)
    datastore = DataStore(config)

    return mdb, datastore

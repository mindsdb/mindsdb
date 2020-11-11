import time
from pathlib import Path
import json
import requests
import subprocess
import atexit
import os
import asyncio
import shutil

from mindsdb.utilities.fs import create_dirs_recursive
from mindsdb.utilities.config import Config
from mindsdb.interfaces.native.mindsdb import MindsdbNative
from mindsdb.interfaces.datastore.datastore import DataStore
from mindsdb.utilities.ps import wait_port, is_port_in_use
from mindsdb_native import CONFIG

USE_EXTERNAL_DB_SERVER = bool(int(os.getenv('USE_EXTERNAL_DB_SERVER') or "1"))

EXTERNAL_DB_CREDENTIALS = str(Path.home().joinpath('.mindsdb_credentials.json'))

MINDSDB_DATABASE = f'mindsdb_{int(time.time()*1000)}' if USE_EXTERNAL_DB_SERVER else 'mindsdb'

dir_path = os.path.dirname(os.path.realpath(__file__))

TEST_CONFIG = dir_path + '/config/config.json'

TESTS_ROOT = Path(__file__).parent.absolute().joinpath('../../').resolve()

START_TIMEOUT = 15

OUTPUT = None  # [None|subprocess.DEVNULL]

TEMP_DIR = Path(__file__).parent.absolute().joinpath('../../temp/').resolve()
TEMP_DIR.mkdir(parents=True, exist_ok=True)


def prepare_config(config, mindsdb_database='mindsdb', override_integration_config={}, override_api_config={}):
    for key in config._config['integrations']:
        config._config['integrations'][key]['enabled'] = False

    if USE_EXTERNAL_DB_SERVER:
        with open(EXTERNAL_DB_CREDENTIALS, 'rt') as f:
            cred = json.loads(f.read())
            for key in cred:
                if f'default_{key}' in config._config['integrations']:
                    config._config['integrations'][f'default_{key}'].update(cred[key])

    for integration in override_integration_config:
        if integration in config._config['integrations']:
            config._config['integrations'][integration].update(override_integration_config[integration])
        else:
            config._config['integrations'][integration] = override_integration_config[integration]

    for api in override_api_config:
        config._config['api'][api].update(override_api_config[api])

    config['api']['mysql']['database'] = mindsdb_database
    config['api']['mongodb']['database'] = mindsdb_database

    storage_dir = TEMP_DIR.joinpath('storage')
    if storage_dir.is_dir():
        shutil.rmtree(str(storage_dir))
    config._config['storage_dir'] = str(storage_dir)

    create_dirs_recursive(config.paths)

    temp_config_path = str(TEMP_DIR.joinpath('config.json').resolve())
    with open(temp_config_path, 'wt') as f:
        json.dump(config._config, f, indent=4, sort_keys=True)

    return temp_config_path


def close_ssh_tunnel(sp, port):
    sp.kill()
    # NOTE line below will close connection in ALL test instances.
    # sp = subprocess.Popen(f'for pid in $(lsof -i :{port} -t); do kill -9 $pid; done', shell=True)
    sp = subprocess.Popen(f'ssh -S /tmp/.mindsdb-ssh-ctrl-{port} -O exit ubuntu@3.220.66.106', shell=True)
    sp.wait()


def open_ssh_tunnel(port, direction='R'):
    cmd = f'ssh -i ~/.ssh/db_machine -S /tmp/.mindsdb-ssh-ctrl-{port} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -fMN{direction} 127.0.0.1:{port}:127.0.0.1:{port} ubuntu@3.220.66.106'
    sp = subprocess.Popen(
        cmd.split(' '),
        stdout=OUTPUT,
        stderr=OUTPUT
    )
    atexit.register(close_ssh_tunnel, sp=sp, port=port)


if USE_EXTERNAL_DB_SERVER:
    config = Config(TEST_CONFIG)
    open_ssh_tunnel(5005, 'L')
    wait_port(5005, timeout=10)
    r = requests.get('http://127.0.0.1:5005/port')
    if r.status_code != 200:
        raise Exception('Cant get port to run mindsdb')
    mindsdb_port = r.content.decode()
    open_ssh_tunnel(mindsdb_port, 'R')
    print(f'use mindsdb port={mindsdb_port}')
    config._config['api']['mysql']['port'] = mindsdb_port
    config._config['api']['mongodb']['port'] = mindsdb_port

    with open(EXTERNAL_DB_CREDENTIALS, 'rt') as f:
        credentials = json.loads(f.read())
    override = {}
    for key, value in credentials.items():
        override[f'default_{key}'] = value
    TEST_CONFIG = prepare_config(config, override_integration_config=override)


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


def stop_mindsdb(sp):
    sp.kill()
    sp = subprocess.Popen('kill -9 $(lsof -t -i:47334)', shell=True)
    sp.wait()
    sp = subprocess.Popen('kill -9 $(lsof -t -i:47335)', shell=True)
    sp.wait()
    sp = subprocess.Popen('kill -9 $(lsof -t -i:47336)', shell=True)
    sp.wait()


def run_environment(config, apis=['mysql'], override_integration_config={}, override_api_config={}, mindsdb_database='mindsdb'):
    temp_config_path = prepare_config(config, mindsdb_database, override_integration_config, override_api_config)
    config = Config(temp_config_path)

    api_str = ','.join(apis)
    sp = subprocess.Popen(
        ['python3', '-m', 'mindsdb', '--api', api_str, '--config', temp_config_path],
        close_fds=True,
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

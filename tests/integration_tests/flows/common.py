import psutil
import shutil
import time
from pathlib import Path
import json
import docker
import requests
import subprocess
import atexit
import os

from mindsdb.interfaces.native.mindsdb import MindsdbNative
from mindsdb.interfaces.datastore.datastore import DataStore
from mindsdb.interfaces.database.database import DatabaseWrapper


dir_path = os.path.dirname(os.path.realpath(__file__))
TEST_CONFIG = dir_path + '/config/config.json'

TESTS_ROOT = Path(__file__).parent.absolute().joinpath('../../').resolve()

START_TIMEOUT = 15

OUTPUT = None  # [None|subprocess.DEVNULL]

TEMP_DIR = Path(__file__).parent.absolute().joinpath('../../temp/').resolve()
TEMP_DIR.mkdir(parents=True, exist_ok=True)


def is_port_in_use(port_num):
    portsinuse = []
    conns = psutil.net_connections()
    portsinuse = [x.laddr[1] for x in conns if x.status == 'LISTEN']
    portsinuse.sort()
    return int(port_num) in portsinuse


def wait_port(port_num, timeout):
    start_time = time.time()

    in_use = is_port_in_use(port_num)
    while in_use is False and (time.time() - start_time) < timeout:
        time.sleep(2)
        in_use = is_port_in_use(port_num)

    return in_use


def wait_api_ready(config):
    port_num = config['api']['mysql']['port']
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


def prepare_config(config, dbs):
    if isinstance(dbs, list) is False:
        dbs = [dbs]
    for key in config._config['integrations'].keys():
        config._config['integrations'][key]['enabled'] = key in dbs

    storage_dir = TEMP_DIR.joinpath('storage')
    config['storage_dir'] = storage_dir

    paths = config.paths
    for key in paths:
        p = storage_dir.joinpath(key)
        p.mkdir(mode=0o777, exist_ok=True, parents=True)
        paths[key] = str(p)

    temp_config_path = str(TEMP_DIR.joinpath('config.json').resolve())
    with open(temp_config_path, 'wt') as f:
        json.dump(config._config, f, indent=4, sort_keys=True)

    return temp_config_path


def is_container_run(name):
    docker_client = docker.from_env()
    containers = docker_client.containers.list()
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
    sp = subprocess.Popen('kill -9 $(lsof -t -i:47335)', shell=True)
    sp.wait()


def run_environment(db, config):
    DEFAULT_DB = f'default_{db}'

    temp_config_path = prepare_config(config, DEFAULT_DB)

    if db == 'mssql':
        db_ready = True
    else:
        if is_container_run(f'{db}-test') is False:
            subprocess.Popen(
                ['./cli.sh', db],
                cwd=TESTS_ROOT.joinpath('docker/').resolve(),
                stdout=OUTPUT,
                stderr=OUTPUT
            )
            atexit.register(stop_container, name=db)
        db_ready = wait_db(config, DEFAULT_DB)

    if db_ready:
        sp = subprocess.Popen(
            ['python3', '-m', 'mindsdb', '--api', 'mysql', '--config', temp_config_path],
            stdout=OUTPUT,
            stderr=OUTPUT
        )
        atexit.register(stop_mindsdb, sp=sp)

    api_ready = db_ready and wait_api_ready(config)

    if db_ready is False or api_ready is False:
        print(f'Failed by timeout. {db} started={db_ready}, MindsDB started={api_ready}')
        raise Exception()

    mdb = MindsdbNative(config)
    datastore = DataStore(config)

    return mdb, datastore

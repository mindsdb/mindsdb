import time
from pathlib import Path
import json
import requests
import subprocess
import atexit
import os
import asyncio
import shutil
import csv

from pandas import DataFrame

from mindsdb.utilities.fs import create_dirs_recursive
from mindsdb.utilities.config import Config
from mindsdb.interfaces.native.mindsdb import MindsdbNative
from mindsdb.interfaces.datastore.datastore import DataStore
from mindsdb.utilities.ps import wait_port, is_port_in_use
from mindsdb_native import CONFIG

HTTP_API_ROOT = 'http://localhost:47334/api'

DATASETS_PATH = os.getenv('DATASETS_PATH')

USE_EXTERNAL_DB_SERVER = bool(int(os.getenv('USE_EXTERNAL_DB_SERVER') or "1"))

EXTERNAL_DB_CREDENTIALS = str(Path.home().joinpath('.mindsdb_credentials.json'))

MINDSDB_DATABASE = f'mindsdb_{int(time.time()*1000)}' if USE_EXTERNAL_DB_SERVER else 'mindsdb'

dir_path = os.path.dirname(os.path.realpath(__file__))

TEST_CONFIG = dir_path + '/config/config.json'

TESTS_ROOT = Path(__file__).parent.absolute().joinpath('../../').resolve()

START_TIMEOUT = 15

OUTPUT = None  # [None|subprocess.DEVNULL]

TEMP_DIR = Path(__file__).parent.absolute().joinpath('../../').joinpath(
    f'temp/test_storage_{int(time.time()*1000)}/' if USE_EXTERNAL_DB_SERVER else 'temp/test_storage/'
).resolve()
TEMP_DIR.mkdir(parents=True, exist_ok=True)

DATASETS_COLUMN_TYPES = {
    'us_health_insurance': [
        ('age', int),
        ('sex', str),
        ('bmi', float),
        ('children', int),
        ('smoker', str),
        ('region', str),
        ('charges', float)
    ],
    'hdi': [
        ('Population', int),
        ('Area', int),
        ('Pop_Density', int),
        ('GDP_per_capita_USD', int),
        ('Literacy', float),
        ('Infant_mortality', int),
        ('Development_Index', float)
    ],
    'used_car_price': [
        ('model', str),
        ('year', int),
        ('price', int),
        ('transmission', str),
        ('mileage', int),
        ('fueltype', str),
        ('tax', int),
        ('mpg', float),
        ('enginesize', float)
    ],
    'home_rentals': [
        ('number_of_rooms', int),
        ('number_of_bathrooms', int),
        ('sqft', int),
        ('location', str),
        ('days_on_market', int),
        ('initial_price', int),
        ('neighborhood', str),
        ('rental_price', int)
    ],
    'concrete_strength': [
        ('id', int),
        ('cement', float),
        ('slag', float),
        ('flyAsh', float),
        ('water', float),
        ('superPlasticizer', float),
        ('coarseAggregate', float),
        ('fineAggregate', float),
        ('age', int),
        ('concrete_strength', float)
    ]
}


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


def make_test_csv(name, data):
    test_csv_path = TEMP_DIR.joinpath(f'{name}.csv').resolve()
    df = DataFrame(data)
    df.to_csv(test_csv_path, index=False)
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


def upload_csv(query, columns_map, db_types_map, table_name, csv_path, escape='`', template=None):
    template = template or 'create table test_data.%s (%s);'
    query(template % (
        table_name,
        ','.join([f'{escape}{col_name}{escape} {db_types_map[col_type]}' for col_name, col_type in columns_map])
    ))

    with open(csv_path) as f:
        csvf = csv.reader(f)
        for i, row in enumerate(csvf):
            if i == 0:
                continue
            if i % 100 == 0:
                print(f'inserted {i} rows')
            vals = []
            for i, col in enumerate(columns_map):
                col_type = col[1]
                try:
                    if col_type is int:
                        vals.append(str(int(float(row[i]))))
                    elif col_type is str:
                        vals.append(f"'{row[i]}'")
                    else:
                        vals.append(str(col_type(row[i])))
                except Exception:
                    vals.append('null')

            query(f'''INSERT INTO test_data.{table_name} VALUES ({','.join(vals)})''')


def condition_dict_to_str(condition):
    ''' convert dict to sql WHERE conditions

        :param condition: dict
        :return: str
    '''
    s = []
    for name, value in condition.items():
        if isinstance(value, str):
            s.append(f"{name}='{value}'")
        elif value is None:
            s.append(f'{name} is null')
        else:
            s.append(f'{name}={value}')

    return ' AND '.join(s)


def get_all_pridict_fields(fields):
    ''' make list off all prediciton fields
    '''
    fieldes = list(fields.keys())
    for field_name, field_type in fields.items():
        fieldes.append(f'{field_name}_confidence')
        fieldes.append(f'{field_name}_explain')
        if field_type in [int, float]:
            fieldes.append(f'{field_name}_min')
            fieldes.append(f'{field_name}_max')
    return fieldes


def check_prediction_values(row, to_predict):
    try:
        for field_name, field_type in to_predict.items():
            if field_type is int or field_type is float:
                assert isinstance(row[field_name], (int, float))
                assert isinstance(row[f'{field_name}_min'], (int, float))
                assert isinstance(row[f'{field_name}_max'], (int, float))
                assert row[f'{field_name}_max'] > row[f'{field_name}_min']
            elif field_type is str:
                assert isinstance(row[field_name], str)
            else:
                assert False

            assert isinstance(row[f'{field_name}_confidence'], (int, float))
            assert isinstance(row[f'{field_name}_explain'], (str, dict))
    except Exception:
        print('Wrong values in row:')
        print(row)
        return False
    return True

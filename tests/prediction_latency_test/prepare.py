import os
import atexit
import importlib.util
import time
import csv
import shutil
import json
from subprocess import Popen

import pandas as pd
import docker
import requests
import psutil

from mindsdb_native import Predictor
import schemas as schema
from config import CONFIG



DATASETS_PATH = os.getenv("DATASETS_PATH")
CONFIG_PATH = os.getenv("CONFIG_PATH")
predictors_dir = os.getenv("MINDSDB_STORAGE_PATH")
# datasets = ["monthly_sunspots", "metro_traffic_ts"]
# datasets = CONFIG['datasets'].keys()

# handlers = {"monthly_sunspots": lambda df: monthly_sunspots_handler(df)}
# predict_targets = {"monthly_sunspots": 'Sunspots',
#         "metro_traffic_ts": 'traffic_volume'}


class Dataset:
    def __init__(self, name, **kwargs):
        self.name = name
        self.target = kwargs.get("target")
        self.handler_file = kwargs.get("handler_file", None)
        if self.handler_file is not None:
            self.handler = self._get_handler()
        else:
            self.handler = None

    def _get_handler(self):
        spec = importlib.util.spec_from_file_location("common", os.path.abspath(self.handler_file))
        handler = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(handler)
        return handler.handler

datasets = [Dataset(key, **CONFIG['datasets'][key]) for key  in CONFIG['datasets'].keys()]


def monthly_sunspots_handler(df):
    months = df['Month']
    for i, val in enumerate(months):
        months[i] = val + "-01"


def copy_version_info(dataset):
    dst = os.path.join(predictors_dir, dataset, "versions.json")
    src = os.path.join(predictors_dir, "..", "versions.json")
    shutil.copyfile(src, dst)

def get_handler(handler_path):
    spec = importlib.util.spec_from_file_location("common", os.path.abspath(handler_path))
    handler = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(handler)
    return handler.handler


def create_models():
    for dataset in datasets:
        dataset_root_path = os.path.join(DATASETS_PATH, dataset.name)
        print(f"dataset_root_path: {dataset_root_path}")
        to_predict = dataset.target

        data_path = f"{dataset.name}_train.csv"
        print(f"data_path: {data_path}")
        model = Predictor(name=dataset.name)
        try:
            # model.learn(to_predict=to_predict, from_data=data_path, rebuild_model=False)
            model.learn(to_predict=to_predict, from_data=data_path, rebuild_model=True)
        except FileNotFoundError:
            print(f"model {dataset.name} doesn't exist")
            print("creating....")
            model.learn(to_predict=to_predict, from_data=data_path)
        copy_version_info(dataset.name)

def add_integration():
    db_info = CONFIG['database']
    with open(CONFIG_PATH, 'r') as f:
        config = json.load(f)
    integration_name = "prediction_clickhouse"
    config['integrations'][integration_name] = {}
    config['integrations'][integration_name]['publish'] = True
    config['integrations'][integration_name]['host'] = db_info["host"]
    config['integrations'][integration_name]['port'] = db_info["port"]
    config['integrations'][integration_name]['user'] = db_info["user"]
    config['integrations'][integration_name]['password'] = db_info["password"]
    config['integrations'][integration_name]['type'] = 'clickhouse'
    with open(CONFIG_PATH, 'w') as f:
        json.dump(config, f, indent=4, sort_keys=True)


def split_datasets():
    for dataset in datasets:
        data_path = os.path.join(DATASETS_PATH, dataset.name, "data.csv")
        df = pd.read_csv(data_path)
        if dataset.handler is not None:
            dataset.handler(df)
        all_len = len(df)
        train_len = int(float(all_len) * 0.8)
        train_df = df[:train_len]
        test_df = df[train_len:]
        test_df = test_df.drop(columns=[dataset.target,])
        train_df.to_csv(f"{dataset.name}_train.csv", index=False)
        test_df.to_csv(f"{dataset.name}_test.csv", index=False)

def stop_mindsdb(ppid):
    pprocess = psutil.Process(ppid)
    pids = [x.pid for x in pprocess.children(recursive=True)]
    pids.append(ppid)
    for pid in pids:
        try:
            os.kill(pid, 9)
        # process may be killed by OS due to some reasons in that moment
        except ProcessLookupError:
            pass

def run_mindsdb():
    sp = Popen(['python', '-m', 'mindsdb', '--config', CONFIG_PATH],
               close_fds=True)

    time.sleep(30)
    atexit.register(stop_mindsdb, sp.pid)

def run_clickhouse():
    docker_client = docker.from_env(version='auto')
    image = "yandex/clickhouse-server:latest"
    container_params = {'name': 'clickhouse-latency-test',
            'remove': True,
            'network_mode': 'host',
            }
            # 'ports': {"9000/tcp": 9000,
            #     "8123/tcp": 8123},
            # 'environment': {"CLICKHOUSE_PASSWORD": "iyDNE5g9fw9kdrCLIKoS3bkOJkE",
                # "CLICKHOUSE_USER": "root"}}
    container = docker_client.containers.run(image, detach=True, **container_params)
    atexit.register(container.stop)
    return container

def prepare_db():
    db = schema.database
    query(f'DROP DATABASE IF EXISTS {db}')
    query(f'CREATE DATABASE {db}')

    for dataset in datasets:
        query(schema.tables[dataset.name])
        with open(f'{dataset.name}_train.csv') as fp:
            csv_fp = csv.reader(fp)
            for i, row in enumerate(csv_fp):
                if i == 0:
                    continue

                for i in range(len(row)):
                    try:
                        if '.' in row[i]:
                            row[i] = float(row[i])
                        else:
                            if row[i].isdigit():
                                row[i] = int(row[i])
                    except Exception as e:
                        print(e)

                query('INSERT INTO ' + schema.database + '.' + dataset.name + ' VALUES ({})'.format(
                    str(row).lstrip('[').rstrip(']')
                ))

def query(query):

    if 'CREATE ' not in query.upper() and 'INSERT ' not in query.upper():
        query += ' FORMAT JSON'

    db_info = CONFIG['database']

    host = db_info['host']
    port = db_info['port']
    user = db_info['user']
    password = db_info['password']

    connect_string = f'http://{host}:{port}'

    params = {'user': user, 'password': password}

    res = requests.post(
        connect_string,
        data=query,
        params=params,
        headers={"Connection": "close"}
    )

    if res.status_code != 200:
        print(f"error uploading: {query}")
        print(res.text, res.status_code)
    assert res.status_code == 200
    return res.text

def prepare_env(prepare_data=True,
                use_docker=True,
                setup_db=True,
                train_models=True):
    if prepare_data:
        split_datasets()
    if train_models:
        create_models()
    add_integration()
    if use_docker:
        print("running docker")
        run_clickhouse()
        time.sleep(5)
    if setup_db:
        print("preparing db")
        prepare_db()
    print("running mindsdb")
    run_mindsdb()

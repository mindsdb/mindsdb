import os
import time
import csv
import shutil
import json
from subprocess import Popen

import pandas as pd
import docker
import requests

predictors_dir = "/home/itsyplen/repos/work/MindsDB/mindsdb/var/predictors"
os.environ["MINDSDB_STORAGE_PATH"] = predictors_dir
from mindsdb_native import Predictor
import schemas as schema

# DATASETS_PATH = "/Users/itsyplen/repos/MindsDB/private-benchmarks/benchmarks/datasets"
DATASETS_PATH = "/home/itsyplen/repos/work/MindsDB/private-benchmarks/benchmarks/datasets"
datasets = ["monthly_sunspots", "metro_traffic_ts"]

handlers = {"monthly_sunspots": lambda df: monthly_sunspots_handler(df)}
predict_targets = {"monthly_sunspots": 'Sunspots',
        "metro_traffic_ts": 'traffic_volume'}

CONFIG_PATH = "/home/itsyplen/repos/work/MindsDB/mindsdb/etc/config.json"


def monthly_sunspots_handler(df):
    months = df['Month']
    for i, val in enumerate(months):
        months[i] = val + "-01"


def copy_version_info(dataset):
    dst = os.path.join(predictors_dir, dataset, "versions.json")
    src = os.path.join(predictors_dir, "..", "versions.json")
    shutil.copyfile(src, dst)

def create_models():
    for dataset in datasets:
        dataset_root_path = os.path.join(DATASETS_PATH, dataset)
        print(f"dataset_root_path: {dataset_root_path}")
        to_predict = predict_targets[dataset]

        data_path = f"{dataset}_train.csv"
        print(f"data_path: {data_path}")
        model = Predictor(name=dataset)
        try:
            model.learn(to_predict=to_predict, from_data=data_path, rebuild_model=False)
            # model.learn(to_predict=to_predict, from_data=data_path, rebuild_model=True)
        except FileNotFoundError:
            print(f"model {dataset} doesn't exist")
            print("creating....")
            model.learn(to_predict=to_predict, from_data=data_path)
        copy_version_info(dataset)

def add_integration():
    with open(CONFIG_PATH, 'r') as f:
        config = json.load(f)
    integration_name = "prediction_clickhouse"
    # integrations = config['integrations']
    config['integrations'][integration_name] = {}
    config['integrations'][integration_name]['publish'] = True
    config['integrations'][integration_name]['host'] = "127.0.0.1"
    config['integrations'][integration_name]['port'] = 8123
    config['integrations'][integration_name]['user'] = 'root'
    config['integrations'][integration_name]['password'] = "iyDNE5g9fw9kdrCLIKoS3bkOJkE"
    config['integrations'][integration_name]['type'] = 'clickhouse'
    with open(CONFIG_PATH, 'w') as f:
        json.dump(config, f, indent=4, sort_keys=True)


def split_datasets():
    for dataset in datasets:
        data_path = os.path.join(DATASETS_PATH, dataset, "data.csv")
        df = pd.read_csv(data_path)
        if dataset in handlers:
            handlers[dataset](df)
        all_len = len(df)
        print(f"{dataset} len: {all_len}")
        train_len = int(float(all_len) * 0.8)
        train_df = df[:train_len]
        test_df = df[train_len:]
        test_df = test_df.drop(columns=[predict_targets[dataset],])
        train_df.to_csv(f"{dataset}_train.csv", index=False)
        test_df.to_csv(f"{dataset}_test.csv", index=False)


def run_mindsdb():
    sp = Popen(['python', '-m', 'mindsdb'],
               close_fds=True)

    time.sleep(30)
    return sp

def run_clickhouse():
    docker_client = docker.from_env(version='auto')
    image = "yandex/clickhouse-server:latest"
    container_params = {'name': 'clickhouse-latency-test',
            'remove': True,
            'network_mode': 'host',
            # 'ports': {"9000/tcp": 9000,
            #     "8123/tcp": 8123},
            'environment': {"CLICKHOUSE_PASSWORD": "iyDNE5g9fw9kdrCLIKoS3bkOJkE",
                "CLICKHOUSE_USER": "root"}}
    container = docker_client.containers.run(image, detach=True, **container_params)
    return container

def prepare_db():
    db = schema.database
    query(f'DROP DATABASE IF EXISTS {db}')
    query(f'CREATE DATABASE {db}')

    for dataset in schema.datasets:
        # table = schema.tables[dataset]
        query(schema.tables[dataset])
        with open(f'{dataset}_train.csv') as fp:
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

                query('INSERT INTO ' + schema.database + '.' + dataset + ' VALUES ({})'.format(
                    str(row).lstrip('[').rstrip(']')
                ))

def query(query):

    if 'CREATE ' not in query.upper() and 'INSERT ' not in query.upper():
        query += ' FORMAT JSON'

    host = "127.0.0.1"
    port = 8123
    # user = "default"
    # password = ""
    user = "root"
    password = "iyDNE5g9fw9kdrCLIKoS3bkOJkE"

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

def ping_clickhouse():
    queries = ["SELECT * FROM mindsdb.predictors",
               "SELECT traffic_volume AS predicted FROM mindsdb.metro_traffic_ts WHERE weather_description=='sky is clear' AND weather_main=='Mist'"]
    # query = "SHOW TABLES FROM mindsdb FORMAT JSON"
    # query = "SELECT * FROM mindsdb.metro_traffic_ts LIMIT 1 FORMAT JSON"
    host = "127.0.0.1"
    port = 8123
    # user = "default"
    # password = ""
    user = "root"
    password = "iyDNE5g9fw9kdrCLIKoS3bkOJkE"

    connect_string = f'http://{host}:{port}'

    params = {'user': user, 'password': password}

    for query in queries:
        res = requests.post(
            connect_string,
            data=query,
            params=params
        )

        print(res.text, res.status_code)


if __name__ == "__main__":

    # split_datasets()
    # create_models()
    # print("adding integration")
    # add_integration()
    container = run_clickhouse()
    time.sleep(5)
    print("preparing db")
    prepare_db()
    print("running mindsb")
    run_mindsdb()
    print("ping clickhouse")
    ping_clickhouse()
    # try:
    #     prepare_db()
    # finally:
    #     print("before removing", container.status)
    #     container.stop()
    #     print("after removing")
    #     print(container.status)

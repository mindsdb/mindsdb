import os
import time
import csv
import pandas as pd
import docker
import requests

from mindsdb_native import Predictor
import schemas as schema

DATASETS_PATH = "/Users/itsyplen/repos/MindsDB/private-benchmarks/benchmarks/datasets"
datasets = ["monthly_sunspots", "metro_traffic_ts"]

handlers = {"monthly_sunspots": lambda df: monthly_sunspots_handler(df)}
predict_targets = {"monthly_sunspots": 'Sunspots',
        "metro_traffic_ts": 'traffic_volume'}

def monthly_sunspots_handler(df):
    months = df['Month']
    for i, val in enumerate(months):
        months[i] = val + "-01"


def create_models():
    for dataset in datasets:
        dataset_root_path = os.path.join(DATASETS_PATH, dataset)
        print(f"dataset_root_path: {dataset_root_path}")
        to_predict = predict_targets[dataset]

        data_path = f"{dataset}_train.csv"
        print(f"data_path: {data_path}")
        model = Predictor(name=dataset)
        try:
            # model.learn(to_predict=to_predict, from_data=data_path, rebuild_model=False)
            model.learn(to_predict=to_predict, from_data=data_path, rebuild_model=True)
        except FileNotFoundError:
            print(f"model {dataset} doesn't exist")
            print("creating....")
            model.learn(to_predict=to_predict, from_data=data_path)


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
        train_df.to_csv(f"{dataset}_train.csv", index=False)
        test_df.to_csv(f"{dataset}_test.csv", index=False)


def run_clickhouse():
    docker_client = docker.from_env(version='auto')
    image = "yandex/clickhouse-server:latest"
    container_params = {'name': 'clickhouse-latency-test',
            'remove': True,
            # 'network_mode': 'host',
            'ports': {"9000/tcp": 9000,
                "8123/tcp": 8123},
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

    host = "localhost"
    port = 8123
    user = "default"
    password = ""
    # user = "root"
    # password = "iyDNE5g9fw9kdrCLIKoS3bkOJkE"

    connect_string = f'http://{host}:{port}'

    params = {'user': user, 'password': password}

    res = requests.post(
        connect_string,
        data=query,
        params=params
    )

    if res.status_code != 200:
        print(f"error uploading: {query}")
        print(res.text, res.status_code)
    assert res.status_code == 200

if __name__ == "__main__":
    # split_datasets()
    # create_models()


    container = run_clickhouse()
    time.sleep(5)
    print("preparing db")
    prepare_db()
    # try:
    #     prepare_db()
    # finally:
    #     print("before removing", container.status)
    #     container.stop()
    #     print("after removing")
    #     print(container.status)

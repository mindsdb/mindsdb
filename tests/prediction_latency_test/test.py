import os
import time
import argparse

import pandas as pd

class NativeDataFrame:
    def __init__(self, dataset):
        self.dataset = dataset
        self.df = pd.read_csv(f"{dataset}_test.csv")
        self.predictor = Predictor(name=dataset)

    def predict(self, row_number=1):
        return self.predictor.predict(self.df[:row_number])

    def __repr__(self):
        return f"{self.dataset}_{self.__class__.__name__}"

    def __str__(self):
        return self.__repr__()

class NativeClickhouse:
    host = '127.0.0.1'
    user = 'default'
    password = ''

    def __init__(self, dataset):
        self.dataset = dataset
        self.predictor = Predictor(name=dataset)
        self.query_template = f"SELECT * FROM test_data.{dataset} LIMIT %s"

    def predict(self, row_number=1):
        _query = self.query_template % row_number
        return self.predictor.predict(when_data=ClickhouseDS(_query,
                                                             host=self.host,
                                                             user=self.user,
                                                             password=self.password))

    def __repr__(self):
        return f"{self.dataset}_{self.__class__.__name__}"

    def __str__(self):
        return self.__repr__()

class AutoML:
    def __init__(self, dataset):
        self.dataset = dataset
        self.where_template = f"SELECT * FROM test_data.{self.dataset} LIMIT %s"
        self.query_template = f"SELECT {predict_targets[self.dataset]} FROM mindsdb.{self.dataset} WHERE select_data_query='%s'"

    def predict(self, row_number=1):
        where = self.where_template % row_number
        _query = self.query_template % where
        return query(_query)

    def __repr__(self):
        return f"{self.dataset}_{self.__class__.__name__}"

    def __str__(self):
        return self.__repr__()

parser = argparse.ArgumentParser(description='Prediction latency test.')
parser.add_argument("datasets_path", type=str, help="path to private-benchmarks/benchmarks/datasets dir")
parser.add_argument("--predictors_dir", type=str, default=os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../var/predictors')), help="path to mindsdb predictors dir. if not specified related path from this file will be used.")
parser.add_argument("--config_path", type=str, default=os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../etc/config.json')), help="full path to config.json. if not specified related path from this file will be used.")
parser.add_argument("--use_docker", type=bool, default=True, help="launch clickhouse in docker, if False will use local DB assuming that it is installed.")
parser.add_argument("--prepare_datasource", type=bool, default=True, help="prepare train/test sets from initial benchmark datasets")
parser.add_argument("--prepare_db", type=bool, default=True, help="fulfill database by test data")


if __name__ == '__main__':
    args = parser.parse_args()
    print(f"DATASETS_PATH: {args.datasets_path}")
    print(f"MINDSDB_STORAGE_PATH: {args.predictors_dir}")
    print(f"CONFIG_PATH: {args.config_path}")

    os.environ["MINDSDB_STORAGE_PATH"] = args.predictors_dir
    os.environ["CONFIG_PATH"] = args.config_path
    os.environ["DATASETS_PATH"] = args.datasets_path




    from mindsdb_native import Predictor, ClickhouseDS
    from prepare import query, datasets, predict_targets, prepare_env

    prepare_env(prepare_data=args.prepare_datasource,
                use_docker=args.use_docker,
                setup_db=args.prepare_db)

    rows = [1, ] + list(range(20, 101))
    for_report = {}
    for dataset in datasets:
        for predictor_type in [NativeDataFrame, NativeClickhouse, AutoML]:
            predictor = predictor_type(dataset)
            for_report[str(predictor)] = []
            for row_num in rows:
                started = time.time()
                predictor.predict(row_number=row_num)
                duration = time.time() - started
                duration = round(duration, 5)
                for_report[str(predictor)].append(duration)

    df = pd.DataFrame(for_report)
    df.index = rows

    print("GOT NEXT TEST RESULTS:")
    print(df)
    df.to_csv("latency_prediction_result.csv")
    print("Done. Results saved to latency_prediction_result.csv")

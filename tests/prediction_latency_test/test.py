import os
import time

import pandas as pd

predictors_dir = "/home/itsyplen/repos/work/MindsDB/mindsdb/var/predictors"
os.environ["MINDSDB_STORAGE_PATH"] = predictors_dir
from mindsdb_native import Predictor, ClickhouseDS

from prepare import query, datasets, predict_targets


def native_predictor_call(dataset, rows_number=1):
    p = Predictor(name=dataset)
    _query = f"SELECT * FROM test_data.{dataset} LIMIT {rows_number}"
    print(f"native_call query: {_query}")
    p.predict(when_data=ClickhouseDS(_query,
                                     host='127.0.0.1',
                                     user='root',
                                     password="iyDNE5g9fw9kdrCLIKoS3bkOJkE"))

class NativePredictorWithDataFrame:
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

class NativePredictorWithClickhouseDS:
    host = '127.0.0.1'
    user = 'root'
    password = "iyDNE5g9fw9kdrCLIKoS3bkOJkE"

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


def automl_call(dataset, rows_number=1):
    where = f"SELECT * FROM test_data.{dataset} LIMIT {rows_number}"
    _query = f"SELECT {predict_targets[dataset]} FROM mindsdb.{dataset} WHERE select_data_query='{where}'"
    print(f"automl_call query: {_query}")
    query(_query)


sep = "-" * 50
# rows = [1, ] + list(range(20, 101))
rows = [1, ] + list(range(20, 30))
for_report = {}
# for dataset in datasets:
#     print(f"{sep}{dataset}{sep}")
#     for row_number in rows:
#         for func in [native_predictor_call, automl_call]:
#             started = time.time()
#             func(dataset, rows_number=row_number)
#             finished = time.time()
#             print(f"{func.__name__} with rows={row_number} took {finished-started}")
#             duration = round(finished - started, 5)
#             key = f"{dataset}_{func.__name__}"
#             if key not in for_report:
#                 for_report[key] = [duration]
#             else:
#                 for_report[key].append(duration)

df = pd.DataFrame(for_report)
df.index = rows

print(df)

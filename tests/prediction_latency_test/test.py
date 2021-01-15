import os
import time

import pandas as pd

predictors_dir = "/home/itsyplen/repos/work/MindsDB/mindsdb/var/predictors"
os.environ["MINDSDB_STORAGE_PATH"] = predictors_dir
from mindsdb_native import Predictor, ClickhouseDS

from prepare import query, datasets, predict_targets


class NativeDataFrame:
    def __init__(self, dataset):
        self.dataset = dataset
        self.df = pd.read_csv(f"{dataset}_test.csv")
        self.predictor = Predictor(name=dataset)

    def predict(self, row_number=1):
        # print(f"{self.__class__.__name__}_{self.dataset} call predict with {row_number} rows")
        return self.predictor.predict(self.df[:row_number])

    def __repr__(self):
        return f"{self.dataset}_{self.__class__.__name__}"

    def __str__(self):
        return self.__repr__()

class NativeClickhouse:
    host = '127.0.0.1'
    user = 'root'
    password = "iyDNE5g9fw9kdrCLIKoS3bkOJkE"

    def __init__(self, dataset):
        self.dataset = dataset
        self.predictor = Predictor(name=dataset)
        self.query_template = f"SELECT * FROM test_data.{dataset} LIMIT %s"

    def predict(self, row_number=1):
        _query = self.query_template % row_number
        # print(f"{self.__class__.__name__}_{self.dataset} call predict with {_query}")
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
        # print(f"{self.__class__.__name__}_{self.dataset} call predict with {_query}")
        return query(_query)

    def __repr__(self):
        return f"{self.dataset}_{self.__class__.__name__}"

    def __str__(self):
        return self.__repr__()


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

print(df)

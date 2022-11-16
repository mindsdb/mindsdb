import os
import time
from unittest.mock import patch
import pandas as pd

from mindsdb_sql import parse_sql

# How to run:
#  env PYTHONPATH=./ pytest tests/unit/test_xgboost_handler.py
# Warning: uncomment and run too many sql-testcase in detector and forecaster will cost a long time
from mindsdb.integrations.handlers.merlion_handler.adapters import DefaultForecasterAdapter, SarimaForecasterAdapter, \
    ProphetForecasterAdapter, MSESForecasterAdapter, IsolationForestDetectorAdapter, \
    WindStatsDetectorAdapter, ProphetDetectorAdapter
# from mindsdb.integrations.handlers.merlion_handler.adapters import DefaultDetectorAdapter
from .executor_test_base import BaseExecutorTest

import pandas as pd
from pathlib import Path
import requests


class TestXgboost(BaseExecutorTest):
    def get_titanic_train_df(self) -> pd.DataFrame:
        df = pd.read_csv("/home/drill/Data/titanic/train.csv")
        return df

    def get_titanic_test_df(self) -> pd.DataFrame:
        df = pd.read_csv("/home/drill/Data/titanic/test.csv")
        return df

    def run_mindsdb_sql(self, sql):
        return self.command_executor.execute_command(
            parse_sql(sql, dialect='mindsdb')
        )

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_xgboost_regressor_sql(self, mock_handler):
        # prepare data
        df_train = self.get_titanic_train_df()
        df_test = self.get_titanic_test_df()
        self.set_handler(mock_handler, name='pg', tables={'titanic_train': df_train, "titanic_test": df_test})
        self.exec_train_and_predict(mock_handler=mock_handler, model_name="regressor", predict_val="Age",
                                    is_classifier=False)

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_xgboost_classifier_sql(self, mock_handler):
        # prepare data
        df_train = self.get_titanic_train_df()
        df_test = self.get_titanic_test_df()
        self.set_handler(mock_handler, name='pg', tables={'titanic_train': df_train, "titanic_test": df_test})
        self.exec_train_and_predict(mock_handler=mock_handler, model_name="classifier", predict_val="Survived")

    def exec_train_and_predict(self, mock_handler, model_name, predict_val, is_classifier=True):
        # create predictor
        train_cols = "PassengerId, Pclass, Sex, Age, SibSp, Parch, Fare, Cabin, Embarked"
        if is_classifier:
            train_cols = "PassengerId, Pclass, Sex, Age, SibSp, Parch, Fare, Cabin, Embarked, Survived"
        create_sql = f'''
                    CREATE PREDICTOR {model_name}_predictor
                    FROM pg
                    (select {train_cols} from titanic_train) 
                    PREDICT {predict_val}
                    using engine='xgboost'
                '''
        ret = self.run_mindsdb_sql(sql=create_sql)
        assert ret.error_code is None, "train failed: " + model_name

        self.wait_training(model_name=f'{model_name}_predictor')
        if is_classifier:
            select_str = F"p.{predict_val}, p.{predict_val}__prob"
        else:
            select_str = F"p.{predict_val}"
        predict_sql = f'''
                    select {select_str}
                    from {model_name}_predictor p
                    inner join pg.titanic_test t
                    '''
        ret = self.run_mindsdb_sql(sql=predict_sql)
        assert ret.error_code is None, "detect failed: " + model_name

    def wait_training(self, model_name):
        # wait
        done = False
        for attempt in range(900):
            ret = self.run_mindsdb_sql(
                f"select status from predictors where name='{model_name}'"
            )
            if len(ret.data) > 0:
                if ret.data[0][0] == 'complete':
                    done = True
                    break
                elif ret.data[0][0] == 'error':
                    break
            time.sleep(0.5)
        if not done:
            raise RuntimeError("predictor didn't created: " + model_name)

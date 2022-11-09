# How to run:
#   env PYTHONPATH=./ pytest tests/unit/test_rayserve_handler.py
import pandas as pd
from flask import Flask, jsonify, request
import multiprocessing
import requests
import time
from unittest.mock import patch
import json
import pickle
import base64


from mindsdb_sql import parse_sql
from .executor_test_base import BaseExecutorTest


app = Flask(__name__)
mock_server_port = 8129

@app.route('/ready', methods=['POST'])
def ready():
    rt = {"success": True}
    return json.dumps(rt)

@app.route('/my_model/train', methods=['POST'])
def train():
    data_dic = json.loads(request.data)
    df_bytes = base64.decodebytes(data_dic["df"].encode())
    df = pickle.loads(df_bytes)
    column_set = set(df.columns)
    assert 3 == len(column_set)
    assert 'col1' in column_set
    assert 'col2' in column_set
    assert 'label' in column_set
    assert "label" == data_dic["target"]
    assert isinstance(data_dic["predictor_id"], int)
    assert "rayserve_predictor" == data_dic["predictor_name"]
    rt = {"success": True}
    return json.dumps(rt)


@app.route('/my_model/predict', methods=['POST'])
def predict():
    data_dic = json.loads(request.data)
    df_bytes = base64.decodebytes(data_dic["df"].encode())
    df = pickle.loads(df_bytes)
    column_set = set(df.columns)
    assert 2 == len(column_set)
    assert 'col1' in column_set
    assert 'col2' in column_set
    assert "label" == data_dic["target"]
    assert isinstance(data_dic["predictor_id"], int)
    assert "rayserve_predictor" == data_dic["predictor_name"]
    rt = {data_dic["target"]: [1] * len(df)}
    return json.dumps(rt)


class TestRayServe(BaseExecutorTest):
    global mock_server_port

    def setup_class(cls):
        BaseExecutorTest.setup_class(cls)
        processor = multiprocessing.Process(target=app.run, kwargs={"debug": True,
                                                                    "host": "0.0.0.0", "port": mock_server_port})
        processor.daemon = True
        processor.start()
        i = 0
        while i < 30:
            try:
                resp = requests.post(f"http://127.0.0.1:{mock_server_port}/ready")
                if 200 == resp.status_code:
                    break
            except Exception as e:
                print("wait mock server to start")
            time.sleep(1)
            i += 1
        assert i < 30, "launch mock server failed"

    def set_project(self):
        r = self.db.Project.query.filter_by(name='mindsdb').first()
        if r is not None:
            self.db.session.delete(r)

        r = self.db.Project(
            id=1,
            name='mindsdb',
        )
        self.db.session.add(r)
        self.db.session.commit()

    def run_mindsdb_sql(self, sql):
        return self.command_executor.execute_command(
            parse_sql(sql, dialect='mindsdb')
        )

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_rayserve_create_and_query_sql(self, mock_handler):
        self.set_project()
        # prepare data
        fake_train = pd.DataFrame(data={"col1": [1, 2, 3], "col2": [4, 5, 6], "label": [1, 0, 1]})
        fake_pred = pd.DataFrame(data={"col1": [2, 3, 5], "col2": [3, 2, 6]})
        self.set_handler(mock_handler, name='pg', tables={'fake_train': fake_train, 'fake_pred': fake_pred})
        # test create predictor
        create_sql = f'''
                            CREATE PREDICTOR mindsdb.rayserve_predictor
                            FROM pg
                            (select col1, col2, label from fake_train)
                            PREDICT label
                            USING engine='rayserve', train_url='http://127.0.0.1:{mock_server_port}/my_model/train',
                                    predict_url='http://127.0.0.1:{mock_server_port}/my_model/predict'
                            '''
        ret = self.run_mindsdb_sql(sql=create_sql)
        assert ret.error_code != 200, "train failed: rayserve_predictor"
        self.wait_training(model_name=f'rayserve_predictor')

        # test create predictor
        create_sql = f'''
                                select t.col1, t.col2, p.label
                                from rayserve_predictor p
                                inner join pg.fake_pred t;
                            '''
        ret = self.run_mindsdb_sql(sql=create_sql)
        assert "col1" == ret.columns[0].name, "prediction result missing column col1"
        assert "col2" == ret.columns[1].name, "prediction result missing column col2"
        assert "label" == ret.columns[2].name, "prediction result missing column label"
        assert [2, 3, 1] == ret.data[0], "wrong prediction result line 0"
        assert [3, 2, 1] == ret.data[1], "wrong prediction result line 1"
        assert [5, 6, 1] == ret.data[2], "wrong prediction result line 2"

    def wait_training(self, model_name):
        # wait
        done = False
        for attempt in range(900):
            ret = self.run_mindsdb_sql(
                f"select status from mindsdb.predictors where name='{model_name}'"
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



# How to run:
#  env PYTHONPATH=./:$PYTHONPATH pytest tests/unit/ml_handlers/test_mlflow.py -ls

import time
import pytest
from unittest.mock import patch

from mindsdb_sql import parse_sql

from tests.unit.executor_test_base import BaseExecutorTest
from mindsdb.integrations.handlers.mlflow_handler.mlflow_handler import MLflowHandler


# TODO: fix patches
class TestMLFlow(BaseExecutorTest):
    def run_sql(self, sql):
        return self.command_executor.execute_command(
            parse_sql(sql, dialect='mindsdb')
        )

    @patch('mlflow.tracking.MlflowClient')
    @patch.object(MLflowHandler, '_check_model_url')
    @patch('mindsdb.integrations.handlers.mlflow_handler.mlflow_handler.requests.post')
    def test_mlflow(
            self,
            mock_internal_post,
            mock_handler_url_method,
            mock_mlflow_client
    ):
        mock_mlflow_client.search_registered_models.side_effect = ['test_mlflow']
        mock_internal_post.side_effect = requests.Request(json=['negative_sentiment'])
        mock_handler_url_method.side_effect = True
        ret = self.run_sql('''
           CREATE PREDICTOR mindsdb.test_mlflow
           PREDICT c
           USING 
             engine='mlflow',
             model_name='test_mlflow',
             mlflow_server_url='http://0.0.0.0:5001/',
             mlflow_server_path='sqlite:////mlflow.db',
             predict_url='http://localhost:5000/invocations';
        ''')
        assert ret.error_code is None

        time.sleep(3)

        ret = self.run_sql('''
           SELECT p.*
           FROM mindsdb.test_mlflow as p
           WHERE text="The tsunami is coming, seek high ground";
        ''')
        assert ret.error_code is None
        assert ret.c == '0'


if __name__ == '__main__':
    pytest.main(['test_mlflow.py'])

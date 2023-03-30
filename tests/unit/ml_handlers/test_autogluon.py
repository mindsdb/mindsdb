import time
from unittest.mock import patch
import pandas as pd
from mindsdb_sql import parse_sql

# added a try except block to deal with relative path import errors
try:
    from tests.unit.executor_test_base import BaseExecutorTest
except ImportError:
    from ..executor_test_base import BaseExecutorTest


class TestAutoGluon(BaseExecutorTest):
    def setup_method(self):
        super().setup_method()
        self.set_executor()
        # dataset, string values
        df = pd.DataFrame(range(1, 50), columns=['a'])
        df['b'] = 50 - df.a
        df['c'] = round((df['a'] * 3 + df['b']) / 50)
        self.df = df
        self.run_sql('create database proj')

    def wait_predictor(self, project, name):
        # wait
        done = False
        for attempt in range(200):
            ret = self.run_sql(
                f"select * from {project}.models where name='{name}'"
            )
            if not ret.empty:
                if ret['STATUS'][0] == 'complete':
                    done = True
                    break
                elif ret['STATUS'][0] == 'error':
                    break
            time.sleep(10)
        if not done:
            raise RuntimeError("predictor wasn't created")

    def run_sql(self, sql):
        ret = self.command_executor.execute_command(
            parse_sql(sql, dialect='mindsdb')
        )
        assert ret.error_code is None
        if ret.data is not None:
            columns = [
                col.alias if col.alias is not None else col.name
                for col in ret.columns
            ]
            return pd.DataFrame(ret.data, columns=columns)

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_simple(self, mock_handler):

        self.set_handler(mock_handler, name='pg', tables={'df': self.df})

        # create predictor
        self.run_sql('''create model proj.modelx from pg (select * from df) predict c using engine='autogluon',
        tag = 'test_model';''')
        self.wait_predictor('proj', 'modelx')

        # run predict
        ret = self.run_sql('''
           SELECT p.*
           FROM pg.df as t
           JOIN proj.modelx as p
           where t.c=1
        ''')
        avg_c = pd.to_numeric(ret.c).mean()
        # value is around 1
        assert (avg_c > 0.9) and (avg_c < 1.1)

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_describe_model(self, mock_handler):
        self.set_handler(mock_handler, name='pg', tables={'df': self.df})

        # create predictor
        self.run_sql('''
            create model proj.modelx
            from pg (select * from df)
            predict c
            using
                engine='autogluon',
                tag = 'test_model';
            ''')
        self.wait_predictor('proj', 'modelx')

        # run predict
        ret = self.run_sql('''
            DESCRIBE MODEL proj.modelx.model
            ''')
        # value is greater than 0, since atleast one model has been evaluated
        assert (len(ret) > 0) and \
               all(x in ['model', 'score_test', 'fit_order', 'pred_time_test', 'fit_time'] for x in ret.columns)

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_describe_feature(self, mock_handler):
        self.set_handler(mock_handler, name='pg', tables={'df': self.df})

        # create predictor
        self.run_sql('''
            create model proj.modelx
            from pg (select * from df)
            predict c
            using
                engine='autogluon',
                tag = 'test_model';
            ''')
        self.wait_predictor('proj', 'modelx')

        # run describe
        ret = self.run_sql('''
            DESCRIBE MODEL proj.modelx.features
            ''')
        # value is greater than 1, since atleast target and atleast one feature is used and the roles feature and
        # target are present
        assert (len(ret) > 1) and ret['role'].isin(['feature', 'target']).all()

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_describe_model_noattr(self, mock_handler):
        self.set_handler(mock_handler, name='pg', tables={'df': self.df})

        # create predictor
        self.run_sql('''
            create model proj.modelx
            from pg (select * from df)
            predict c
            using
                engine='autogluon',
                tag = 'test_model';
            ''')
        self.wait_predictor('proj', 'modelx')

        # run predict
        ret = self.run_sql('''
            DESCRIBE MODEL proj.modelx
            ''')
        # value is greater than 0, since atleast one model has been evaluated and relevant colums are present
        assert len(ret) > 0 and \
               all(x in ['best_model', 'eval_metric', 'original_features', 'problem_type'] for x in ret.columns)

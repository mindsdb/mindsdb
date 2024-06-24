import time
from unittest.mock import patch
import pandas as pd

from mindsdb_sql import parse_sql


from unit.executor_test_base import BaseExecutorTest


class TestAutoGluon(BaseExecutorTest):
    def wait_predictor(self, project, name):
        # wait
        done = False
        for attempt in range(200):
            ret = self.run_sql(
                f"select * from {project}.models where name='{name}'"
            )
            # print(ret['STATUS'][0])
            if not ret.empty:
                if ret['STATUS'][0] == 'complete':
                    done = True
                    break
                elif ret['STATUS'][0] == 'error':
                    # print(f"{ret['ERROR'][0]}")
                    break
            time.sleep(15)
        if not done:
            raise RuntimeError("predictor wasn't created")

    def run_sql(self, sql):
        ret = self.command_executor.execute_command(
            parse_sql(sql, dialect='mindsdb')
        )
        assert ret.error_code is None
        if ret.data is not None:
            return ret.data.to_df()

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_simple(self, mock_handler):

        # dataset, string values
        df = pd.DataFrame(range(1, 50), columns=['a'])
        df['b'] = 50 - df.a
        df['c'] = round((df['a'] * 3 + df['b']) / 50)

        self.set_handler(mock_handler, name='pg', tables={'df': df})

        # create project
        self.run_sql('create database proj;')

        # create predictor
        self.run_sql('''
            create model proj.modelx
            from pg (select * from df)
            predict c
            using
                engine='autogluon';
        ''')

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


# df = pd.DataFrame(range(1, 50), columns=['a'])
# df['b'] = 50 - df.a
# df['c'] = round((df['a']*3 + df['b']) / 50)

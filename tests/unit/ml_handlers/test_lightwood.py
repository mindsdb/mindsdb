import time
from unittest.mock import patch
import pandas as pd

from mindsdb_sql import parse_sql


from tests.unit.executor_test_base import BaseExecutorTest


class TestLW(BaseExecutorTest):

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
            time.sleep(0.5)
        if not done:
            raise RuntimeError("predictor didn't created")

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

        # dataset, string values
        df = pd.DataFrame(range(1, 50), columns=['a'])
        df['b'] = 50 - df.a
        df['c'] = round((df['a']*3 + df['b']) / 50)

        self.set_handler(mock_handler, name='pg', tables={'df': df})

        # create project
        self.run_sql('create database proj')

        # create predictor
        self.run_sql('''
           create model proj.modelx
           from pg (select * from df)
           predict c
           using 
             submodels = [{
                "module": "Regression",
                "args": {}
             }]
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

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_ts(self, mock_handler):
        # TS
        df2 = pd.DataFrame(pd.date_range(start='1/1/2018', end='1/31/2018'), columns=['t'])
        df3 = df2.copy()

        df2['a'] = 'a'
        df2['x'] = range(1, 32)

        df3['a'] = 'b'
        df3['x'] = range(11, 42)

        df = pd.concat([df2, df3])
        self.set_handler(mock_handler, name='pg', tables={'df': df})

        # create project
        self.run_sql('create database proj')

        # TS predictor
        # create predictor
        self.run_sql('''
           create model proj.modelx
           from pg (select * from df)
           predict x
           order by t
           group by a
           window 5
           horizon 3
        ''')
        self.wait_predictor('proj', 'modelx')

        # run predict
        ret = self.run_sql('''
           SELECT p.*
           FROM pg.df as t 
           JOIN proj.modelx as p
           where t.a='b' and t.t > latest
        ''')
        # LW can predict
        assert list(ret.x) == [42, 43, 44]

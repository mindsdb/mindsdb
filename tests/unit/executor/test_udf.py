import os
from textwrap import dedent
from tempfile import TemporaryDirectory

from unittest.mock import patch

import pandas as pd
import pytest

from mindsdb_sql.parser.dialects.mindsdb import CreateMLEngine
from mindsdb_sql.parser.ast import Identifier

from tests.unit.executor_test_base import BaseExecutorDummyML


@pytest.mark.parametrize('byom_type', ['inhouse', 'venv'])
class TestBYOM(BaseExecutorDummyML):

    def _create_engine(self, name, code, **kwargs):
        with TemporaryDirectory(prefix='udf_test_') as temp_dir:
            code_path = os.path.join(temp_dir, 'code.py')
            reqs_path = os.path.join(temp_dir, 'reqs.py')

            open(code_path, 'w').write(code)
            open(reqs_path, 'w').write('')

            params = {
                'code': code_path,
                'modules': reqs_path,
            }
            params.update(kwargs)

            ret = self.command_executor.execute_command(
                CreateMLEngine(
                    name=Identifier(name),
                    handler='byom',
                    params=params
                )
            )
            assert ret.error_code is None

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_udf(self, data_handler, byom_type):

        df = pd.DataFrame([
            {'a': 3, 'b': 4, 'c': 'a', 'd': 'b'},
        ])
        self.set_handler(data_handler, name='pg', tables={'sample': df})

        code = dedent("""
            from os import listdir # imported function

            def fibo(num: int) -> int:
                if num < 2:
                    return num
                return fibo(num - 1) + fibo(num - 2)

            # not annotated
            def add1(a, b):
                return a + b

            # annotated
            def add2(a: int, b: int) -> int:
                return a + b
        """)

        self._create_engine(name='myml', code=code,
                            type=byom_type, mode='custom_function')

        ret = self.run_sql('''
            select myml.fibo(b) x,
                   myml.add1(a,b) y,
                   myml.add2(a,b) z
            from pg.sample
        ''')
        assert ret['x'][0] == 3
        assert ret['y'][0] == '34'
        assert ret['z'][0] == 7

        # test without table
        ret = self.run_sql('''
            select myml.fibo(4) x
        ''')
        assert ret['x'][0] == 3

    def test_byom(self, byom_type):

        code = dedent("""
            from datetime import datetime
            import pandas as pd

            class MyBYOM():

                def train(self, df, target_col, args=None):
                    self.target_col = target_col
                    self.value = '>my_response'

                def predict(self, df):
                    df[self.target_col] = df['input_col'] + self.value

                    return df[[self.target_col]]
        """)

        self._create_engine(name='myml', code=code, type=byom_type)

        self.run_sql('''
            create model m1
            predict output_col
            using engine='myml',
                  join_learn_process=true
        ''')

        ret = self.run_sql('''
            select * from m1
            where input_col = 'my_input'
        ''')
        assert ret['output_col'][0] == 'my_input>my_response'

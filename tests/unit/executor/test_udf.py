import os
from textwrap import dedent
from tempfile import TemporaryDirectory

from unittest.mock import patch

import pandas as pd

from mindsdb_sql.parser.dialects.mindsdb import CreateMLEngine
from mindsdb_sql.parser.ast import Identifier

from tests.unit.executor_test_base import BaseExecutorDummyML


class TestUDF(BaseExecutorDummyML):

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_simple(self, data_handler):
        df = pd.DataFrame([
            {'a': 3, 'b': 4, 'c': 'a', 'd': 'b'},
        ])
        self.set_handler(data_handler, name='pg', tables={'sample': df})

        # for speed up
        os.environ['MINDSDB_BYOM_DEFAULT_TYPE'] = 'inhouse'

        code = dedent("""
            class MyBYOM():
                def fibo(self, num: int) -> int:
                    if num < 2:
                        return num
                    return self.fibo(num - 1) + self.fibo(num - 2)

                # not annotated
                def add1(self, a, b):
                    return a + b

                # annotated
                def add2(self, a: int, b: int) -> int:
                    return a + b

                # required for byom
                def predict(self): pass
                def train(self): pass
        """)

        # add model
        with TemporaryDirectory(prefix='jobs_test_') as temp_dir:
            code_path = os.path.join(temp_dir, 'code.py')
            reqs_path = os.path.join(temp_dir, 'reqs.py')

            open(code_path, 'w').write(code)
            open(reqs_path, 'w').write('')

            ret = self.command_executor.execute_command(
                CreateMLEngine(
                    name=Identifier('myml'),
                    handler='byom',
                    params={
                        'code': code_path,
                        'modules': reqs_path
                    }
                )
            )
            assert ret.error_code is None

        ret = self.run_sql('''
            select myml.fibo(b) x,
                   myml.add1(a,b) y,
                   myml.add2(a,b) z
            from pg.sample
        ''')
        assert ret['x'][0] == 3
        assert ret['y'][0] == '34'
        assert ret['z'][0] == 7

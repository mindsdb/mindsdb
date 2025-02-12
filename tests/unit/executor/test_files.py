import tempfile
import shutil
from pathlib import Path

import pandas as pd

from tests.unit.executor_test_base import BaseExecutorDummyML


class TestFiles(BaseExecutorDummyML):

    def test_create_table(self):
        df = pd.DataFrame([
            {'a': 6, 'c': 1},
            {'a': 7, 'c': 2},
        ])
        self.set_data('table1', df)

        self.run_sql('''
            create table files.myfile
            select * from dummy_data.table1
        ''')

        self.run_sql('''
            create or replace table files.myfile
            select * from dummy_data.table1
        ''')

        ret = self.run_sql('select count(*) c from files.myfile')
        assert ret['c'][0] == 2

        self.run_sql('''
            insert into files.myfile (
              select * from dummy_data.table1
            )
        ''')

        ret = self.run_sql('select count(*) c from files.myfile')
        assert ret['c'][0] == 4

        self.run_sql('''
            insert into files.myfile (a)
            values (9)
        ''')

        ret = self.run_sql('select count(*) c from files.myfile')
        assert ret['c'][0] == 5

    def test_multipage(self):

        # copy test file because source will be removed after uloading
        source_path = Path(__file__).parent / 'data' / 'test.xlsx'
        _, file_path = tempfile.mkstemp()
        shutil.copy(source_path, file_path)
        self.file_controller.save_file('test', file_path, source_path.name)

        ret = self.run_sql('select * from files.test')
        assert len(ret) == 2
        first, second = ret[ret.columns[0]]

        # first page
        ret = self.run_sql(f'select * from files.test.{first}')
        assert len(ret.columns) == 4

        # second page
        ret = self.run_sql(f'select * from files.test.{second}')
        assert len(ret.columns) == 2

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

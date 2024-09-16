import datetime as dt
import pytest

import pandas as pd

from tests.unit.executor_test_base import BaseExecutorDummyML


class TestModels(BaseExecutorDummyML):

    def test_empty_df(self):
        # -- create model --
        self.run_sql(
            '''
                CREATE model mindsdb.task_model
                PREDICT a
                using engine='dummy_ml',
                join_learn_process=true
            '''
        )
        self.wait_predictor('mindsdb', 'task_model')

    def test_replace_model(self):
        # create model
        self.run_sql(
            '''
                CREATE or REPLACE model task_model
                PREDICT a
                using engine='dummy_ml',
                join_learn_process=true
            '''
        )
        self.wait_predictor('mindsdb', 'task_model')

        # recreate
        self.run_sql(
            '''
                CREATE or REPLACE model task_model
                PREDICT a
                using engine='dummy_ml',
                join_learn_process=true, my_param='a'
            '''
        )
        self.wait_predictor('mindsdb', 'task_model')

        # test json operator
        resp = self.run_sql("select training_options->'using'->'my_param' param from models where name='task_model' ")

        # FIXME duckdb returns result quoted
        assert resp['param'][0] == '"a"'

    def test_create_validation(self):
        from mindsdb.integrations.libs.ml_exec_base import MLEngineException
        with pytest.raises(MLEngineException):
            self.run_sql(
                '''
                    CREATE model task_model_x
                    PREDICT a
                    using
                       engine='dummy_ml',
                       error=1
                '''
            )

    def test_describe(self):
        self.run_sql(
            '''
                CREATE model mindsdb.pred
                PREDICT p
                using engine='dummy_ml',
                join_learn_process=true
            '''
        )
        ret = self.run_sql('describe mindsdb.pred')
        assert ret['TABLES'][0] == ['info']

        ret = self.run_sql('describe pred')
        assert ret['TABLES'][0] == ['info']

        ret = self.run_sql('describe mindsdb.pred.info')
        assert ret['type'][0] == 'dummy'

        ret = self.run_sql('describe pred.info')
        assert ret['type'][0] == 'dummy'

    def test_create_engine(self):
        self.run_sql('''
            CREATE ML_ENGINE my_engine
            FROM dummy_ml
            USING
               unquoted_arg = yourkey,
               json_arg = {
                  "type": "service_account",
                  "project_id": "123456"
               }
        ''')

        self.run_sql(
            '''
               CREATE model pred
                PREDICT p
                using engine='my_engine',
                join_learn_process=true
            '''
        )

        ret = self.run_sql('select * from pred where a=1')
        args = ret['engine_args'][0]

        # check unquoted value
        assert args['unquoted_arg'] == 'yourkey'

        # check json value
        assert args['json_arg']['project_id'] == '123456'

    def test_model_column_maping(self):
        df = pd.DataFrame([
            {'a': 10, 'c': 30},
            {'a': 20, 'c': 40},
        ])
        self.set_data('tbl', df)

        self.run_sql(
            '''
                CREATE model mindsdb.pred
                PREDICT p
                using engine='dummy_ml',
                join_learn_process=true
            '''
        )
        ret = self.run_sql('''
            select * from dummy_data.tbl t
            join pred m on m.input = t.a
        ''')
        assert ret['output'][0] == 10

        # without aliases
        ret = self.run_sql('''
            select * from dummy_data.tbl
            join pred on pred.input = tbl.c
        ''')
        assert ret['output'][0] == 30

        # get mapped column
        ret = self.run_sql('''
            select t.a from dummy_data.tbl t
            join pred m on m.input = t.a
        ''')
        assert ret['a'][0] == 10

    def test_predict_partition(self):
        df = pd.DataFrame([
            {'a': 1, 'b': dt.datetime(2020, 1, 1)},
            {'a': 2, 'b': dt.datetime(2020, 1, 2)},
            {'a': 3, 'b': dt.datetime(2020, 1, 3)},
            {'a': 4, 'b': dt.datetime(2020, 1, 5)},
            {'a': 5, 'b': dt.datetime(2020, 1, 6)},
            {'a': 6, 'b': dt.datetime(2020, 1, 7)},
        ])
        self.save_file('tasks', df)

        # -- create model --
        self.run_sql(
            '''
                CREATE model mindsdb.task_model
                from files (select * from tasks)
                PREDICT a
                using engine='dummy_ml',
                join_learn_process=true
            '''
        )

        # use model
        ret = self.run_sql('''
             SELECT *
               FROM files.tasks as t
               JOIN mindsdb.task_model as m
               using partition_size=2
        ''')

        # the same rows in output
        assert len(ret) == 6
        assert set(df['a']) == set(ret['a'])

        # all predicted
        assert list(ret.predicted.unique()) == [42]

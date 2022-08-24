from unittest.mock import patch
import pandas as pd

from lightwood.api import dtype

from mindsdb_sql import parse_sql


# How to run:
#  env PYTHONPATH=./ pytest tests/unit/test_executor.py

from .executor_test_base import BaseTestCase


class Test(BaseTestCase):
    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_integration_select(self, mock_handler):

        data = [[1, 'x'], [1, 'y']]
        df = pd.DataFrame(data, columns=['a', 'b'])
        self.set_handler(mock_handler, name='pg', tables={'tasks': df})

        ret = self.command_executor.execute_command(parse_sql('select * from pg.tasks'))
        assert ret.error_code is None
        assert ret.data == data

        # check sql in query method
        assert mock_handler().query.mock_calls[0].args[0].to_string() == 'SELECT * FROM tasks'

    def test_predictor_1_row(self):
        predicted_value = 3.14
        predictor = {
            'name': 'task_model',
            'predict': 'p',
            'dtypes': {
                'p': dtype.float,
                'a': dtype.integer,
                'b': dtype.categorical
            },
            'predicted_value': predicted_value
        }
        self.set_predictor(predictor)
        ret = self.command_executor.execute_command(parse_sql(f'''
             select * from mindsdb.task_model where a = 2
           ''', dialect='mindsdb'))
        ret_df = self.ret_to_df(ret)
        assert ret_df['p'][0] == predicted_value


class TestTableau(BaseTestCase):
    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_predictor_nested_select(self, mock_handler):
        df = pd.DataFrame([
            {'a': 1, 'b': 'one'},
            {'a': 2, 'b': 'two'},
            {'a': 1, 'b': 'three'},
        ])
        self.set_handler(mock_handler, name='pg', tables={'tasks': df})

        # --- use predictor ---
        predictor = {
            'name': 'task_model',
            'predict': 'p',
            'dtypes': {
                'p': dtype.float,
                'a': dtype.integer,
                'b': dtype.categorical
            },
            'predicted_value': 3.14
        }
        self.set_predictor(predictor)
        ret = self.command_executor.execute_command(parse_sql(f'''
              SELECT 
              `Custom SQL Query`.`a` AS `height`,
              last(`Custom SQL Query`.`b`) AS `length1`
            FROM (
               SELECT res.a, res.b 
               FROM pg.tasks as source
               JOIN mindsdb.task_model as res
            ) `Custom SQL Query`
            group by 1
            LIMIT 1
                ''', dialect='mindsdb'))
        assert ret.error_code is None

        # second column is having last value of 'b'
        assert ret.data[0][1] == 'three'

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_predictor_tableau_header(self, mock_handler):
        df = pd.DataFrame([
            {'a': 1, 'b': 'one'},
            {'a': 2, 'b': 'two'},
            {'a': 1, 'b': 'three'},
        ])
        self.set_handler(mock_handler, name='pg', tables={'tasks': df})

        # --- use predictor ---
        predicted_value = 5
        predictor = {
            'name': 'task_model',
            'predict': 'p',
            'dtypes': {
                'p': dtype.float,
                'a': dtype.integer,
                'b': dtype.categorical
            },
            'predicted_value': predicted_value
        }
        self.set_predictor(predictor)
        ret = self.command_executor.execute_command(parse_sql(f'''
           SELECT 
              SUM(1) AS `cnt__0B4A4E8BD11C48FFB4730D4D2C32191A_ok`,
              sum(`Custom SQL Query`.`a`) AS `sum_height_ok`,
              max(`Custom SQL Query`.`p`) AS `sum_length1_ok`
            FROM (
              SELECT res.a, res.p 
               FROM pg.tasks as source
               JOIN mindsdb.task_model as res
            ) `Custom SQL Query`
            HAVING (COUNT(1) > 0)
                ''', dialect='mindsdb'))

        # second column is having last value of 'b'
        # 3: count rows, 4: sum of 'a', 5 max of prediction
        assert ret.data[0] == [3, 4, 5]


class TestWithNativeQuery(BaseTestCase):
    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_integration_native_query(self, mock_handler):

        data = [[3, 'y'], [1, 'y']]
        df = pd.DataFrame(data, columns=['a', 'b'])
        self.set_handler(mock_handler, name='pg', tables={'tasks': df})

        ret = self.command_executor.execute_command(parse_sql(
              'select max(a) from pg (select * from tasks) group by b',
            dialect='mindsdb'))

        # native query was called
        assert mock_handler().native_query.mock_calls[0].args[0] == 'select * from tasks'
        assert ret.data[0][0] == 3

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_view_native_query(self, mock_handler):
        data = [[3, 'y'], [1, 'y']]
        df = pd.DataFrame(data, columns=['a', 'b'])
        self.set_handler(mock_handler, name='pg', tables={'tasks': df})

        # --- create view ---
        ret = self.command_executor.execute_command(parse_sql(
            'create view vtasks (select * from pg (select * from tasks))',
            dialect='mindsdb')
        )
        # no error
        assert ret.error_code is None

        # --- select from view ---
        ret = self.command_executor.execute_command(parse_sql(
            'select * from views.vtasks',
            dialect='mindsdb')
        )
        assert ret.error_code is None
        # view response equals data from integration
        assert ret.data == data

        # --- create predictor ---
        mock_handler.reset_mock()
        ret = self.command_executor.execute_command(parse_sql(
            '''
                CREATE PREDICTOR task_model 
                FROM views 
                (select * from vtasks) 
                PREDICT a
            ''',
            dialect='mindsdb'))
        assert ret.error_code is None

        # learn was called
        assert self.mock_learn.mock_calls[0].args[0].name.to_string() == 'task_model'
        # integration was called
        # TODO: integration is not called during learn process because learn function is mocked
        #   (data selected inside learn function)
        # assert mock_handler().native_query.mock_calls[0].args[0] == 'select * from tasks'

        # --- drop view ---
        ret = self.command_executor.execute_command(parse_sql(
            'drop view vtasks',
            dialect='mindsdb'))
        assert ret.error_code is None

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_use_predictor_with_view(self, mock_handler):
        # set integration data

        df = pd.DataFrame([
            {'a': 1, 'b': 'one'},
            {'a': 2, 'b': 'two'},
            {'a': 1, 'b': 'three'},
        ])
        self.set_handler(mock_handler, name='pg', tables={'tasks': df})

        view_name = 'vtasks'
        # --- create view ---
        ret = self.command_executor.execute_command(parse_sql(
            f'create view {view_name} (select * from pg (select * from tasks))',
            dialect='mindsdb')
        )
        assert ret.error_code is None

        # --- use predictor ---
        predicted_value = 3.14
        predictor = {
            'name': 'task_model',
            'predict': 'p',
            'dtypes': {
                'p': dtype.float,
                'a': dtype.integer,
                'b': dtype.categorical
            },
            'predicted_value': predicted_value
        }
        self.set_predictor(predictor)
        ret = self.command_executor.execute_command(parse_sql(f'''
           select task_model.p 
           from views.{view_name}
           join mindsdb.task_model
           where {view_name}.a = 2
        ''', dialect='mindsdb'))
        assert ret.error_code is None

        # native query was called
        assert mock_handler().native_query.mock_calls[0].args[0] == 'select * from tasks'

        # check predictor call

        # prediction was called
        assert self.mock_predict.mock_calls[0].args[0] == 'task_model'

        # input = one row whit a==2
        when_data = self.mock_predict.mock_calls[0].args[1]
        assert len(when_data) == 1
        assert when_data[0]['a'] == 2

        # check prediction
        assert ret.data[0][0] == predicted_value
        assert len(ret.data) == 1

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_use_ts_predictor_with_view(self, mock_handler):
        # set integration data

        df = pd.DataFrame([
            {'a': 1, 't': 1, 'g': 'x'},
            {'a': 2, 't': 2, 'g': 'x'},
            {'a': 3, 't': 3, 'g': 'x'},
            {'a': 4, 't': 1, 'g': 'y'},
            {'a': 5, 't': 2, 'g': 'y'},
            {'a': 6, 't': 3, 'g': 'y'},
            {'a': 7, 't': 1, 'g': 'z'},
            {'a': 8, 't': 2, 'g': 'z'},
            {'a': 9, 't': 3, 'g': 'z'},
        ])
        self.set_handler(mock_handler, name='pg', tables={'tasks': df})

        view_name = 'vtasks'
        # --- create view ---
        ret = self.command_executor.execute_command(parse_sql(
            f'create view {view_name} (select * from pg (select * from tasks))',
            dialect='mindsdb')
        )
        assert ret.error_code is None

        # --- use TS predictor ---
        predicted_value = 'right'
        predictor = {
            'name': 'task_model',
            'predict': 'p',
            'problem_definition': {
                'timeseries_settings': {
                    'is_timeseries': True,
                    'window': 10,
                    'order_by': 't',
                    'group_by': 'g',
                    'horizon': 1
                }
            },
            'dtypes': {
                'p': dtype.categorical,
                'a': dtype.integer,
                't': dtype.integer,
                'g': dtype.categorical,
            },
            'predicted_value': predicted_value
        }
        self.set_predictor(predictor)
        ret = self.command_executor.execute_command(parse_sql(f'''
           select task_model.*
           from views.{view_name}
           join mindsdb.task_model
           where {view_name}.g = 'y'
        ''', dialect='mindsdb'))
        assert ret.error_code is None

        # native query was called without filters
        assert mock_handler().native_query.mock_calls[0].args[0] == 'select * from tasks'

        # check predictor call
        # prediction was called
        assert self.mock_predict.mock_calls[0].args[0] == 'task_model'

        # input to predictor is only 3 rows g=='y'
        when_data = self.mock_predict.mock_calls[0].args[1]
        assert len(when_data) == 3
        for row in when_data:
            assert row['g'] == 'y'

        # check prediction
        # output is has  g=='y' or None
        ret_df = self.ret_to_df(ret)
        assert ret_df[(ret_df['g'] != 'y') & (~ret_df['g'].isna())].empty

        # p is predicted value
        assert ret_df['p'][0] == predicted_value

import os.path
from unittest.mock import patch
import pandas as pd
import datetime as dt
import pytest
import tempfile

import numpy as np
from lightwood.api import dtype

from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender

# How to run:
#  env PYTHONPATH=./ pytest tests/unit/test_executor.py

from .executor_test_base import BaseExecutorTestMockModel


class Test(BaseExecutorTestMockModel):
    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_integration_select(self, mock_handler):

        data = [[1, 'x'], [1, 'y']]
        df = pd.DataFrame(data, columns=['a', 'b'])
        self.set_handler(mock_handler, name='pg', tables={'tasks': df})

        ret = self.command_executor.execute_command(parse_sql('select * from pg.tasks'))
        assert ret.error_code is None
        assert ret.data == data

        # check sql in query method
        assert mock_handler().query.call_args[0][0].to_string() == 'SELECT * FROM tasks'

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

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_dates(self, mock_handler):
        df = pd.DataFrame([
            {'a': 1, 'b': dt.datetime(2020, 1, 1)},
            {'a': 2, 'b': dt.datetime(2020, 1, 2)},
            {'a': 1, 'b': dt.datetime(2020, 1, 3)},
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
            SELECT a, last(b)
            FROM (
               SELECT res.a, res.b 
               FROM pg.tasks as source
               JOIN mindsdb.task_model as res
            ) 
            group by 1
            order by a
           ''', dialect='mindsdb'))
        assert ret.error_code is None

        assert len(ret.data) == 2
        # is last datetime value of a = 1
        assert ret.data[0][1].isoformat() == dt.datetime(2020, 1, 3).isoformat()

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_ts_predictor(self, mock_handler):
        # set integration data

        df = pd.DataFrame([
            {'a': 1, 't': dt.datetime(2020, 1, 1), 'g': 'x'},
            {'a': 2, 't': dt.datetime(2020, 1, 2), 'g': 'x'},
            {'a': 3, 't': dt.datetime(2020, 1, 3), 'g': 'x'},
        ])
        self.set_handler(mock_handler, name='pg', tables={'tasks': df})

        # --- use TS predictor ---

        predictor = {
            'name': 'task_model',
            'predict': 'a',
            'problem_definition': {
                'timeseries_settings': {
                    'is_timeseries': True,
                    'window': 2,
                    'order_by': 't',
                    'group_by': 'g',
                    'horizon': 3
                }
            },
            'dtypes': {
                'a': dtype.integer,
                't': dtype.date,
                'g': dtype.categorical,
            },
            'predicted_value': ''
        }
        self.set_predictor(predictor)

        # set predictor output
        predict_result = [
            # window
            {'a': 2, 't': dt.datetime(2020, 1, 2), 'g': 'x', '__mindsdb_row_id': 2},
            {'a': 3, 't': dt.datetime(2020, 1, 3), 'g': 'x', '__mindsdb_row_id': 3},
            # horizon
            {'a': 1, 't': dt.datetime(2020, 1, 4), 'g': 'x', '__mindsdb_row_id': None},
            {'a': 1, 't': dt.datetime(2020, 1, 5), 'g': 'x', '__mindsdb_row_id': None},
            {'a': 1, 't': dt.datetime(2020, 1, 6), 'g': 'x', '__mindsdb_row_id': None},
        ]
        self.mock_predict.side_effect = lambda *a, **b: predict_result

        # = latest  ______________________
        ret = self.command_executor.execute_command(parse_sql(f'''
                select p.* from pg.tasks t
                join mindsdb.task_model p
                where t.t = latest
            ''', dialect='mindsdb'))
        assert ret.error_code is None

        ret_df = self.ret_to_df(ret)
        # one key with max value of a
        assert ret_df.shape[0] == 1
        assert ret_df.t[0] == dt.datetime(2020, 1, 3)

        # > latest ______________________
        ret = self.command_executor.execute_command(parse_sql(f'''
                select p.* from pg.tasks t
                join mindsdb.task_model p
                where t.t > latest
            ''', dialect='mindsdb'))
        assert ret.error_code is None

        ret_df = self.ret_to_df(ret)
        assert ret_df.shape[0] == 3
        assert ret_df.t.min() == dt.datetime(2020, 1, 4)

        # > date ______________________
        ret = self.command_executor.execute_command(parse_sql(f'''
                select p.* from pg.tasks t
                join mindsdb.task_model p
                where t.t > '2020-01-02'
            ''', dialect='mindsdb'))
        assert ret.error_code is None

        ret_df = self.ret_to_df(ret)
        assert ret_df.shape[0] == 4
        assert ret_df.t.min() == dt.datetime(2020, 1, 3)

        # between ______________________
        # set predictor output
        predict_result = [
            # window
            {'a': 1, 't': dt.datetime(2020, 1, 1), 'g': 'x', '__mindsdb_row_id': 1},
            {'a': 2, 't': dt.datetime(2020, 1, 2), 'g': 'x', '__mindsdb_row_id': 2},
            {'a': 3, 't': dt.datetime(2020, 1, 3), 'g': 'x', '__mindsdb_row_id': 3},
            # horizon
            {'a': 1, 't': dt.datetime(2020, 1, 4), 'g': 'x', '__mindsdb_row_id': None},
            {'a': 1, 't': dt.datetime(2020, 1, 5), 'g': 'x', '__mindsdb_row_id': None},
            {'a': 1, 't': dt.datetime(2020, 1, 6), 'g': 'x', '__mindsdb_row_id': None},

        ]
        self.mock_predict.side_effect = lambda *a, **b: predict_result

        ret = self.command_executor.execute_command(parse_sql(f'''
                select p.* from pg.tasks t
                join mindsdb.task_model p
                where t.t between '2020-01-02' and '2020-01-03' 
            ''', dialect='mindsdb'))
        assert ret.error_code is None

        ret_df = self.ret_to_df(ret)
        assert ret_df.shape[0] == 2
        assert ret_df.t.min() == dt.datetime(2020, 1, 2)
        assert ret_df.t.max() == dt.datetime(2020, 1, 3)

    def test_ts_predictor_file(self):
        # set integration data

        # save as file
        df = pd.DataFrame([
            {'a': 1, 't': '2021', 'g': 'x'},
            {'a': 2, 't': '2022', 'g': 'x'},
            {'a': 3, 't': '2023', 'g': 'x'},
        ])

        file_path = tempfile.mkstemp(prefix='file_')[1]

        df.to_csv(file_path)

        self.file_controller.save_file('tasks', file_path, 'tasks')

        # --- use TS predictor ---

        predictor = {
            'name': 'task_model',
            'predict': 'a',
            'problem_definition': {
                'timeseries_settings': {
                    'is_timeseries': True,
                    'window': 2,
                    'order_by': 't',
                    'group_by': 'g',
                    'horizon': 3
                }
            },
            'dtypes': {
                'a': dtype.integer,
                't': dtype.float,
                'g': dtype.categorical,
            },
            'predicted_value': ''
        }
        self.set_predictor(predictor)

        # set predictor output
        predict_result = [
            # window
            {'a': 2, 't': np.float64(2022.), 'g': 'x', '__mindsdb_row_id': 2},
            {'a': 3, 't': np.float64(2023.), 'g': 'x', '__mindsdb_row_id': 3},
            # horizon
            {'a': 1, 't': np.float64(2024.), 'g': 'x', '__mindsdb_row_id': None},
            {'a': 1, 't': np.float64(2024.), 'g': 'x', '__mindsdb_row_id': None},
            {'a': 1, 't': np.float64(2025.), 'g': 'x', '__mindsdb_row_id': None},
        ]
        self.mock_predict.side_effect = lambda *a, **b: predict_result

        # > latest ______________________
        ret = self.command_executor.execute_command(parse_sql(f'''
                select p.* from files.tasks t
                join mindsdb.task_model p
                where t.t > latest
            ''', dialect='mindsdb'))
        assert ret.error_code is None

        ret_df = self.ret_to_df(ret)
        assert ret_df.shape[0] == 3
        assert ret_df.t.min() == 2024.


    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_drop_database(self, mock_handler):
        self.set_handler(mock_handler, name='pg', tables={})

        # remove existing
        ret = self.command_executor.execute_command(parse_sql(f'''
                drop database pg
               ''', dialect='mindsdb'))
        assert ret.error_code is None

        # try one more time
        from mindsdb.api.mysql.mysql_proxy.utilities import SqlApiException
        try:
            self.command_executor.execute_command(parse_sql(f'''
                    drop database pg
                   ''', dialect='mindsdb'))
        except SqlApiException as e:
            assert 'not exists' in str(e)
        else:
            raise Exception('SqlApiException expected')

        # try files
        try:
            self.command_executor.execute_command(parse_sql(f'''
                    drop database files
                   ''', dialect='mindsdb'))
        except Exception as e:
            assert 'is system database' in str(e)
        else:
            raise Exception('SqlApiException expected')


class TestCompexQueries(BaseExecutorTestMockModel):
    df = pd.DataFrame([
        {'a': 1, 'b': 'aaa', 'c': dt.datetime(2020, 1, 1)},
        {'a': 2, 'b': 'bbb', 'c': dt.datetime(2020, 1, 2)},
        {'a': 1, 'b': 'ccc', 'c': dt.datetime(2020, 1, 3)},
    ])

    task_predictor = {
        'name': 'task_model',
        'predict': 'p',
        'dtypes': {
            'p': dtype.float,
            'a': dtype.integer,
            'b': dtype.categorical,
            'c': dtype.datetime
        },
        'predicted_value': 'ccc'
    }

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_union(self, mock_handler):

        self.set_handler(mock_handler, name='pg', tables={'tasks': self.df})

        # --- use predictor ---
        self.set_predictor(self.task_predictor)
        sql = '''
             SELECT a as a1, b as target
              FROM pg.tasks
           UNION {union}
             SELECT model.a as a2, model.p as target2
              FROM pg.tasks as t
             JOIN mindsdb.task_model as model
             WHERE t.a=1           
        '''
        # union all
        ret = self.command_executor.execute_command(
            parse_sql(sql.format(union='ALL'), dialect='mindsdb'))
        assert ret.error_code is None

        ret_df = self.ret_to_df(ret)
        assert list(ret_df.columns) == ['a1', 'target']
        assert ret_df.shape[0] == 3 + 2

        # union
        ret = self.command_executor.execute_command(
            parse_sql(sql.format(union=''), dialect='mindsdb'))
        assert ret.error_code is None

        ret_df = self.ret_to_df(ret)
        assert list(ret_df.columns) == ['a1', 'target']
        assert ret_df.shape[0] == 3

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_update_from_select(self, mock_handler):
        self.set_handler(mock_handler, name='pg', tables={'tasks': self.df})

        # --- use predictor ---
        self.set_predictor(self.task_predictor)
        sql = '''
            update 
                pg.table2                   
            set
                a1 = df.a,
                c1 = df.c
            from                            
                (
                    SELECT model.a as a, model.b as b, model.p as c
                      FROM pg.tasks as t
                     JOIN mindsdb.task_model as model
                     WHERE t.a=1 
                )
                as df
            where  
                table2.a1 = df.a 
                and table2.b1 = df.b     
        '''

        ret = self.command_executor.execute_command(
            parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # 1 select and 2 updates
        assert mock_handler().query.call_count == 3

        # second is update
        assert mock_handler().query.call_args_list[1][0][0].to_string() == "update table2 set a1=1, c1='ccc' where (a1 = 1) AND (b1 = 'ccc')"

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_create_table(self, mock_handler):
        self.set_handler(mock_handler, name='pg', tables={'tasks': self.df})

        self.set_predictor(self.task_predictor)
        sql = '''
              create table pg.table1                          
              (
                      SELECT model.a as a, model.b as b, model.p as c
                        FROM pg.tasks as t
                       JOIN mindsdb.task_model as model
                       WHERE t.a=1 
             )
          '''

        ret = self.command_executor.execute_command(
            parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        calls = mock_handler().query.call_args_list

        render = SqlalchemyRender('postgres')

        def to_str(query):
            s = render.get_string(query)
            s = s.strip().replace('\n', ' ').replace('\t', '').replace('  ', ' ')
            return s

        # select for predictor
        assert to_str(calls[0][0][0]) == 'SELECT * FROM tasks AS t WHERE t.a = 1'

        # create table
        assert to_str(calls[1][0][0]) == 'CREATE TABLE table1 ( a INTEGER, b TEXT, c TEXT )'

        # load table
        assert to_str(calls[2][0][0]) == "INSERT INTO table1 (a, b, c) VALUES (1, 'aaa', 'ccc'), (1, 'ccc', 'ccc')"

        assert len(calls) == 3

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_create_insert(self, mock_handler):
        self.set_handler(mock_handler, name='pg', tables={'tasks': self.df})

        self.set_predictor(self.task_predictor)
        sql = '''
               insert into pg.table1                          
               (
                       SELECT model.a as a, model.b as b, model.p as c
                         FROM pg.tasks as t
                        JOIN mindsdb.task_model as model
                        WHERE t.a=1 
              )
           '''

        ret = self.command_executor.execute_command(
            parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        calls = mock_handler().query.call_args_list

        render = SqlalchemyRender('postgres')

        def to_str(query):
            s = render.get_string(query)
            s = s.strip().replace('\n', ' ').replace('\t', '').replace('  ', ' ')
            return s

        # select for predictor
        assert to_str(calls[0][0][0]) == 'SELECT * FROM tasks AS t WHERE t.a = 1'

        # load table
        assert to_str(calls[1][0][0]) == "INSERT INTO table1 (a, b, c) VALUES (1, 'aaa', 'ccc'), (1, 'ccc', 'ccc')"

        assert len(calls) == 2

    # @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    # def test_union_type_mismatch(self, mock_handler):
    #     self.set_handler(mock_handler, name='pg', tables={'tasks': self.df})
    #
    #     sql = '''
    #          SELECT a, b  FROM pg.tasks
    #        UNION
    #          SELECT b, a  FROM pg.tasks
    #     '''
    #     from mindsdb.api.mysql.mysql_proxy.utilities import ErSqlWrongArguments
    #     with pytest.raises(ErSqlWrongArguments):
    #         self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))


class TestTableau(BaseExecutorTestMockModel):

    task_table = pd.DataFrame([
        {'a': 1, 'b': 'one'},
        {'a': 2, 'b': 'two'},
        {'a': 1, 'b': 'three'},
    ])

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_predictor_nested_select(self, mock_handler):

        self.set_handler(mock_handler, name='pg', tables={'tasks': self.task_table})

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

        self.set_handler(mock_handler, name='pg', tables={'tasks': self.task_table})

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


    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_predictor_tableau_header_alias(self, mock_handler):

        self.set_handler(mock_handler, name='pg', tables={'tasks': self.task_table})

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
              max(a1) AS a1,
              min(a2) AS a2
            FROM (
              SELECT source.a as a1, source.a as a2 
               FROM pg.tasks as source
               JOIN mindsdb.task_model as res
            ) t1
            HAVING (COUNT(1) > 0)
                ''', dialect='mindsdb'))

        # second column is having last value of 'b'
        # 3: count rows, 4: sum of 'a', 5 max of prediction
        assert ret.data[0] == [2, 1]

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_integration_subselect_no_alias(self, mock_handler):

        self.set_handler(mock_handler, name='pg', tables={'tasks': self.task_table})

        ret = self.command_executor.execute_command(parse_sql(f'''
           SELECT max(y2) FROM (          
              select a as y2  from pg.tasks
           ) 
        ''', dialect='mindsdb'))

        # second column is having last value of 'b'
        # 3: count rows, 4: sum of 'a', 5 max of prediction
        assert ret.data[0] == [2]


class TestWithNativeQuery(BaseExecutorTestMockModel):
    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_integration_native_query(self, mock_handler):

        data = [[3, 'y'], [1, 'y']]
        df = pd.DataFrame(data, columns=['a', 'b'])
        self.set_handler(mock_handler, name='pg', tables={'tasks': df})

        ret = self.command_executor.execute_command(parse_sql(
              'select max(a) from pg (select * from tasks) group by b',
            dialect='mindsdb'))

        # native query was called
        assert mock_handler().native_query.call_args[0][0] == 'select * from tasks'
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
        assert self.mock_learn.call_args[0][0].name.to_string() == 'task_model'
        # integration was called
        # TODO: integration is not called during learn process because learn function is mocked
        #   (data selected inside learn function)
        # assert mock_handler().native_query.call_args[0][0] == 'select * from tasks'

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
        assert mock_handler().native_query.call_args[0][0] == 'select * from tasks'

        # check predictor call

        # prediction was called
        assert self.mock_predict.call_args[0][0] == 'task_model'

        # input = one row whit a==2
        when_data = self.mock_predict.call_args[0][1]
        assert len(when_data) == 1
        assert when_data[0]['a'] == 2

        # check prediction
        assert ret.data[0][0] == predicted_value
        assert len(ret.data) == 1

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_use_ts_predictor_with_view(self, mock_handler):
        # set integration data

        df = pd.DataFrame([
            {'a': 1, 't': dt.datetime(2020, 1, 1), 'g': 'x'},
            {'a': 2, 't': dt.datetime(2020, 1, 2), 'g': 'x'},
            {'a': 3, 't': dt.datetime(2020, 1, 3), 'g': 'x'},
            {'a': 4, 't': dt.datetime(2020, 1, 1), 'g': 'y'},
            {'a': 5, 't': dt.datetime(2020, 1, 2), 'g': 'y'},
            {'a': 6, 't': dt.datetime(2020, 1, 3), 'g': 'y'},
            {'a': 7, 't': dt.datetime(2020, 1, 1), 'g': 'z'},
            {'a': 8, 't': dt.datetime(2020, 1, 2), 'g': 'z'},
            {'a': 9, 't': dt.datetime(2020, 1, 3), 'g': 'z'},
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
                't': dtype.date,
                'g': dtype.categorical,
            },
            'predicted_value': predicted_value
        }
        self.set_predictor(predictor)
        ret = self.command_executor.execute_command(parse_sql(f'''
           select task_model.*
           from views.{view_name}
           join mindsdb.task_model
           where {view_name}.t = latest
        ''', dialect='mindsdb'))
        assert ret.error_code is None

        # native query was called without filters
        assert mock_handler().native_query.call_args[0][0] == 'select * from tasks'

        # check predictor call
        # prediction was called
        assert self.mock_predict.call_args[0][0] == 'task_model'

        # input to predictor all 9 rows
        when_data = self.mock_predict.call_args[0][1]
        assert len(when_data) == 9

        # all group values in input
        group_values = {'x', 'y', 'z'}
        assert set(pd.DataFrame(when_data)['g'].unique()) == group_values

        # check prediction
        # output is has  g=='y' or None
        ret_df = self.ret_to_df(ret)
        # all group values in output
        assert set(ret_df['g'].unique()) == group_values

        # p is predicted value
        assert ret_df['p'][0] == predicted_value

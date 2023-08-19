from unittest.mock import patch
import datetime as dt
import tempfile
import pytest

import pandas as pd
import numpy as np

from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender

from mindsdb.api.mysql.mysql_proxy.utilities.sql import query_df
from mindsdb.api.mysql.mysql_proxy.utilities.lightwood_dtype import dtype

# How to run:
#  env PYTHONPATH=./ pytest tests/unit/test_executor.py

from .executor_test_base import BaseExecutorMockPredictor


class Test(BaseExecutorMockPredictor):
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

        ret = self.command_executor.execute_command(parse_sql('''
             select p, a from mindsdb.task_model where a = 2
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

        ret = self.command_executor.execute_command(parse_sql('''
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
            {'a': 11, 't': dt.datetime(2021, 1, 1), 'g': 'y'},
            {'a': 12, 't': dt.datetime(2021, 1, 2), 'g': 'y'},
            {'a': 33, 't': dt.datetime(2021, 1, 3), 'g': 'y'},
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
            {'a': 4, 't': dt.datetime(2020, 1, 4), 'g': 'x', '__mindsdb_row_id': None},
            {'a': 5, 't': dt.datetime(2020, 1, 5), 'g': 'x', '__mindsdb_row_id': None},
            {'a': 6, 't': dt.datetime(2020, 1, 6), 'g': 'x', '__mindsdb_row_id': None},

            # window
            {'a': 12, 't': dt.datetime(2021, 1, 2), 'g': 'y', '__mindsdb_row_id': 2},
            {'a': 13, 't': dt.datetime(2021, 1, 3), 'g': 'y', '__mindsdb_row_id': 3},
            # horizon
            {'a': 14, 't': dt.datetime(2021, 1, 4), 'g': 'y', '__mindsdb_row_id': None},
            {'a': 15, 't': dt.datetime(2021, 1, 5), 'g': 'y', '__mindsdb_row_id': None},
            {'a': 16, 't': dt.datetime(2021, 1, 6), 'g': 'y', '__mindsdb_row_id': None},
        ]
        predict_result = pd.DataFrame(predict_result)
        self.mock_predict.side_effect = lambda *a, **b: predict_result

        # = latest  ______________________
        ret = self.command_executor.execute_command(parse_sql('''
                select t.t as t0, p.* from pg.tasks t
                join mindsdb.task_model p
                where t.t = latest
            ''', dialect='mindsdb'))
        assert ret.error_code is None

        data = self.ret_to_df(ret).to_dict('records')
        # one key with max value of a
        assert len(data) == 2
        #  first row
        assert data[0]['a'] == 3
        assert data[0]['t'] == dt.datetime(2020, 1, 3)
        assert data[0]['g'] == 'x'

        # second
        assert data[1]['a'] == 13
        assert data[1]['t'] == dt.datetime(2021, 1, 3)
        assert data[1]['g'] == 'y'

        # > latest ______________________
        ret = self.command_executor.execute_command(parse_sql('''
                select t.t as t0, p.* from pg.tasks t
                join mindsdb.task_model p
                where t.t > latest
            ''', dialect='mindsdb'))
        assert ret.error_code is None

        ret_df = self.ret_to_df(ret)
        # 1st group
        ret_df1 = ret_df[ret_df['g'] == 'x']
        assert ret_df1.shape[0] == 3
        assert ret_df1.t.min() == dt.datetime(2020, 1, 4)
        # table shouldn't join
        assert ret_df1.t0.iloc[0] is None

        # 2nd group
        ret_df1 = ret_df[ret_df['g'] == 'y']
        assert ret_df1.shape[0] == 3
        assert ret_df1.t.min() == dt.datetime(2021, 1, 4)
        # table shouldn't join
        assert ret_df1.t0.iloc[0] is None

        # > date ______________________
        ret = self.command_executor.execute_command(parse_sql('''
                select t.t as t0, p.* from pg.tasks t
                join mindsdb.task_model p
                where t.t > '2020-01-02'
            ''', dialect='mindsdb'))
        assert ret.error_code is None

        ret_df = self.ret_to_df(ret)

        # 1st group
        ret_df1 = ret_df[ret_df['g'] == 'x']
        assert ret_df1.shape[0] == 4
        assert ret_df1.t.min() == dt.datetime(2020, 1, 3)

        # 2nd group
        ret_df1 = ret_df[ret_df['g'] == 'y']
        assert ret_df1.shape[0] == 5  # all records from predictor
        assert ret_df1.t.min() == dt.datetime(2021, 1, 2)

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
        predict_result = pd.DataFrame(predict_result)
        self.mock_predict.side_effect = lambda *a, **b: predict_result

        ret = self.command_executor.execute_command(parse_sql('''
                select p.* from pg.tasks t
                join mindsdb.task_model p
                where t.t between '2020-01-02' and '2020-01-03'
            ''', dialect='mindsdb'))
        assert ret.error_code is None

        ret_df = self.ret_to_df(ret)
        assert ret_df.shape[0] == 2
        assert ret_df.t.min() == dt.datetime(2020, 1, 2)
        assert ret_df.t.max() == dt.datetime(2020, 1, 3)

        # ------- limit -------
        ret = self.command_executor.execute_command(parse_sql('''
                select p.* from pg.tasks t
                join mindsdb.task_model p
                where t.t between '2020-01-02' and '2020-01-03'
                limit 1
            ''', dialect='mindsdb'))
        assert ret.error_code is None

        ret_df = self.ret_to_df(ret)
        assert ret_df.shape[0] == 1
        assert ret_df.t.min() == dt.datetime(2020, 1, 2)

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_ts_predictor_no_group(self, mock_handler):
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
        predict_result = pd.DataFrame(predict_result)
        self.mock_predict.side_effect = lambda *a, **b: predict_result

        # = latest  ______________________
        ret = self.command_executor.execute_command(parse_sql('''
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
        ret = self.command_executor.execute_command(parse_sql('''
                select t.t as t0, p.* from pg.tasks t
                join mindsdb.task_model p
                where t.t > latest
            ''', dialect='mindsdb'))
        assert ret.error_code is None

        ret_df = self.ret_to_df(ret)
        assert ret_df.shape[0] == 3
        assert ret_df.t.min() == dt.datetime(2020, 1, 4)
        # table shouldn't join
        assert ret_df.t0[0] is None

        # > date ______________________
        ret = self.command_executor.execute_command(parse_sql('''
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
        predict_result = pd.DataFrame(predict_result)
        self.mock_predict.side_effect = lambda *a, **b: predict_result

        ret = self.command_executor.execute_command(parse_sql('''
                select p.* from pg.tasks t
                join mindsdb.task_model p
                where t.t between '2020-01-02' and '2020-01-03'
            ''', dialect='mindsdb'))
        assert ret.error_code is None

        ret_df = self.ret_to_df(ret)
        assert ret_df.shape[0] == 2
        assert ret_df.t.min() == dt.datetime(2020, 1, 2)
        assert ret_df.t.max() == dt.datetime(2020, 1, 3)

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_ts_predictor_fix_order_by_modification(self, mock_handler):
        # set integration data

        df = pd.DataFrame([
            {'a': 1, 't': dt.datetime(2020, 1, 1, 10, 0, 0), 'g': 'x'},
            {'a': 2, 't': dt.datetime(2020, 1, 1, 10, 1, 0), 'g': 'x'},
            {'a': 3, 't': dt.datetime(2020, 1, 1, 10, 2, 0), 'g': 'x'},
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
            {'a': 2, 't': dt.datetime(2020, 1, 1, 10, 1, 0), 'g': 'x', '__mindsdb_row_id': 2},
            {'a': 3, 't': dt.datetime(2020, 1, 1, 10, 2, 0), 'g': 'x', '__mindsdb_row_id': 3},
            # horizon
            {'a': 1, 't': dt.datetime(2020, 1, 1, 10, 3, 0), 'g': 'x', '__mindsdb_row_id': None},
            {'a': 1, 't': dt.datetime(2020, 1, 1, 10, 4, 0), 'g': 'x', '__mindsdb_row_id': None},
            {'a': 1, 't': dt.datetime(2020, 1, 1, 10, 5, 0), 'g': 'x', '__mindsdb_row_id': None},
        ]
        predict_result = pd.DataFrame(predict_result)
        self.mock_predict.side_effect = lambda *a, **b: predict_result

        # > latest ______________________
        ret = self.command_executor.execute_command(parse_sql('''
                 select t.t as t0, p.* from pg.tasks t
                 join mindsdb.task_model p
                 where t.t > latest
             ''', dialect='mindsdb'))
        assert ret.error_code is None

        ret_df = self.ret_to_df(ret)
        assert ret_df.shape[0] == 3
        assert ret_df.t.min() == dt.datetime(2020, 1, 1, 10, 3, 0)
        # table shouldn't join
        assert ret_df.t0[0] is None

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
        predict_result = pd.DataFrame(predict_result)
        self.mock_predict.side_effect = lambda *a, **b: predict_result

        # > latest ______________________
        ret = self.command_executor.execute_command(parse_sql('''
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
        ret = self.command_executor.execute_command(parse_sql('''
                drop database pg
               ''', dialect='mindsdb'))
        assert ret.error_code is None

        # try one more time
        try:
            ret = self.command_executor.execute_command(parse_sql('''
                    drop database pg
                   ''', dialect='mindsdb'))
        except Exception as e:
            assert 'not exists' in str(e)
        else:
            raise Exception('SqlApiException expected')

        # try if exists
        ret = self.command_executor.execute_command(parse_sql('''
                drop database if exists pg
                ''', dialect='mindsdb'))
        assert ret.error_code is None

        # try files
        try:
            self.command_executor.execute_command(parse_sql('''
                    drop database files
                   ''', dialect='mindsdb'))
        except Exception as e:
            assert 'is system database' in str(e)
        else:
            raise Exception('SqlApiException expected')

    def test_wrong_using(self):
        with pytest.raises(Exception) as exc_info:
            self.command_executor.execute_command(parse_sql(
                '''
                    CREATE PREDICTOR task_model
                    FROM mindsdb
                    (select * from vtasks)
                    PREDICT a
                    using a=1 b=2  -- no ',' here
                ''',
                dialect='mindsdb')
            )

        assert 'Syntax error' in str(exc_info.value)


class TestComplexQueries(BaseExecutorMockPredictor):
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
    def test_update_in_integration(self, mock_handler):
        self.set_handler(mock_handler, name='pg', tables={})

        sql = '''
            update
                pg.table2
            set
                a1 = 1,
                c1 = 'ccc'
            where
                b1 = 'b'
        '''

        ret = self.command_executor.execute_command(
            parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # 1 select and 2 updates
        assert mock_handler().query.call_count == 1

        # second is update
        assert mock_handler().query.call_args_list[0][0][0].to_string() == "update table2 set a1=1, c1='ccc' where b1 = 'b'"

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_delete_in_integration(self, mock_handler):
        self.set_handler(mock_handler, name='pg', tables={})

        sql = '''
             delete from
                 pg.table2
             where
                 b1 = 'b'
         '''

        ret = self.command_executor.execute_command(
            parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # 1 select and 2 updates
        assert mock_handler().query.call_count == 1

        # second is update
        assert mock_handler().query.call_args_list[0][0][0].to_string() == "DELETE FROM table2 WHERE b1 = 'b'"

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


class TestTableau(BaseExecutorMockPredictor):
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
        ret = self.command_executor.execute_command(parse_sql('''
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
        ret = self.command_executor.execute_command(
            parse_sql(
                '''
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
                ''',
                dialect='mindsdb'
            )
        )

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
        ret = self.command_executor.execute_command(
            parse_sql(
                '''
                    SELECT
                    max(a1) AS a1,
                    min(a2) AS a2
                    FROM (
                    SELECT source.a as a1, source.a as a2
                    FROM pg.tasks as source
                    JOIN mindsdb.task_model as res
                    ) t1
                    HAVING (COUNT(1) > 0)
                ''',
                dialect='mindsdb'
            )
        )

        # second column is having last value of 'b'
        # 3: count rows, 4: sum of 'a', 5 max of prediction
        assert ret.data[0] == [2, 1]

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_integration_subselect_no_alias(self, mock_handler):

        self.set_handler(mock_handler, name='pg', tables={'tasks': self.task_table})

        ret = self.command_executor.execute_command(parse_sql('''
           SELECT max(y2) FROM (
              select a as y2  from pg.tasks
           )
        ''', dialect='mindsdb'))

        # second column is having last value of 'b'
        # 3: count rows, 4: sum of 'a', 5 max of prediction
        assert ret.data[0] == [2]


class TestWithNativeQuery(BaseExecutorMockPredictor):
    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_integration_native_query(self, mock_handler):

        data = [[3, 'y'], [1, 'y']]
        df = pd.DataFrame(data, columns=['a', 'b'])
        self.set_handler(mock_handler, name='pg', tables={'tasks': df})

        ret = self.command_executor.execute_command(
            parse_sql(
                'select max(a) from pg (select * from tasks) group by b',
                dialect='mindsdb'
            )
        )

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
            'create view mindsdb.vtasks (select * from pg (select * from tasks))',
            dialect='mindsdb')
        )
        # no error
        assert ret.error_code is None

        # test create if not exists
        ret = self.command_executor.execute_command(parse_sql(
            'create view if not exists mindsdb.vtasks (select * from pg (select * from tasks))',
            dialect='mindsdb')
        )
        assert ret.error_code is None

        # --- select from view ---
        ret = self.command_executor.execute_command(parse_sql(
            'select * from mindsdb.vtasks',
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
                FROM mindsdb
                (select * from vtasks)
                PREDICT a
                using
                join_learn_process=true
            ''',
            dialect='mindsdb'))
        assert ret.error_code is None

        # test creating with if not exists
        ret = self.command_executor.execute_command(parse_sql(
            '''
                CREATE PREDICTOR IF NOT EXISTS task_model
                FROM mindsdb
                (select * from vtasks)
                PREDICT a
                using
                join_learn_process=true
            ''',
            dialect='mindsdb'))
        assert ret.error_code is None

        # learn was called.
        # TODO check input to ML handler
        # assert self.mock_create.call_args[0][0].name.to_string() == 'task_model'  # it exec in separate process
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
            f'create view mindsdb.{view_name} (select * from pg (select * from tasks))',
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
           select m.p, v.a
           from mindsdb.{view_name} v
           join mindsdb.task_model m
           where v.a = 2
        ''', dialect='mindsdb'))
        assert ret.error_code is None

        # native query was called
        assert mock_handler().native_query.call_args[0][0] == 'select * from tasks'

        # check predictor call
        # input = one row whit a==2
        data_in = self.mock_predict.call_args[0][1]
        assert len(data_in) == 1
        assert data_in[0]['a'] == 2

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
           from mindsdb.{view_name}
           join mindsdb.task_model
           where {view_name}.t = latest
        ''', dialect='mindsdb'))
        assert ret.error_code is None

        # native query was called without filters
        assert mock_handler().native_query.call_args[0][0] == 'select * from tasks'

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


class TestSteps(BaseExecutorMockPredictor):

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def disabled_test_join_2_tables(self, mock_handler):
        # tests for FilterStep and limitoffsetStep
        # disabled: current JoinStep is not supporting join with condition

        df = pd.DataFrame([
            {'a': 1, 't': dt.datetime(2020, 1, 1), 'g': 'x'},
            {'a': 2, 't': dt.datetime(2020, 1, 2), 'g': 'x'},
            {'a': 3, 't': dt.datetime(2020, 1, 3), 'g': 'x'},
        ])

        self.set_handler(mock_handler, name='pg', tables={'tasks': df, 'task2': df})
        ret = self.command_executor.execute_command(parse_sql('''
            select t.* from pg.tasks
            join pg.tasks2 on tasks.a=tasks2.a
            where t.a > 1
            limit 1
        ''', dialect='mindsdb'))
        assert ret.error_code is None


class TestExecutionTools:

    def test_query_df(self):
        d = [
            {'TRAINING_OPTIONS':
                {'b': {
                    'x': 'A',
                    'y': 'B'}}},
            {'TRAINING_OPTIONS':
                {'b': {
                    'x': 'A'}}}
        ]
        df = pd.DataFrame(d)
        query_df(df, 'select * from models')


class TestIfExistsIfNotExists(BaseExecutorMockPredictor):

    def setup_method(self):
        super().setup_method()
        self.set_executor(mock_lightwood=True, mock_model_controller=True, import_dummy_ml=True)

    def test_ml_engine(self):
        # create an ml engine
        sql = """
            CREATE ML_ENGINE test_engine
            FROM lightwood
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # create the same ml engine without if not exists throws an error
        with pytest.raises(Exception) as exc_info:
            self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert 'already exists' in str(exc_info.value)

        # create the same ml engine with if not exists doesn't throw an error
        sql = """
            CREATE ML_ENGINE IF NOT EXISTS test_engine
            FROM lightwood
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # create an engine with if not exists should indeed create a new engine
        sql = """
            CREATE ML_ENGINE IF NOT EXISTS test_engine2
            FROM lightwood
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # check that the engine was indeed created
        sql = """
            SHOW ML_ENGINES
            WHERE name = 'test_engine2'
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None
        assert len(ret.data) == 1

        # drop the engine
        sql = """
            DROP ML_ENGINE test_engine2
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # check that the engine was indeed dropped
        sql = """
            SHOW ML_ENGINES
            WHERE name = 'test_engine2'
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None
        assert len(ret.data) == 0

        # drop again without if exists should throw an error
        sql = """
            DROP ML_ENGINE test_engine2
        """
        with pytest.raises(Exception) as exc_info:
            self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert 'does not exist' in str(exc_info.value)

        # drop again with if exists should not throw an error
        sql = """
            DROP ML_ENGINE IF EXISTS test_engine2
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

    def test_predictor(self):
        # create a predictor from dummy ml handler
        sql = """
            CREATE MODEL test_predictor
            PREDICT target
            USING
                engine = 'dummy_ml'
            """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # create the same predictor without if not exists throws an error
        with pytest.raises(Exception) as exc_info:
            self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert 'already exists' in str(exc_info.value)

        # create the same predictor with if not exists doesn't throw an error
        sql = """
            CREATE MODEL IF NOT EXISTS test_predictor
            PREDICT target
            USING
                engine = 'dummy_ml'
            """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # create a predictor with if not exists should indeed create a new predictor
        sql = """
            CREATE MODEL IF NOT EXISTS test_predictor2
            PREDICT target
            USING
                engine = 'dummy_ml'
            """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # check that the predictor was indeed created
        sql = """
            SHOW MODELS
            WHERE name = 'test_predictor2'
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None
        assert len(ret.data) == 1

        # drop the predictor
        sql = """
            DROP MODEL test_predictor2
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # check that the predictor was indeed dropped
        sql = """
            SHOW MODELS
            WHERE name = 'test_predictor2'
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # drop again without if exists should throw an error
        sql = """
            DROP MODEL test_predictor2
        """
        with pytest.raises(Exception) as exc_info:
            self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert 'does not exist' in str(exc_info.value)

        # drop again with if exists should not throw an error
        sql = """
            DROP MODEL IF EXISTS test_predictor2
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

    def test_project(self):
        sql = """
            CREATE PROJECT another_test_project
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # create the same project without if not exists throws an error
        with pytest.raises(Exception) as exc_info:
            self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert 'already exists' in str(exc_info.value)

        # create the same project with if not exists doesn't throw an error
        sql = """
            CREATE PROJECT IF NOT EXISTS another_test_project
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

    def test_database_integration(self):
        sql = """
            CREATE DATABASE test_database
            WITH engine = 'mindsdb'
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # create the same database without if not exists throws an error
        with pytest.raises(Exception) as exc_info:
            self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert 'already exists' in str(exc_info.value)

        # create the same database with if not exists doesn't throw an error
        sql = """
            CREATE DATABASE IF NOT EXISTS test_database
            WITH engine = 'mindsdb'
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # create a database with if not exists should indeed create a new database
        sql = """
            CREATE DATABASE IF NOT EXISTS test_database2
            WITH engine = 'mindsdb'
        """

        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # check that the database was indeed created
        sql = """
            SHOW DATABASES
            WHERE name = 'test_database2'
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None
        assert len(ret.data) == 1

        # drop the database
        sql = """
            DROP DATABASE test_database2
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # check that the database was indeed dropped
        sql = """
            SHOW DATABASES
            WHERE name = 'test_database2'
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None
        assert len(ret.data) == 0

        # drop again without if exists should throw an error
        sql = """
            DROP DATABASE test_database2
        """
        with pytest.raises(Exception) as exc_info:
            self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert 'does not exist' in str(exc_info.value)

        # drop again with if exists should not throw an error
        sql = """
            DROP DATABASE IF EXISTS test_database2
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

    def test_job(self):
        # create a simple job
        sql = """
            CREATE JOB test_job (
                SELECT 1
            )
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # create the same job without if not exists throws an error
        with pytest.raises(Exception) as exc_info:
            self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert 'already exists' in str(exc_info.value)

        # create the same job with if not exists doesn't throw an error
        sql = """
            CREATE JOB IF NOT EXISTS test_job (
                SELECT 1
            )
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # create a job with if not exists should indeed create a new job
        sql = """
            CREATE JOB IF NOT EXISTS test_job2 (
                SELECT 1
            )
        """

        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # drop the job
        sql = """
            DROP JOB test_job2
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # drop again without if exists should throw an error
        sql = """
            DROP JOB test_job2
        """
        with pytest.raises(Exception) as exc_info:
            self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert 'does not exist' in str(exc_info.value)

        # drop again with if exists should not throw an error
        sql = """
            DROP JOB IF EXISTS test_job2
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_view(self, mock_handler):
        df = pd.DataFrame([
            {'a': 1, 'b': 'one'},
            {'a': 2, 'b': 'two'},
            {'a': 1, 'b': 'three'},
        ])
        self.set_handler(mock_handler, name='pg', tables={'tasks': df})
        sql = """
            CREATE VIEW test_view AS (
            SELECT * FROM pg.tasks
            )
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # create the same view without if not exists throws an error
        with pytest.raises(Exception) as exc_info:
            self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert 'already exists' in str(exc_info.value)

        # create the same view with if not exists doesn't throw an error
        sql = """
            CREATE VIEW IF NOT EXISTS test_view AS (
            SELECT * FROM pg.tasks
            )
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # create a view with if not exists should indeed create a new view
        sql = """
            CREATE VIEW IF NOT EXISTS test_view2 AS (
            SELECT * FROM pg.tasks
            )
        """

        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # check that the view was indeed created
        sql = """
            SHOW FULL TABLES
            WHERE tables_in_mindsdb = 'test_view2'
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None
        assert len(ret.data) == 1

        # drop the view
        sql = """
            DROP VIEW test_view2
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

        # check that the view was indeed dropped
        sql = """
            SHOW FULL TABLES
            WHERE tables_in_mindsdb = 'test_view2'
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None
        assert len(ret.data) == 0

        # drop again without if exists should throw an error
        sql = """
            DROP VIEW test_view2
        """
        with pytest.raises(Exception) as exc_info:
            self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert 'does not exist' in str(exc_info.value)

        # drop again with if exists should not throw an error
        sql = """
            DROP VIEW IF EXISTS test_view2
        """
        ret = self.command_executor.execute_command(parse_sql(sql, dialect='mindsdb'))
        assert ret.error_code is None

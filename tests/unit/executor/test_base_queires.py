from unittest.mock import patch
import datetime as dt
import pytest

import pandas as pd

from tests.unit.executor_test_base import BaseExecutorDummyML


class TestSelect(BaseExecutorDummyML):

    def test_view(self):
        df = pd.DataFrame([
            {'a': 1, 'b': dt.datetime(2020, 1, 1)},
            {'a': 2, 'b': dt.datetime(2020, 1, 2)},
            {'a': 1, 'b': dt.datetime(2020, 1, 3)},
        ])
        self.save_file('tasks', df)

        self.run_sql('''
            create view mindsdb.vTasks (
                select * from files.tasks where a=1
            )
        ''')

        # -- create model --
        self.run_sql(
            '''
                CREATE model mindsdb.task_model
                from mindsdb (select * from Vtasks)
                PREDICT a
                using engine='dummy_ml'
            '''
        )
        self.wait_predictor('mindsdb', 'task_model')

        # use model
        ret = self.run_sql('''
             SELECT m.*
               FROM mindsdb.vtasks as t
               JOIN mindsdb.task_model as m
        ''')

        assert len(ret) == 2
        assert ret.predicted[0] == 42

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_complex_joins(self, data_handler):
        df1 = pd.DataFrame([
            {'a': 1, 'c': 1, 'b': dt.datetime(2020, 1, 1)},
            {'a': 2, 'c': 1, 'b': dt.datetime(2020, 1, 2)},
            {'a': 1, 'c': 3, 'b': dt.datetime(2020, 1, 3)},
            {'a': 3, 'c': 2, 'b': dt.datetime(2020, 1, 2)},
        ])
        df2 = pd.DataFrame([
            {'a': 6, 'c': 1},
            {'a': 4, 'c': 2},
            {'a': 2, 'c': 3},
        ])
        self.set_data('tbl1', df1)
        self.set_data('tbl2', df2)

        self.run_sql(
            '''
                CREATE model mindsdb.pred
                PREDICT p
                using engine='dummy_ml',
                join_learn_process=true
            '''
        )

        self.run_sql('''
            create view mindsdb.view2 (
                select * from dummy_data.tbl2 where a!=4
            )
        ''')

        # --- test join table-table-table ---
        ret = self.run_sql('''
            SELECT t1.a as t1a,  t3.a t3a
              FROM dummy_data.tbl1 as t1
              JOIN dummy_data.tbl2 as t2 on t1.c=t2.c
              LEFT JOIN dummy_data.tbl1 as t3 on t2.a=t3.a
              where t1.a=1
        ''')

        # must be 2 rows
        assert len(ret) == 2

        # all t1.a values are 1
        assert list(ret.t1a) == [1, 1]

        # t3.a has 2 and None
        assert len(ret[ret.t3a == 2]) == 1
        assert len(ret[ret.t3a.isna()]) == 1

        # --- test join table-predictor-view ---
        ret = self.run_sql('''
            SELECT t1.a t1a, t3.a t3a, m.*
              FROM dummy_data.tbl1 as t1
              JOIN mindsdb.pred m
              LEFT JOIN mindsdb.view2 as t3 on t1.c=t3.c
              where t1.a>1
        ''')

        # must be 2 rows
        assert len(ret) == 2

        # t1.a > 1
        assert ret[ret.t1a <= 1].empty

        # view: a!=4
        assert ret[ret.t3a == 4].empty

        # t3.a has 6 and None
        assert len(ret[ret.t3a == 6]) == 1
        assert len(ret[ret.t3a.isna()]) == 1

        # contents predicted values
        assert list(ret.predicted.unique()) == [42]

        # --- tests table-subselect-view ---

        ret = self.run_sql('''
            SELECT t1.a t1a,
                   t2.t1a t2t1a, t2.t3a t2t3a,
                   t3.c t3c, t3.a t3a
              FROM dummy_data.tbl1 as t1
              JOIN (
                  SELECT t1.a as t1a,  t3.a t3a
                  FROM dummy_data.tbl1 as t1
                  JOIN dummy_data.tbl2 as t2 on t1.c=t2.c
                  LEFT JOIN dummy_data.tbl1 as t3 on t2.a=t3.a
                  where t1.a=1
              ) t2 on t2.t3a = t1.a
              LEFT JOIN mindsdb.view2 as t3 on t1.c=t3.c
              where t1.a>1
        ''')

        # 1 row
        assert len(ret) == 1

        # check row values
        row = ret.iloc[0].to_dict()
        assert row['t1a'] == 2
        assert row['t2t3a'] == 2

        assert row['t2t1a'] == 1
        assert row['t3c'] == 1

        assert row['t3a'] == 6

    def test_complex_queries(self):

        # -- set up data --

        stores = pd.DataFrame(
            columns=['id', 'region_id', 'format'],
            data=[
                [1, 1, 'c'],
                [2, 2, 'a'],
                [3, 2, 'a'],
                [4, 2, 'b'],
                [5, 1, 'b'],
                [6, 2, 'b'],
            ]
        )
        regions = pd.DataFrame(
            columns=['id', 'name'],
            data=[
                [1, 'asia'],
                [2, 'europe'],
            ]
        )
        self.save_file('stores', stores)
        self.save_file('regions', regions)

        # -- create view --
        self.run_sql('''
            create view mindsdb.stores_view (
                select * from files.stores
            )
        ''')

        # -- create model --
        self.run_sql(
            '''
                CREATE model model1
                from files (select * from stores)
                PREDICT format
                using engine='dummy_ml'
            '''
        )
        self.wait_predictor('mindsdb', 'model1')

        self.run_sql(
            '''
                CREATE model model2
                from files (select * from stores)
                PREDICT format
                using engine='dummy_ml'
            '''
        )
        self.wait_predictor('mindsdb', 'model2')

        # -- joins / conditions / unions --

        sql = '''
            select
               m1.predicted / 2 a,  -- 42/2=21
               s.id + (select id from files.regions where id=1) b -- =3
             from files.stores s
             join files.regions r on r.id = s.region_id
             join model1 m1
             join model2 m2
               where
                   m1.model_param = (select 100 + id from files.stores where id=1)
                   and s.region_id=(select id from files.regions where id=2) -- only region_id=2
                   and s.format='a'
                   and s.id = r.id -- cross table condition
            union
              select id, id from files.regions where id = 1  -- 2nd row with [1,1]
            union
              select id, id from files.stores where id = 2   -- 2nd row with [2,2]
        '''

        ret = self.run_sql(sql)
        assert len(ret) == 3

        assert list(ret.iloc[0]) == [21, 3]
        assert list(ret.iloc[1]) == [1, 1]
        assert list(ret.iloc[2]) == [2, 2]

        # -- aggregating / grouping / cases --
        case = '''
            case when s.id=1 then 10
                 when s.id=2 then 20
                 when s.id=3 then 30
                 else 100
            end
        '''

        sql = f'''
             SELECT
               -- values for region_id=2: [20, 30, 100, 100]
               MAX({case}) c_max,   -- =100
               MIN({case}) c_min,   -- =20
               SUM({case}) c_sum,   -- =250
               COUNT({case}) c_count, -- =4
               AVG({case}) c_avg   -- 250/4=62.5
            from stores_view s  -- view is used
             join files.regions r on r.id = s.region_id
             join model1 m1
            group by r.id -- 2 records
            having max(r.id) = 2 -- 1 record
        '''

        ret = self.run_sql(sql)

        assert len(ret) == 1

        assert ret.c_max[0] == 100
        assert ret.c_min[0] == 20
        assert ret.c_sum[0] == 250
        assert ret.c_count[0] == 4
        assert ret.c_avg[0] == 62.5

        sql = '''
           SELECT
             s.*,
            ROW_NUMBER() OVER(PARTITION BY r.id ORDER BY s.id) ROW_NUMBER,
            RANK() OVER(PARTITION BY r.id ORDER BY s.format) RANK,
            DENSE_RANK() OVER(PARTITION BY r.id ORDER BY s.format) DENSE_RANK,
            PERCENT_RANK() OVER(PARTITION BY r.id ORDER BY s.id) PERCENT_RANK,
            CUME_DIST() OVER(PARTITION BY r.id ORDER BY s.id) CUME_DIST,
            NTILE(2) OVER(PARTITION BY r.id ORDER BY s.id) NTILE,
            LAG(s.id, 1) OVER(PARTITION BY r.id ORDER BY s.id) LAG,
            LEAD(s.id, 1) OVER(PARTITION BY r.id ORDER BY s.id) LEAD,
            FIRST_VALUE(s.format) OVER(PARTITION BY r.id ORDER BY s.id) FIRST_VALUE,
            LAST_VALUE(s.format) OVER(PARTITION BY r.id ORDER BY s.id) LAST_VALUE,
            NTH_VALUE(s.id, 1) OVER(PARTITION BY r.id ORDER BY s.id) NTH_VALUE
           from files.stores s
             join files.regions r on r.id = s.region_id
             join model1 m1
            order by r.id, s.id
        '''
        ret = self.run_sql(sql)

        assert list(ret.ROW_NUMBER) == [1, 2, 1, 2, 3, 4]
        assert list(ret.RANK) == [2, 1, 1, 1, 3, 3]
        assert list(ret.DENSE_RANK) == [2, 1, 1, 1, 2, 2]

        assert list(ret.FIRST_VALUE) == ['c', 'c', 'a', 'a', 'a', 'a']
        assert list(ret.LAST_VALUE) == ['c', 'b', 'a', 'a', 'b', 'b']

        # -- unions functions --

        # TODO Correlated subqueries (not implemented)

    def test_last(self):
        df = pd.DataFrame([
            {'a': 1, 'b': 'a'},
            {'a': 2, 'b': 'b'},
            {'a': 3, 'b': 'c'},
        ])
        self.set_data('tasks', df)

        # -- create model --
        self.run_sql(
            '''
                CREATE model task_model
                from dummy_data (select * from tasks)
                PREDICT a
                using engine='dummy_ml',
                join_learn_process=true
            '''
        )

        # --- check web editor  ---
        ret = self.run_sql('''
            select * from dummy_data.tasks where a>last
         ''')
        # first call is empty
        assert len(ret) == 0

        # add rows to dataframe
        df.loc[len(df.index)] = [4, 'd']  # should be tracked
        df.loc[len(df.index)] = [0, 'z']  # not tracked
        self.set_data('tasks', df)

        ret = self.run_sql('''
            select * from dummy_data.tasks where a>last
        ''')

        # second call content one new line
        assert len(ret) == 1
        assert ret.a[0] == 4

        # --- TEST view ---

        # view without target
        with pytest.raises(Exception) as exc_info:
            self.run_sql('''
                create view v1 (
                    select b from dummy_data.tasks where a>last
                )
            ''')
        assert 'should be in query target' in str(exc_info.value)

        # view with target
        self.run_sql('''
            create view v1 (
                select * from dummy_data.tasks where a>last
            )
        ''')

        ret = self.run_sql('''
          select * from v1
        ''')
        # first call is empty
        assert len(ret) == 0

        # add row to dataframe
        df.loc[len(df.index)] = [5, 'a']
        self.set_data('tasks', df)

        ret = self.run_sql('''
            select * from v1
        ''')

        # second call content one new line
        assert len(ret) == 1
        assert ret.a[0] == 5

        # add row to dataframe
        df.loc[len(df.index)] = [6, 'a']
        self.set_data('tasks', df)

        # use model
        ret = self.run_sql('''
             SELECT m.*
               FROM v1 as t
               JOIN task_model as m
        ''')

        # second call content one new line
        assert len(ret) == 1

        # -- view with model

        self.run_sql('''
            create view v2 (
                select t.a+1 as a from dummy_data.tasks t
                JOIN task_model as m
                where t.a>last
            )
       ''')

        ret = self.run_sql('select * from v2')
        # first call is empty
        assert len(ret) == 0

        # add row to dataframe
        df.loc[len(df.index)] = [7, 'a']
        self.set_data('tasks', df)

        ret = self.run_sql('select * from v2')

        # second call content one new line
        assert len(ret) == 1
        assert ret.a[0] == 8

    def test_last_coalesce(self):
        df = pd.DataFrame([
            {'a': 1, 'b': 'a'},
            {'a': 2, 'b': 'b'},
            {'a': 3, 'b': 'c'},
        ])

        self.set_data('tasks', df)

        sql = '''
            select * from dummy_data.tasks
            where a > coalesce(last, 1)
        '''

        # first call two rows
        ret = self.run_sql(sql)
        assert len(ret) == 2

        # second call zero rows
        ret = self.run_sql(sql)
        assert len(ret) == 0

        # add rows to dataframe
        df.loc[len(df.index)] = [4, 'd']  # should be tracked
        df.loc[len(df.index)] = [0, 'z']  # not tracked
        self.set_data('tasks', df)

        ret = self.run_sql(sql)

        # have to be one new line
        assert len(ret) == 1
        assert ret.a[0] == 4

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_interval(self, data_handler):
        df = pd.DataFrame([
            {'last_date': dt.datetime(2020, 1, 2)},
        ])
        self.set_handler(data_handler, name='pg', tables={'branch': df})

        ret = self.run_sql("select (last_date + INTERVAL '2 days') d from pg.branch")

        assert ret.d[0] == dt.datetime(2020, 1, 4)

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_duplicated_cols(self, data_handler):
        df1 = pd.DataFrame([
            {'id': 1, 'a': 1},
            {'id': 2, 'a': 2},
            {'id': 3, 'a': 3},
        ])
        df2 = pd.DataFrame([
            {'id': 1, 'a': 10},
            {'id': 2, 'a': 20},
        ])
        self.set_handler(data_handler, name='pg', tables={'tbl1': df1, 'tbl2': df2})

        ret = self.run_sql('''
            select * from pg.tbl1 as a
            join pg.tbl2 as b on a.id=b.id
        ''')

        first_row = ret.to_dict('split')['data'][0]
        assert first_row == [1, 1, 1, 10]

    def test_system_vars(self):

        ret = self.run_sql('select @@session.auto_increment_increment, @@character_set_client')

        assert ret.iloc[0, 0] == 1
        assert ret.iloc[0, 1] == 'utf8'


class TestDML(BaseExecutorDummyML):

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_create_empty_table(self, data_handler):
        self.set_handler(data_handler, name='pg', tables={})

        self.run_sql('create table pg.table1 (a DATE, b INTEGER)')

        calls = data_handler().query.call_args_list
        sql = calls[0][0][0].to_string()
        assert sql.strip() == 'CREATE TABLE table1 (a DATE, b INTEGER)'

    def test_delete_from_table(self):
        df1 = pd.DataFrame([
            {'a': 1}
        ])
        self.set_data('tbl1', df1)

        self.run_sql('delete from tbl1 where a=1', database='dummy_data')

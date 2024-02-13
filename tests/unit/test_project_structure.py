from unittest.mock import patch
import datetime as dt
import time
import pytest

import pandas as pd

from mindsdb_sql import parse_sql

from .executor_test_base import BaseExecutorDummyML


@pytest.fixture(scope="class")
def scheduler():
    from mindsdb.interfaces.jobs.scheduler import Scheduler
    scheduler_ = Scheduler({})

    yield scheduler_

    scheduler_.stop_thread()


class TestProjectStructure(BaseExecutorDummyML):

    def wait_predictor(self, project, name, filter=None):
        # wait
        done = False
        for attempt in range(200):
            sql = f"select * from {project}.models_versions where name='{name}'"
            if filter is not None:
                for k, v in filter.items():
                    sql += f" and {k}='{v}'"
            ret = self.run_sql(sql)
            if not ret.empty:
                if ret['STATUS'][0] == 'complete':
                    done = True
                    break
                elif ret['STATUS'][0] == 'error':
                    break
            time.sleep(0.5)
        if not done:
            raise RuntimeError("predictor didn't created")

    def run_sql(self, sql, throw_error=True, database='mindsdb'):
        self.command_executor.session.database = database
        ret = self.command_executor.execute_command(
            parse_sql(sql, dialect='mindsdb')
        )
        if throw_error:
            assert ret.error_code is None
        if ret.data is not None:
            columns = [
                col.alias if col.alias is not None else col.name
                for col in ret.columns
            ]
            return pd.DataFrame(ret.data, columns=columns)

    def get_models(self):
        models = {}
        for p in self.db.Predictor.query.all():
            models[p.id] = p
        return models

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_version_managing(self, data_handler):
        from mindsdb.utilities.exception import EntityNotExistsError
        # set up

        df = pd.DataFrame([
            {'a': 1, 'b': dt.datetime(2020, 1, 1)},
            {'a': 2, 'b': dt.datetime(2020, 1, 2)},
            {'a': 1, 'b': dt.datetime(2020, 1, 3)},
        ])
        self.set_handler(data_handler, name='pg', tables={'tasks': df})

        # ================= retrain cycles =====================

        # create folder
        self.run_sql('create database proj')

        # -- create model --
        ret = self.run_sql(
            '''
                CREATE model proj.task_model
                from pg (select * from tasks)
                PREDICT a
                using engine='dummy_ml',
                tag = 'first',
                join_learn_process=true
            '''
        )
        assert ret['NAME'][0] == 'task_model'
        assert ret['ENGINE'][0] == 'dummy_ml'
        self.wait_predictor('proj', 'task_model')

        # tag works in create model
        ret = self.run_sql('select * from proj.models')
        assert ret['TAG'][0] == 'first'

        # use model
        ret = self.run_sql('''
             SELECT m.*
               FROM pg.tasks as t
               JOIN proj.task_model as m
        ''')

        assert len(ret) == 3
        assert ret.predicted[0] == 42

        # -- retrain predictor with tag --
        data_handler.reset_mock()
        ret = self.run_sql(
            '''
                retrain proj.task_model
                from pg (select * from tasks where a=2)
                PREDICT b
                using tag = 'second',
                join_learn_process=true
            '''
        )
        assert ret['NAME'][0] == 'task_model'
        assert ret['TAG'][0] == 'second'
        self.wait_predictor('proj', 'task_model', {'tag': 'second'})

        # get current model
        ret = self.run_sql('select * from proj.models')

        # check target
        assert ret['PREDICT'][0] == 'b'

        # check label
        assert ret['TAG'][0] == 'second'

        # use model
        ret = self.run_sql('''
             SELECT m.*
               FROM pg.tasks as t
               JOIN proj.task_model as m
        ''')
        assert ret.predicted[0] == 42

        # used model has tag 'second'
        models = self.get_models()
        model_id = ret.predictor_id[0]
        assert models[model_id].label == 'second'

        # -- retrain again with active=0 --
        data_handler.reset_mock()
        self.run_sql(
            '''
                retrain proj.task_model
                from pg (select * from tasks where a=2)
                PREDICT a
                using tag='third', active=0
            '''
        )
        self.wait_predictor('proj', 'task_model', {'tag': 'third'})

        ret = self.run_sql('select * from proj.models')

        # check target is from previous retrain
        assert ret['PREDICT'][0] == 'b'

        # use model
        ret = self.run_sql('''
             SELECT m.*
               FROM pg.tasks as t
               JOIN proj.task_model as m
        ''')

        # used model has tag 'second' (previous)
        models = self.get_models()
        model_id = ret.predictor_id[0]
        assert models[model_id].label == 'second'

        # ================ working with inactive versions =================

        # run 3rd version model and check used model version
        ret = self.run_sql('''
             SELECT m.*
               FROM pg.tasks as t
               JOIN proj.task_model.3 as m
        ''')

        # 3rd version was used
        models = self.get_models()
        model_id = ret.predictor_id[0]
        assert models[model_id].label == 'third'

        # one-line query model by version
        ret = self.run_sql('SELECT * from proj.task_model.3 where a=1 and b=2')
        model_id = ret.predictor_id[0]
        assert models[model_id].label == 'third'

        # check exception: not existing version
        with pytest.raises(EntityNotExistsError) as exc_info:
            self.run_sql(
                'SELECT * from proj.task_model.4 where a=1 and b=2',
            )

        # ===================== one-line with 'use database'=======================

        # active
        ret = self.run_sql('SELECT * from task_model where a=1 and b=2', database='proj')
        model_id = ret.predictor_id[0]
        assert models[model_id].label == 'second'

        # inactive
        ret = self.run_sql('SELECT * from task_model.3 where a=1 and b=2', database='proj')
        model_id = ret.predictor_id[0]
        assert models[model_id].label == 'third'

        # ================== managing versions =========================

        # check 'show models' command in different combination
        # Show models <from | in> <project> where <expr>
        ret = self.run_sql('Show models')
        assert len(ret) == 1 and ret['NAME'][0] == 'task_model'

        ret = self.run_sql('Show models from proj')
        assert len(ret) == 1 and ret['NAME'][0] == 'task_model'

        ret = self.run_sql('Show models in proj')
        assert len(ret) == 1 and ret['NAME'][0] == 'task_model'

        ret = self.run_sql("Show models where name='task_model'")
        assert len(ret) == 1 and ret['NAME'][0] == 'task_model'

        # model is not exists
        ret = self.run_sql("Show models from proj where name='xxx'")
        assert len(ret) == 0

        # ----------------

        # See all versions
        ret = self.run_sql('select * from proj.models_versions')
        # we have all tags in versions
        assert set(ret['TAG']) == {'first', 'second', 'third'}

        # Set active selected version
        self.run_sql('''
           update proj.models_versions
           set active=1
           where version=1 and name='task_model'
        ''')

        # get active version
        ret = self.run_sql('select * from proj.models_versions where active = 1')
        assert ret['TAG'][0] == 'first'

        # use active version ?

        # Delete specific version
        self.run_sql('''
           delete from proj.models_versions
           where version=2
           and name='task_model'
        ''')

        # deleted version not in list
        ret = self.run_sql('select * from proj.models_versions')
        assert len(ret) == 2
        assert 'second' not in ret['TAG']

        # try to use deleted version
        with pytest.raises(EntityNotExistsError) as exc_info:
            self.run_sql(
                'SELECT * from proj.task_model.2 where a=1',
            )

        # exception with deleting active version
        with pytest.raises(Exception) as exc_info:
            self.run_sql('''
               delete from proj.models_versions
               where version=1
               and name='task_model'
            ''')
        assert "Can't remove active version" in str(exc_info.value)

        # exception with deleting non-existing version
        with pytest.raises(Exception) as exc_info:
            self.run_sql('''
               delete from proj.models_versions
               where version=11
               and name='task_model'
            ''')
        assert "is not found" in str(exc_info.value)

        # ----------------------------------------------------

        # retrain without all params
        self.run_sql(
            '''
                retrain proj.task_model
            '''
        )
        self.wait_predictor('proj', 'task_model', {'version': '4'})

        # ----------------------------------------------------

        # drop predictor and check model is deleted and no versions
        self.run_sql('drop model proj.task_model')
        ret = self.run_sql('select * from proj.models')
        assert len(ret) == 0

        # versions are also deleted
        ret = self.run_sql('select * from proj.models_versions')
        assert len(ret) == 0

    def test_view(self):
        df = pd.DataFrame([
            {'a': 1, 'b': dt.datetime(2020, 1, 1)},
            {'a': 2, 'b': dt.datetime(2020, 1, 2)},
            {'a': 1, 'b': dt.datetime(2020, 1, 3)},
        ])
        self.save_file('tasks', df)

        self.run_sql('''
            create view mindsdb.vtasks (
                select * from files.tasks where a=1
            )
        ''')

        # -- create model --
        self.run_sql(
            '''
                CREATE model mindsdb.task_model
                from mindsdb (select * from vtasks)
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
                join_learn_process=true
            '''
        )
        self.wait_predictor('mindsdb', 'task_model')

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
        self.set_handler(data_handler, name='pg', tables={'tbl1': df1, 'tbl2': df2})

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
                select * from pg.tbl2 where a!=4
            )
        ''')

        # --- test join table-table-table ---
        ret = self.run_sql('''
            SELECT t1.a as t1a,  t3.a t3a
              FROM pg.tbl1 as t1
              JOIN pg.tbl2 as t2 on t1.c=t2.c
              LEFT JOIN pg.tbl1 as t3 on t2.a=t3.a
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
              FROM pg.tbl1 as t1
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
              FROM pg.tbl1 as t1
              JOIN (
                  SELECT t1.a as t1a,  t3.a t3a
                  FROM pg.tbl1 as t1
                  JOIN pg.tbl2 as t2 on t1.c=t2.c
                  LEFT JOIN pg.tbl1 as t3 on t2.a=t3.a
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

        # -- window functions --
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

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_last(self, data_handler):
        df = pd.DataFrame([
            {'a': 1, 'b': 'a'},
            {'a': 2, 'b': 'b'},
            {'a': 3, 'b': 'c'},
        ])
        self.set_handler(data_handler, name='pg', tables={'tasks': df})

        # -- create model --
        self.run_sql(
            '''
                CREATE model task_model
                from pg (select * from tasks)
                PREDICT a
                using engine='dummy_ml'
            '''
        )

        # --- check web editor  ---
        ret = self.run_sql('''
            select * from pg.tasks where a>last
         ''')
        # first call is empty
        assert len(ret) == 0

        # add rows to dataframe
        df.loc[len(df.index)] = [4, 'd']  # should be tracked
        df.loc[len(df.index)] = [0, 'z']  # not tracked

        ret = self.run_sql('''
            select * from pg.tasks where a>last
        ''')

        # second call content one new line
        assert len(ret) == 1
        assert ret.a[0] == 4

        # --- TEST view ---

        # view without target
        with pytest.raises(Exception) as exc_info:
            self.run_sql('''
                create view v1 (
                    select b from pg.tasks where a>last
                )
            ''')
        assert 'should be in query target' in str(exc_info.value)

        # view with target
        self.run_sql('''
            create view v1 (
                select * from pg.tasks where a>last
            )
        ''')

        ret = self.run_sql('''
          select * from v1
        ''')
        # first call is empty
        assert len(ret) == 0

        # add row to dataframe
        df.loc[len(df.index)] = [5, 'a']

        ret = self.run_sql('''
            select * from v1
        ''')

        # second call content one new line
        assert len(ret) == 1
        assert ret.a[0] == 5

        # add row to dataframe
        df.loc[len(df.index)] = [6, 'a']

        # use model
        ret = self.run_sql('''
             SELECT m.*
               FROM v1 as t
               JOIN task_model as m
        ''')

        # second call content one new line
        assert len(ret) == 1

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_last_in_job(self, data_handler, scheduler):
        df = pd.DataFrame([
            {'a': 1, 'b': 'a'},
            {'a': 2, 'b': 'b'},
        ])
        self.set_handler(data_handler, name='pg', tables={'tasks': df})

        # -- create model --
        self.run_sql(
            '''
                CREATE model task_model
                from pg (select * from tasks)
                PREDICT a
                using engine='dummy_ml'
            '''
        )

        # create job to update table
        self.run_sql('''
          create job j1  (
            create table files.t1  (
                SELECT m.*
                   FROM pg.tasks as t
                   JOIN task_model as m
                   where t.a > last and t.b='b'
            )
          )
          start now
          every hour
        ''')

        scheduler.check_timetable()

        # table size didn't change
        calls = data_handler().query.call_args_list
        sql = calls[0][0][0].to_string()
        # getting current last value
        assert 'ORDER BY a DESC LIMIT 1' in sql

        # insert new record to source db

        df.loc[len(df.index)] = [6, 'a']

        data_handler.reset_mock()
        # shift 'next run' and run once again
        job = self.db.Jobs.query.filter(self.db.Jobs.name == 'j1').first()
        job.next_run_at = job.start_at - dt.timedelta(seconds=1)  # different time because there is unique key
        self.db.session.commit()

        scheduler.check_timetable()

        calls = data_handler().query.call_args_list

        assert len(calls) == 1
        sql = calls[0][0][0].to_string()
        # getting next value, greater than max previous
        assert 'a > 2' in sql
        assert "b = 'b'" in sql


class TestJobs(BaseExecutorDummyML):

    def run_sql(self, sql, throw_error=True, database='mindsdb'):
        self.command_executor.session.database = database
        ret = self.command_executor.execute_command(
            parse_sql(sql, dialect='mindsdb')
        )
        if throw_error:
            assert ret.error_code is None
        if ret.data is not None:
            columns = [
                col.alias if col.alias is not None else col.name
                for col in ret.columns
            ]
            return pd.DataFrame(ret.data, columns=columns)

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_job(self, data_handler, scheduler):
        df1 = pd.DataFrame([
            {'a': 1, 'c': 1, 'b': dt.datetime(2020, 1, 1)},
            {'a': 2, 'c': 1, 'b': dt.datetime(2020, 1, 2)},
            {'a': 1, 'c': 3, 'b': dt.datetime(2020, 1, 3)},
            {'a': 3, 'c': 2, 'b': dt.datetime(2020, 1, 2)},
        ])
        self.set_handler(data_handler, name='pg', tables={'tbl1': df1})

        self.run_sql('create database proj1')
        # create job
        self.run_sql('create job j1 (select * from models; select * from models)', database='proj1')

        # check jobs table
        ret = self.run_sql('select * from jobs', database='proj1')
        assert len(ret) == 1, "should be 1 job"
        row = ret.iloc[0]
        assert row.NAME == 'j1'
        assert row.START_AT is not None, "start date didn't calc"
        assert row.NEXT_RUN_AT is not None, "next date didn't calc"
        assert row.SCHEDULE_STR is None

        # new project
        self.run_sql('create database proj2')

        # create job with start time and schedule
        self.run_sql('''
            create job proj2.j2 (
                select * from pg.tbl1 where b>'{{PREVIOUS_START_DATETIME}}'
            )
            start now
            every hour
        ''', database='proj1')

        # check jobs table
        ret = self.run_sql('select * from proj2.jobs')
        assert len(ret) == 1, "should be 1 job"
        row = ret.iloc[0]
        assert row.NAME == 'j2'
        assert row.SCHEDULE_STR == 'every hour'

        # check global jobs table
        ret = self.run_sql('select * from information_schema.jobs')
        # all jobs in list
        assert len(ret) == 2
        assert set(ret.NAME.unique()) == {'j1', 'j2'}

        # drop first job
        self.run_sql('drop job proj1.j1')

        # ------------ executing

        # run scheduler once
        data_handler.reset_mock()

        scheduler.check_timetable()

        # check query to integration
        job = self.db.Jobs.query.filter(self.db.Jobs.name == 'j2').first()

        # check jobs table
        ret = self.run_sql('select * from jobs', database='proj2')
        # next run is about 60 minutes from previous
        minutes = (ret.NEXT_RUN_AT - ret.START_AT)[0].seconds / 60
        assert minutes > 58 and minutes < 62

        # check history table
        ret = self.run_sql('select * from jobs_history', database='proj2')
        # proj2.j2 was run one time
        assert len(ret) == 1
        assert ret.PROJECT[0] == 'proj2' and ret.NAME[0] == 'j2'

        # run once again
        scheduler.check_timetable()

        # job wasn't executed
        ret = self.run_sql('select * from jobs_history', database='proj2')
        assert len(ret) == 1

        # shift 'next run' and run once again
        job = self.db.Jobs.query.filter(self.db.Jobs.name == 'j2').first()
        job.next_run_at = job.start_at - dt.timedelta(seconds=1)  # different time because there is unique key
        self.db.session.commit()

        data_handler.reset_mock()
        scheduler.check_timetable()

        ret = self.run_sql('select * from jobs_history', database='proj2')
        assert len(ret) == 2  # was executed

        # check global history table
        ret = self.run_sql('select * from information_schema.jobs_history', database='proj2')
        assert len(ret) == 2  # was executed

    def test_inactive_job(self, scheduler):

        # create job
        self.run_sql('create job j1 (select * from models)')

        # check jobs table
        ret = self.run_sql('select * from jobs')
        assert len(ret) == 1, "should be 1 job"

        # deactivate
        job = self.db.Jobs.query.filter(self.db.Jobs.name == 'j1').first()
        job.active = False
        self.db.session.commit()

        # run scheduler
        scheduler.check_timetable()

        ret = self.run_sql('select * from jobs_history')
        # no history
        assert len(ret) == 0

    def test_conditional_job(self, scheduler):
        df = pd.DataFrame([
            {'a': 1, 'b': '2'},
        ])
        self.save_file('tasks', df)

        # create job
        job_str = '''
           create job j1 (
              CREATE model pred
                PREDICT p
                using engine='dummy_ml',
                join_learn_process=true
           )
           if (
               select * from files.tasks where a={var}
           )
        '''

        self.run_sql(job_str.format(var=2))

        # check jobs table
        ret = self.run_sql('select * from jobs')
        assert len(ret) == 1, "should be 1 job"

        # run scheduler
        scheduler.check_timetable()

        # check no models created
        ret = self.run_sql('select * from models where name="pred"')
        assert len(ret) == 0

        # --- attempt2 ---

        self.run_sql(job_str.format(var=1))

        # check jobs table, still one job - previous was one time job
        ret = self.run_sql('select * from jobs')
        assert len(ret) == 1, "should be 1 job"

        # run scheduler
        scheduler.check_timetable()

        # check 1 model
        ret = self.run_sql('select * from models where name="pred"')
        assert len(ret) == 1


class TestTriggers(BaseExecutorDummyML):
    def test_triggers(self):
        # TODO
        ...

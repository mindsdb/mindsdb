from unittest.mock import patch
import datetime as dt
import time

import pandas as pd
from lightwood.api import dtype

from mindsdb_sql import parse_sql

from .executor_test_base import BaseExecutorDummyML


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

    def run_sql(self, sql):
        ret = self.command_executor.execute_command(
            parse_sql(sql, dialect='mindsdb')
        )
        assert ret.error_code is None
        if ret.data is not None:
            columns = [
                col.alias if col.alias is not None else col.name
                for col in ret.columns
            ]
            return pd.DataFrame(ret.data, columns=columns)

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_flow(self, data_handler):
        # set up

        df = pd.DataFrame([
            {'a': 1, 'b': dt.datetime(2020, 1, 1)},
            {'a': 2, 'b': dt.datetime(2020, 1, 2)},
            {'a': 1, 'b': dt.datetime(2020, 1, 3)},
        ])
        self.set_handler(data_handler, name='pg', tables={'tasks': df})

        # ----------------

        # create folder
        self.run_sql('create database proj')

        # -- create model --
        self.run_sql(
            '''
                CREATE PREDICTOR proj.task_model
                from pg (select * from tasks)
                PREDICT a
                using engine='dummy_ml'
            '''
        )
        self.wait_predictor('proj', 'task_model')

        assert data_handler().native_query.call_args[0][0] == 'select * from tasks'

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
        self.run_sql(
            '''
                retrain proj.task_model
                from pg (select * from tasks where a=2)
                PREDICT b
                using tag = 'new'
            '''
        )
        self.wait_predictor('proj', 'task_model', {'tag': 'new'})

        # get current model
        ret = self.run_sql('select * from proj.models')

        # check target
        assert ret['PREDICT'][0] == 'b'

        # check label
        assert ret['TAG'][0] == 'new'

        # check integration sql
        assert data_handler().native_query.call_args[0][0] == 'select * from tasks where a=2'

        # use model
        ret = self.run_sql('''
             SELECT m.*
               FROM pg.tasks as t
               JOIN proj.task_model as m
        ''')
        assert ret.predicted[0] == 42

        # -- retrain again with active=0 --
        data_handler.reset_mock()
        self.run_sql(
            '''
                retrain proj.task_model
                from pg (select * from tasks where a=2)
                PREDICT a
                using tag='new2', active=0
            '''
        )
        self.wait_predictor('proj', 'task_model', {'tag': 'new2'})

        ret = self.run_sql('select * from proj.models')

        # check target is from previous retrain
        assert ret['PREDICT'][0] == 'b'

        # list of versions
        ret = self.run_sql('select * from proj.models_versions')
        # we have all tags in versions
        assert set(ret['TAG']) == {None, 'new', 'new2'}

        # TODO:
        # run predict with old version
        # switch version
        # drop version

        # drop predictor and check model is deleted and no versions
        self.run_sql('drop predictor proj.task_model')
        ret = self.run_sql('select * from proj.models')
        assert len(ret) == 0

        ret = self.run_sql('select * from proj.models_versions')
        assert len(ret) == 0

        # TODO: all the same with TS


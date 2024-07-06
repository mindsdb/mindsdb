import datetime as dt
import pytest

import pandas as pd

from tests.unit.executor_test_base import BaseExecutorDummyML


class TestProjectStructure(BaseExecutorDummyML):

    def test_version_managing(self):
        from mindsdb.utilities.exception import EntityNotExistsError
        # set up
        self.set_data('tasks', pd.DataFrame([
            {'a': 1, 'b': dt.datetime(2020, 1, 1)},
            {'a': 2, 'b': dt.datetime(2020, 1, 2)},
            {'a': 1, 'b': dt.datetime(2020, 1, 3)},
        ]))

        # ================= retrain cycles =====================

        # create folder
        self.run_sql('create database proj')

        # -- create model --
        ret = self.run_sql(
            '''
                CREATE model proj.task_model
                from proj (select * from dummy_data.tasks)
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
               FROM dummy_data.tasks as t
               JOIN proj.task_model as m
        ''')

        assert len(ret) == 3
        assert ret.predicted[0] == 42

        # -- retrain predictor with tag --
        ret = self.run_sql(
            '''
                retrain proj.task_model
                from dummy_data (select * from tasks where a=2)
                PREDICT b
                using tag = 'second',
                join_learn_process=true
            '''
        )
        assert ret['NAME'][0] == 'task_model'
        assert ret['TAG'][0] == 'second'
        self.wait_predictor('proj', 'task_model', filters={'tag': 'second'})

        # get current model
        ret = self.run_sql('select * from proj.models where active=1')

        # check target
        assert ret['PREDICT'][0] == 'b'

        # check label
        assert ret['TAG'][0] == 'second'

        # use model
        ret = self.run_sql('''
             SELECT m.*
               FROM dummy_data.tasks as t
               JOIN proj.task_model as m
        ''')
        assert ret.predicted[0] == 42

        # used model has tag 'second'
        models = self.get_models()
        model_id = ret.predictor_id[0]
        assert models[model_id].label == 'second'

        # -- retrain again with active=0 --
        self.run_sql(
            '''
                retrain proj.task_model
                from dummy_data (select * from tasks where a=2)
                PREDICT a
                using tag='third', active=0
            '''
        )
        self.wait_predictor('proj', 'task_model', filters={'tag': 'third'})

        ret = self.run_sql('select * from proj.models where active=1')

        # check target is from previous retrain
        assert ret['PREDICT'][0] == 'b'

        # use model
        ret = self.run_sql('''
             SELECT m.*
               FROM dummy_data.tasks as t
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
               FROM dummy_data.tasks as t
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
        # mindsdb project
        assert len(ret) == 0

        ret = self.run_sql('Show models from proj')
        # it also shows versions
        assert len(ret) == 3 and ret['NAME'][0] == 'task_model'

        # ret = self.run_sql('Show models in proj')
        # assert len(ret) == 3 and ret['NAME'][0] == 'task_model'

        ret = self.run_sql("Show models from proj where name='task_model'")
        assert len(ret) == 3 and ret['NAME'][0] == 'task_model'

        # model is not exists
        ret = self.run_sql("Show models from proj where name='xxx'")
        assert len(ret) == 0

        # ----------------

        # See all versions
        ret = self.run_sql('select * from proj.models')
        # we have all tags in versions
        assert set(ret['TAG']) == {'first', 'second', 'third'}

        # Set active selected version
        self.run_sql('set active proj.task_model.1')

        # get active version
        ret = self.run_sql('select * from proj.models where active = 1')
        assert ret['TAG'][0] == 'first'

        # use active version ?

        # Delete specific version
        self.run_sql('drop model proj.task_model.2')

        # deleted version not in list
        ret = self.run_sql('select * from proj.models')
        assert len(ret) == 2
        assert 'second' not in ret['TAG']

        # try to use deleted version
        with pytest.raises(EntityNotExistsError) as exc_info:
            self.run_sql(
                'SELECT * from proj.task_model.2 where a=1',
            )

        # exception with deleting active version
        with pytest.raises(Exception) as exc_info:
            self.run_sql('drop model proj.task_model.1')
        assert "Can't remove active version" in str(exc_info.value)

        # exception with deleting non-existing version
        with pytest.raises(Exception) as exc_info:
            self.run_sql('drop model proj.task_model.11')
        assert "is not found" in str(exc_info.value)

        # ----------------------------------------------------

        # retrain without all params
        self.run_sql(
            '''
                retrain proj.task_model
            '''
        )
        self.wait_predictor('proj', 'task_model', filters={'version': '4'})

        # ----------------------------------------------------

        # drop predictor and check model is deleted and no versions
        self.run_sql('drop model proj.task_model')
        ret = self.run_sql('select * from proj.models')
        assert len(ret) == 0

        # versions are also deleted
        ret = self.run_sql('select * from proj.models')
        assert len(ret) == 0

    def test_project_names_duplicate(self):
        # create folder
        self.run_sql('create project proj1')

        self.run_sql("create database db1 using engine='dummy_data'")

        with pytest.raises(Exception):
            self.run_sql('create project db1')

        with pytest.raises(Exception):
            self.run_sql("create database proj1 using engine='dummy_data'")

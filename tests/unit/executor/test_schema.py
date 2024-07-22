from unittest.mock import patch
import datetime as dt
import pytest

import pandas as pd


from tests.unit.executor_test_base import BaseExecutorDummyML


class TestSchema(BaseExecutorDummyML):

    def test_show(self):
        for item in ('chatbots', 'knowledge_bases', 'agents', 'skills', 'jobs'):

            self.run_sql(f'show {item}')

    def test_schema(self):

        # --- create objects + describe ---
        # todo: create knowledge base (requires chromadb)

        df = pd.DataFrame([
            {'a': 6, 'c': 1},
        ])
        self.set_data('table1', df)

        # project
        self.run_sql('create project proj2')

        # ml_engine
        self.run_sql('''
            CREATE ML_ENGINE engine1 from dummy_ml
        ''')

        # job
        self.run_sql('create job j1 (select * from models) every hour')
        self.run_sql('create job proj2.j2 (select * from models) every hour')

        df = self.run_sql('describe job j1')
        assert df.NAME[0] == 'j1' and df.QUERY[0] == 'select * from models'

        # view
        self.run_sql('create view v1 (select * from models)')
        self.run_sql('create view proj2.v2 (select * from models)')

        df = self.run_sql('describe view v1')
        assert df.NAME[0] == 'v1' and df.QUERY[0] == 'select * from models'

        # model
        self.run_sql('''
                CREATE model pred1
                PREDICT p
                using engine='dummy_ml',
                join_learn_process=true
        ''')
        self.run_sql('''
                CREATE model proj2.pred2
                PREDICT p
                using engine='dummy_ml',
                join_learn_process=true
        ''')
        # and retrain first model
        self.run_sql('''
                RETRAIN pred1
                using engine='dummy_ml'
        ''')

        # trigger
        self.run_sql('''
              create trigger trigger1
              on dummy_data.table1 (show models)
        ''')
        self.run_sql('''
              create trigger proj2.trigger2
              on dummy_data.table1 (show models)
        ''')

        df = self.run_sql('describe trigger trigger1')
        assert df.NAME[0] == 'trigger1' and df.QUERY[0] == 'show models'

        # agent
        self.run_sql('''
              CREATE AGENT agent1
              USING model = 'pred1'
        ''')
        self.run_sql('''
              CREATE AGENT proj2.agent2
              USING model = 'pred2' -- it looks up in agent's project
        ''')

        df = self.run_sql('describe agent agent1')
        assert df.NAME[0] == 'agent1' and df.MODEL_NAME[0] == 'pred1'

        # chatbot
        self.run_sql('''
              CREATE CHATBOT chatbot1
              USING database = "dummy_data",
                    agent = "agent1"
        ''')
        self.run_sql('''
              CREATE CHATBOT proj2.chatbot2
              USING database = "dummy_data",
                    agent = "agent2"  -- it looks up in chatbot's project
        ''')

        df = self.run_sql('describe chatbot chatbot1')
        assert df.NAME[0] == 'chatbot1' and df.DATABASE[0] == 'dummy_data'

        # skill
        self.run_sql('''
         CREATE SKILL skill1
            USING type = 'text_to_sql',
                database = 'dummy_data', tables = ['table1'];
        ''')
        self.run_sql('''
         CREATE SKILL proj2.skill2
            USING type = 'text_to_sql',
                database = 'dummy_data', tables = ['table1'];
        ''')

        df = self.run_sql('describe skill skill1')
        assert df.NAME[0] == 'skill1' and df.TYPE[0] == 'text_to_sql'

        # --- SHOW ---

        # handlers
        df = self.run_sql('show handlers')
        assert 'dummy_ml' in list(df.NAME)

        # projects
        df = self.run_sql('show projects')
        objects = list(df.iloc[:, 0])
        assert 'mindsdb' in objects
        assert 'proj2' in objects

        # databases
        df = self.run_sql('show databases')
        objects = list(df.iloc[:, 0])
        assert 'information_schema' in objects
        assert 'log' in objects

        # ml engines
        df = self.run_sql('show ml_engines')
        assert 'engine1' in list(df.NAME)

        # project objects
        def _test_proj_obj(table_name, obj_name):
            # check: obj1 is current project, obj2 in proj2

            df = self.run_sql(f'show {table_name}')
            assert len(df) == 1 and f'{obj_name}1' in list(df.NAME)

            df = self.run_sql(f'show {table_name} from proj2')
            assert len(df) == 1 and f'{obj_name}2' in list(df.NAME)

        _test_proj_obj('jobs', 'j')
        _test_proj_obj('views', 'v')
        _test_proj_obj('triggers', 'trigger')
        _test_proj_obj('chatbots', 'chatbot')
        _test_proj_obj('agents', 'agent')
        _test_proj_obj('skills', 'skill')

        # model
        df = self.run_sql('show models')
        # two versions of same model
        assert len(df[df.NAME != 'pred1']) == 0 and len(df) == 2

        df = self.run_sql('show models from proj2')
        assert 'pred2' in list(df.NAME) and len(df) == 1

        # --- information_schema ---

        # handlers
        df = self.run_sql('select * from information_schema.HANDLERS')
        assert 'dummy_ml' in list(df.NAME)

        # databases
        df = self.run_sql('select * from information_schema.DATABASES')
        assert 'mindsdb' in list(df.NAME)
        assert 'proj2' in list(df.NAME)
        assert 'log' in list(df.NAME)

        # ml engines
        df = self.run_sql('select * from information_schema.ML_ENGINES')
        assert 'engine1' in list(df.NAME)

        # project objects
        def _test_proj_obj(table_name, obj_name):
            # obj1 in mindsdb, obj2 in proj2

            df = self.run_sql(f'select * from information_schema.{table_name}')
            assert len(df) == 2

            df1 = df[df.PROJECT == 'mindsdb']
            assert df1.iloc[0].NAME == f'{obj_name}1'

            df1 = df[df.PROJECT == 'proj2']
            assert df1.iloc[0].NAME == f'{obj_name}2'

        _test_proj_obj('JOBS', 'j')
        _test_proj_obj('VIEWS', 'v')
        _test_proj_obj('TRIGGERS', 'trigger')
        _test_proj_obj('CHATBOTS', 'chatbot')
        _test_proj_obj('AGENTS', 'agent')
        _test_proj_obj('SKILLS', 'skill')

        # models
        df = self.run_sql('select * from information_schema.MODELS')
        # two versions of pred1 and one version of pred2
        assert len(df[df.NAME == 'pred1']) == 2
        assert len(df[df.NAME == 'pred2']) == 1

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_select_columns(self, data_handler):
        df = pd.DataFrame([[1, 'x'], [2, 'y']], columns=['aa', 'bb'])

        self.set_handler(data_handler, name='pg', tables={'tbl1': df})

        ret = self.run_sql("SELECT * FROM information_schema.columns WHERE table_schema='pg'")

        assert list(ret['COLUMN_NAME']) == ['aa', 'bb']

    def test_llm_log(self):
        from mindsdb.interfaces.database.log import LLMLogTable

        ret = self.run_sql('select * from log.llm_log')
        assert len(ret) == 0

        record = self.db.Predictor(
            id=1,
            project_id=0,
            name='test'
        )
        self.db.session.add(record)
        self.db.session.commit()

        for j in range(2):
            for i in range(3 + j):
                record = self.db.LLMLog(
                    api_key=f'api_key_{j}',
                    model_id=1,
                    input='test_input',
                    output='test_output',
                    prompt_tokens=i,
                    completion_tokens=i,
                    total_tokens=i,
                    start_time=dt.datetime.now(),
                    end_time=dt.datetime.now()
                )
                self.db.session.add(record)
                self.db.session.commit()

        ret = self.run_sql('select * from log.llm_log')
        assert len(ret) == 7
        assert sorted([x.upper() for x in list(ret.columns)]) == sorted([x.upper() for x in LLMLogTable.columns])

        with pytest.raises(Exception):
            self.run_sql('select company_id from log.llm_log')

        ret = self.run_sql("select model_name, input, output, api_key from log.llm_log where api_key = 'api_key_1'")
        assert len(ret) == 4
        assert len(ret.columns) == 4
        assert ret['model_name'][0] == 'test'
        assert ret['api_key'][0] == 'api_key_1'

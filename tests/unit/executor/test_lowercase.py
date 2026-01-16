from unittest.mock import patch

import pytest
import pandas as pd

from tests.unit.executor_test_base import BaseExecutorDummyML
from tests.unit.executor.test_agent import set_litellm_embedding


class TestLowercase(BaseExecutorDummyML):
    def test_view_name_lowercase(self):
        # mix-case
        self.run_sql("CREATE VIEW `MyView` AS (SELECT 1)")

        res = self.run_sql("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_name = 'MyView'")
        assert res["TABLE_TYPE"][0] == "VIEW"

        with pytest.raises(Exception):
            self.run_sql("DROP VIEW MyView")
        self.run_sql("DROP VIEW `MyView`")

        views_names = ["myview", "MyView", "MYVIEW"]
        for view_name in views_names:
            another_name = "myVIEW"
            self.run_sql(f"CREATE VIEW {view_name} AS (SELECT 1)")

            res = self.run_sql(f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_name = '{view_name.lower()}'")
            assert res["TABLE_TYPE"][0] == "VIEW"

            # alter view: wrong quoted case
            with pytest.raises(Exception):
                self.run_sql(f"ALTER VIEW `{another_name}` AS (SELECT 2)")

            # alter view: wrong case
            self.run_sql(f"ALTER VIEW {another_name} AS (SELECT 2)")

            # select: wrong quoted case
            with pytest.raises(Exception):
                self.run_sql(f"SELECT * FROM `{another_name}`")

            self.run_sql(f"SELECT * FROM {another_name}")

            # dropL wrong quoted case
            with pytest.raises(Exception):
                self.run_sql(f"DROP VIEW `{another_name}`")

            self.run_sql(f"DROP VIEW {another_name}")

    def test_project_name_lowercase(self):
        # quoted name in mix case
        self.run_sql("CREATE DATABASE `MyProject`")

        res = self.run_sql("SELECT * FROM INFORMATION_SCHEMA.DATABASES WHERE name = 'MyProject'")
        assert len(res) == 1

        with pytest.raises(Exception):
            self.run_sql("DROP DATABASE MyProject")
        self.run_sql("DROP DATABASE `MyProject`")

        # processing is slightly different for 'projects' (without engine) and integrations, so we do cycle for both.
        for engine in ["", " WITH ENGINE = 'dummy_data'"]:
            for project_name in ["myproject", "MyProject", "MYPROJECT"]:
                another_name = "myPROJECT"
                self.run_sql(f"CREATE DATABASE {project_name} {engine}")

                res = self.run_sql(f"SELECT * FROM INFORMATION_SCHEMA.DATABASES where name = '{another_name}'")
                assert len(res) == 0

                res = self.run_sql(f"SELECT * FROM INFORMATION_SCHEMA.DATABASES where name = '{project_name.lower()}'")
                assert res["TYPE"][0] == ("project" if engine == "" else "data")

                # FIXME
                # with pytest.raises(Exception):
                #     self.execute(f"""
                #         SELECT * FROM `{another_name}`.models
                #     """)
                if engine == "":
                    # change name for projects
                    with pytest.raises(Exception):
                        self.run_sql(f"ALTER DATABASE `{another_name}` NAME = '{another_name.lower()}'")
                    with pytest.raises(Exception):
                        self.run_sql(f"ALTER DATABASE {another_name} NAME = '{another_name}'")
                    self.run_sql(f"ALTER DATABASE {another_name} NAME = '{another_name.lower()}'")

                with pytest.raises(Exception):
                    self.run_sql(f"DROP DATABASE `{another_name}`")

                self.run_sql(f"DROP DATABASE {another_name}")

    def test_ml_engine_name_lowercase(self):
        # mixed case
        self.run_sql("CREATE ML_ENGINE `MyMlEngine` FROM dummy_ml")

        res = self.run_sql("SELECT * FROM INFORMATION_SCHEMA.ML_ENGINES WHERE name ='MyMlEngine'")
        assert len(res) == 1

        with pytest.raises(Exception):
            self.run_sql("DROP ML_ENGINE MyMlEngine")
        self.run_sql("DROP ML_ENGINE `MyMlEngine`")

        for engine_name in ["mymlengine", "MyMlEngine", "MYMLENGINE"]:
            another_name = "myMLEngine"
            self.run_sql(f"CREATE ML_ENGINE {engine_name} FROM dummy_ml")

            res = self.run_sql(f"SELECT * FROM INFORMATION_SCHEMA.ML_ENGINES WHERE name = '{engine_name.lower()}'")
            assert res["HANDLER"][0] == "dummy_ml"

            with pytest.raises(Exception):
                self.run_sql(f"DROP ML_ENGINE `{another_name}`")

            self.run_sql(f"DROP ML_ENGINE {another_name}")

    def test_model_name_lowercase(self):
        self.run_sql("CREATE ML_ENGINE myengine FROM dummy_ml")
        df = pd.DataFrame(
            [
                {"a": 1, "b": "one"},
                {"a": 2, "b": "two"},
            ]
        )
        self.set_data("tasks", df)

        # mixed case
        self.run_sql("CREATE MODEL `MyModel` PREDICT a USING engine='myengine', join_learn_process=true")

        res = self.run_sql("SELECT * FROM INFORMATION_SCHEMA.MODELS WHERE name ='MyModel'")
        assert len(res) == 1

        with pytest.raises(Exception):
            self.run_sql("DROP MODEL MyModel")
        self.run_sql("DROP MODEL `MyModel`")

        # mixed project
        self.run_sql("CREATE DATABASE `MyProj`")
        self.run_sql("CREATE MODEL `MyProj`.MyModel PREDICT a USING engine='myengine', join_learn_process=true")

        res = self.run_sql("SELECT * FROM INFORMATION_SCHEMA.MODELS WHERE name ='mymodel'")
        assert len(res) == 1

        with pytest.raises(Exception):
            self.run_sql("DROP MODEL MyProj.MyModel")
        self.run_sql("DROP MODEL `MyProj`.MyModel")

        for model_name in ["mymodel", "MyModel", "MYMODEL"]:
            another_name = "myMODEL"
            self.run_sql(f"CREATE MODEL {model_name} PREDICT a USING engine='myengine', join_learn_process=true")

            res = self.run_sql(f"SELECT * FROM INFORMATION_SCHEMA.MODELS WHERE name = '{model_name.lower()}'")
            assert res["ENGINE"][0] == "dummy_ml"

            with pytest.raises(Exception):
                self.run_sql(f"RETRAIN MODEL `{another_name}` using join_learn_process=true")

            self.run_sql(f"RETRAIN MODEL {another_name} using join_learn_process=true")

            with pytest.raises(Exception):
                self.run_sql(f"""
                    FINETUNE MODEL `{another_name}` FROM dummy_data (select * from tasks) using join_learn_process=true
                """)

            self.run_sql(f"""
                FINETUNE MODEL {another_name} FROM dummy_data (select * from tasks) using join_learn_process=true
            """)

            with pytest.raises(Exception):
                self.run_sql(f"DROP MODEL `{another_name}`")
            self.run_sql(f"DROP MODEL {another_name}")

    def test_agent_name_lowercase(self):
        agent_params = """
                model='gpt-3.5-turbo',
                provider='openai',
                prompt_template='Answer the user input in a helpful way using tools',
                max_iterations=5,
                mode='retrieval'
        """


        # mixed case: agent
        self.run_sql(f"create agent `MyAGENT` using {agent_params}")

        res = self.run_sql("select * from information_schema.agents where name = 'MyAGENT'")
        assert len(res) == 1

        with pytest.raises(Exception):
            self.run_sql("drop agent MyAGENT")
        self.run_sql("drop agent `MyAGENT`")

        for agent_name in "myagent", "MyAgent", "MYAGENT":
            another_agent_name = "myAGENT"

            self.run_sql(f"""
                create agent {agent_name} using {agent_params}
            """)

            # switch to lowercase
            self.run_sql(f"""
                update agent {agent_name} set {agent_params}
            """)

            ret = self.run_sql(f"select * from information_schema.agents where name = '{agent_name.lower()}'")
            assert len(ret) == 1

            with pytest.raises(Exception):
                self.run_sql(f"drop agent `{another_agent_name}`")
            self.run_sql(f"drop agent {another_agent_name}")

    @patch("litellm.embedding")
    @patch("openai.OpenAI")
    def test_knowledgebase_name_lowercase(self, mock_openai, mock_litellm_embedding):
        set_litellm_embedding(mock_litellm_embedding)

        kb_params = """
            using embedding_model = {
                "provider": "bedrock",
                "model_name": "dummy_model",
                "api_key": "dummy_key"
            }
        """

        # mixed case
        self.run_sql(f"CREATE KNOWLEDGE BASE `MyKB` {kb_params}")

        res = self.run_sql("SELECT * FROM INFORMATION_SCHEMA.KNOWLEDGE_BASES WHERE name = 'MyKB'")
        assert len(res) == 1

        with pytest.raises(Exception):
            self.run_sql("DROP KNOWLEDGE BASE MyKB")
        self.run_sql("DROP KNOWLEDGE BASE `MyKB`")

        for kb_name in ["mykb", "MyKB", "MYKB"]:
            another_kb_name = "myKB"

            self.run_sql(f"CREATE KNOWLEDGE BASE {kb_name} {kb_params}")

            res = self.run_sql(f"""
                SELECT * FROM INFORMATION_SCHEMA.KNOWLEDGE_BASES WHERE name = '{kb_name.lower()}'
            """)
            assert res["NAME"][0] == "mykb"

            with pytest.raises(Exception):
                self.run_sql(f"DROP KNOWLEDGE BASE `{another_kb_name}`")
            self.run_sql(f"DROP KNOWLEDGE BASE {another_kb_name}")

    def test_job_name_lowercase(self):
        # mixed case
        self.run_sql("CREATE JOB `MyJOB` (select 1)")

        res = self.run_sql("SELECT * FROM INFORMATION_SCHEMA.JOBS WHERE name = 'MyJOB'")
        assert len(res) == 1

        with pytest.raises(Exception):
            self.run_sql("DROP JOB MyJOB")
        self.run_sql("DROP JOB `MyJOB`")

        for job_name in ["myjob", "Myjob", "MYJOB"]:
            another_name = "myjoB"
            self.run_sql(f"CREATE JOB {job_name} (select 1)")

            res = self.run_sql(f"SELECT * FROM INFORMATION_SCHEMA.JOBS WHERE name = '{job_name.lower()}'")
            assert len(res) == 1

            with pytest.raises(Exception):
                self.run_sql(f"DROP JOB `{another_name}`")

            self.run_sql(f"DROP JOB {another_name}")

    def test_chatbot_lowercase(self):
        self.run_sql("create agent my_agent using model={'provider': 'openai', 'model_name': 'gpt-3.5'}")

        self.run_sql("create database my_db using engine='dummy_data'")

        # mixed case
        self.run_sql("CREATE CHATBOT `MyChatbot` USING database = 'my_db',  agent = 'my_agent'")

        res = self.run_sql("SELECT * FROM INFORMATION_SCHEMA.CHATBOTS WHERE name = 'MyChatbot'")
        assert len(res) == 1

        with pytest.raises(Exception):
            self.run_sql("DROP CHATBOT MyChatbot")
        self.run_sql("DROP CHATBOT `MyChatbot`")

        for name in ["mychatbot", "MyChatbot", "MYCHATBOT"]:
            another_name = "myChatbot"
            self.run_sql(f"CREATE CHATBOT {name} USING database = 'my_db',  agent = 'my_agent'")

            res = self.run_sql(f"SELECT * FROM INFORMATION_SCHEMA.CHATBOTS WHERE name = '{name.lower()}'")
            assert len(res) == 1

            self.run_sql(f"UPDATE CHATBOT {name} SET agent = 'my_agent'")

            with pytest.raises(Exception):
                self.run_sql(f"DROP CHATBOT `{another_name}`")

            self.run_sql(f"DROP CHATBOT {name}")

    def test_database_lowercase(self):
        # mixed case
        self.run_sql("CREATE DATABASE `MyDB` using engine='dummy_data'")

        res = self.run_sql("SELECT * FROM INFORMATION_SCHEMA.DATABASES WHERE name = 'MyDB'")
        assert len(res) == 1

        with pytest.raises(Exception):
            self.run_sql("DROP DATABASE MyDB")
        self.run_sql("DROP DATABASE `MyDB`")

        for name in ["mydb", "MyDB", "MYDB"]:
            another_name = "myDb"
            self.run_sql(f"create database {name} using engine='dummy_data'")

            res = self.run_sql(f"SELECT * FROM INFORMATION_SCHEMA.DATABASES WHERE name = '{name.lower()}'")
            assert len(res) == 1

            with pytest.raises(Exception):
                self.run_sql(f"DROP DATABASE `{another_name}`")

            self.run_sql(f"DROP DATABASE {name}")

    def test_trigger_lowercase(self):
        # mixed case
        self.run_sql("CREATE TRIGGER `MyTrigger` on dummy_data.table1 (select 1)")

        res = self.run_sql("SELECT * FROM INFORMATION_SCHEMA.TRIGGERS WHERE name = 'MyTrigger'")
        assert len(res) == 1

        with pytest.raises(Exception):
            self.run_sql("DROP TRIGGER MyTrigger")
        self.run_sql("DROP TRIGGER `MyTrigger`")

        for name in ["mytrigger", "MyTrigger", "MYTRIGGER"]:
            another_name = "myTrigger"
            self.run_sql(f"create TRIGGER {name} on dummy_data.table1 (select 1)")

            res = self.run_sql(f"SELECT * FROM INFORMATION_SCHEMA.TRIGGERS WHERE name = '{name.lower()}'")
            assert len(res) == 1

            with pytest.raises(Exception):
                self.run_sql(f"DROP TRIGGER `{another_name}`")

            self.run_sql(f"DROP TRIGGER {name}")

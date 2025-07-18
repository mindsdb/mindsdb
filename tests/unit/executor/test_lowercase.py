from unittest.mock import patch

import pytest
import pandas as pd

from tests.unit.executor_test_base import BaseExecutorMockPredictor
from tests.unit.executor.test_agent import set_litellm_embedding


class TestLowercase(BaseExecutorMockPredictor):
    def setup_method(self, method):
        super().setup_method()
        self.set_executor(mock_lightwood=True, mock_model_controller=True, import_dummy_ml=True)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_view_name_lowercase(self, mock_handler):
        df = pd.DataFrame(
            [
                {"a": 1, "b": "one"},
                {"a": 2, "b": "two"},
            ]
        )
        self.set_handler(mock_handler, name="pg", tables={"tasks": df})

        # Error: quoted name in mx-case
        with pytest.raises(Exception):
            self.execute("CREATE VIEW `MyView` AS (SELECT * FROM pg.tasks)")

        views_names = ["myview", "MyView", "MYVIEW"]
        for view_name in views_names:
            another_name = "myVIEW"
            self.execute(f"CREATE VIEW {view_name} AS (SELECT * FROM pg.tasks)")

            res = self.execute(f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_name = '{view_name.lower()}'")
            assert res.data.to_df()["TABLE_TYPE"][0] == "VIEW"

            # alter view: wrong quoted case
            with pytest.raises(Exception):
                self.execute(f"ALTER VIEW `{another_name}` AS (SELECT * FROM pg.tasks WHERE a = 1)")

            # alter view: wrong case
            self.execute(f"ALTER VIEW {another_name} AS (SELECT * FROM pg.tasks WHERE a = 1)")

            # select: wrong quoted case
            with pytest.raises(Exception):
                self.execute(f"SELECT * FROM `{another_name}`")

            self.execute(f"SELECT * FROM {another_name}")

            # dropL wrong quoted case
            with pytest.raises(Exception):
                self.execute(f"DROP VIEW `{another_name}`")

            self.execute(f"DROP VIEW {another_name}")

    def test_project_name_lowercase(self):
        project_name = "MyProject"

        with pytest.raises(Exception):
            self.execute(f"CREATE DATABASE `{project_name}`")

        # processing is slightly different for 'projects' (without engine) and integrations, so we do cycle for both.
        for engine in ["", " WITH ENGINE = 'dummy_data'"]:
            for project_name in ["myproject", "MyProject", "MYPROJECT"]:
                another_name = "myPROJECT"
                self.execute(f"CREATE DATABASE {project_name} {engine}")

                res = self.execute(f"SELECT * FROM INFORMATION_SCHEMA.DATABASES where name = '{another_name}'")
                assert len(res.data) == 0

                res = self.execute(f"SELECT * FROM INFORMATION_SCHEMA.DATABASES where name = '{project_name.lower()}'")
                assert res.data.to_df()["TYPE"][0] == ("project" if engine == "" else "data")

                # FIXME
                # with pytest.raises(Exception):
                #     self.execute(f"""
                #         SELECT * FROM `{another_name}`.models
                #     """)
                if engine == "":
                    # change name for projects
                    with pytest.raises(Exception):
                        self.execute(f"ALTER DATABASE `{another_name}` NAME = '{another_name.lower()}'")
                    with pytest.raises(Exception):
                        self.execute(f"ALTER DATABASE {another_name} NAME = '{another_name}'")
                    self.execute(f"ALTER DATABASE {another_name} NAME = '{another_name.lower()}'")

                with pytest.raises(Exception):
                    self.execute(f"DROP DATABASE `{another_name}`")

                self.execute(f"DROP DATABASE {another_name}")

    def test_ml_engine_name_lowercase(self):
        with pytest.raises(Exception):
            self.execute("CREATE ML_ENGINE `MyMlEngine` FROM dummy_ml")

        for engine_name in ["mymlengine", "MyMlEngine", "MYMLENGINE"]:
            another_name = "myMLEngine"
            self.execute(f"CREATE ML_ENGINE {engine_name} FROM dummy_ml")

            res = self.execute(f"SELECT * FROM INFORMATION_SCHEMA.ML_ENGINES WHERE name = '{engine_name.lower()}'")
            assert res.data.to_df()["HANDLER"][0] == "dummy_ml"

            with pytest.raises(Exception):
                self.execute(f"DROP ML_ENGINE `{another_name}`")

            self.execute(f"DROP ML_ENGINE {another_name}")

    def test_model_name_lowercase(self):
        self.execute("CREATE ML_ENGINE myengine FROM dummy_ml")
        df = pd.DataFrame(
            [
                {"a": 1, "b": "one"},
                {"a": 2, "b": "two"},
            ]
        )
        self.set_data("tasks", df)

        with pytest.raises(Exception):
            self.execute("CREATE MODEL `MyModel` PREDICT a USING engine='myengine', join_learn_process=true")

        for model_name in ["mymodel", "MyModel", "MYMODEL"]:
            another_name = "myMODEL"
            self.execute(f"CREATE MODEL {model_name} PREDICT a USING engine='myengine', join_learn_process=true")

            res = self.execute(f"SELECT * FROM INFORMATION_SCHEMA.MODELS WHERE name = '{model_name.lower()}'")
            assert res.data.to_df()["ENGINE"][0] == "dummy_ml"

            with pytest.raises(Exception):
                self.execute(f"RETRAIN MODEL `{another_name}` using join_learn_process=true")

            self.execute(f"RETRAIN MODEL {another_name} using join_learn_process=true")

            with pytest.raises(Exception):
                self.execute(f"""
                    FINETUNE MODEL `{another_name}` FROM dummy_data (select * from tasks) using join_learn_process=true
                """)

            self.execute(f"""
                FINETUNE MODEL {another_name} FROM dummy_data (select * from tasks) using join_learn_process=true
            """)

            with pytest.raises(Exception):
                self.execute(f"DROP MODEL `{another_name}`")
            self.execute(f"DROP MODEL {another_name}")

    def test_agent_name_lowercase(self):
        agent_params = """
            using
                model='gpt-3.5-turbo',
                provider='openai',
                prompt_template='Answer the user input in a helpful way using tools',
                max_iterations=5,
                mode='retrieval',
        """

        skill_params = """
            using
                type = 'retrieval',
                source = 'kb_review',
                description = 'user reviews'
        """

        with pytest.raises(Exception):
            self.run_sql(f"create skill `MySKILL` {skill_params}")

        self.run_sql(f"create skill MySKILL {skill_params}")

        with pytest.raises(Exception):
            self.run_sql(f"""
                create agent `MyAGENT` {agent_params}
                skills=['myskill']
            """)
        self.run_sql("drop skill MySKILL")

        for agent_name, skill_name in [("myagent", "myskill"), ("MyAgent", "MySkill"), ("MYAGENT", "MYSKILL")]:
            another_skill_name = "mySKILL"
            another_agent_name = "myAGENT"

            self.run_sql(f"create skill {skill_name} {skill_params}")

            self.run_sql(f"""
                create agent {agent_name} {agent_params}
                skills=['{skill_name.lower()}']
            """)

            ret = self.run_sql(f"select * from information_schema.agents where name = '{agent_name.lower()}'")
            assert len(ret) == 1

            ret = self.run_sql(f"select * from information_schema.skills where name = '{skill_name.lower()}'")
            assert len(ret) == 1

            with pytest.raises(Exception):
                self.run_sql(f"drop agent `{another_agent_name}`")
            self.run_sql(f"drop agent {another_agent_name}")

            with pytest.raises(Exception):
                self.run_sql(f"drop skill `{another_skill_name}`")
            self.run_sql(f"drop skill {another_skill_name}")

    @patch("litellm.embedding")
    @patch("openai.OpenAI")
    def test_knowledgebase_name_lowercase(self, mock_openai, mock_litellm_embedding):
        set_litellm_embedding(mock_litellm_embedding)

        kb_params = """
            using embedding_model = {
                "provider": "openai",
                "model_name": "dummy_model",
                "api_key": "dummy_key"
            }
        """

        with pytest.raises(Exception):
            self.run_sql(f"CREATE KNOWLEDGE BASE `MyKB` {kb_params}")

        for kb_name in ["mykb", "MyKB", "MYKB"]:
            another_kb_name = "myKB"

            self.execute(f"CREATE KNOWLEDGE BASE {kb_name} {kb_params}")

            res = self.execute(f"""
                SELECT * FROM INFORMATION_SCHEMA.KNOWLEDGE_BASES WHERE name = '{kb_name.lower()}'
            """)
            assert res.data.to_df()["NAME"][0] == "mykb"

            with pytest.raises(Exception):
                self.execute(f"DROP KNOWLEDGE BASE `{another_kb_name}`")
            self.execute(f"DROP KNOWLEDGE BASE {another_kb_name}")

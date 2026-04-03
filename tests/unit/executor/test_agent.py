import time
import os
import json
from textwrap import dedent

from unittest.mock import patch, MagicMock, AsyncMock

import pandas as pd
import pytest
import sys

from mindsdb.interfaces.agents.langchain_agent import SkillData
from tests.unit.executor_test_base import BaseExecutorDummyML
from tests.unit.executor.test_knowledge_base import set_litellm_embedding


def set_openai_completion(mock_openai, response):
    if not isinstance(response, list):
        response = [response]

    mock_openai.agent_calls = []
    calls = []
    responses = []

    def resp_f(messages, *args, **kwargs):
        # return all responses in sequence, then yield only latest from list
        if len(response) == 1:
            resp = response[0]
        else:
            resp = response.pop(0)

        # log langchain agent calls, exclude previous part of message
        agent_call = messages[0]["content"]
        if len(calls) > 0:
            # remove previous call
            prev_call = calls[-1]
            if agent_call.startswith(prev_call):
                agent_call = agent_call[len(prev_call) :]
            # remove previous agent response
            prev_response = responses[-1]
            pos = agent_call.find(prev_response)
            if pos != -1:
                agent_call = agent_call[pos + len(prev_response) :]

        mock_openai.agent_calls.append(agent_call)
        calls.append(messages[0]["content"])
        responses.append(resp)

        return {"choices": [{"message": {"role": "assistant", "content": resp}}]}

    mock_openai().chat.completions.create.side_effect = resp_f


def get_dataset_planets():
    data = [
        ["1000", "Moon"],
        ["1001", "Jupiter"],
        ["1002", "Venus"],
    ]
    return pd.DataFrame(data, columns=["id", "planet_name"])


class TestAgent(BaseExecutorDummyML):
    @pytest.mark.slow
    def test_mindsdb_provider(self):
        from mindsdb.api.executor.exceptions import ExecutorException

        agent_response = "how can I help you"
        # model
        self.run_sql(
            f"""
                CREATE model base_model
                PREDICT output
                using
                  column='question',
                  output='{agent_response}',
                  engine='dummy_ml',
                  join_learn_process=true
            """
        )

        self.run_sql("CREATE ML_ENGINE langchain FROM langchain")

        agent_params = """
            USING
                provider='mindsdb',
                model = "base_model", -- <
                prompt_template="Answer the user input in a helpful way"
        """
        self.run_sql(f"""
            CREATE AGENT my_agent {agent_params}
        """)
        with pytest.raises(ExecutorException):
            self.run_sql(f"""
                CREATE AGENT my_agent {agent_params}
            """)
        self.run_sql(f"""
            CREATE AGENT IF NOT EXISTS my_agent {agent_params}
        """)

        ret = self.run_sql("select * from my_agent where question = 'hi'")

        assert agent_response in ret.answer[0]

    @pytest.mark.skipif(
        sys.platform in ["darwin", "win32"], reason="Mocking doesn't work on Windows or macOS for some reason"
    )
    @patch("openai.OpenAI")
    def test_openai_provider_with_model(self, mock_openai):
        agent_response = "how can I assist you today?"
        set_openai_completion(mock_openai, agent_response)

        self.run_sql("CREATE ML_ENGINE langchain FROM langchain")

        self.run_sql("""
            CREATE MODEL lang_model
                PREDICT answer USING
            engine = "langchain",
            model = "gpt-3.5-turbo",
            openai_api_key='--',
            prompt_template="Answer the user input in a helpful way";
         """)

        time.sleep(5)

        self.run_sql("""
            CREATE AGENT my_agent
            USING
              model='lang_model'
         """)
        ret = self.run_sql("select * from my_agent where question = 'hi'")

        assert agent_response in ret.answer[0]

    @patch("openai.OpenAI")
    def test_openai_provider(self, mock_openai):
        agent_response = "how can I assist you today?"
        set_openai_completion(mock_openai, agent_response)

        self.run_sql("""
            CREATE AGENT my_agent
            USING
             provider='openai',
             model = "gpt-3.5-turbo",
             openai_api_key='-key-',
             prompt_template="Answer the user input in a helpful way"
         """)
        ret = self.run_sql("select * from my_agent where question = 'hi'")

        # check model params
        assert mock_openai.call_args_list[-1][1]["api_key"] == "-key-"
        assert mock_openai().chat.completions.create.call_args_list[-1][1]["model"] == "gpt-3.5-turbo"

        assert agent_response in ret.answer[0]

        # test join
        df = pd.DataFrame(
            [
                {"q": "hi"},
            ]
        )
        self.save_file("questions", df)

        ret = self.run_sql("""
            select * from files.questions t
            join my_agent a on a.question=t.q
        """)

        assert agent_response in ret.answer[0]

        # empty query
        ret = self.run_sql("""
            select * from files.questions t
            join my_agent a on a.question=t.q
            where t.q = ''
        """)
        assert len(ret) == 0

    @patch("openai.OpenAI")
    def test_openai_params_as_dict(self, mock_openai):
        agent_response = "how can I assist you today?"
        set_openai_completion(mock_openai, agent_response)

        self.run_sql("""
            CREATE AGENT my_agent
            USING
             model = {
                "provider": 'openai',
                "model_name": "gpt-42",
                "api_key": '-secret-'
             },
             prompt_template="Answer the user input in a helpful way"
         """)
        ret = self.run_sql("select * from my_agent where question = 'hi'")

        # check model params
        assert mock_openai.call_args_list[-1][1]["api_key"] == "-secret-"
        assert mock_openai().chat.completions.create.call_args_list[-1][1]["model"] == "gpt-42"

        assert agent_response in ret.answer[0]

    @patch("mindsdb.utilities.config.Config.get")
    @patch("openai.OpenAI")
    def test_agent_with_default_llm_params(self, mock_openai, mock_config_get):
        # Mock the config.get method to return default LLM parameters
        def config_get_side_effect(key, default=None):
            if key == "default_llm":
                return {
                    "provider": "openai",
                    "model_name": "gpt-4o",
                    "api_key": "sk-abc123",
                    "base_url": "https://api.openai.com/v1",
                    "api_version": "2024-02-01",
                    "method": "multi-class",
                }
            elif key == "default_project":
                return "mindsdb"
            return default

        mock_config_get.side_effect = config_get_side_effect

        agent_response = "how can I assist you today?"
        set_openai_completion(mock_openai, agent_response)

        # Create an agent with only provider specified - should use default LLM params
        self.run_sql("""
            CREATE AGENT default_params_agent
            USING
             provider='openai',
             model='gpt-4o',
             prompt_template="Answer the user input in a helpful way"
         """)

        # Check that the agent was created with the default parameters
        agent_info = self.run_sql("SELECT * FROM information_schema.agents WHERE name = 'default_params_agent'")

        # Verify model_name is set correctly
        assert agent_info["MODEL_NAME"].iloc[0] == "gpt-4o"

        # Verify the agent has the user-specified parameters but not default parameters
        agent_params = json.loads(agent_info["PARAMS"].iloc[0])
        assert agent_params.get("prompt_template") == "Answer the user input in a helpful way"

        # Default parameters should NOT be stored in the database
        # They will be applied at runtime via get_agent_llm_params
        assert "base_url" not in agent_params
        assert "api_version" not in agent_params
        assert "method" not in agent_params

        # Mock the OpenAI client for the agent execution
        with (
            patch("openai.AsyncOpenAI") as mock_async_openai,
            patch("langchain_openai.chat_models.base.ChatOpenAI.validate_environment", return_value=None),
            patch("mindsdb.interfaces.agents.langchain_agent.LangchainAgent._initialize_args") as mock_initialize_args,
        ):
            # Set up the mock for async client
            mock_async_client = MagicMock()
            mock_async_openai.return_value = mock_async_client

            # Configure the mock completion
            mock_completion = MagicMock()
            mock_completion.choices = [MagicMock()]
            mock_completion.choices[0].message.content = agent_response
            mock_async_client.chat.completions.create = AsyncMock(return_value=mock_completion)

            # Mock _initialize_args to capture the merged parameters
            mock_initialize_args.return_value = {
                "model_name": "gpt-4o",
                "provider": "openai",
                "api_key": "sk-abc123",
                "base_url": "https://api.openai.com/v1",
                "api_version": "2024-02-01",
                "method": "multi-class",
                "prompt_template": "You are an assistant, answer using the tables connected",
            }

            # Test that the agent works
            ret = self.run_sql("select * from default_params_agent where question = 'hi'")
            assert agent_response in ret.answer[0]

            # Verify that _initialize_args was called, which means our runtime parameter merging is used
            mock_initialize_args.assert_called()

        # Now create an agent with explicit parameters that should override defaults
        self.run_sql("""
            CREATE AGENT explicit_params_agent
            USING
             provider='openai',
             model = "gpt-3.5-turbo",
             base_url='https://custom-url.com/',
             prompt_template="Answer the user input in a helpful way"
         """)

        # Check that the agent was created with the explicit parameters
        agent_info = self.run_sql("SELECT * FROM information_schema.agents WHERE name = 'explicit_params_agent'")

        # Verify the agent has the explicit parameters (overriding defaults)
        agent_params = json.loads(agent_info["PARAMS"].iloc[0])
        assert agent_params.get("base_url") == "https://custom-url.com/"  # Explicit value should be stored
        assert agent_params.get("prompt_template") == "Answer the user input in a helpful way"  # User-specified value

        # Default parameters should NOT be stored in the database
        assert "api_version" not in agent_params
        assert "method" not in agent_params

        # Mock the OpenAI client for the second agent execution
        with (
            patch("openai.AsyncOpenAI") as mock_async_openai,
            patch("langchain_openai.chat_models.base.ChatOpenAI.validate_environment", return_value=None),
            patch("mindsdb.interfaces.agents.langchain_agent.LangchainAgent._initialize_args") as mock_initialize_args,
        ):
            # Set up the mock for async client
            mock_async_client = MagicMock()
            mock_async_openai.return_value = mock_async_client

            # Configure the mock completion
            mock_completion = MagicMock()
            mock_completion.choices = [MagicMock()]
            mock_completion.choices[0].message.content = agent_response
            mock_async_client.chat.completions.create = AsyncMock(return_value=mock_completion)

            # Mock _initialize_args to capture the merged parameters
            mock_initialize_args.return_value = {
                "model_name": "gpt-3.5-turbo",
                "provider": "openai",
                "api_key": "sk-abc123",
                "base_url": "https://custom-url.com/",
                "api_version": "2024-02-01",
                "method": "multi-class",
                "prompt_template": "You are an assistant, answer using the tables connected",
            }

            # Test that the agent works with explicit parameters
            ret = self.run_sql("select * from explicit_params_agent where question = 'hi'")
            assert agent_response in ret.answer[0]

            # Verify that _initialize_args was called, which means our runtime parameter merging is used
            mock_initialize_args.assert_called()

    @patch("mindsdb.utilities.config.Config.get")
    @patch("openai.OpenAI")
    def test_agent_minimal_syntax_with_default_llm(self, mock_openai, mock_config_get):
        """Test that agent creation works with minimal syntax using default_llm config"""

        # Mock the config.get method to return default LLM parameters
        def config_get_side_effect(key, default=None):
            if key == "default_llm":
                return {
                    "provider": "openai",
                    "model_name": "gpt-4o",
                    "api_key": "sk-abc123",
                    "base_url": "https://api.openai.com/v1",
                    "api_version": "2024-02-01",
                    "method": "multi-class",
                }
            elif key == "default_project":
                return "mindsdb"
            elif key == "cache":
                return {"type": "local"}
            return default

        mock_config_get.side_effect = config_get_side_effect

        agent_response = "response from minimal syntax agent"
        set_openai_completion(mock_openai, agent_response)

        # Create an agent with minimal syntax - should use all default LLM params
        self.run_sql("""
            CREATE AGENT minimal_syntax_agent
            USING
              data = {
                "tables": ['test.table1', 'test.table2']
              }
         """)

        # Check that the agent was created with the default parameters
        agent_info = self.run_sql("SELECT * FROM information_schema.agents WHERE name = 'minimal_syntax_agent'")

        # Verify model_name is None (as expected when using default LLM)
        assert agent_info["MODEL_NAME"].iloc[0] is None

        # Verify the agent has the default parameters and include_tables
        agent_params = json.loads(agent_info["PARAMS"].iloc[0])
        assert "data" in agent_params
        assert agent_params["data"]["tables"] == ["test.table1", "test.table2"]

        # Mock the OpenAI client for the agent execution
        with (
            patch("openai.AsyncOpenAI") as mock_async_openai,
            patch("langchain_openai.chat_models.base.ChatOpenAI.validate_environment", return_value=None),
            patch("mindsdb.interfaces.agents.langchain_agent.LangchainAgent._initialize_args") as mock_initialize_args,
        ):
            # Set up the mock for async client
            mock_async_client = MagicMock()
            mock_async_openai.return_value = mock_async_client

            # Configure the mock completion
            mock_completion = MagicMock()
            mock_completion.choices = [MagicMock()]
            mock_completion.choices[0].message.content = agent_response
            mock_async_client.chat.completions.create = AsyncMock(return_value=mock_completion)

            # Mock _initialize_args to capture the merged parameters
            mock_initialize_args.return_value = {
                "model_name": "gpt-4o",
                "provider": "openai",
                "api_key": "sk-abc123",
                "base_url": "https://api.openai.com/v1",
                "api_version": "2024-02-01",
                "method": "multi-class",
                "include_tables": ["test.table1", "test.table2"],
                "prompt_template": "You are an assistant, answer using the tables connected",
            }

            # Test that the agent works
            ret = self.run_sql("select * from minimal_syntax_agent where question = 'hi'")
            assert agent_response in ret.answer[0]

            # Verify that _initialize_args was called, which means our runtime parameter merging is used
            mock_initialize_args.assert_called()

    @patch("openai.OpenAI")
    def test_agent_with_tables(self, mock_openai):
        sd = SkillData(name="test", type="", project_id=1, params={"tables": []}, agent_tables_list=[])

        sd.params = {"tables": ["x", "y"]}
        sd.agent_tables_list = ["x", "y"]
        assert sd.restriction_on_tables == {None: {"x", "y"}}

        sd.params = {"tables": ["x", "y"]}
        sd.agent_tables_list = ["x", "y", "z"]
        assert sd.restriction_on_tables == {None: {"x", "y"}}

        sd.params = {"tables": ["x", "y"]}
        sd.agent_tables_list = ["x"]
        assert sd.restriction_on_tables == {None: {"x"}}

        sd.params = {"tables": ["x", "y"]}
        sd.agent_tables_list = ["z"]
        with pytest.raises(ValueError):
            print(sd.restriction_on_tables)

        sd.params = {"tables": ["x", {"schema": "S", "table": "y"}]}
        sd.agent_tables_list = ["x"]
        assert sd.restriction_on_tables == {None: {"x"}}

        sd.params = {"tables": ["x", {"schema": "S", "table": "y"}]}
        sd.agent_tables_list = ["x", {"schema": "S", "table": "y"}]
        assert sd.restriction_on_tables == {None: {"x"}, "S": {"y"}}

        sd.params = {
            "tables": [{"schema": "S", "table": "x"}, {"schema": "S", "table": "y"}, {"schema": "S", "table": "z"}]
        }
        sd.agent_tables_list = [
            {"schema": "S", "table": "y"},
            {"schema": "S", "table": "z"},
            {"schema": "S", "table": "f"},
        ]
        assert sd.restriction_on_tables == {"S": {"y", "z"}}

        sd.params = {"tables": [{"schema": "S", "table": "x"}, {"schema": "S", "table": "y"}]}
        sd.agent_tables_list = [{"schema": "S", "table": "z"}]
        with pytest.raises(ValueError):
            print(sd.restriction_on_tables)

        self.run_sql("""
            create skill test_skill
            using
            type = 'text2sql',
            database = 'example_db',
            tables = ['table_1', 'table_2'],
            description = "this is sales data";
        """)

        self.run_sql("""
            create agent test_agent
            using
            model='gpt-3.5-turbo',
            provider='openai',
            openai_api_key='--',
            prompt_template='Answer the user input in a helpful way using tools',
            skills=[{
                'name': 'test_skill',
                'tables': ['table_2', 'table_3']
            }];
        """)

        resp = self.run_sql("""select * from information_schema.agents where name = 'test_agent';""")
        assert len(resp) == 1
        assert resp["SKILLS"][0] == ["test_skill"]

        agent_response = "how can I assist you today?"
        set_openai_completion(mock_openai, agent_response)
        self.run_sql("select * from test_agent where question = 'test?'")

    @pytest.mark.skipif(sys.platform == "darwin", reason="Fails on macOS")
    @patch("openai.OpenAI")
    def test_agent_stream(self, mock_openai):
        agent_response = "how can I assist you today?"
        set_openai_completion(mock_openai, agent_response)

        self.run_sql("""
            CREATE AGENT my_agent
            USING
             provider='openai',
             model = "gpt-3.5-turbo",
             openai_api_key='--',
             prompt_template="Answer the user input in a helpful way"
         """)

        agents_controller = self.command_executor.session.agents_controller
        agent = agents_controller.get_agent("my_agent")

        messages = [{"question": "hi"}]
        found = False
        for chunk in agents_controller.get_completion(agent, messages, stream=True):
            if chunk.get("output") == agent_response:
                found = True
        if not found:
            raise AttributeError("Agent response is not found")

    @patch("litellm.embedding")
    @patch("openai.OpenAI")
    def test_agent_retrieval(self, mock_openai, mock_litellm_embedding):
        set_litellm_embedding(mock_litellm_embedding)
        self.run_sql("""
            create knowledge base kb_review
            using
                embedding_model = {
                    "provider": "bedrock",
                    "model_name": "dummy_model",
                    "api_key": "dummy_key"
                }
        """)

        self.run_sql("""
          create skill retr_skill
          using
              type = 'retrieval',
              source = 'kb_review',
              description = 'user reviews'
        """)

        os.environ["OPENAI_API_KEY"] = "--"

        self.run_sql("""
          create agent retrieve_agent
           using
          model='gpt-3.5-turbo',
          provider='openai',
          prompt_template='Answer the user input in a helpful way using tools',
          skills=['retr_skill'],
          max_iterations=5,
          mode='retrieval'
        """)

        agent_response = "the answer is yes"
        user_question = "answer my question"

        set_openai_completion(
            mock_openai,
            [
                # first step, use kb
                dedent(f"""
              Thought: Do I need to use a tool? Yes
              Action: retr_skill
              Action Input: {user_question}
            """),
                # step2, answer to user
                agent_response,
            ],
        )

        with patch("mindsdb.interfaces.knowledge_base.controller.KnowledgeBaseTable.select_query") as kb_select:
            # kb response
            kb_select.return_value = pd.DataFrame([{"id": 1, "content": "ok", "metadata": {}}])
            ret = self.run_sql(f"""
                select * from retrieve_agent where question = '{user_question}'
            """)

            # check agent output
            assert agent_response in ret.answer[0]

            # check kb input
            args, _ = kb_select.call_args
            assert user_question in args[0].where.args[1].value

    # should not be possible to drop demo agent
    def test_drop_demo_agent(self):
        """should not be possible to drop demo agent"""
        from mindsdb.api.executor.exceptions import ExecutorException

        self.run_sql("""
            CREATE AGENT my_demo_agent
            USING
                provider='openai',
                model = "gpt-3.5-turbo",
                openai_api_key='--',
                prompt_template="--",
                is_demo=true;
         """)
        with pytest.raises(ExecutorException):
            self.run_sql("drop agent my_agent")

        self.run_sql("""
            create skill my_demo_skill
            using
            type = 'text2sql',
            database = 'example_db',
            description = "",
            is_demo=true;
        """)

        with pytest.raises(ExecutorException):
            self.run_sql("drop skill my_demo_skill")

    @patch("openai.OpenAI")
    def test_agent_default_prompt_template(self, mock_openai):
        """Test that agents work correctly with default prompt templates in different modes"""
        agent_response = "default prompt template response"
        set_openai_completion(mock_openai, agent_response)

        # Test non-retrieval mode with no prompt_template (should use default)
        self.run_sql("""
            CREATE AGENT default_prompt_agent
            USING
                provider='openai',
                model = "gpt-3.5-turbo",
                openai_api_key='--'
         """)
        ret = self.run_sql("select * from default_prompt_agent where question = 'test question'")
        assert agent_response in ret.answer[0]

        # Test retrieval mode with no prompt_template (should use default retrieval template)
        self.run_sql("""
            CREATE AGENT default_retrieval_agent
            USING
                provider='openai',
                model = "gpt-3.5-turbo",
                openai_api_key='--',
                mode='retrieval'
         """)
        ret = self.run_sql("select * from default_retrieval_agent where question = 'test question'")
        assert agent_response in ret.answer[0]

    @staticmethod
    def _action(name, action_input=""):
        return dedent(f"""
                    Thought: Do I need to use a tool? Yes
                    Action: {name}
                    Action Input: {action_input}
                """)

    @patch("openai.OpenAI")
    @patch("mindsdb.integrations.handlers.litellm_handler.litellm_handler.embedding")
    def test_agent_permissions(self, mock_litellm_embedding, mock_openai):
        set_litellm_embedding(mock_litellm_embedding)

        kb_sql = """
            create knowledge base %s
            using embedding_model = {"provider": "bedrock", "model_name": "titan"}
        """
        self.run_sql(kb_sql % "kb_show1")
        self.run_sql(kb_sql % "kb_show2")
        self.run_sql(kb_sql % "kb_hide")

        df = get_dataset_planets()

        self.save_file("show1", df)
        self.save_file("show2", df)
        self.save_file("hide", df)

        self.run_sql("""
            insert into kb_show1
            select id, planet_name content from files.show1
        """)

        self.run_sql("""
            CREATE AGENT my_agent
            USING
              model = "gpt-3.5-turbo",
              openai_api_key='--',
              data = {
                "knowledge_bases": ["kb_show*"],
                "tables": ["files.show*"]
              };
         """)

        # ===== Access to forbidden KBs =====

        set_openai_completion(
            mock_openai,
            [
                self._action("kb_info_tool", "kb_hide"),
                self._action("kb_query_tool", "select * from kb_hide where content='Moon'"),
                "Hi!",
            ],
        )
        self.run_sql("select * from my_agent where question = 'test'")

        # result of kb_info_tool
        assert "Knowledge base kb_hide not found" in mock_openai.agent_calls[1]
        # it shows available KBs
        assert "kb_show*" in mock_openai.agent_calls[1]

        # result of kb_query_tool
        assert "Knowledge base kb_hide not found" in mock_openai.agent_calls[2]

        # ===== Access to exposed KBs =====
        set_openai_completion(
            mock_openai,
            [
                self._action("kb_list_tool"),
                self._action("kb_info_tool", "kb_show1"),
                self._action("kb_query_tool", "select * from kb_show1 where content='Moon' limit 1"),
                "Hi!",
            ],
        )
        self.run_sql("select * from my_agent where question = 'test'")

        # result of kb_list_tool
        assert "kb_hide" not in mock_openai.agent_calls[1]
        assert "kb_show1" in mock_openai.agent_calls[1]
        assert "kb_show2" in mock_openai.agent_calls[1]

        # result of kb_info_tool, shows KB name and info
        assert "kb_show1" in mock_openai.agent_calls[2]
        assert "Sample Data" in mock_openai.agent_calls[2]
        assert "Schema Information" in mock_openai.agent_calls[2]

        # result of kb_query_tool
        assert "Moon" in mock_openai.agent_calls[3]

        # ===== access to forbidden files =====

        set_openai_completion(
            mock_openai,
            [
                self._action("sql_db_schema", "files.hide"),
                self._action("sql_db_query", "select * from files.hide"),
                "Hi!",
            ],
        )
        self.run_sql("select * from my_agent where question = 'test'")
        # result of sql_db_schema
        assert "hide not found" in mock_openai.agent_calls[1]
        # result of sql_db_query
        assert "hide not found" in mock_openai.agent_calls[2]
        # it shows available tables
        assert "show*" in mock_openai.agent_calls[2]

        # ===== access to exposed files =====

        set_openai_completion(
            mock_openai,
            [
                self._action("sql_db_list_tables"),
                self._action("sql_db_schema", "files.show1"),
                self._action("sql_db_query", "select * from files.show1 where id = '1001'"),
                "Hi!",
            ],
        )

        self.run_sql("select * from my_agent where question = 'test'")

        # result of sql_db_list_tables
        assert "hide" not in mock_openai.agent_calls[1]
        assert "show1" in mock_openai.agent_calls[1]
        assert "show1" in mock_openai.agent_calls[1]

        # result of sql_db_schema
        assert "show1" in mock_openai.agent_calls[2]  # table name
        assert "planet_name" in mock_openai.agent_calls[2]  # column
        assert "Moon" in mock_openai.agent_calls[2]  # content

        # result of sql_db_query
        assert "Jupiter" in mock_openai.agent_calls[3]

        # ===== autogenerated skill is removed when agent is dropped =====
        self.run_sql("DROP AGENT my_agent")

        self.run_sql("""
            CREATE AGENT my_agent
            USING
              model = "gpt-3.5-turbo",
              openai_api_key='--',
              data = {
                 "knowledge_bases": ["kb_show*"]
              };
         """)

        set_openai_completion(
            mock_openai,
            [
                self._action("sql_db_list_tables"),
                "Hi!",
            ],
        )
        self.run_sql("select * from my_agent where question = 'test'")
        assert "files" not in mock_openai.agent_calls[1]

    @patch("openai.OpenAI")
    @patch("mindsdb.integrations.handlers.litellm_handler.litellm_handler.embedding")
    def test_agent_new_syntax(self, mock_litellm_embedding, mock_openai):
        set_litellm_embedding(mock_litellm_embedding)

        df = get_dataset_planets()
        # create 2 files and KBs
        for i in (1, 2):
            self.run_sql(f"""
                create knowledge base kb{i}
                using embedding_model = {{"provider": "bedrock", "model_name": "titan"}}
            """)
            self.save_file(f"file{i}", df)

            self.run_sql(f"""
                insert into kb{i}
                select id, planet_name content from files.file{i} where id != 1000
            """)

        self.run_sql("""
            CREATE AGENT my_agent
            USING
              model = {
                "provider": 'openai',
                "model_name": "gpt-42",
                "api_key": '-secret-'
              },
              data = {
                 "knowledge_bases": ["kb1"],
                 "tables": ["files.file1"]
              },
              prompt_template='important user instruction №42'
         """)

        # exposed
        set_openai_completion(
            mock_openai,
            [
                self._action("kb_info_tool", "kb1"),
                self._action("sql_db_schema", "files.file1"),
                "Hi!",
            ],
        )
        self.run_sql("select * from my_agent where question = 'test'")
        assert "Jupiter" in mock_openai.agent_calls[1]
        assert "Jupiter" in mock_openai.agent_calls[2]  # column

        # not exposed
        set_openai_completion(
            mock_openai,
            [
                self._action("kb_info_tool", "kb2"),
                self._action("sql_db_schema", "files.file2"),
                "Hi!",
            ],
        )
        ret = self.run_sql("select * from my_agent where question = 'test'")
        assert "kb2 not found" in mock_openai.agent_calls[1]
        assert "file2 not found" in mock_openai.agent_calls[2]

        # check model params
        assert mock_openai.call_args_list[-1][1]["api_key"] == "-secret-"
        assert mock_openai().chat.completions.create.call_args_list[-1][1]["model"] == "gpt-42"

        # check agent response
        assert "Hi!" in ret.answer[0]

        # check prompt template
        assert "important user instruction №42" in mock_openai.agent_calls[0]

        # --- ALTER AGENT ---
        self.run_sql("""
            ALTER AGENT my_agent
            USING
              model = {
                "provider": 'openai',
                "model_name": "gpt-18",
                "api_key": '-almost secret-'
              },
              data = {
                 "knowledge_bases": ["kb2"],
                 "tables": ["files.file2"]
              },
              prompt_template='important system prompt №37'
        """)

        # check exposed
        set_openai_completion(
            mock_openai,
            [
                self._action("kb_info_tool", "kb2"),
                self._action("sql_db_schema", "files.file2"),
                "Hi!",
            ],
        )
        self.run_sql("select * from my_agent where question = 'test'")
        assert "Jupiter" in mock_openai.agent_calls[1]
        assert "Jupiter" in mock_openai.agent_calls[2]  # column

        # not exposed
        set_openai_completion(
            mock_openai,
            [
                self._action("kb_info_tool", "kb1"),
                self._action("sql_db_schema", "files.file1"),
                "Hi!",
            ],
        )
        ret = self.run_sql("select * from my_agent where question = 'test'")
        assert "kb1 not found" in mock_openai.agent_calls[1]
        assert "file1 not found" in mock_openai.agent_calls[2]

        # check model params
        assert mock_openai.call_args_list[-1][1]["api_key"] == "-almost secret-"
        assert mock_openai().chat.completions.create.call_args_list[-1][1]["model"] == "gpt-18"

        # check agent response
        assert "Hi!" in ret.answer[0]

        # check prompt template
        assert "important system prompt №37" in mock_openai.agent_calls[0]

    @patch("openai.OpenAI")
    @patch("mindsdb.integrations.handlers.litellm_handler.litellm_handler.embedding")
    def test_agent_accept_wrong_quoting(self, mock_litellm_embedding, mock_openai):
        set_litellm_embedding(mock_litellm_embedding)
        self.run_sql("""
            create knowledge base kb1
            using embedding_model = {"provider": "bedrock", "model_name": "titan"}
        """)
        df = get_dataset_planets()

        self.save_file("file1", df)

        self.run_sql("""
            CREATE AGENT my_agent
            USING
              model = "gpt-3.5-turbo",
              openai_api_key='--',
              data = {
                 "knowledge_bases": ["kb1"],
                 "tables": ["files.file1", "files.file2.*"]
              }
         """)
        self.run_sql("""
            insert into kb1
            select id, planet_name content from files.file1
        """)

        # # exposed
        set_openai_completion(
            mock_openai,
            [
                self._action("kb_query_tool", "SELECT * FROM `mindsdb.kb1` WHERE id = '1001'"),
                self._action("sql_db_query", "SELECT * FROM `files.file1` WHERE id = '1001';"),
                "Hi!",
            ],
        )
        self.run_sql("select * from my_agent where question = 'test'")

        assert "Jupiter" in mock_openai.agent_calls[1]
        assert "Jupiter" in mock_openai.agent_calls[2]

    @patch("openai.OpenAI")
    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_3_part_table(self, mock_pg, mock_openai):
        df = get_dataset_planets()
        self.set_handler(mock_pg, name="pg", tables={"planets": df}, schema="public")

        self.run_sql("""
            CREATE AGENT my_agent
            USING
              model = "gpt-3.5-turbo",
              openai_api_key='--',
              data = {
                 "tables": ["pg.public.*"] 
              }
        """)

        set_openai_completion(
            mock_openai,
            [
                # test getting table info
                self._action("sql_db_schema", "pg.public.planets"),
                # test wrong quoting
                self._action("sql_db_query", "SELECT * FROM pg.public.planets WHERE id = '1000'"),
                self._action("sql_db_query", "SELECT * FROM `pg.public`.planets WHERE id = '1000'"),
                self._action("sql_db_query", "SELECT * FROM `pg.public`.`planets` WHERE id = '1000'"),
                "Hi!",
            ],
        )
        self.run_sql("select * from my_agent where question = 'test'")

        # result of sql_db_schema
        assert "planets" in mock_openai.agent_calls[1]  # table name
        assert "Jupiter" in mock_openai.agent_calls[1]  # column
        # results of sql_db_query
        assert "Moon" in mock_openai.agent_calls[2]
        assert "Moon" in mock_openai.agent_calls[3]
        assert "Moon" in mock_openai.agent_calls[4]

    @patch("openai.OpenAI")
    @patch(
        "mindsdb.interfaces.agents.langchain_agent.LangchainAgent.run_agent",
        return_value=pd.DataFrame([["ok", None, None]], columns=["answer", "context", "trace_id"]),
    )
    def test_agent_query_param_override(self, mock_run_agent, mock_openai):
        """
        Test that agent parameters can be overridden per-query using the USING clause in SELECT.
        """
        agent_response = "override test response"
        set_openai_completion(mock_openai, agent_response)

        self.run_sql(
            """
            CREATE AGENT override_agent
            USING
                model = 'gpt-4o',
                openai_api_key = 'sk-override',
                prompt_template = 'Answer questions',
                timeout = 60;
            """
        )

        self.run_sql(
            """
            SELECT * FROM override_agent
            WHERE question = 'How are you?'
            USING timeout=5;
            """
        )
        assert mock_run_agent.call_args_list[0][0][2].get("timeout") == 5

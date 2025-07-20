import time
import os
import json
from textwrap import dedent

from unittest.mock import patch, MagicMock, AsyncMock
import threading
from contextlib import contextmanager

import pandas as pd
import pytest
import sys

from tests.unit.executor_test_base import BaseExecutorDummyML
from mindsdb.interfaces.agents.langchain_agent import SkillData


@contextmanager
def task_monitor():
    from mindsdb.interfaces.tasks.task_monitor import TaskMonitor

    monitor = TaskMonitor()

    stop_event = threading.Event()
    worker = threading.Thread(target=monitor.start, daemon=True, args=(stop_event,))
    worker.start()

    yield worker

    stop_event.set()
    worker.join()


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


def dummy_embeddings(string):
    # Imitates embedding generation: create vectors which are similar for similar words in inputs

    embeds = [0] * 25**2
    base = 25

    string = string.lower().replace(",", " ").replace(".", " ")
    for word in string.split():
        # encode letters to numbers
        values = []
        for letter in word:
            val = ord(letter) - 97
            val = min(max(val, 0), 122)
            values.append(val)

        # first two values are position in vector
        pos = values[0] * base + values[1]

        # the next 4: are value of the vector
        values = values[2:6]
        emb = sum([val / base ** (i + 1) for i, val in enumerate(values)])

        embeds[pos] += emb

    return embeds


def set_litellm_embedding(mock_litellm_embedding):
    def resp_f(input, *args, **kwargs):
        mock_response = MagicMock()
        mock_response.data = [{"embedding": dummy_embeddings(s)} for s in input]
        return mock_response

    mock_litellm_embedding.side_effect = resp_f


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
              include_tables = ['test.table1', 'test.table2'];
         """)

        # Check that the agent was created with the default parameters
        agent_info = self.run_sql("SELECT * FROM information_schema.agents WHERE name = 'minimal_syntax_agent'")

        # Verify model_name is None (as expected when using default LLM)
        assert agent_info["MODEL_NAME"].iloc[0] is None

        # Verify the agent has the default parameters and include_tables
        agent_params = json.loads(agent_info["PARAMS"].iloc[0])
        assert "include_tables" in agent_params
        assert agent_params["include_tables"] == ["test.table1", "test.table2"]

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
              include_knowledge_bases = ['kb_show*'],
              include_tables = ['files.show*'];
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

    @patch("openai.OpenAI")
    @patch("mindsdb.integrations.handlers.litellm_handler.litellm_handler.embedding")
    def test_agent_data(self, mock_litellm_embedding, mock_openai):
        set_litellm_embedding(mock_litellm_embedding)
        self.run_sql("""
            create knowledge base kb1
            using embedding_model = {"provider": "bedrock", "model_name": "titan"}
        """)
        df = get_dataset_planets()

        self.save_file("file1", df)
        self.save_file("file2", df)

        self.run_sql("""
            CREATE AGENT my_agent
            USING
              model = "gpt-3.5-turbo",
              openai_api_key='--',
              data = {
                 "knowledge_bases": ["kb1"],
                 "tables": ["files.file1", "files.file2"]
              }
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

        assert "Schema Information" in mock_openai.agent_calls[1]
        assert "planet_name" in mock_openai.agent_calls[2]  # column

        # not exposed
        set_openai_completion(
            mock_openai,
            [
                self._action("kb_info_tool", "kb3"),
                self._action("sql_db_schema", "files.file3"),
                "Hi!",
            ],
        )
        self.run_sql("select * from my_agent where question = 'test'")

        assert "kb3 not found" in mock_openai.agent_calls[1]
        assert "file3 not found" in mock_openai.agent_calls[2]

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
                 "tables": ["files.file1"]
              }
         """)
        self.run_sql("""
            insert into kb1
            select id, planet_name content from files.file1
        """)

        # exposed
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


class TestKB(BaseExecutorDummyML):
    def _create_kb(
        self,
        name,
        embedding_model=None,
        reranking_model=None,
        content_columns=None,
        id_column=None,
        metadata_columns=None,
    ):
        self.run_sql(f"drop knowledge base if exists {name}")

        if embedding_model is None:
            embedding_model = {
                "provider": "bedrock",
                "model_name": "dummy_model",
                "api_key": "dummy_key",
            }

        kb_params = {
            "embedding_model": embedding_model,
        }
        if reranking_model is not None:
            kb_params["reranking_model"] = reranking_model
        if content_columns is not None:
            kb_params["content_columns"] = content_columns
        if id_column is not None:
            kb_params["id_column"] = id_column
        if metadata_columns is not None:
            kb_params["metadata_columns"] = metadata_columns

        param_str = ""
        if kb_params:
            param_items = []
            for k, v in kb_params.items():
                param_items.append(f"{k}={json.dumps(v)}")
            param_str = ",".join(param_items)

        self.run_sql(f"""
            create knowledge base {name}
            using
                {param_str}
        """)

    @patch("mindsdb.integrations.handlers.litellm_handler.litellm_handler.embedding")
    def test_kb(self, mock_litellm_embedding):
        self._create_kb("kb_review")

        set_litellm_embedding(mock_litellm_embedding)
        self.run_sql("insert into kb_review (content) values ('review')")

        # selectable
        ret = self.run_sql("select * from kb_review")
        assert len(ret) == 1

        # show tables in default chromadb
        ret = self.run_sql("show knowledge bases")

        db_name = ret.STORAGE[0].split(".")[0]
        ret = self.run_sql(f"show tables from {db_name}")
        # only one default collection there
        assert len(ret) == 1

    @patch("mindsdb.integrations.handlers.litellm_handler.litellm_handler.embedding")
    def test_kb_metadata(self, mock_litellm_embedding):
        record = {
            "review": "all is good, haven't used yet",
            "url": "https://laptops.com/123",
            "product": "probook",
            "specs": "Core i5; 8Gb; 1920х1080",
            "id": 123,
        }
        df = pd.DataFrame([record])
        self.save_file("reviews", df)

        # ---  case 1: kb with default columns settings ---
        self._create_kb("kb_review")

        set_litellm_embedding(mock_litellm_embedding)

        self.run_sql("""
            insert into kb_review
            select review as content, id from files.reviews
        """)

        ret = self.run_sql("select * from kb_review where _original_doc_id = 123")
        assert len(ret) == 1
        assert ret["chunk_content"][0] == record["review"]

        # delete by metadata
        self.run_sql("delete from kb_review where _original_doc_id = 123")
        ret = self.run_sql("select * from kb_review where _original_doc_id = 123")
        assert len(ret) == 0

        # insert without id
        self.run_sql("""
            insert into kb_review
            select review as content, product, url from files.reviews
        """)

        # id column wasn't used
        ret = self.run_sql("select * from kb_review where _original_doc_id = 123")
        assert len(ret) == 0

        # product/url in metadata
        ret = self.run_sql(
            "select metadata->>'product' as product, metadata->>'url' as url from kb_review where product = 'probook'"
        )
        assert len(ret) == 1
        assert ret["product"][0] == record["product"]
        assert ret["url"][0] == record["url"]

        # ---  case 2: kb with defined columns ---
        self._create_kb(
            "kb_review", content_columns=["review", "product"], id_column="url", metadata_columns=["specs", "id"]
        )

        set_litellm_embedding(mock_litellm_embedding)
        self.run_sql("""
            insert into kb_review
            select * from files.reviews
        """)

        ret = self.run_sql(
            "select chunk_content, metadata->>'specs' as specs, metadata->>'id' as id from kb_review"
        )  # url in id

        assert len(ret) == 2  # two columns are split in two records

        # review/product in content
        content = list(ret["chunk_content"])
        assert record["review"] in content
        assert record["product"] in content

        # specs/id in metadata
        assert ret["specs"][0] == record["specs"]
        assert str(ret["id"][0]) == str(record["id"])

        # ---  case 3: content is defined, id is id, the rest goes to metadata ---
        self._create_kb("kb_review", content_columns=["review"])

        set_litellm_embedding(mock_litellm_embedding)
        self.run_sql("""
            insert into kb_review
            select * from files.reviews
        """)

        ret = self.run_sql("""
                select chunk_content,
                 metadata->>'specs' as specs, metadata->>'product' as product, metadata->>'url' as url
                from kb_review 
                where _original_doc_id = 123 -- id is id
        """)
        assert len(ret) == 1
        # review in content
        assert ret["chunk_content"][0] == record["review"]

        # specs/url/product in metadata
        assert ret["specs"][0] == record["specs"]
        assert ret["url"][0] == record["url"]
        assert ret["product"][0] == record["product"]

    def _get_ral_table(self):
        data = [
            ["1000", "Green beige", "Beige verdastro"],
            ["1004", "Golden yellow", "Giallo oro"],
            ["9016", "Traffic white", "Bianco traffico"],
            ["9023", "Pearl dark grey", "Grigio scuro perlato"],
        ]

        return pd.DataFrame(data, columns=["ral", "english", "italian"])

    @patch("mindsdb.integrations.handlers.litellm_handler.litellm_handler.embedding")
    def test_join_kb_table(self, mock_litellm_embedding):
        df = self._get_ral_table()
        self.save_file("ral", df)

        self._create_kb("kb_ral")

        set_litellm_embedding(mock_litellm_embedding)
        self.run_sql("""
            insert into kb_ral
            select ral id, english content from files.ral
        """)

        ret = self.run_sql("""
            select t.italian, k.id, t.ral from kb_ral k
            join files.ral t on t.ral = k.id
            where k.content = 'white'
            limit 2
        """)

        assert len(ret) == 2
        # values are matched
        diff = ret[ret["ral"] != ret["id"]]
        assert len(diff) == 0

        # =================   operators  =================
        ret = self.run_sql("""
            select * from kb_ral
            where id = '1000'
        """)
        assert len(ret) == 1
        assert ret["id"][0] == "1000"

        ret = self.run_sql("""
            select * from kb_ral
            where id != '1000'
        """)
        assert len(ret) == 3
        assert "1000" not in ret["id"]

        ret = self.run_sql("""
            select * from kb_ral
            where id in ('1000', '1004')
        """)
        assert len(ret) == 2
        assert set(ret["id"]) == {"1000", "1004"}

        ret = self.run_sql("""
            select * from kb_ral
            where id not in ('1000', '1004')
        """)
        assert len(ret) == 2
        assert set(ret["id"]) == {"9016", "9023"}

    @pytest.mark.slow
    @pytest.mark.skipif(sys.platform == "win32", reason="Causes hard crash on windows.")
    @patch("mindsdb.integrations.handlers.litellm_handler.litellm_handler.embedding")
    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_kb_partitions(self, mock_handler, mock_litellm_embedding):
        df = self._get_ral_table()
        self.save_file("ral", df)

        df = pd.concat([df] * 30)
        # unique ids
        df["id"] = list(map(str, range(len(df))))

        self.set_handler(mock_handler, name="pg", tables={"ral": df})

        def check_partition(insert_sql):
            self._create_kb("kb_part", content_columns=["english"])

            # load kb
            set_litellm_embedding(mock_litellm_embedding)
            ret = self.run_sql(insert_sql)
            # inserts returns query
            query_id = ret["ID"][0]

            # wait loaded
            for i in range(1000):
                time.sleep(0.2)
                ret = self.run_sql(f"select * from information_schema.queries where id = {query_id}")
                if ret["ERROR"][0] is not None:
                    raise RuntimeError(ret["ERROR"][0])
                if ret["FINISHED_AT"][0] is not None:
                    break

            # check content
            ret = self.run_sql("select * from kb_part")
            assert len(ret) == len(df)

            # check queries table
            ret = self.run_sql(f"select * from information_schema.queries where id = {query_id}")
            assert len(ret) == 1
            rec = ret.iloc[0]
            assert "kb_part" in ret["SQL"][0]
            assert ret["ERROR"][0] is None
            assert ret["FINISHED_AT"][0] is not None

            # test describe
            ret = self.run_sql("describe knowledge base kb_part")
            assert len(ret) == 1
            rec_d = ret.iloc[0]
            assert rec_d["PROCESSED_ROWS"] == rec["PROCESSED_ROWS"]
            assert rec_d["INSERT_STARTED_AT"] == rec["STARTED_AT"]
            assert rec_d["INSERT_FINISHED_AT"] == rec["FINISHED_AT"]
            assert rec_d["QUERY_ID"] == query_id

            # del query
            self.run_sql(f"SELECT query_cancel({rec['ID']})")
            ret = self.run_sql("select * from information_schema.queries")
            assert len(ret) == 0

            ret = self.run_sql("describe knowledge base kb_part")
            assert len(ret) == 1
            rec_d = ret.iloc[0]
            assert rec_d["PROCESSED_ROWS"] is None
            assert rec_d["INSERT_STARTED_AT"] is None
            assert rec_d["INSERT_FINISHED_AT"] is None
            assert rec_d["QUERY_ID"] is None

        with task_monitor():

            def stream_f(*args, **kwargs):
                chunk_size = int(len(df) / 10) + 1
                for i in range(10):
                    yield df[chunk_size * i : chunk_size * (i + 1) :]

            # --- stream mode ---
            mock_handler().query_stream.side_effect = stream_f

            # test iterate
            check_partition("""
                insert into kb_part SELECT id, english FROM  pg.ral
                using batch_size=20, track_column=id
            """)

            # test threads
            check_partition("""
                insert into kb_part SELECT id, english FROM pg.ral
                using batch_size=20, track_column=id, threads = 3
            """)

            # without track column
            check_partition("""
                insert into kb_part SELECT id, english FROM  pg.ral
                using batch_size=20
            """)

            # --- general mode ---
            mock_handler().query_stream = None

            # test iterate
            check_partition("""
                insert into kb_part SELECT id, english FROM  pg.ral
                using batch_size=20, track_column=id
            """)

            # test threads
            check_partition("""
                insert into kb_part SELECT id, english FROM pg.ral
                using batch_size=20, track_column=id, threads = 3
            """)

    @patch("mindsdb.integrations.handlers.litellm_handler.litellm_handler.embedding")
    def test_kb_algebra(self, mock_litellm_embedding):
        set_litellm_embedding(mock_litellm_embedding)

        lines, i = [], 0
        for color in ("white", "red", "green"):
            for size in ("big", "middle", "small"):
                for shape in ("square", "triangle", "circle"):
                    i += 1
                    lines.append([i, i, f"{color} {size} {shape}", color, size, shape])
        df = pd.DataFrame(lines, columns=["id", "num", "content", "color", "size", "shape"])

        self.save_file("items", df)

        self.run_sql("""
            create knowledge base kb_alg
            using
                embedding_model = {
                    "provider": "bedrock",
                    "model_name": "titan"
                }
        """)

        self.run_sql("""
        insert into kb_alg
            select * from files.items
        """)

        # --- search value excluding others

        ret = self.run_sql("""
           select * from kb_alg where
            content = 'green'
            and content not IN ('square', 'triangle')
            and content is not null
           limit 3
        """)

        # check 3 most relative records
        for content in ret["chunk_content"]:
            assert "green" in content
            assert "square" not in content
            assert "triangle" not in content

        # --- search value excluding other and metadata

        ret = self.run_sql("""
           select * from kb_alg where
            content = 'green'
            and content != 'square'
            and shape != 'triangle'
           limit 3
        """)

        for content in ret["chunk_content"]:
            assert "green" in content
            assert "square" not in content
            assert "triangle" not in content

        # -- searching value in list with excluding

        ret = self.run_sql("""
           select * from kb_alg where
            content in ('green', 'white')
            and content not like 'green'
           limit 3
        """)
        for content in ret["chunk_content"]:
            assert "white" in content

        # -- using OR

        ret = self.run_sql("""
           select * from kb_alg where
               (content like 'green' and size='big') 
            or (content like 'white' and size='small') 
            or (content is null)
           limit 3
        """)
        for content in ret["chunk_content"]:
            if "green" in content:
                assert "big" in content
            else:
                assert "small" in content

        # -- using between and less than

        ret = self.run_sql("""
           select * from kb_alg where
            content like 'white' and num between 3 and 6 and num < 5
           limit 3
        """)
        assert len(ret) == 2

        for _, item in ret.iterrows():
            assert "white" in item["chunk_content"]
            assert item["metadata"]["num"] in (3, 4)

    @patch("mindsdb.integrations.handlers.litellm_handler.litellm_handler.embedding")
    def test_select_allowed_columns(self, mock_litellm_embedding):
        set_litellm_embedding(mock_litellm_embedding)

        # -- no metadata are specified, generated from inserts --
        self._create_kb("kb1")

        self.run_sql("insert into kb1 (id, content, col1) values (1, 'cont1', 'val1')")
        self.run_sql("insert into kb1 (id, content, col2) values (2, 'cont2', 'val2')")

        # existed value
        ret = self.run_sql("select * from kb1 where col1='val1'")
        assert len(ret) == 1 and ret["chunk_content"][0] == "cont1"

        # not existed value
        ret = self.run_sql("select * from kb1 where col1='not exist'")
        assert len(ret) == 0

        # not existed column
        with pytest.raises(ValueError):
            self.run_sql("select * from kb1 where col3='val2'")

        # -- metadata are specified --
        self._create_kb(
            "kb2",
            metadata_columns=["col1", "col2", "col3"],
        )

        self.run_sql("insert into kb2 (id, content, col1) values (1, 'cont1', 'val1')")
        self.run_sql("insert into kb2 (id, content, col2) values (2, 'cont2', 'val2')")

        # existed value
        ret = self.run_sql("select * from kb2 where col1='val1'")
        assert len(ret) == 1 and ret["chunk_content"][0] == "cont1"

        # not existed value
        ret = self.run_sql("select * from kb2 where col3='cont1'")
        assert len(ret) == 0

        # not existed column
        with pytest.raises(ValueError):
            self.run_sql("select * from kb2 where cont10='val2'")

    @patch("mindsdb.interfaces.knowledge_base.llm_client.OpenAI")
    @patch("mindsdb.integrations.utilities.rag.rerankers.base_reranker.BaseLLMReranker.get_scores")
    @patch("mindsdb.integrations.handlers.litellm_handler.litellm_handler.embedding")
    def test_evaluate(self, mock_litellm_embedding, mock_get_scores, mock_openai):
        set_litellm_embedding(mock_litellm_embedding)

        question, answer = "2+2", "4"
        agent_response = f"""
            {{"query": "{question}", "reference_answer": "{answer}"}}
        """
        mock_completion = MagicMock()
        mock_completion.choices = [MagicMock()]
        mock_completion.choices[0].message.content = agent_response
        mock_openai().chat.completions.create.return_value = mock_completion

        # reranking result
        mock_get_scores.side_effect = lambda query, docs: [0.8 for _ in docs]

        df = self._get_ral_table()
        df = df.rename(columns={"english": "content", "ral": "id"})
        self.save_file("ral", df)

        self._create_kb("kb1", reranking_model={"provider": "openai", "model_name": "gpt-3", "api_key": "-"})
        self.run_sql("insert into kb1 SELECT id, content FROM files.ral")

        # --- case 1: use table as source, reranker llm, no evaluate

        ret = self.run_sql("""
            Evaluate knowledge base kb1
            using
              test_table = files.eval_test,
              generate_data = {   
                 'from_sql': 'select content, id from files.ral', 
                 'count': 3 
              }, 
              evaluate=false
        """)

        # reranker model is used
        assert mock_openai().chat.completions.create.call_args_list[0][1]["model"] == "gpt-3"

        # no response
        assert len(ret) == 0

        # check test data
        df_test = self.run_sql("select * from files.eval_test")
        assert len(df_test) == 3
        assert df_test["question"][0] == question
        assert df_test["answer"][0] == answer

        # --- case 2: use kb as source, custom llm, evaluate
        mock_openai.reset_mock()
        self.run_sql("drop table files.eval_test")

        ret = self.run_sql("""
            Evaluate knowledge base kb1
            using
              test_table = files.eval_test,
              generate_data = true,
              llm={'provider': 'openai', 'api_key':'-', 'model_name':'gpt-4'},
              save_to = files.eval_res 
        """)

        # custom model is used
        assert mock_openai().chat.completions.create.call_args_list[0][1]["model"] == "gpt-4"

        # eval resul in response
        assert len(ret) == 1

        # check test data
        df_test = self.run_sql("select * from files.eval_test")
        assert len(df_test) > 0
        assert df_test["question"][0] == question
        assert df_test["answer"][0] == answer

        # check result
        df_res = self.run_sql("select * from files.eval_res")
        assert len(df_res) == 1
        assert df_res["total"][0] == len(df_test)
        # compare with eval response
        assert df_res["total"][0] == ret["total"][0]
        assert df_res["total_found"][0] == ret["total_found"][0]

        # --- case 3: evaluate without generation and saving

        ret = self.run_sql("""
            Evaluate knowledge base kb1
            using
              test_table = files.eval_test
        """)

        # eval resul in response
        assert len(ret) == 1
        # compare with table
        assert df_res["total"][0] == ret["total"][0]
        assert df_res["total_found"][0] == ret["total_found"][0]

        # --- test reranking disabled ---
        mock_get_scores.reset_mock()
        df = self.run_sql("select * from kb1 where content='test'")
        mock_get_scores.assert_called_once()
        assert len(df) > 0

        mock_get_scores.reset_mock()
        df = self.run_sql("select * from kb1 where content='test' and reranking =false")
        mock_get_scores.assert_not_called()
        assert len(df) > 0

import time
import os
import json

from unittest.mock import patch, AsyncMock

import pandas as pd
import pytest
import sys
from openai.types.chat import ChatCompletion
from tests.unit.executor_test_base import BaseExecutorDummyML
from tests.unit.executor.test_knowledge_base import set_litellm_embedding


def action_response(type="final_query", sql="", text=""):
    if text:
        type = "final_text"
    return json.dumps({"sql_query": sql, "type": type, "text": text, "short_description": "a tool"})


def set_openai_completion(mock_openai, llm_response):
    if isinstance(llm_response, str):
        llm_responses = [
            action_response(sql=f"select '{llm_response}' as answer"),
        ]
    else:
        llm_responses = llm_response

    # always add plan response
    llm_responses.insert(0, '{"plan":"my plan is ...", "estimated_steps":3}')

    mock_openai.agent_calls = []
    calls = []
    responses = []

    async def resp_f(messages, *args, **kwargs):
        # return all responses in sequence, then yield only latest from list
        if len(llm_responses) == 1:
            resp = llm_responses[0]
        else:
            resp = llm_responses.pop(0)

        # log agent calls, exclude previous part of message
        combined_message = "\n".join([m["content"] for m in messages if m["content"]])
        agent_call = combined_message
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
        calls.append(combined_message)
        responses.append(resp)

        num = len(mock_openai.agent_calls)
        data = {
            "id": "chatcmpl-123",
            "object": "chat.completion",
            "created": 1234567890 + num,
            "model": "gpt-3.5-turbo",
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": f"call_{num}",
                                "type": "function",
                                "function": {"name": "final_result", "arguments": resp},
                            }
                        ],
                    },
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 10, "completion_tokens": 2, "total_tokens": 12},
        }

        return ChatCompletion(**data)

    mock_openai().chat.completions.create = AsyncMock(side_effect=resp_f)


def get_dataset_planets():
    data = [
        ["1000", "Moon"],
        ["1001", "Jupiter"],
        ["1002", "Venus"],
    ]
    return pd.DataFrame(data, columns=["id", "planet_name"])


class TestAgent(BaseExecutorDummyML):
    def setup_method(self):
        super().setup_method()
        from mindsdb.utilities.config import config

        config["knowledge_bases"]["disable_autobatch"] = True

    @pytest.mark.slow
    def unused_test_mindsdb_provider(self):
        # pydantic agent doesn't support using mindsdb model
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
    def unused_test_openai_provider_with_model(self, mock_openai):
        # pydantic agent doesn't support using mindsdb model

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

    @patch("pydantic_ai.providers.openai.AsyncOpenAI")
    def test_openai_provider(self, mock_openai):
        agent_response = "how can I assist you today?"
        set_openai_completion(mock_openai, agent_response)

        self.run_sql("""
            CREATE AGENT my_agent
            USING
             model = {
                'provider': 'openai',
                'model_name': "gpt-3.5-turbo",
                'api_key': '-key-'
             },
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

        mock_openai.reset_mock()
        set_openai_completion(mock_openai, agent_response)

        ret = self.run_sql("""
            select * from files.questions t
            join my_agent a on a.question=t.q
        """)

        assert agent_response in ret.answer[0]

        # empty query
        mock_openai.reset_mock()
        set_openai_completion(mock_openai, agent_response)

        ret = self.run_sql("""
            select * from files.questions t
            join my_agent a on a.question=t.q
            where t.q = ''
        """)
        assert len(ret) == 0

    @patch("mindsdb.utilities.config.Config.get")
    @patch("pydantic_ai.providers.openai.AsyncOpenAI")
    def test_agent_with_default_llm_params(self, mock_openai, mock_config_get):
        # Mock the config.get method to return default LLM parameters
        def config_get_side_effect(key, default=None):
            if key == "default_llm":
                return {
                    "provider": "openai",
                    "model_name": "gpt-4o",
                    "api_key": "sk-abc123",
                    "base_url": "https://config-url.com/v1",
                    "api_version": "2024-02-01",
                    "method": "multi-class",
                }
            elif key == "default_project":
                return "mindsdb"
            elif key == "cache":
                return {"type": "none"}
            return default

        mock_config_get.side_effect = config_get_side_effect

        agent_response = "how can I assist you today?"
        set_openai_completion(mock_openai, agent_response)

        # Create an agent with only provider specified - should use default LLM params
        self.run_sql("""
            CREATE AGENT default_params_agent
            USING
             model = {
                'provider': 'openai',
                'base_url': 'https://custom-url.com/',
                'model_name': "gpt-3"
             },
             prompt_template="Answer the user input in a helpful way"
         """)

        # Check that the agent was created with the default parameters
        agent_info = self.run_sql("SELECT * FROM information_schema.agents WHERE name = 'default_params_agent'")

        # Verify the agent has the user-specified parameters but not default parameters
        agent_params = json.loads(agent_info["PARAMS"].iloc[0])
        assert agent_params.get("prompt_template") == "Answer the user input in a helpful way"
        assert agent_params["model"]["model_name"] == "gpt-3"

        # Default parameters should NOT be stored in the database
        # They will be applied at runtime via get_agent_llm_params
        assert "base_url" not in agent_params
        assert "api_version" not in agent_params
        assert "method" not in agent_params

        ret = self.run_sql("select * from default_params_agent where question = 'hi'")
        assert agent_response in ret.answer[0]

        assert mock_openai.call_args_list[-1][1]["api_key"] == "sk-abc123"

        assert mock_openai.call_args_list[-1][1]["base_url"] == "https://custom-url.com/"  # from agent
        assert mock_openai().chat.completions.create.call_args_list[-1][1]["model"] == "gpt-3"  # from agent

        # --- Test that agent creation works with minimal syntax using default_llm config ---

        mock_openai.reset_mock()
        agent_response = "how can I assist you today?"
        set_openai_completion(mock_openai, agent_response)

        # Create an agent with minimal syntax - should use all default LLM params
        self.run_sql("""
            CREATE AGENT minimal_syntax_agent
            USING
              data = {
                "tables": ['test.table1', 'test.table2']
              }
         """)

        ret = self.run_sql("select * from minimal_syntax_agent where question = 'hi'")
        assert agent_response in ret.answer[0]

        assert mock_openai.call_args_list[-1][1]["api_key"] == "sk-abc123"  # from default
        assert mock_openai.call_args_list[-1][1]["base_url"] == "https://config-url.com/v1"  # from default
        assert mock_openai().chat.completions.create.call_args_list[-1][1]["model"] == "gpt-4o"  # from agent

    @pytest.mark.skipif(sys.platform == "darwin", reason="Fails on macOS")
    @patch("pydantic_ai.providers.openai.AsyncOpenAI")
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
            if agent_response in str(chunk.get("content")):
                found = True
        if not found:
            raise AttributeError("Agent response is not found")

    def _create_kb_storage(self, kb_name):
        self.run_sql(f"""
          create database db_{kb_name} 
           with 
           engine='chromadb',
           PARAMETERS = {{
               'persist_directory': '{kb_name}'
           }}
        """)
        return f"db_{kb_name}.default_collection"

    def _drop_kb_storage(self, vector_table_name):
        self.run_sql(f"drop table {vector_table_name}")

        db_name = vector_table_name.split(".")[0]

        self.run_sql(f"drop database {db_name}")

    @patch("litellm.embedding")
    @patch("pydantic_ai.providers.openai.AsyncOpenAI")
    def test_agent_retrieval(self, mock_openai, mock_litellm_embedding):
        set_litellm_embedding(mock_litellm_embedding)

        vector_table_name = self._create_kb_storage("kb_review")
        self.run_sql(f"""
            create knowledge base kb_review
            using
                storage={vector_table_name},
                embedding_model = {{
                    "provider": "bedrock",
                    "model_name": "dummy_model",
                    "api_key": "dummy_key"
                }}
        """)

        os.environ["OPENAI_API_KEY"] = "--"

        self.run_sql("""
          create agent retrieve_agent
           using
          model='gpt-3.5-turbo',
          provider='openai',
          prompt_template='Answer the user input in a helpful way using tools',
          data = {
                "knowledge_bases": ["kb_review"]
          },
          mode='retrieval'
        """)

        agent_response = "the answer is yes"
        user_question = "answer my question"

        set_openai_completion(
            mock_openai,
            [
                action_response(
                    sql=f"select * from kb_review where content='{user_question}'", type="exploratory_query"
                ),
                action_response(sql=f"SELECT '{agent_response}' answer"),
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

        self.run_sql("drop knowledge base kb_review")
        self._drop_kb_storage(vector_table_name)

    # should not be possible to drop demo agent
    def test_drop_demo_agent(self):
        """should not be possible to drop demo agent"""
        from mindsdb.api.executor.exceptions import ExecutorException

        self.run_sql("""
            CREATE AGENT my_demo_agent
            USING
                model = {
                    'provider': 'openai',
                    'model_name': "gpt-3.5-turbo",
                    'api_key': '-key-'
                },
                prompt_template="--",
                is_demo=true;
         """)
        with pytest.raises(ExecutorException):
            self.run_sql("drop agent my_agent")

    @patch("pydantic_ai.providers.openai.AsyncOpenAI")
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

        mock_openai.reset_mock()
        set_openai_completion(mock_openai, agent_response)
        ret = self.run_sql("select * from default_retrieval_agent where question = 'test question'")
        assert agent_response in ret.answer[0]

    @patch("pydantic_ai.providers.openai.AsyncOpenAI")
    @patch("mindsdb.integrations.handlers.litellm_handler.litellm_handler.embedding")
    def test_agent_permissions(self, mock_litellm_embedding, mock_openai):
        set_litellm_embedding(mock_litellm_embedding)

        vector_table_name = self._create_kb_storage("kb_show")

        kb_sql = f"""
            create knowledge base %s
            using 
            storage={vector_table_name},
            embedding_model = {{"provider": "bedrock", "model_name": "titan"}}
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
                action_response(sql="select * from kb_hide where content='Moon'", type="exploratory_query"),
                action_response(sql="SELECT 'Hi!' answer"),
            ],
        )
        self.run_sql("select * from my_agent where question = 'test'")

        # result of query
        assert "kb_hide not found" in mock_openai.agent_calls[2]
        # it shows available KBs
        assert "kb_show*" in mock_openai.agent_calls[2]

        # ===== Access to exposed KBs =====
        set_openai_completion(
            mock_openai,
            [
                action_response(sql="select * from kb_show1 where content='Moon' limit 1", type="exploratory_query"),
                action_response(sql="SELECT 'Hi!' answer"),
            ],
        )
        self.run_sql("select * from my_agent where question = 'test'")

        # result of object info
        assert "kb_hide" not in mock_openai.agent_calls[1]
        assert "kb_show1" in mock_openai.agent_calls[1]
        assert "kb_show2" in mock_openai.agent_calls[1]
        assert "Sample Data" in mock_openai.agent_calls[1]
        assert "Metadata" in mock_openai.agent_calls[1]

        # result of query
        assert "Moon" in mock_openai.agent_calls[2]

        # ===== access to forbidden files =====

        set_openai_completion(
            mock_openai,
            [
                action_response(sql="select * from files.hide", type="exploratory_query"),
                action_response(sql="SELECT 'Hi!' answer"),
            ],
        )
        self.run_sql("select * from my_agent where question = 'test'")
        # result of query
        assert "hide not found" in mock_openai.agent_calls[2]
        # it shows available tables
        assert "show*" in mock_openai.agent_calls[2]

        # ===== access to exposed files =====

        set_openai_completion(
            mock_openai,
            [
                action_response(sql="select * from files.show1 where id = '1001'", type="exploratory_query"),
                action_response(sql="SELECT 'Hi!' answer"),
            ],
        )

        self.run_sql("select * from my_agent where question = 'test'")

        # result of object info
        assert "hide" not in mock_openai.agent_calls[1]
        assert "show1" in mock_openai.agent_calls[1]
        assert "show2" in mock_openai.agent_calls[1]
        assert "Sample Data" in mock_openai.agent_calls[1]
        assert "Metadata" in mock_openai.agent_calls[1]

        # result of query
        assert "Jupiter" in mock_openai.agent_calls[2]

        self.run_sql("drop knowledge base kb_show1")
        self.run_sql("drop knowledge base kb_show2")
        self.run_sql("drop knowledge base kb_hide")
        self._drop_kb_storage(vector_table_name)

    @patch("pydantic_ai.providers.openai.AsyncOpenAI")
    @patch("mindsdb.integrations.handlers.litellm_handler.litellm_handler.embedding")
    def test_agent_new_syntax(self, mock_litellm_embedding, mock_openai):
        set_litellm_embedding(mock_litellm_embedding)
        vector_table_name = self._create_kb_storage("kb")
        df = get_dataset_planets()
        # create 2 files and KBs
        for i in (1, 2):
            self.run_sql(f"""
                create knowledge base kb{i}                
                using 
                storage={vector_table_name},
                embedding_model = {{"provider": "bedrock", "model_name": "titan"}}
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
                action_response(sql="SELECT * FROM kb1", type="exploratory_query"),
                action_response(sql="SELECT * FROM files.file1", type="exploratory_query"),
                action_response(sql="SELECT 'Hi!' answer"),
            ],
        )
        self.run_sql("select * from my_agent where question = 'test'")
        assert "Jupiter" in mock_openai.agent_calls[2]
        assert "Jupiter" in mock_openai.agent_calls[3]  # column

        # not exposed
        set_openai_completion(
            mock_openai,
            [
                action_response(sql="SELECT * FROM kb2", type="exploratory_query"),
                action_response(sql="SELECT * FROM files.file2", type="exploratory_query"),
                action_response(sql="SELECT 'Hi!' answer"),
            ],
        )
        ret = self.run_sql("select * from my_agent where question = 'test'")
        assert "`kb2` not found" in mock_openai.agent_calls[2]
        assert "`file2` not found" in mock_openai.agent_calls[3]

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
                action_response(sql="SELECT * FROM kb2", type="exploratory_query"),
                action_response(sql="SELECT * FROM files.file2", type="exploratory_query"),
                action_response(sql="SELECT 'Hi!' answer"),
            ],
        )
        self.run_sql("select * from my_agent where question = 'test'")
        assert "Jupiter" in mock_openai.agent_calls[2]
        assert "Jupiter" in mock_openai.agent_calls[3]  # column

        # not exposed
        set_openai_completion(
            mock_openai,
            [
                action_response(sql="SELECT * FROM kb1", type="exploratory_query"),
                action_response(sql="SELECT * FROM files.file1", type="exploratory_query"),
                action_response(sql="SELECT 'Hi!' answer"),
            ],
        )
        ret = self.run_sql("select * from my_agent where question = 'test'")
        assert "kb1 not found" in mock_openai.agent_calls[2]
        assert "file1 not found" in mock_openai.agent_calls[3]

        # check model params
        assert mock_openai.call_args_list[-1][1]["api_key"] == "-almost secret-"
        assert mock_openai().chat.completions.create.call_args_list[-1][1]["model"] == "gpt-18"

        # check agent response
        assert "Hi!" in ret.answer[0]

        # check prompt template
        assert "important system prompt №37" in mock_openai.agent_calls[0]

        self.run_sql("drop knowledge base kb1")
        self.run_sql("drop knowledge base kb2")
        self._drop_kb_storage(vector_table_name)

    @patch("pydantic_ai.providers.openai.AsyncOpenAI")
    @patch("mindsdb.integrations.handlers.litellm_handler.litellm_handler.embedding")
    def test_agent_accept_wrong_quoting(self, mock_litellm_embedding, mock_openai):
        set_litellm_embedding(mock_litellm_embedding)
        vector_table_name = self._create_kb_storage("kb1")
        self.run_sql(f"""
            create knowledge base kb1
            using             
            storage={vector_table_name},
            embedding_model = {{"provider": "bedrock", "model_name": "titan"}}
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
                action_response(sql="SELECT * FROM `mindsdb.kb1` WHERE id = '1001'", type="exploratory_query"),
                action_response(sql="SELECT * FROM `files.file1` WHERE id = '1002'", type="exploratory_query"),
                action_response(sql="SELECT 'Hi!' answer"),
            ],
        )
        self.run_sql("select * from my_agent where question = 'test'")

        assert "Jupiter" in mock_openai.agent_calls[2]
        assert "Venus" in mock_openai.agent_calls[3]

        self.run_sql("drop knowledge base kb1")
        self._drop_kb_storage(vector_table_name)

    @patch("pydantic_ai.providers.openai.AsyncOpenAI")
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
                action_response(sql="SELECT * FROM pg.public.planets WHERE id = '1000'", type="exploratory_query"),
                # test getting table info
                action_response(sql="SELECT * FROM `pg.public`.planets WHERE id = '1000'", type="exploratory_query"),
                action_response(sql="SELECT * FROM `pg.public`.`planets` WHERE id = '1000'", type="exploratory_query"),
                action_response(sql="SELECT 1"),
            ],
        )
        self.run_sql("select * from my_agent where question = 'test'")

        # results of sql_db_query
        assert "Moon" in mock_openai.agent_calls[2]
        assert "Moon" in mock_openai.agent_calls[3]
        assert "Moon" in mock_openai.agent_calls[4]

    @patch("mindsdb.interfaces.agents.pydantic_ai_agent.PydanticAIAgent._get_completion_stream")
    def test_agent_query_param_override(self, mock_get_completion):
        """
        Test that agent parameters can be overridden per-query using the USING clause in SELECT.
        """
        mock_get_completion.return_value = [{"type": "data", "content": "-"}]

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
        assert mock_get_completion.call_args_list[0][0][1].get("timeout") == 5

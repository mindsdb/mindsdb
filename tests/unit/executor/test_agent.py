import time
import os
import json
from unittest.mock import patch, MagicMock
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

    def resp_f(*args, **kwargs):
        # return all responses in sequence, then yield only latest from list

        if len(response) == 1:
            resp = response[0]
        else:
            resp = response.pop(0)

        return {"choices": [{"message": {"role": "system", "content": resp}}]}

    mock_openai().chat.completions.create.side_effect = resp_f


def set_litellm_embedding(mock_litellm_embedding, response):
    if not isinstance(response, list):
        response = [response]

    def resp_f(*args, **kwargs):
        mock_response = MagicMock()
        mock_response.data = [{"embedding": emb} for emb in response]
        return mock_response

    mock_litellm_embedding.side_effect = resp_f


class TestAgent(BaseExecutorDummyML):
    @pytest.mark.slow
    def test_mindsdb_provider(self):
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

        self.run_sql("""
            CREATE AGENT my_agent
            USING
             provider='mindsdb',
             model = "base_model", -- <
             prompt_template="Answer the user input in a helpful way"
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
             openai_api_key='--',
             prompt_template="Answer the user input in a helpful way"
         """)
        ret = self.run_sql("select * from my_agent where question = 'hi'")

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
        set_litellm_embedding(mock_litellm_embedding, [[0.1] * 1536])
        self.run_sql("""
            create knowledge base kb_review
            using
                embedding_model = {
                    "provider": "dummy_provider",
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
        from textwrap import dedent

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
                "provider": "dummy_provider",
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

        set_litellm_embedding(mock_litellm_embedding, [[0.1] * 1536])
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

        set_litellm_embedding(mock_litellm_embedding, [[0.1] * 1536])

        self.run_sql("""
            insert into kb_review
            select review as content, id from files.reviews
        """)

        ret = self.run_sql("select * from kb_review where original_doc_id = 123")
        assert len(ret) == 1
        assert ret["chunk_content"][0] == record["review"]

        # delete by metadata
        self.run_sql("delete from kb_review where original_doc_id = 123")
        ret = self.run_sql("select * from kb_review where original_doc_id = 123")
        assert len(ret) == 0

        # insert without id
        self.run_sql("""
            insert into kb_review
            select review as content, product, url from files.reviews
        """)

        # id column wasn't used
        ret = self.run_sql("select * from kb_review where original_doc_id = 123")
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

        set_litellm_embedding(mock_litellm_embedding, [[0.1] * 1536, [0.2] * 1536])
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

        set_litellm_embedding(mock_litellm_embedding, [[0.1] * 1536])
        self.run_sql("""
            insert into kb_review
            select * from files.reviews
        """)

        ret = self.run_sql("""
                select chunk_content,
                 metadata->>'specs' as specs, metadata->>'product' as product, metadata->>'url' as url
                from kb_review 
                where original_doc_id = 123 -- id is id
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

        set_litellm_embedding(mock_litellm_embedding, [[0.1] * 1536, [0.2] * 1536, [0.3] * 1536, [0.4] * 1536])
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
            set_litellm_embedding(mock_litellm_embedding, [[0.1] * 1536] * len(df))
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

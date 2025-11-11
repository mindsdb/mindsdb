import time
import json

from unittest.mock import patch, MagicMock
import threading
from contextlib import contextmanager

import pandas as pd
import pytest
import sys

from tests.unit.executor_test_base import BaseExecutorDummyML
from mindsdb.integrations.utilities.rag.rerankers.base_reranker import ListwiseLLMReranker


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

        # using json operator in filter
        ret = self.run_sql(
            "select metadata->>'product' as product, metadata->>'url' as url "
            "from kb_review where metadata->>'product' = 'probook'"
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

        # metadata as columns
        ret = self.run_sql("""
                select chunk_content, specs, product, url
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

    def test_listwise_reranker_parses_valid_json(self):
        reranker = ListwiseLLMReranker(api_key="-", model="gpt-4o")

        # Fake async LLM response
        class _Msg:
            def __init__(self, content):
                self.content = content

        class _Choice:
            def __init__(self, content):
                self.message = _Msg(content)

        class _Resp:
            def __init__(self, content):
                self.choices = [_Choice(content)]

        async def _fake_call_llm(messages):
            content = '{"ranking": [{"doc_index": 2, "score": 0.9}, {"doc_index": 1, "score": 0.6}, {"doc_index": 3, "score": 0.1}]}'
            return _Resp(content)

        # Bind the async method to this reranker instance
        reranker._call_llm = _fake_call_llm  # type: ignore

        docs = ["A", "B", "C"]
        scores = reranker.get_scores("q", docs)

        assert len(scores) == 3
        # doc_index 2 (B) highest, then A, then C
        assert scores[1] > scores[0] > scores[2]
        # scores are clamped to [0,1]
        assert all(0.0 <= s <= 1.0 for s in scores)

    def test_listwise_reranker_handles_code_fence_and_missing_docs(self):
        reranker = ListwiseLLMReranker(api_key="-", model="gpt-4o")

        class _Msg:
            def __init__(self, content):
                self.content = content

        class _Choice:
            def __init__(self, content):
                self.message = _Msg(content)

        class _Resp:
            def __init__(self, content):
                self.choices = [_Choice(content)]

        async def _fake_call_llm(messages):
            # Returns code-fenced JSON, includes only two entries, one without score
            content = """```json
            {"ranking": [1, {"doc_index": 3, "score": 0.8}]}
            ```"""
            return _Resp(content)

        reranker._call_llm = _fake_call_llm  # type: ignore

        docs = ["D0", "D1", "D2", "D3"]
        scores = reranker.get_scores("q", docs)

        assert len(scores) == 4
        # All scores within [0,1]
        assert all(0.0 <= s <= 1.0 for s in scores)
        # At least doc_index 3 (zero-based 2) should have a relatively high score
        assert scores[2] >= 0.5

    def test_listwise_reranker_json_error_fallback(self):
        reranker = ListwiseLLMReranker(api_key="-", model="gpt-4o")

        class _Msg:
            def __init__(self, content):
                self.content = content

        class _Choice:
            def __init__(self, content):
                self.message = _Msg(content)

        class _Resp:
            def __init__(self, content):
                self.choices = [_Choice(content)]

        async def _fake_call_llm(messages):
            # Invalid JSON forces fallback
            content = "not-json"
            return _Resp(content)

        reranker._call_llm = _fake_call_llm  # type: ignore

        docs = ["X", "Y", "Z"]
        scores = reranker.get_scores("q", docs)

        assert len(scores) == 3
        # Fallback pattern should be descending
        assert scores[0] > scores[1] > scores[2]

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

        # -- chunk_content and '%'
        ret = self.run_sql("""
           select * from kb_alg where
               (chunk_content like '%green%' and size='big') 
            or (chunk_content like '%white%' and size='small') 
            or (chunk_content is null)
           limit 3
        """)
        for content in ret["chunk_content"]:
            if "green" in content:
                assert "big" in content
            else:
                assert "small" in content

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

    @patch("mindsdb.utilities.config.Config.get")
    @patch("mindsdb.integrations.handlers.litellm_handler.litellm_handler.embedding")
    def test_save_default_params(self, mock_litellm_embedding, mock_config_get):
        set_litellm_embedding(mock_litellm_embedding)

        def config_get_side_effect(key, default=None):
            if key == "default_embedding_model":
                return {
                    "provider": "bedrock",
                    "model_name": "dummy_model",
                    "api_key": "dummy_key",
                }
            return default

        mock_config_get.side_effect = config_get_side_effect

        self.run_sql("create knowledge base kb1")

        ret = self.run_sql("describe  knowledge base kb1")

        # default model was saved
        assert "dummy_model" in ret["EMBEDDING_MODEL"][0]

    @patch("mindsdb.integrations.handlers.litellm_handler.litellm_handler.embedding")
    def test_relevance_filtering_gt_operator(self, mock_litellm_embedding):
        """Test relevance filtering with GREATER_THAN operator"""
        set_litellm_embedding(mock_litellm_embedding)

        test_data = [
            {"id": "1", "content": "This is about machine learning and AI"},
            {"id": "2", "content": "This is about cooking recipes"},
            {"id": "3", "content": "This is about artificial intelligence and neural networks"},
            {"id": "4", "content": "This is about gardening tips"},
        ]
        df = pd.DataFrame(test_data)
        self.save_file("test_docs", df)
        self._create_kb("kb_relevance_test")
        self.run_sql("""
            insert into kb_relevance_test
            select id, content from files.test_docs
        """)

        ret = self.run_sql("""
            select * from kb_relevance_test
            where content = 'machine learning'
            and relevance > 0.5
        """)
        assert isinstance(ret, pd.DataFrame)

    @patch("mindsdb.integrations.utilities.rag.rerankers.base_reranker.BaseLLMReranker.get_scores")
    @patch("mindsdb.integrations.handlers.litellm_handler.litellm_handler.embedding")
    def test_alter_kb(self, mock_litellm_embedding, mock_get_scores):
        set_litellm_embedding(mock_litellm_embedding)

        self._create_kb(
            "kb1",
            embedding_model={
                "provider": "bedrock",
                "model_name": "dummy_model",
                "api_key": "embed-key-1",
            },
            reranking_model={"provider": "openai", "model_name": "gpt-3", "api_key": "rerank-key-1"},
        )

        # update KB
        self.run_sql("""
            ALTER KNOWLEDGE BASE kb1
            USING 
                reranking_model={'api_key': 'rerank-key-2'},
                embedding_model={'api_key': 'embed-key-2'},
                id_column='my_id',
                content_columns=['my_content'],                
                metadata_columns=['my_meta']
        """)

        # check updated values in database
        kb = self.db.KnowledgeBase.query.filter_by(name="kb1").first()
        assert kb.params["id_column"] == "my_id"
        assert kb.params["content_columns"] == ["my_content"]
        assert kb.params["metadata_columns"] == ["my_meta"]

        assert kb.params["reranking_model"]["model_name"] == "gpt-3"
        assert kb.params["reranking_model"]["api_key"] == "rerank-key-2"

        assert kb.params["embedding_model"]["api_key"] == "embed-key-2"

        # update embedding fails
        with pytest.raises(ValueError):
            self.run_sql("ALTER KNOWLEDGE BASE kb1 USING embedding_model={'model_name': 'my_model'}")

        with pytest.raises(ValueError):
            self.run_sql("ALTER KNOWLEDGE BASE kb1 USING embedding_model={'provider': 'ollama'}")

        # different provider: params are replaced
        self.run_sql("ALTER KNOWLEDGE BASE kb1 USING reranking_model={'provider': 'ollama', 'model_name': 'mistral'}")
        kb = self.db.KnowledgeBase.query.filter_by(name="kb1").first()

        assert kb.params["reranking_model"]["provider"] == "ollama"
        assert "api_key" not in kb.params["reranking_model"]

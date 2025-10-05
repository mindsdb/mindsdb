import json
import time
import datetime as dt
import os

import pytest
import mindsdb_sdk

from mindsdb.utilities import log
from tests.integration.conftest import HTTP_API_ROOT


logger = log.getLogger(__name__)


class HiddenVar(str):
    """
    Doesn't show value of var in console
    """

    def __repr__(self):
        return "..."


def get_configurations():
    storages = [{"engine": "chromadb", "persist_directory": "default_collection"}]

    if "PGVECTOR_PASSWORD" in os.environ:
        pgvector_local = {
            "engine": "pgvector",
            "user": "vector",
            "host": "samples.mindsdb.com",
            "port": "5432",
            "password": os.environ["PGVECTOR_PASSWORD"],
            "database": "vector",
        }
        storages.append(pgvector_local)

    if "OPENAI_API_KEY" in os.environ:
        embedding_model = {
            "provider": "openai",
            "model_name": "text-embedding-ada-002",
            "api_key": HiddenVar(os.environ["OPENAI_API_KEY"]),
        }
        for storage in storages:
            name = f"{storage['engine']}-{embedding_model['provider']}"
            yield pytest.param(storage, embedding_model, id=name)


def get_rerank_configurations():
    # configurations with reranking model

    configurations = []
    for params in get_configurations():
        if isinstance(params, list):
            storage, embedding_model = params
        else:
            # is pytest.param
            storage, embedding_model = params.values

        if embedding_model["provider"] == "openai":
            reranking_model = embedding_model.copy()
            reranking_model["model_name"] = "gpt-4"
            configurations.append([storage, embedding_model, reranking_model])

        if "GEMINI_API_KEY" in os.environ:
            reranking_model = {
                "provider": "gemini",
                "model_name": "gemini-2.0-flash",
                "api_key": HiddenVar(os.environ["GEMINI_API_KEY"]),
            }
            configurations.append([storage, embedding_model, reranking_model])

    for storage, embedding_model, reranking_model in configurations:
        name = f"{storage['engine']}-{embedding_model['provider']}-{reranking_model['provider']}"
        yield pytest.param(storage, embedding_model, reranking_model, id=name)


class KBTestBase:
    @classmethod
    def setup_class(cls):
        cls.con = mindsdb_sdk.connect(HTTP_API_ROOT.removesuffix("/api"))

        cls.create_example_db()

    @classmethod
    def create_example_db(cls):
        name = "example_db"

        try:
            cls.con.databases.get(name)
            return name

        except AttributeError:
            pass

        cls.con.databases.create(
            name,
            engine="postgres",
            connection_args={
                "user": "demo_user",
                "password": "demo_password",
                "host": "samples.mindsdb.com",
                "port": "5432",
                "database": "demo",
                "schema": "demo_data",
            },
        )
        return name

    def create_vector_db(self, connection_args, name):
        connection_args = connection_args.copy()
        engine = connection_args.pop("engine")

        # TODO update database parameters. for now keep existing connection
        # try:
        #     self.con.databases.drop(name)
        # except RuntimeError as e:
        #     if "Database does not exists" not in str(e):
        #         raise e

        try:
            self.con.databases.create(name, engine=engine, connection_args=connection_args)
        except RuntimeError:
            ...

        return name

    def run_sql(self, sql):
        logger.debug(">>>", sql)
        resp = self.con.query(sql).fetch()
        logger.debug("--- response ---")
        logger.debug(resp)
        return resp

    def create_kb(self, name, storage, embedding_model, reranking_model=None, params=None):
        # remove if exists
        engine = storage["engine"]
        db_name = f"test_vectordb_{engine}_{name}"
        table_name = f"tbl_{name}"

        #  -- clean --
        try:
            self.con.knowledge_bases.drop(name)
        except Exception:
            ...

        try:
            db = self.con.databases.get(db_name)

            db.tables.drop(table_name)
            self.con.databases.drop(db_name)
        except Exception:
            ...

        # -- create --

        # prepare KB
        kb_params = {
            "embedding_model": embedding_model,
            # "metadata_columns": ["status", "category"],
            # "content_columns": ["message_body"],
            # "id_column": "id",
        }
        if params is not None:
            kb_params.update(params)

        if reranking_model is not None:
            kb_params["reranking_model"] = reranking_model

        param_str = ""
        if kb_params:
            param_items = []
            for k, v in kb_params.items():
                param_items.append(f"{k}={json.dumps(v)}")
            param_str = ",".join(param_items)

        self.create_vector_db(storage, db_name)
        param_str += f", storage = {db_name}.{table_name}"

        self.run_sql(f"""
            create knowledge base {name}
            using {param_str}
        """)


class Disable_TestKB(KBTestBase):
    @pytest.mark.parametrize("storage, embedding_model", get_configurations())
    def test_base_syntax(self, storage, embedding_model):
        self.create_kb("test_kb_crm", storage, embedding_model)

        # -------------- insert --------
        logger.debug("insert from table")
        count_rows = 10  # content too small to be chunked
        self.run_sql(f"""
            insert into test_kb_crm
            select pk id, message_body content, ticket_id from example_db.demo.crm_demo
            order by pk
            limit {count_rows}
        """)

        logger.debug("insert from values")
        for i in range(2):
            # do it twice second time it will be updated
            self.run_sql("""
                insert into test_kb_crm (id, content) values
                (1000, 'Help'), (1001, 'Thank you'), (1002, 'Thank you')
            """)
        count_rows += 3

        # ------------  checking columns  ------------
        # Simplified SQL Syntax
        kb_columns = ["id", "chunk_content", "metadata", "distance", "relevance"]

        logger.debug("Select all without conditions")
        ret = self.run_sql("select * from test_kb_crm")
        assert len(ret) == count_rows
        for column in kb_columns:
            assert column in ret.columns, f"Column {column} does not exist in response"

        logger.debug("Select one column without conditions")
        for column in kb_columns:
            ret = self.run_sql(f"select {column} from test_kb_crm")
            assert len(ret) == count_rows
            assert list(ret.columns) == [column], f"Response don''t have column {column}"

        # ---------- selecting options --------

        logger.debug("Limit")
        ret = self.run_sql("select id, chunk_content from test_kb_crm limit 4")
        assert len(ret) == 4

        logger.debug("Limit with content")
        ret = self.run_sql("select id, chunk_content, distance from test_kb_crm where content = 'help' limit 4")
        assert len(ret) == 4
        assert ret["id"][0] == 1000

        logger.debug("filter by id")
        ret = self.run_sql("select id, chunk_content from test_kb_crm where id = 1001")
        assert len(ret) == 1
        assert ret["chunk_content"][0] == "Thank you"

        ret = self.run_sql("select id, chunk_content from test_kb_crm where id != 1000 limit 4")
        assert len(ret) == 4
        assert 1000 not in ret["id"]

        # in, not in
        ret = self.run_sql("select id, chunk_content from test_kb_crm where id in (1001, 1000)")
        assert len(ret) == 2
        assert set(ret["id"]) == {1000, 1001}

        ret = self.run_sql("select id, chunk_content from test_kb_crm where id not in ('1001', '1000') limit 4")
        assert len(ret) == 4
        assert "1000" not in list(ret["id"])

        if storage["engine"] == "pgvector":
            # some operators don't work with chromadb

            # like / not like
            ret = self.run_sql("select id, metadata, chunk_content from test_kb_crm where ticket_id like '%1001'")
            assert len([row for _, row in ret.iterrows() if "1001" not in str(row["metadata"])]) == 0

            ret = self.run_sql("select id, metadata, chunk_content from test_kb_crm where ticket_id not like '%1001'")
            assert len([row for _, row in ret.iterrows() if "1001" in str(row["metadata"])]) == 0

        logger.debug("outer query")
        ret = self.run_sql(
            "select chunk_content, count(1) count, max(id) max from test_kb_crm where id > 999 group by chunk_content order by max(id) desc"
        )
        assert len(ret) == 2
        assert ret["max"][0] == 1002
        assert ret["count"][0] == 2
        assert ret["chunk_content"][0] == "Thank you"

        # ------------------- join with table -------------
        ret = self.run_sql("""
            select k.chunk_content, t.message_body, k.id, t.pk
            from test_kb_crm k
            join example_db.demo.crm_demo t on t.pk = k.id
            where k.content = 'Help' and k.id not in (1000, 1001, 1002)
            limit 4
        """)

        row = ret.iloc[0]
        assert row["chunk_content"] == row["message_body"]
        assert row["id"] == row["pk"]

        # -----------------  modify data ---------------

        # delete
        self.run_sql("delete from test_kb_crm where id = 1")
        ret = self.run_sql("select * from test_kb_crm where id = 1")
        assert len(ret) == 0

        self.run_sql("delete from test_kb_crm where id in (1001, 2)")
        ret = self.run_sql("select * from test_kb_crm where id in (1001, 2)")
        assert len(ret) == 0

        # update
        ret = self.run_sql("select * from test_kb_crm where id = 1002")
        chunk_id = ret["chunk_id"][0]

        self.run_sql(f"update test_kb_crm set content = 'FINE' where chunk_id = '{chunk_id}'")
        ret = self.run_sql("select * from test_kb_crm where id = 1002")
        assert len(ret) == 1
        assert ret["chunk_content"][0] == "FINE"

        # TODO update by id don't work
        #   should it update all chunks?

        # Test deletion of Knowledge Bases
        self.run_sql("drop knowledge base test_kb_crm")

        ret = self.run_sql("describe knowledge base test_kb_crm")
        assert len(ret) == 0

    @pytest.mark.parametrize("storage, embedding_model", get_configurations())
    def test_no_reranking(self, storage, embedding_model):
        # --- Test data ingestion ---

        def to_date(s):
            return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")

        # Create KB and start load in thread
        self.create_kb(
            "test_kb_crm_meta",
            storage,
            embedding_model,
            params={
                "metadata_columns": ["status", "category"],
                "content_columns": ["message_body"],
                "id_column": "pk",
            },
        )

        logger.debug("start loading")
        ret = self.run_sql("""
            insert into test_kb_crm_meta
            select * from example_db.demo.crm_demo
            using batch_size=100, track_column=pk
        """)

        if "ID" not in ret.columns:
            raise RuntimeError("Query is not partitioned")

        duration = None
        for i in range(100):  # 100 sec min max
            time.sleep(1)

            ret = self.run_sql("describe knowledge base test_kb_crm_meta")
            record = ret.iloc[0]

            if record["INSERT_FINISHED_AT"] is not None:
                duration = (to_date(record["INSERT_FINISHED_AT"]) - to_date(record["INSERT_STARTED_AT"])).seconds
                logger.debug(f"loading completed in {duration}s")
                break

            if record["ERROR"] is not None:
                raise RuntimeError(record["ERROR"])
        if duration is None:
            raise RuntimeError("Timeout to finish query")

        # --- test metadata ---

        # -- Metadata search
        ret = self.run_sql("""
            SELECT *
            FROM test_kb_crm_meta
            WHERE category = "Battery";
        """)
        assert set(ret.metadata.apply(lambda x: x.get("category"))) == {"Battery"}

        ret = self.run_sql("""
            SELECT *
            FROM test_kb_crm_meta
            WHERE status = "solving" AND category = "Battery"
        """)
        assert set(ret.metadata.apply(lambda x: x.get("category"))) == {"Battery"}
        assert set(ret.metadata.apply(lambda x: x.get("status"))) == {"solving"}

        # -- Content + metadata search
        ret = self.run_sql("""
            SELECT *
            FROM test_kb_crm_meta
            WHERE status = "solving" AND content = "noise";
        """)
        assert set(ret.metadata.apply(lambda x: x.get("status"))) == {"solving"}
        assert "noise" in ret.chunk_content[0]

        # -- Content + metadata search with limit
        ret = self.run_sql("""
            SELECT *
            FROM test_kb_crm_meta
            WHERE status = "solving" AND content = "noise"
            LIMIT 5;
        """)
        assert set(ret.metadata.apply(lambda x: x.get("status"))) == {"solving"}
        assert "noise" in ret.chunk_content[0]
        assert len(ret) == 5

        # -- Content + metadata search with limit and re-ranking threshold
        ret = self.run_sql("""
            SELECT *
            FROM test_kb_crm_meta
            WHERE status = "solving" AND content = "noise" AND relevance>=0.5
        """)
        assert set(ret.metadata.apply(lambda x: x.get("status"))) == {"solving"}
        assert "noise" in ret.chunk_content[0]  # first line contents word
        assert len(ret[ret.relevance < 0.5]) == 0

    @pytest.mark.parametrize("storage, embedding_model, reranking_model", get_rerank_configurations())
    def test_with_reranking(self, storage, embedding_model, reranking_model):
        # --- reranking ---
        self.create_kb(
            "test_kb_crm_rerank",
            storage,
            embedding_model,
            reranking_model,
            params={
                "metadata_columns": ["status", "category"],
                "content_columns": ["message_body"],
                "id_column": "pk",
            },
        )

        self.run_sql("""
            INSERT INTO test_kb_crm_rerank (
                SELECT * FROM example_db.demo.crm_demo order by pk limit 50 
            );
        """)

        threshold = 0.5
        ret = self.run_sql(f"""
            SELECT *
            FROM test_kb_crm_rerank
            WHERE status = "solving" AND content = "noise" AND relevance>={threshold}
        """)
        assert set(ret.metadata.apply(lambda x: x.get("status"))) == {"solving"}
        for item in ret.chunk_content:
            assert "noise" in item  # all lines line contents word

        assert len(ret[ret.relevance < threshold]) == 0

        # --- evaluate ---

        ret = self.run_sql("""
            Evaluate knowledge base test_kb_crm_rerank
            using
              test_table = files.test_eval_kb_crm_test,
              generate_data = {   
                 'from_sql': 'SELECT message_body content, pk id FROM example_db.demo.crm_demo order by pk limit 50',
                 'count': 2
             }, 
             evaluate=true
        """)
        assert ret["total_found"][0] == ret["total"][0]
        test_df = self.run_sql("select * from files.test_eval_kb_crm_test")
        assert len(test_df) == ret["total"][0]

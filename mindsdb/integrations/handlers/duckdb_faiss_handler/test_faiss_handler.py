from unittest.mock import patch

import pandas as pd

from tests.unit.executor.test_knowledge_base import TestKB as BaseTestKB, set_litellm_embedding


class TestFAISS(BaseTestKB):
    "Run unit tests using FAISS handler as storage"

    def _get_storage_table(self, kb_name):
        try:
            self.run_sql(f"""
                 DROP DATABASE faiss_{kb_name}
             """)
        except Exception:
            pass

        self.run_sql(f"""
             CREATE DATABASE faiss_{kb_name}
             WITH ENGINE = 'duckdb_faiss'
         """)

        try:
            self.run_sql(f"""
                 drop table faiss_{kb_name}.kb_faiss
             """)
        except Exception:
            pass

        return f"faiss_{kb_name}.kb_faiss"

    @patch("mindsdb.integrations.handlers.litellm_handler.litellm_handler.embedding")
    def test_ivf_index(self, mock_litellm_embedding):
        set_litellm_embedding(mock_litellm_embedding)

        df = self._get_ral_table()

        df = pd.concat([df] * 300)
        # unique ids
        df["id"] = list(map(str, range(len(df))))

        self.save_file("ral", df)

        self._create_kb("kb_ral", content_columns=["english"])

        self.run_sql(
            """
                insert into kb_ral
                select id, english from files.ral
            """
        )

        self.run_sql("CREATE INDEX ON KNOWLEDGE_BASE kb_ral WITH (nlist=10)")

        # search works
        ret = self.run_sql("select * from kb_ral  where k.content = 'white' limit 1")
        assert "white" in ret["chunk_content"][0]

        # --  test insert  --
        self.run_sql("insert into kb_ral (id, english) values (10000, 'magpie')")
        # search
        ret = self.run_sql("select * from kb_ral  where k.content = 'magpie' limit 1")
        assert "magpie" in ret["chunk_content"][0]

        # --  test delete  --
        self.run_sql("delete from kb_ral where id=10000")
        # search
        ret = self.run_sql("select * from kb_ral  where k.content = 'magpie' limit 1")
        assert len(ret) == 0 or "magpie" not in ret["chunk_content"][0]

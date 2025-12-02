from tests.unit.executor.test_knowledge_base import TestKB as BaseTestKB


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

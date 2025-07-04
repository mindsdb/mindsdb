import pytest
import pandas as pd
from unittest.mock import patch
from tests.unit.executor_test_base import BaseExecutorMockPredictor

class TestLowercase(BaseExecutorMockPredictor):
    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_create_view_name_lowercase(self, mock_handler):
        df = pd.DataFrame([
            {"a": 1, "b": "one"},
            {"a": 2, "b": "two"},
        ])
        self.set_handler(mock_handler, name="pg", tables={"tasks": df})

        # Error: quoted name in mx-case
        with pytest.raises(Exception):
            self.execute("""
                CREATE VIEW `MyView` AS (SELECT * FROM pg.tasks)
            """)

        views_names = ['myview', 'MyView', 'MYVIEW']
        for view_name in views_names:
            another_name = 'myVIEW'
            self.execute(f"""
                CREATE VIEW {view_name} AS (SELECT * FROM pg.tasks)
            """)

            res = self.execute(f"""
                SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_name = '{view_name.lower()}'
            """)
            assert res.data.to_df()['TABLE_TYPE'][0] == 'VIEW'

            # alter view: wrong quoted case
            with pytest.raises(Exception):
                self.execute(f"""
                    ALTER VIEW `{another_name}` AS (SELECT * FROM pg.tasks WHERE a = 1)
                """)

            # alter view: wrong case
            self.execute(f"""
                ALTER VIEW {another_name} AS (SELECT * FROM pg.tasks WHERE a = 1)
            """)

            # select: wrong quoted case
            with pytest.raises(Exception):
                self.execute(f"""
                    SELECT * FROM `{another_name}`
                """)

            self.execute(f"""
                SELECT * FROM {another_name}
            """)

            # dropL wrong quoted case
            with pytest.raises(Exception):
                self.execute(f"""
                    DROP VIEW `{another_name}`
                """)

            self.execute(f"""
                DROP VIEW {another_name}
            """)

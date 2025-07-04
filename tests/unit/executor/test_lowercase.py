import pytest
import pandas as pd
from unittest.mock import patch
from tests.unit.executor_test_base import BaseExecutorMockPredictor

class TestLowercase(BaseExecutorMockPredictor):
    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_view_name_lowercase(self, mock_handler):
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

    def test_project_name_lowercase(self):
        project_name = 'MyProject'

        with pytest.raises(Exception):
            self.execute(f"""
            CREATE DATABASE `{project_name}`
        """)

        # processing is slightly different for 'projects' (without engine) and integrations, so we do cycle for both.
        for engine in ['', " WITH ENGINE = 'dummy_data'"]:
            for project_name in ['myproject', 'MyProject', 'MYPROJECT']:
                another_name = 'myPROJECT'
                self.execute(f"""
                    CREATE DATABASE {project_name} {engine}
                """)

                res = self.execute(f"""
                    SELECT * FROM INFORMATION_SCHEMA.DATABASES where name = '{another_name}'
                """)
                assert len(res.data) == 0

                res = self.execute(f"""
                    SELECT * FROM INFORMATION_SCHEMA.DATABASES where name = '{project_name.lower()}'
                """)
                assert res.data.to_df()['TYPE'][0] == ('project' if engine == '' else 'data')

                # FIXME
                # with pytest.raises(Exception):
                #     self.execute(f"""
                #         SELECT * FROM `{another_name}`.models
                #     """)
                # ALTER DB !

                with pytest.raises(Exception):
                    self.execute(f"""
                        DROP DATABASE `{another_name}`
                    """)

                self.execute(f"""
                    DROP DATABASE {another_name}
                """)

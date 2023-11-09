import importlib
import os

import pandas as pd
import pytest
from mindsdb_sql import parse_sql

from ..unit.executor_test_base import BaseExecutorTest

try:
    importlib.import_module("requests")
    REQUESTS_INSTALLED = True
except ImportError:
    REQUESTS_INSTALLED = False


@pytest.mark.skipif(not REQUESTS_INSTALLED, reason="requests package is not installed")
class TestLightdashHandler(BaseExecutorTest):

    def run_sql(self, sql):
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))
        assert ret.error_code is None
        if ret.data is not None:
            columns = [
                col.alias if col.alias is not None else col.name for col in ret.columns
            ]
            return pd.DataFrame(ret.data, columns=columns)

    def setup_method(self):
        super().setup_method()
        self.api_key = os.environ.get("LIGHTDASH_API_KEY")
        self.base_url = os.environ.get("LIGHTDASH_BASE_URL")
        self.project_uuid = os.environ.get("LIGHTDASH_PROJECT_UUID")
        self.space_uuid = os.environ.get("LIGHTDASH_SPACE_UUID")
        self.chart_uuid = os.environ.get("LIGHTDASH_CHART_UUID")
        self.chart_version_uuid = os.environ.get("LIGHTDASH_CHART_VERSION_UUID")
        self.scheduler_uuid = os.environ.get("LIGHTDASH_SCHEDULER_UUID")
        self.job_id = os.environ.get("LIGHTDASH_JOB_ID")
        self.run_sql(f"""
            CREATE DATABASE lightdash_datasource
            WITH ENGINE = "lightdash",
            PARAMETERS = {{
              "api_key": '{self.api_key}',
              "base_url": '{self.base_url}'
            }};
        """)

    def test_basic_select_from(self):
        sql = """
            SELECT * FROM lightdash_datasource.user;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM lightdash_datasource.user_ability;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM lightdash_datasource.org;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM lightdash_datasource.org_projects;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM lightdash_datasource.org_members;
        """
        self.run_sql(sql)
        sql = f"""
            SELECT * FROM lightdash_datasource.project_table WHERE project_uuid='{self.project_uuid}';
        """
        self.run_sql(sql)
        sql = f"""
            SELECT * FROM lightdash_datasource.warehouse_connection WHERE project_uuid='{self.project_uuid}';
        """
        self.run_sql(sql)
        sql = f"""
            SELECT * FROM lightdash_datasource.dbt_connection WHERE project_uuid='{self.project_uuid}';
        """
        self.run_sql(sql)
        sql = f"""
            SELECT * FROM lightdash_datasource.dbt_env_vars WHERE project_uuid='{self.project_uuid}';
        """
        self.run_sql(sql)
        sql = f"""
            SELECT * FROM lightdash_datasource.charts WHERE project_uuid='{self.project_uuid}';
        """
        self.run_sql(sql)
        sql = f"""
            SELECT * FROM lightdash_datasource.spaces WHERE project_uuid='{self.project_uuid}';
        """
        self.run_sql(sql)
        sql = f"""
            SELECT * FROM lightdash_datasource.access WHERE project_uuid='{self.project_uuid}';
        """
        self.run_sql(sql)
        sql = f"""
            SELECT * FROM lightdash_datasource.validation WHERE project_uuid='{self.project_uuid}';
        """
        self.run_sql(sql)
        sql = f"""
            SELECT * FROM lightdash_datasource.dashboards WHERE project_uuid='{self.project_uuid}' AND space_uuid='{self.space_uuid}';
        """
        self.run_sql(sql)
        sql = f"""
            SELECT * FROM lightdash_datasource.queries WHERE project_uuid='{self.project_uuid}' AND space_uuid='{self.space_uuid}';
        """
        self.run_sql(sql)
        sql = f"""
            SELECT * FROM lightdash_datasource.chart_history WHERE chart_uuid='{self.chart_uuid}';
        """
        self.run_sql(sql)
        sql = f"""
            SELECT * FROM lightdash_datasource.chart_config WHERE chart_uuid='{self.chart_uuid}' AND version_uuid='{self.chart_version_uuid}';
        """
        self.run_sql(sql)
        sql = f"""
            SELECT * FROM lightdash_datasource.chart_additional_metrics WHERE chart_uuid='{self.chart_uuid}' AND version_uuid='{self.chart_version_uuid}';
        """
        self.run_sql(sql)
        sql = f"""
            SELECT * FROM lightdash_datasource.chart_table_calculations WHERE chart_uuid='{self.chart_uuid}' AND version_uuid='{self.chart_version_uuid}';
        """
        self.run_sql(sql)
        sql = f"""
            SELECT * FROM lightdash_datasource.scheduler_logs WHERE project_uuid='{self.project_uuid}';
        """
        self.run_sql(sql)
        sql = f"""
            SELECT * FROM lightdash_datasource.scheduler WHERE scheduler_uuid='{self.scheduler_uuid}';
        """
        self.run_sql(sql)
        sql = f"""
            SELECT * FROM lightdash_datasource.scheduler_jobs WHERE scheduler_uuid='{self.scheduler_uuid}';
        """
        self.run_sql(sql)
        sql = f"""
            SELECT * FROM lightdash_datasource.scheduler_job_status WHERE job_id='{self.job_id}';
        """
        self.run_sql(sql)

    def test_complex_select(self):
        sql = """
            SELECT firstName, lastName FROM lightdash_datasource.user;
        """
        assert self.run_sql(sql).shape[1] == 2
        sql = f"""
            SELECT name FROM lightdash_datasource.project_table WHERE project_uuid='{self.project_uuid}';
        """
        assert self.run_sql(sql).shape[1] == 1

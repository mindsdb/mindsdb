import os
import pytest

from mindsdb_sql_parser import parse_sql

from tests.unit.executor_test_base import BaseExecutorTest


class TestUnify(BaseExecutorTest):

    def run_sql(self, sql):
        ret = self.command_executor.execute_command(parse_sql(sql))
        assert ret.error_code is None
        if ret.data is not None:
            return ret.data.to_df()

    """
    Integration tests for the Unify handler.
    """
    @pytest.fixture(autouse=True, scope="function")
    def setup_method(self):
        """
        Setup test environment, creating a project
        """
        super().setup_method()
        self.run_sql("create database proj")

    @pytest.mark.skipif(os.environ.get('UNIFY_API_KEY') is None, reason='Missing API key!')
    def test_unify_correct_flow(self):
        self.run_sql(
            f"""
        CREATE ML_ENGINE unify_engine
        FROM unify
        USING
            unify_api_key = '{os.environ.get('UNIFY_API_KEY')}';
        """
        )

        self.run_sql(
            """
        CREATE MODEL proj.test_unify_correct_flow
        PREDICT output
        USING
            engine='unify_engine',
            model = 'llama-3-8b-chat',
            provider = 'together-ai',
            question_column = 'text';
        """
        )
        self.wait_predictor("proj", "test_unify_correct_flow")

        result_df = self.run_sql(
            """
            SELECT text, output
            FROM proj.test_unify_correct_flow
            WHERE text = 'Hello';
        """
        )
        assert "hello" in result_df["output"].iloc[0].lower()

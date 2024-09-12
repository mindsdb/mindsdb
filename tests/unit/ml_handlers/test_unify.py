import os
import pytest
import pandas as pd

from mindsdb_sql import parse_sql

from ..executor_test_base import BaseExecutorTest

class TestUnify(BaseExecutorTest):
    # def wait_predictor(self, project, name):
    #     # wait
    #     done = False
    #     for _ in range(200):
    #         ret = self.run_sql(f"select * from {project}.models where name='{name}'")
    #         if not ret.empty:
    #             if ret["STATUS"][0] == "complete":
    #                 done = True
    #                 break
    #             elif ret["STATUS"][0] == "error":
    #                 break
    #         time.sleep(0.5)
    #     if not done:
    #         raise RuntimeError("predictor wasn't created")
    
    def run_sql(self, sql):
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))
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
        # self.run_sql(
        #     f"""
        #     CREATE ML_ENGINE unify-engine
        #     FROM unify
        #     USING
        #     unify_api_key = '{os.environ.get('UNIFY_API_KEY')}';
        #     """
        # )
    @pytest.mark.skipif(os.environ.get('UNIFY_API_KEY') is None, reason='Missing API key!')
    
    def test_unify_correct_flow(self):
        self.run_sql(
        f"""
        CREATE MODEL proj.test_unify_correct_flow
        PREDICT output
        USING
            engine='unify',
            model = 'llama-3-8b-chat',
            provider = 'together-ai',
            column = 'text',
            api_key='{os.environ.get('UNIFY_API_KEY')}';
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
        assert "Hello" in result_df["answer"].iloc[0].lower()

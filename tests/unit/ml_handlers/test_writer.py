import os
import time
from unittest.mock import patch

import pandas as pd
import pytest
from mindsdb_sql import parse_sql

from tests.unit.executor_test_base import BaseExecutorTest

WRITER_API_KEY = os.environ.get("WRITER_API_KEY")
os.environ["WRITER_API_KEY"] = WRITER_API_KEY

WRITER_ORG_ID = os.environ.get("WRITER_ORG_ID")
os.environ["WRITER_ORG_ID"] = WRITER_ORG_ID


class TestWriter(BaseExecutorTest):
    def wait_predictor(self, project, name):
        # wait
        done = False
        for attempt in range(200):
            ret = self.run_sql(f"select * from {project}.models where name='{name}'")
            if not ret.empty:
                if ret["STATUS"][0] == "complete":
                    done = True
                    break
                elif ret["STATUS"][0] == "error":
                    break
            time.sleep(0.5)
        if not done:
            raise RuntimeError("predictor wasn't created")

    def run_sql(self, sql):
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))
        assert ret.error_code is None
        if ret.data is not None:
            columns = [
                col.alias if col.alias is not None else col.name for col in ret.columns
            ]
            return pd.DataFrame(ret.data, columns=columns)

    def test_missing_required_keys(self):
        # create project
        self.run_sql("create database proj")

        self.run_sql(
            """
                CREATE MODEL proj.test_writer_handler_missing_required_args
                PREDICT answer
                USING
                   engine="writer"
                   """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_writer_handler_missing_required_args")

    def test_unsupported_vector_store(self):
        self.run_sql("create database proj")
        self.run_sql(
            f"""
            create model proj.test_unsupported_vector_store
            predict answer
            using
                engine='writer',
                writer_api_key='{WRITER_API_KEY}',
                writer_org_id='{WRITER_ORG_ID}',
                vector_store_name='unsupported_vector_store'
        """
        )

        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_unsupported_vector_store")

    def test_unknown_arguments(self):
        self.run_sql("create database proj")
        self.run_sql(
            f"""
            create model proj.test_writer_unknown_arguments
            predict answer
            using
                engine='writer',
                writer_api_key='{WRITER_API_KEY}',
                writer_org_id='{WRITER_ORG_ID}',
                evidently_wrong_argument='wrong value'  --- this is a wrong argument name
        """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_writer_unknown_arguments")

    @pytest.mark.xfail(
        reason="there seems to be an issue with running inner queries, it appears to be a potential bug in the mock handler"
    )
    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_qa(self, postgres_mock_handler):
        # create project
        self.run_sql("create database proj")
        df = pd.DataFrame.from_dict(
            {
                "context": [
                    "For adults and children age 5 and older, OTC decongestants, "
                    "antihistamines and pain relievers might offer some symptom relief. "
                    "However, they won't prevent a cold or shorten its duration, and most have some side effects.",
                ]
            }
        )
        self.set_handler(postgres_mock_handler, name="pg", tables={"df": df})

        self.run_sql(
            f"""
           create model proj.test_writer_writer_qa
           from pg (select * from df)
           predict answer
           using
                engine='writer',
                writer_api_key='{WRITER_API_KEY}',
                writer_org_id='{WRITER_ORG_ID}';
        """
        )
        self.wait_predictor("proj", "test_writer_writer_qa")

        result_df = self.run_sql(
            """
            SELECT p.answer
            FROM proj.test_writer_writer_qa as p
            WHERE question='What is the best treatment for a cold?'
        """
        )
        assert "cold" in result_df["answer"].iloc[0].lower()

    def test_invalid_prompt_template(self):
        # create project
        self.run_sql("create database proj")
        self.run_sql(
            f"""
           create model proj.test_invalid_prompt_template_format
           predict completion
           using
                engine='writer',
                prompt_template="not valid format",
                writer_api_key='{WRITER_API_KEY}',
                writer_org_id='{WRITER_ORG_ID}';
        """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_invalid_prompt_template_format")

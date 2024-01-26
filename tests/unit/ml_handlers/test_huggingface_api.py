from unittest.mock import patch
import os
import pytest
import pandas as pd
import time

from mindsdb_sql import parse_sql

from .base_ml_test import BaseMLAPITest

@pytest.mark.skipif(os.environ.get('HUGGINGFACE_API_KEY') is None, reason='Missing Huggingface API key!')
class TestHuggingFaceAPI(BaseMLAPITest):
    def wait_predictor(self, project, name):
        # wait
        done = False
        for attempt in range(200):
            ret = self.run_sql(
                f"select * from {project}.models where name='{name}'"
            )
            # print(ret['STATUS'][0])
            if not ret.empty:
                if ret['STATUS'][0] == 'complete':
                    done = True
                    break
                elif ret['STATUS'][0] == 'error':
                    # print(f"{ret['ERROR'][0]}")
                    break
            time.sleep(1)
        if not done:
            raise RuntimeError("predictor wasn't created")

    def run_sql(self, sql):
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))
        assert ret.error_code is None
        if ret.data is not None:
            columns = [col.alias if col.alias is not None else col.name for col in ret.columns]
            return pd.DataFrame(ret.data, columns=columns)

    def test_text_classification(self):
        self.run_sql("CREATE DATABASE proj")

        texts = ["I like you. I love you", "I don't like you. I hate you"]
        df = pd.DataFrame(texts, columns=['texts'])

        self.set_data('df', df)

        self.run_sql(
            f"""
            CREATE MODEL proj.test_hfapi_text_classification
            PREDICT sentiment
            USING
              task = 'text-classification',
              engine = 'hf_api_engine',
              api_key='{self.get_api_key('HUGGINGFACE_API_KEY')}',
              input_column = 'text'
            """
        )

        self.wait_predictor('proj', 'test_hfapi_text_classification')

        result_df = self.run_sql(
            """
            SELECT sentiment
            FROM proj.test_hfapi_text_classification
            WHERE
            text='I like you. I love you'
            """
        )

        assert "positive" in result_df["sentiment"].iloc[0].lower()

import os
import pytest
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from integration_tests.flows.http_test_helpers import HTTPHelperMixin


OPEN_AI_API_KEY = os.environ.get("OPEN_AI_API_KEY")


@pytest.mark.usefixtures("mindsdb_app")
class TestOpenAIHandler(HTTPHelperMixin):
    
    def test_missing_required_keys(self):
        query = f"""
            CREATE MODEL sentiment_classifier_gpt3
            PREDICT sentiment
            USING
            engine = 'openai',
            api_key = '{OPEN_AI_API_KEY}';
        """
        self.sql_via_http(query, RESPONSE_TYPE.ERROR)

        def test_invalid_openai_name_parameter(self):
            query = f"""
                CREATE MODEL sentiment_classifier_gpt3
                PREDICT sentiment
                USING
                engine = 'openai',
                api_key = '{OPEN_AI_API_KEY}',
                model_name = 'invalid_model_name';
        """
        self.sql_via_http(query, RESPONSE_TYPE.ERROR)

import os
import pytest
import mindsdb
import openai
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from integration_tests.flows.http_test_helpers import HTTPHelperMixin
from integration_tests.flows.conftest import *  # noqa: F403,F401

# used by (required for) mindsdb_app fixture in conftest
API_LIST = [
    "http",
]

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

    def test_openai_integration(self):
        openai.api_key = OPEN_AI_API_KEY
        available_models = [model.id for model in openai.Model.list()]
        model_name = available_models[0] # assumes there is at least one model available
        predictor = mindsdb.Predictor(name='test_openai_integration', model_type='nlp', predict_when='accuracy', query={
            'model_name': model_name,
            'engine': 'openai',
            'json_struct': {
                'rental_price': 'rental price',
                'location': 'location',
                'nob': 'number of bathrooms'
            },
            'input_text': 'sentence'
        })
        result = predictor.predict({'sentence': 'This is a test sentence.'})
        assert isinstance(result, dict)

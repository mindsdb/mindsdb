import os
import time
import pytest

from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from ..integration_tests.flows.http_test_helpers import HTTPHelperMixin

# used by mindsdb_app fixture in conftest
OVERRIDE_CONFIG = {
    'integrations': {},
}
# used by (required for) mindsdb_app fixture in conftest
API_LIST = ["http", ]

OPEN_AI_API_KEY = os.environ.get("OPEN_AI_API_KEY")
os.environ["OPENAI_API_KEY"] = OPEN_AI_API_KEY


@pytest.mark.usefixtures("mindsdb_app")
class TestOpenAIHandler(HTTPHelperMixin):

    @classmethod
    def setup_class(cls):
        cls.config = {'integrations': {}}
        cls.initial_integrations_names = list(cls.config['integrations'].keys())

    def test_missing_required_keys(self):
        query = f"""
            CREATE MODEL sentiment_classifier_gpt3
            PREDICT sentiment
            USING
            engine = 'openai',
            api_key = '{OPEN_AI_API_KEY}';
        """
        self.sql_via_http(query, RESPONSE_TYPE.ERROR)

    def test_qa_no_context_flow(self):
        """
        Replicates docs page example:
            https://docs.mindsdb.com/custom-model/openai#operation-mode-1-answering-questions-without-context
        """

        query = f"""
            CREATE MODEL qa_no_context
            PREDICT answer
            USING
            engine = 'openai',
            question_column = 'question',
            api_key = '{OPEN_AI_API_KEY}';
        """
        self.sql_via_http(query, RESPONSE_TYPE.TABLE)
        time.sleep(10)  # wait for model registration

        query = """
        SELECT question, answer
        FROM qa_no_context
        WHERE question = 'Where is Stockholm located?';
        """
        self.sql_via_http(query, RESPONSE_TYPE.TABLE)

    def test_qa_context_flow(self):
        """
        Replicates docs page example:
            https://docs.mindsdb.com/custom-model/openai#operation-mode-2-answering-questions-with-context
        """

        query = f"""
            CREATE MODEL qa_context
            PREDICT answer
            USING
                engine = 'openai',
                question_column = 'question',
                context_column = 'context',
                api_key = '{OPEN_AI_API_KEY}';
        """
        self.sql_via_http(query, RESPONSE_TYPE.TABLE)
        time.sleep(10)  # wait for model registration

        query = """
        SELECT context, question, answer
        FROM qa_context
        WHERE context = 'Answer with a joke'
        AND question = 'How to cook soup?';
        """
        self.sql_via_http(query, RESPONSE_TYPE.TABLE)

    def test_prompt_template_flow(self):
        """
        Replicates docs page example:
            https://docs.mindsdb.com/custom-model/openai#operation-mode-3-prompt-completion

        We use quadruple curly braces to escape them properly to doubles in the query string.
        """

        query = f"""
        CREATE MODEL openai_template_model
        PREDICT answer
        USING
            engine = 'openai',
            prompt_template = 'Context: {{{{context}}}}. Question: {{{{question}}}}. Answer:',
            max_tokens = 100,
            temperature = 0.3,
            api_key = '{OPEN_AI_API_KEY}';
        """

        self.sql_via_http(query, RESPONSE_TYPE.TABLE)
        time.sleep(10)  # wait for model registration

        query = """
        SELECT context, question, answer
        FROM openai_template_model
        WHERE context = 'Answer accurately'
        AND question = 'How many planets exist in the solar system?';
        """

        self.sql_via_http(query, RESPONSE_TYPE.TABLE)

        query = """
        SELECT instruction, answer
        FROM openai_template_model
        WHERE instruction = 'Speculate extensively'
        USING
            prompt_template = '{{{{instruction}}}}. What does Tom Hanks like?',
            max_tokens = 100,
            temperature = 0.5;
        """

        self.sql_via_http(query, RESPONSE_TYPE.TABLE)

        # test invalid template
        query = """
        SELECT instruction, answer
        FROM openai_template_model
        WHERE instruction = 'Speculate extensively'
        USING
            prompt_template = 'What does Tom Hanks like?',
            max_tokens = 100,
            temperature = 0.5;
        """

        self.sql_via_http(query, RESPONSE_TYPE.ERROR)

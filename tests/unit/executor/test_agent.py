import os

import pytest

from tests.unit.executor_test_base import BaseExecutorDummyML

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")


class TestAgent(BaseExecutorDummyML):

    def test_mindsdb_provider(self):

        agent_response = 'how can I help you'
        # model
        self.run_sql(
            f'''
                CREATE model base_model
                PREDICT output
                using
                  column='question',
                  output='{agent_response}',
                  engine='dummy_ml',
                  join_learn_process=true
            '''
        )

        self.run_sql('CREATE ML_ENGINE langchain FROM langchain')

        self.run_sql('''
            CREATE AGENT my_agent
            USING
             provider='mindsdb',
             model_name = "base_model", -- <
             prompt_template="Answer the user input in a helpful way"
         ''')
        ret = self.run_sql("select * from my_agent where question = 'hi'")

        assert agent_response in ret.answer[0]

    @pytest.mark.skipif(OPENAI_API_KEY is None, reason='Missing OpenAI API key (OPENAI_API_KEY env variable)')
    def test_non_mindsdb_provider(self):

        # possible to only specify model_name and prompt_template for non-mindsdb models from supported providers
        self.run_sql('''
            CREATE AGENT my_agent
            USING
            model_name = "gpt-4",
            prompt_template="Answer the user input in a helpful way"
        ''')
        ret = self.run_sql("select * from my_agent where question = 'hi'")

        assert ret.answer[0]

        self.run_sql('''
            CREATE AGENT my_agent_with_provider
            USING
            model_name = "gpt-4",
            provider='openai',
            prompt_template="Answer the user input in a helpful way"
        ''')

        ret = self.run_sql("select * from my_agent_with_provider where question = 'hi'")

        assert ret.answer[0]

    def test_mindsdb_model_without_provider(self):

        agent_response = 'how can I help you'
        # model
        self.run_sql(
            f'''
                CREATE model base_model
                PREDICT output
                using
                  column='question',
                  output='{agent_response}',
                  engine='dummy_ml',
                  join_learn_process=true
            '''
        )

        self.run_sql('CREATE ML_ENGINE langchain FROM langchain')

        with pytest.raises(Exception):
            # using mindsdb model without specifying provider will fail
            self.run_sql('''
                CREATE AGENT my_agent
                USING
                 model_name = "base_model", -- <
                 prompt_template="Answer the user input in a helpful way"
             ''')

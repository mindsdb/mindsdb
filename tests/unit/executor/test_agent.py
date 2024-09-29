import pandas as pd
from tests.unit.executor_test_base import BaseExecutorDummyML

from unittest.mock import patch


def set_openai_completion(mock_openai, response):
    mock_openai().chat.completions.create.return_value = {
        'choices': [{
            'message': {
                'role': 'system',
                'content': response
            }
        }]
    }


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
             model = "base_model", -- <
             prompt_template="Answer the user input in a helpful way"
         ''')
        ret = self.run_sql("select * from my_agent where question = 'hi'")

        assert agent_response in ret.answer[0]

    @patch('openai.OpenAI')
    def test_openai_provider_with_model(self, mock_openai):
        agent_response = 'how can I assist you today?'
        set_openai_completion(mock_openai, agent_response)

        self.run_sql('CREATE ML_ENGINE langchain FROM langchain')

        self.run_sql('''
            CREATE MODEL lang_model
                PREDICT answer USING
            engine = "langchain",
            model = "gpt-3.5-turbo",
            openai_api_key='--',
            prompt_template="Answer the user input in a helpful way";
         ''')

        self.run_sql('''
            CREATE AGENT my_agent
            USING
              model='lang_model'
         ''')
        ret = self.run_sql("select * from my_agent where question = 'hi'")

        assert agent_response in ret.answer[0]

    @patch('openai.OpenAI')
    def test_openai_provider(self, mock_openai):
        agent_response = 'how can I assist you today?'
        set_openai_completion(mock_openai, agent_response)

        self.run_sql('''
            CREATE AGENT my_agent
            USING
             provider='openai',
             model = "gpt-3.5-turbo",
             openai_api_key='--',
             prompt_template="Answer the user input in a helpful way"
         ''')
        ret = self.run_sql("select * from my_agent where question = 'hi'")

        assert agent_response in ret.answer[0]

        # test join
        df = pd.DataFrame([
            {'q': 'hi'},
        ])
        self.save_file('questions', df)

        ret = self.run_sql('''
            select * from files.questions t
            join my_agent a on a.question=t.q
        ''')

        assert agent_response in ret.answer[0]

        # empty query
        ret = self.run_sql('''
            select * from files.questions t
            join my_agent a on a.question=t.q
            where t.q = ''
        ''')
        assert len(ret) == 0

    @patch('openai.OpenAI')
    def test_agent_stream(self, mock_openai):
        agent_response = 'how can I assist you today?'
        set_openai_completion(mock_openai, agent_response)

        self.run_sql('''
            CREATE AGENT my_agent
            USING
             provider='openai',
             model = "gpt-3.5-turbo",
             openai_api_key='--',
             prompt_template="Answer the user input in a helpful way"
         ''')

        agents_controller = self.command_executor.session.agents_controller
        agent = agents_controller.get_agent('my_agent')

        messages = [{'question': 'hi'}]
        found = False
        for chunk in agents_controller.get_completion(agent, messages, stream=True):
            if chunk.get('output') == agent_response:
                found = True
        if not found:
            raise AttributeError('Agent response is not found')

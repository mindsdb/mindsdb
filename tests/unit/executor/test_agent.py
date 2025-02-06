import os
from unittest.mock import patch

import pandas as pd
import pytest

from tests.unit.executor_test_base import BaseExecutorDummyML
from mindsdb.interfaces.agents.langchain_agent import SkillData


def set_openai_completion(mock_openai, response):

    if not isinstance(response, list):
        response = [response]

    def resp_f(*args, **kwargs):
        # return all responses in sequence, then yield only latest from list

        if len(response) == 1:
            resp = response[0]
        else:
            resp = response.pop(0)

        return {
            'choices': [{
                'message': {
                    'role': 'system',
                    'content': resp
                }
            }]
        }

    mock_openai().chat.completions.create.side_effect = resp_f


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
    def test_agent_with_tables(self, mock_openai):
        sd = SkillData(
            name='test', type='', project_id=1,
            params={'tables': []},
            agent_tables_list=[]
        )

        sd.params = {'tables': ['x', 'y']}
        sd.agent_tables_list = ['x', 'y']
        assert sd.restriction_on_tables == {None: {'x', 'y'}}

        sd.params = {'tables': ['x', 'y']}
        sd.agent_tables_list = ['x', 'y', 'z']
        assert sd.restriction_on_tables == {None: {'x', 'y'}}

        sd.params = {'tables': ['x', 'y']}
        sd.agent_tables_list = ['x']
        assert sd.restriction_on_tables == {None: {'x'}}

        sd.params = {'tables': ['x', 'y']}
        sd.agent_tables_list = ['z']
        with pytest.raises(ValueError):
            print(sd.restriction_on_tables)

        sd.params = {'tables': ['x', {'schema': 'S', 'table': 'y'}]}
        sd.agent_tables_list = ['x']
        assert sd.restriction_on_tables == {None: {'x'}}

        sd.params = {'tables': ['x', {'schema': 'S', 'table': 'y'}]}
        sd.agent_tables_list = ['x', {'schema': 'S', 'table': 'y'}]
        assert sd.restriction_on_tables == {None: {'x'}, 'S': {'y'}}

        sd.params = {'tables': [{'schema': 'S', 'table': 'x'}, {'schema': 'S', 'table': 'y'}, {'schema': 'S', 'table': 'z'}]}
        sd.agent_tables_list = [{'schema': 'S', 'table': 'y'}, {'schema': 'S', 'table': 'z'}, {'schema': 'S', 'table': 'f'}]
        assert sd.restriction_on_tables == {'S': {'y', 'z'}}

        sd.params = {'tables': [{'schema': 'S', 'table': 'x'}, {'schema': 'S', 'table': 'y'}]}
        sd.agent_tables_list = [{'schema': 'S', 'table': 'z'}]
        with pytest.raises(ValueError):
            print(sd.restriction_on_tables)

        self.run_sql('''
            create skill test_skill
            using
            type = 'text2sql',
            database = 'example_db',
            tables = ['table_1', 'table_2'],
            description = "this is sales data";
        ''')

        self.run_sql('''
            create agent test_agent
            using
            model='gpt-3.5-turbo',
            provider='openai',
            openai_api_key='--',
            prompt_template='Answer the user input in a helpful way using tools',
            skills=[{
                'name': 'test_skill',
                'tables': ['table_2', 'table_3']
            }];
        ''')

        resp = self.run_sql('''select * from information_schema.agents where name = 'test_agent';''')
        assert len(resp) == 1
        assert resp['SKILLS'][0] == ['test_skill']

        agent_response = 'how can I assist you today?'
        set_openai_completion(mock_openai, agent_response)
        self.run_sql("select * from test_agent where question = 'test?'")

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

    @patch('openai.OpenAI')
    def test_agent_retrieval(self, mock_openai):

        self.run_sql(
            '''
                CREATE model emb_model
                PREDICT output
                using
                  column='content',
                  engine='dummy_ml',
                  join_learn_process=true
            '''
        )

        self.run_sql('create knowledge base kb_review using model=emb_model')

        self.run_sql('''
          create skill retr_skill
          using
              type = 'retrieval',
              source = 'kb_review',
              description = 'user reviews'
        ''')

        os.environ['OPENAI_API_KEY'] = '--'

        self.run_sql('''
          create agent retrieve_agent
           using
          model='gpt-3.5-turbo',
          provider='openai',
          prompt_template='Answer the user input in a helpful way using tools',
          skills=['retr_skill'],
          max_iterations=5,
          mode='retrieval'
        ''')

        agent_response = 'the answer is yes'
        user_question = 'answer my question'
        from textwrap import dedent
        set_openai_completion(mock_openai, [
            # first step, use kb
            dedent(f'''
              Thought: Do I need to use a tool? Yes
              Action: retr_skill
              Action Input: {user_question}
            '''),

            # step2, answer to user
            agent_response
        ])

        with patch('mindsdb.interfaces.knowledge_base.controller.KnowledgeBaseTable.select_query') as kb_select:
            # kb response
            kb_select.return_value = pd.DataFrame([{'id': 1, 'content': 'ok', 'metadata': {}}])
            ret = self.run_sql(f'''
                select * from retrieve_agent where question = '{user_question}'
            ''')

            # check agent output
            assert agent_response in ret.answer[0]

            # check kb input
            args, _ = kb_select.call_args
            assert user_question in args[0].where.args[1].value

    def test_kb(self):

        self.run_sql(
            '''
                CREATE model emb_model
                PREDICT predicted
                using
                  column='content',
                  engine='dummy_ml',
                  join_learn_process=true
            '''
        )

        self.run_sql('create knowledge base kb_review using model=emb_model')

        self.run_sql("insert into kb_review (content) values ('review')")

        # selectable
        ret = self.run_sql("select * from kb_review")
        assert len(ret) == 1

        # show tables in default chromadb
        ret = self.run_sql("show knowledge bases")

        db_name = ret.STORAGE[0].split('.')[0]
        ret = self.run_sql(f"show tables from {db_name}")
        # only one default collection there
        assert len(ret) == 1

    def test_drop_demo_agent(self):
        """should not be possible to drop demo agent
        """
        from mindsdb.api.executor.exceptions import ExecutorException
        self.run_sql('''
            CREATE AGENT my_demo_agent
            USING
                provider='openai',
                model = "gpt-3.5-turbo",
                openai_api_key='--',
                prompt_template="--",
                is_demo=true;
         ''')
        with pytest.raises(ExecutorException):
            self.run_sql('drop agent my_agent')

        self.run_sql('''
            create skill my_demo_skill
            using
            type = 'text2sql',
            database = 'example_db',
            description = "",
            is_demo=true;
        ''')

        with pytest.raises(ExecutorException):
            self.run_sql('drop skill my_demo_skill')

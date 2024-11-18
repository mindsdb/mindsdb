from typing import List
import datetime as dt
import pandas as pd

from mindsdb.interfaces.storage import db

from .types import BotException
from .types import ChatBotMessage


class ModelExecutor:

    def __init__(self, chat_task, model_name):
        self.chat_task = chat_task

        model = chat_task.session.model_controller.get_model(
            model_name,
            project_name=chat_task.project_name
        )
        model_record = db.Predictor.query.get(model['id'])
        integration_record = db.Integration.query.get(model_record.integration_id)

        self.model_info = {
            'model_name': model_name,
            'mode': model_record.learn_args['using'].get('mode'),
            'user_column': model_record.learn_args['using'].get('user_column'),
            'bot_column': model_record.learn_args['using'].get('assistant_column'),
            'output': model_record.to_predict[0],
            'engine': integration_record.engine,
        }

        # redefined prompt
        self.prompt = None

    def call(self, history: List[ChatBotMessage], functions):
        model_info = self.model_info

        if model_info['mode'] != 'conversational':
            raise BotException('Not supported')

        messages, user_name = self._chat_history_to_conversation(history, model_info)
        if model_info['engine'] == 'langchain':

            all_tools = []
            for function in functions:
                all_tools.append({
                    'name': function.name,
                    'func': function.callback,
                    'description': function.description
                })

            context_list = [
                f"- Today's date is {dt.datetime.now().strftime('%Y-%m-%d')}."
                f" It must be used to understand the input date from string like 'tomorrow', 'today', 'yesterday'"
            ]
            context = '\n'.join(context_list)

            # call model
            params = {'tools': all_tools, 'context': context, 'prompt': self.prompt}

            predictions = self.chat_task.project_datanode.predict(
                model_name=model_info['model_name'],
                df=pd.DataFrame(messages),
                params=params
            )

        else:
            predictions = self.chat_task.project_datanode.predict(
                model_name=model_info['model_name'],
                df=pd.DataFrame(messages),
                params={'prompt': self.prompt, 'user_info': {'user_name': user_name}}
            )

        output_col = model_info['output']
        model_output = predictions.iloc[-1][output_col]
        return model_output

    def _chat_history_to_conversation(self, history, model_info):

        bot_username = self.chat_task.bot_params['bot_username']
        user_name = None

        question_col = model_info['user_column']
        answer_col = model_info['bot_column']

        messages = []

        for message in history:
            text = message.text

            if text is None or text.strip() == '':
                # skip empty rows
                continue

            if message.user != bot_username:
                user_name = message.user

                # create new message row
                messages.append({question_col: text, answer_col: None})
            else:
                if len(messages) == 0:
                    # add empty row
                    messages.append({question_col: None, answer_col: None})

                # update answer in previous column
                messages[-1][answer_col] = text
        return messages, user_name

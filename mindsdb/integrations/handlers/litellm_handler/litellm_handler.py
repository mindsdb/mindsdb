import os
from typing import Optional, Dict

from litellm import completion
import pandas as pd

from mindsdb.utilities.config import Config
from mindsdb.integrations.libs.base import BaseMLEngine

from mindsdb.utilities.log import get_log


logger = get_log("integrations.anthropic_handler")

class liteLLMHandler(BaseMLEngine):
    """
    Integration with the liteLLM Python Library
    """
    name = 'litellm'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_chat_model = 'gpt-3.5-turbo'
        self.supported_chat_models = [
            'gpt-3.5-turbo', 
            'gpt-4',
            'gpt-3.5-turbo-16k-0613',
            'gpt-3.5-turbo-16k',
            'text-davinci-003'
            'command-nightly',
            "claude-2", 
            "claude-instant-1"
        ]
        self.generative = True


    def create(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        if 'using' not in args:
            raise Exception("liteLLM engine requires a USING clause! Refer to its documentation for more details.")
        if 'model' not in args['using']:
            args['using']['model'] = self.default_chat_model
        elif args['using']['model'] not in self.supported_chat_models:
            raise Exception(f"Invalid chat model. Please use one of {self.supported_chat_models}")        
        self.model_storage.json_set('args', args)

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        args = self.model_storage.json_get('args')

        # SET API keys as env variables
        # see more information here: https://litellm.readthedocs.io/en/latest/supported/

        input_column = args['using']['column']

        if input_column not in df.columns:
            raise RuntimeError(f'Column "{input_column}" not found in input data')
        
        result_df = pd.DataFrame() 

        result_df['predictions'] = df[input_column].apply(self.predict_answer)     

        result_df = result_df.rename(columns={'predictions': args['target']})
        
        return result_df


    def predict_answer(self, messages):
        """ 
        connects with the selected model to predict the answer for the particular question

        """ 

        args = self.model_storage.json_get('args')

        result = completion(
            model=args['using']['model'],
            messages=messages
        )

        return result ['choices'][0]['message']['content']


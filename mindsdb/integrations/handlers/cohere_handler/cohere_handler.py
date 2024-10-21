import os
from typing import Optional, Dict

import cohere
import pandas as pd

from mindsdb.utilities.config import Config
from mindsdb.integrations.libs.base import BaseMLEngine

from mindsdb.utilities import log

from mindsdb.integrations.utilities.handler_utils import get_api_key


logger = log.getLogger(__name__)

class CohereHandler(BaseMLEngine):
    """
    Integration with the Cohere Python Library
    """
    name = 'cohere'

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        if 'using' not in args:
            raise Exception("Cohere engine requires a USING clause! Refer to its documentation for more details.")

        self.generative = True
        self.model_storage.json_set('args', args)

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:

        args = self.model_storage.json_get('args')

        input_keys = list(args.keys())

        logger.info(f"Input keys: {input_keys}!")

        input_column = args['using']['column']

        if input_column not in df.columns:
            raise RuntimeError(f'Column "{input_column}" not found in input data')
        
        result_df = pd.DataFrame() 

        if args['using']['task'] == 'text-summarization':
            result_df['predictions'] = df[input_column].apply(self.predict_text_summary)     

        elif args['using']['task'] == 'text-generation':
            result_df['predictions'] = df[input_column].apply(self.predict_text_generation)
      
        else:
            raise Exception(f"Task {args['using']['task']} is not supported!")

        result_df = result_df.rename(columns={'predictions': args['target']})
        
        return result_df


    def predict_text_summary(self,text):
        """ 
        connects with cohere api to predict the summary of the input text

        """ 

        args = self.model_storage.json_get('args')

        api_key = get_api_key('cohere', args["using"], self.engine_storage, strict=False)
        co = cohere.Client(api_key)

        response = co.summarize(text)
        text_summary = response.summary

        return text_summary

    def predict_text_generation(self,text):
        """    
        connects with cohere api to predict the next prompt of the input text

        """
        args = self.model_storage.json_get('args')

        api_key = get_api_key('cohere', args["using"], self.engine_storage, strict=False)
        co = cohere.Client(api_key)

        response = co.generate(text)
        text_generated = response.generations[0].text

        return text_generated

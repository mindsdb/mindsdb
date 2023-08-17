import os
from typing import Optional, Dict

import cohere
import pandas as pd

from mindsdb.utilities.config import Config
from mindsdb.integrations.libs.base import BaseMLEngine

from mindsdb.utilities import log


logger = log.getLogger(__name__)

class CohereHandler(BaseMLEngine):
    """
    Integration with the Cohere Python Library
    """
    name = 'cohere'

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        super().__init__(*args)

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

        elif args['using']['task'] == 'language-detection':
            result_df['predictions'] = df[input_column].apply(self.predict_language)     
      
        else:
            raise Exception(f"Task {args['using']['task']} is not supported!")

        result_df = result_df.rename(columns={'predictions': args['target']})
        
        return result_df


    def _get_cohere_api_key(self, args, strict=True):
        """ 
        API_KEY preference order:
            1. provided at model creation
            2. provided at engine creation
            3. COHERE_API_KEY env variable
            4. cohere.api_key setting in config.json
        """ 
        # 1
        if 'api_key' in args:
            return args['api_key']
        # 2
        connection_args = self.engine_storage.get_connection_args()
        if 'api_key' in connection_args:
            return connection_args['api_key']
        # 3
        api_key = os.getenv('COHERE_API_KEY')
        if api_key is not None:
            return api_key
        # 4
        config = Config()
        openai_cfg = config.get('cohere', {})
        if 'api_key' in openai_cfg:
            return openai_cfg['api_key']

        if strict:
            raise Exception(f'Missing API key "api_key". Either re-create this ML_ENGINE specifying the `api_key` parameter,\
                 or re-create this model and pass the API key with `USING` syntax.')  

    def predict_text_summary(self,text):
        """ 
        connects with cohere api to predict the summary of the input text

        """ 

        args = self.model_storage.json_get('args')

        api_key = self._get_cohere_api_key(args)
        co = cohere.Client(api_key)

        response = co.summarize(text)
        text_summary = response.summary

        return text_summary

    def predict_text_generation(self,text):
        """    
        connects with cohere api to predict the next prompt of the input text

        """
        args = self.model_storage.json_get('args')

        api_key = self._get_cohere_api_key(args)
        co = cohere.Client(api_key)

        response = co.generate(text)
        text_generated = response.generations[0].text

        return text_generated

    def predict_language(self,text):
        """    
        connects with cohere api to predict the input language 

        """
        args = self.model_storage.json_get('args')

        api_key = self._get_cohere_api_key(args)
        co = cohere.Client(api_key)

        response = co.detect_language([text])
        language_detected = response.results[0].language_name

        return language_detected
    

import os
from typing import Optional, Dict

from llama_index import GPTVectorStoreIndex,download_loader
import pandas as pd

from mindsdb.utilities.config import Config
from mindsdb.integrations.libs.base import BaseMLEngine

from mindsdb.utilities.log import get_log


logger = get_log("integrations.llama_index_handler")



class LlamaIndexHandler(BaseMLEngine):
    """
    Integration with the LlamaIndex Python Library
    """
    name = 'llama_index'

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.default_index_class  = 'GPTVectorStoreIndex'
        self.default_query_engine = 'as_query_engine' 
        self.supported_index_class = ['GPTVectorStoreIndex']
        self.supported_query_engine = ['as_query_engine'] 


    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:

        if 'using' not in args:
            raise Exception("LlamaIndex engine requires a USING clause! Refer to its documentation for more details.")

        if 'source_url_link' not in args['using']:
            raise Exception("LlamaIndex engine requires a source_url_link parameter.Refer to its documentation for more details.")

        if 'openai_api_key' not in args['using']:
            raise Exception("LlamaIndex engine requires a openai_api_key parameter.Refer to its documentation for more details.")

        if 'index_class' not in args['using']:
            args['using']['index_class'] = self.default_index_class
        elif args['using']['index_class'] not in self.supported_index_class:
            raise Exception(f"Invalid index class argument. Please use one of {self.supported_index_class}")

        if 'query_engine' not in args['using']:
            args['using']['query_engine'] = self.default_query_engine
        elif args['using']['query_engine'] not in self.supported_query_engine:
            raise Exception(f"Invalid operation mode. Please use one of {self.supported_query_engine}")

        self.model_storage.json_set('args', args)


    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        
        args = self.model_storage.json_get('args')

        input_keys = list(args.keys())

        os.environ['OPENAI_API_KEY'] = args['using']['openai_api_key']

        SimpleWebPageReader = download_loader("SimpleWebPageReader")
        documents = SimpleWebPageReader(html_to_text=True).load_data([args['using']['source_url_link']])

        index = GPTVectorStoreIndex.from_documents(documents)
    
        query_engine = index.as_query_engine()
        response = query_engine.query(df['question'].iat[0])

        df['predictions'] = response

        df = df.rename(columns={'predictions': args['target']})

        return df


    def _get_llama_index_api_key(self, args, strict=True):
        """ 
        API_KEY preference order:
            1. provided at model creation
            2. provided at engine creation
            3. OPENAI_API_KEY env variable
            4. llama_index.OPENAI_API_KEY setting in config.json
        """ 
        # 1
        if 'OPENAI_API_KEY' in args:
            return args['OPENAI_API_KEY']
        # 2
        connection_args = self.engine_storage.get_connection_args()
        if 'OPENAI_API_KEY' in connection_args:
            return connection_args['OPENAI_API_KEY']
        # 3
        api_key = os.getenv('OPENAI_API_KEY')
        if api_key is not None:
            return api_key
        # 4
        config = Config()
        openai_cfg = config.get('llama_index', {})
        if 'OPENAI_API_KEY' in openai_cfg:
            return openai_cfg['OPENAI_API_KEY']

        if strict:
            raise Exception(f'Missing API key "OPENAI_API_KEY". Either re-create this ML_ENGINE specifying the `OPENAI_API_KEY` parameter,\
                 or re-create this model and pass the API key with `USING` syntax.')  

    
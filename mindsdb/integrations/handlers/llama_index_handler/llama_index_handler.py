import os
from typing import Optional, Dict
import dill
from llama_index import GPTVectorStoreIndex,download_loader
from llama_index.readers.schema.base import Document
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
        self.supported_index_class = ['GPTVectorStoreIndex']
        self.supported_query_engine = ['as_query_engine'] 
        self.default_reader = 'DFReader'
        self.supported_reader = ['DFReader','SimpleWebPageReader']
  
    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        
        
        if 'using' not in args:
            raise Exception("LlamaIndex engine requires a USING clause! Refer to its documentation for more details.")

        if args['using']['reader'] == 'SimpleWebPageReader':
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

        if 'reader' not in args['using']:
            args['using']['reader'] = self.default_reader
        elif args['using']['reader'] not in self.supported_reader:
            raise Exception(f"Invalid operation mode. Please use one of {self.supported_query_engine}")

       
        documents_df_reader = []
        documents_url_reader = []
        pred = None
        if args['using']['reader'] == 'DFReader':
            for row in df.itertuples():
                doc_str = ", ".join([str(entry) for entry in row])
                documents_df_reader.append(Document(doc_str))  
            pred = documents_df_reader   
      
        elif args['using']['reader'] == 'SimpleWebPageReader':
            SimpleWebPageReader = download_loader("SimpleWebPageReader")
            documents_url_reader = SimpleWebPageReader(html_to_text=True).load_data([args['using']['source_url_link']])
            pred = documents_url_reader 
    
        
        self.model_storage.file_set('pred', dill.dumps(pred))
        self.model_storage.json_set('args', args)

        

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:

        args = self.model_storage.json_get('args')
        data_docs = dill.loads(self.model_storage.file_get('pred'))
        input_keys = list(args.keys())

        input_column = args['using']['input_column']

        if input_column not in df.columns:
            raise RuntimeError(f'Column "{input_column}" not found in input data')

        if args['using']['reader'] == 'DFReader':  
            query_engine  = self.predict_qa_reader(data_docs)
                
        elif args['using']['reader'] == 'SimpleWebPageReader':
            query_engine = self.predict_qa_reader(data_docs)
            
        questions = df[input_column]   
        results = []
        
        
        for question in questions:
            query_results = query_engine.query(question)
            results.append(query_results)
        
        result_df = pd.DataFrame({'question': questions, 'predictions': results})

        result_df = result_df.rename(columns={'predictions': args['target']})

        return result_df


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

    def predict_qa_reader(self,doc):
        """ 
        connects with llama_index python client to predict the 

        """ 
        
        args = self.model_storage.json_get('args')

        os.environ['OPENAI_API_KEY'] = args['using']['openai_api_key']

        documents = doc
        
        if args['using']['index_class'] == 'GPTVectorStoreIndex':
            index = GPTVectorStoreIndex.from_documents(documents)
        else:
            raise Exception(f"Invalid operation mode. Please use one of {self.supported_index_class}.")  
        
        if args['using']['query_engine'] == 'as_query_engine':
            query_engine = index.as_query_engine()
        else:
            raise Exception(f"Invalid operation mode. Please use one of {self.supported_query_engine}.")  

        return query_engine

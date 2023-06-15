import os
from typing import Optional, Dict

import dill
import pandas as pd
from langchain.llms import OpenAI
from llama_index.readers.schema.base import Document
from llama_index import GPTVectorStoreIndex, download_loader, LLMPredictor, ServiceContext

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities.config import Config


class LlamaIndexHandler(BaseMLEngine):
    """ Integration with the LlamaIndex data framework for LLM applications. """
    name = 'llama_index'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generative = True
        self.default_index_class = 'GPTVectorStoreIndex'
        self.supported_index_class = ['GPTVectorStoreIndex']
        self.default_reader = 'DFReader'
        self.supported_reader = ['DFReader', 'SimpleWebPageReader']

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        if 'using' not in args:
            raise Exception("LlamaIndex engine requires a USING clause! Refer to its documentation for more details.")

        if 'index_class' not in args['using']:
            args['using']['index_class'] = self.default_index_class
        elif args['using']['index_class'] not in self.supported_index_class:
            raise Exception(f"Invalid index class argument. Please use one of {self.supported_index_class}")

        if 'reader' not in args['using']:
            args['using']['reader'] = self.default_reader
        elif args['using']['reader'] not in self.supported_reader:
            raise Exception(f"Invalid operation mode. Please use one of {self.supported_reader}")

        if args['using']['reader'] == 'DFReader':
            docstrs = df.apply(lambda x: ','.join([str(entry) for entry in x]), axis=1).tolist()
            reader = list(map(lambda x: Document(x), docstrs))  # TODO: different approach?

        elif args['using']['reader'] == 'SimpleWebPageReader':
            if 'source_url_link' not in args['using']:
                raise Exception("SimpleWebPageReader requires a `source_url_link` parameter. Refer to LlamaIndex documentation for more details.")  # noqa

            SimpleWebPageReader = download_loader("SimpleWebPageReader")
            reader = SimpleWebPageReader(html_to_text=True).load_data([args['using']['source_url_link']])

        else:
            raise Exception(f"Invalid operation mode. Please use one of {self.supported_reader}.")

        self.model_storage.file_set('reader', dill.dumps(reader))
        self.model_storage.json_set('args', args)

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> pd.DataFrame:
        args = self.model_storage.json_get('args')
        data_docs = dill.loads(self.model_storage.file_get('reader'))

        input_column = args['using'].get('input_column', None)

        if input_column is None:
            raise Exception(f'`input_column` must be provided at model creation time or through USING clause when predicting. Please try again.')  # noqa

        if input_column not in df.columns:
            raise Exception(f'Column "{input_column}" not found in input data! Please try again.')

        query_engine = self.predict_qa_reader(data_docs)
        questions = df[input_column]
        results = []

        for question in questions:
            query_results = query_engine.query(question)
            results.append(query_results)

        result_df = pd.DataFrame({'question': questions, args['target']: results})
        return result_df

    def predict_qa_reader(self, documents):
        """ Connects with llama_index python client to perform Q&A. """
        args = self.model_storage.json_get('args')

        openai_api_key = self._get_llama_index_api_key(args['using'])
        llm = OpenAI(openai_api_key=openai_api_key)  # TODO: rest of OpenAI params go here
        service_context = ServiceContext.from_defaults(llm_predictor=LLMPredictor(llm=llm))

        if args['using']['index_class'] == 'GPTVectorStoreIndex':
            index = GPTVectorStoreIndex.from_documents(documents, service_context=service_context)
        else:
            raise Exception(f"Invalid operation mode. Please use one of {self.supported_index_class}.")

        query_engine = index.as_query_engine()
        return query_engine

    def _get_llama_index_api_key(self, args, strict=True):
        """
        API_KEY preference order:
            1. provided at model creation
            2. provided at engine creation
            3. OPENAI_API_KEY env variable
            4. llama_index.OPENAI_API_KEY setting in config.json

        Note: method is not case sensitive.
        """
        key = 'OPENAI_API_KEY'
        for k in key, key.lower():
            # 1
            if args.get(k):
                return args[k]
            # 2
            connection_args = self.engine_storage.get_connection_args()
            if k in connection_args:
                return connection_args[k]
            # 3
            api_key = os.getenv(k)
            if api_key is not None:
                return api_key
            # 4
            config = Config()
            openai_cfg = config.get('llama_index', {})
            if k in openai_cfg:
                return openai_cfg[k]

        if strict:
            raise Exception(f'Missing API key "{k}". Either re-create this ML_ENGINE specifying the `{k}` parameter, or re-create this model and pass the API key with `USING` syntax.')  # noqa

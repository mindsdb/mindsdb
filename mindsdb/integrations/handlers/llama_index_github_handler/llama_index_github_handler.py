import os
from typing import Optional, Dict

import openai
import llama_index
import pandas as pd
from langchain.llms import OpenAI
from llama_index import download_loader
from llama_index import StorageContext, load_index_from_storage
from llama_index import LLMPredictor, OpenAIEmbedding
from llama_index import ServiceContext

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities.config import Config
download_loader("GithubRepositoryReader")
from llama_hub.github_repo import GithubRepositoryReader, GithubClient


class LlamaIndexGithubHandler(BaseMLEngine):
    """This LlamaIndex Integration loads GitHub repositories for building Question Answering agents."""

    name = 'llama_index_github'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_index_class = 'VectorStoreIndex'
        self.supported_index_class = ['VectorStoreIndex', 'GPTVectorStoreIndex']
        self.default_reader = 'GithubRepositoryReader'
        self.supported_reader = ['GithubRepositoryReader']
        self.default_FilterType = 'EXCLUDE'
        self.supported_FilterType = ['EXCLUDE', 'INCLUDE']
        self.default_branch = 'main'

    @staticmethod
    def create_validation(target, args: Optional[dict] = None, **kwargs):
        args = args['using']
        if 'owner' not in args:
            raise Exception('GithubRepositoryReader requires owner parameter')
        if 'repo' not in args:
            raise Exception('GithubRepositoryReader requires repo parameter')
        if 'input_column' not in args:
            raise Exception('input column is required at the time of model creation')
        if 'filter_directories' in args and not isinstance(args['filter_directories'], list):
            raise Exception('filter_directories parameter must be a list')
        if 'filter_file_extensions' in args and not isinstance(args['filter_file_extensions'], list):
            raise Exception('filter_file_extensions parameter must be a list')

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

        if 'filter_type' not in args['using']:
            args['using']['filter_type'] = self.default_FilterType
        elif args['using']['filter_type'].upper() not in self.supported_FilterType:
            raise Exception(f"Invalid filter type. Please use one of {self.supported_FilterType}")

        github_token = self._get_github_token(args['using'])
        if not github_token:
            raise Exception("Github token is required to create a model")

        openai_key = self._get_openai_key(args['using'])
        if not openai_key:
            raise Exception("OpenAI API key is required to create a model")
        # Set the API keys as env variable
        os.environ['OPENAI_API_KEY'] = openai_key

        owner = args['using']['owner']
        repo = args['using']['repo']
        branch = args['using'].get('branch', self.default_branch)
        filter_directories = self._get_filter_directories(args['using'])
        filter_file_extensions = self._get_filter_file_extensions(args['using'])

        github_client = GithubClient(github_token)
        documents = GithubRepositoryReader(
            github_client,
            owner=owner,
            repo=repo,
            verbose=True,
            filter_directories=filter_directories,
            filter_file_extensions=filter_file_extensions,
        ).load_data(branch=branch)

        index_class = args['using']['index_class']
        self.model_storage.json_set('args', args)
        index = self._setup_index(index_class,documents)
        path = self.model_storage.folder_get('context')
        index.storage_context.persist(persist_dir=path)
        self.model_storage.folder_sync('context')

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        args = self.model_storage.json_get('args')
        index_path = self.model_storage.folder_get('context')
        print(f'Loading index from {index_path}')
        service_context = self._get_service_context()
        storage_context = StorageContext.from_defaults(persist_dir=index_path)
        index = load_index_from_storage(storage_context, service_context=service_context)
        query_engine = index.as_query_engine()

        input_column = args['using']['input_column']
        questions = df[input_column]
        answers = []
        for question in questions:
            answer = query_engine.query(question)
            answers.append(answer)
        result = pd.DataFrame({'question': questions, 'answer': answers})
        return result

    def _get_service_context(self):
        args = self.model_storage.json_get('args')
        openai_api_key = self._get_openai_key(args['using'])
        openai.api_key = openai_api_key
        llm_kwargs = {
            'openai_api_key': openai_api_key
        }
        if 'temperature' in args['using']:
            llm_kwargs['temperature'] = args['using']['temperature']
        if 'model_name' in args['using']:
            llm_kwargs['model_name'] = args['using']['model_name']
        if 'max_tokens' in args['using']:
            llm_kwargs['max_tokens'] = args['using']['max_tokens']

        llm = OpenAI(**llm_kwargs)  # TODO: all usual params should go here
        embed_model = OpenAIEmbedding(openai_api_key=openai_api_key)
        service_context = ServiceContext.from_defaults(
            llm_predictor=LLMPredictor(llm=llm),
            embed_model=embed_model
        )
        return service_context


    def _get_filter_directories(self, args):
        """
        Returns directories to filter, if Filter type is EXCLUDE the directories will be excluded
        from the knowledge source, if Filter type is INCLUDE the directories will be included in the
        knowledge source.
        """
        # filter_directories is not provided
        if 'filter_directories' not in args:
            return None

        # default case
        if args['filter_directories'] and not args['filter_type']:
            filter_directories = args['filter_directories']
            return (filter_directories, GithubRepositoryReader.FilterType.EXCLUDE)

        # if filter_type is provided with EXCLUDE or INCLUDE
        if args['filter_directories'] and args['filter_type'].upper() == 'EXCLUDE':
            filter_directories = args['filter_directories']
            return (filter_directories, GithubRepositoryReader.FilterType.EXCLUDE)
        elif args['filter_directories'] and args['filter_type'].upper() == 'INCLUDE':
            filter_directories = args['filter_directories']
            return (filter_directories, GithubRepositoryReader.FilterType.INCLUDE)

    def _get_filter_file_extensions(self, args):
        """
        Returns file extensions to filter, if Filter type is EXCLUDE the file extensions will be excluded
        from the knowledge source, if Filter type is INCLUDE the file extensions will be included in the
        knowledge source.
        """
        # filter_file_extensions is not provided
        if 'filter_file_extensions' not in args:
            return None

        # default case
        if args['filter_file_extensions'] and not args['filter_type']:
            filter_file_extensions = args['filter_file_extensions']
            return (filter_file_extensions, GithubRepositoryReader.FilterType.EXCLUDE)

        # if filter_type is provided with EXCLUDE or INCLUDE
        if args['filter_file_extensions'] and args['filter_type'].upper() == 'INCLUDE':
            filter_file_extensions = args['filter_file_extensions']
            return (filter_file_extensions, GithubRepositoryReader.FilterType.INCLUDE)
        elif args['filter_file_extensions'] and args['filter_type'].upper() == 'EXCLUDE':
            filter_file_extensions = args['filter_file_extensions']
            return (filter_file_extensions, GithubRepositoryReader.FilterType.EXCLUDE)

    def _setup_index(self, index_class, documents):
        indexer = getattr(llama_index, index_class)
        index = indexer.from_documents(documents)
        return index

    def _get_github_token(self, args):
        """
        API_KEY preference order:
            1. provided at model creation
            2. provided at engine creation
            3. GITHUB_TOKEN env variable

        Note: method is not case-sensitive.
        """
        key = "GITHUB_TOKEN"
        for k in key, key.lower():
            if args.get(k):
                return args[k]

            connection_args = self.engine_storage.get_connection_args()
            if connection_args.get(k):
                return connection_args[k]

            api_key = os.getenv(k)
            if os.environ.get(k):
                return api_key

        return None

    def _get_openai_key(self, args):
        """
        API_KEY preference order:
            1. provided at model creation
            2. provided at engine creation
            3. OPENAI_API_KEY env variable
            4. llama_index.OPENAI_API_KEY setting in config.json

        Note: method is not case-sensitive.
        """
        key = "OPENAI_API_KEY"

        for k in key, key.lower():
            if args.get(k):
                return args[k]

            connection_args = self.engine_storage.get_connection_args()
            if connection_args.get(k):
                return connection_args[k]

            api_key = os.getenv(k)
            if os.environ.get(k):
               return api_key

            config = Config()
            openai_cfg = config.get('llama_index', {})
            if k in openai_cfg:
                return openai_cfg[k]

        return None

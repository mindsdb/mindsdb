import os
from typing import Optional, Dict

import pandas as pd

from mindsdb.utilities.log import get_log
from mindsdb.integrations.libs.base import BaseMLEngine
from .ingest import Ingestor
from .question_answer import QuestionAnswerer
from .settings import PERSIST_DIRECTORY, ModelParameters,\
    USER_DEFINED_MODEL_PARAMS, DEFAULT_EMBEDDING_MODEL


# these require no additional arguments

logger =get_log(logger_name=__name__)

class WriterHandler(BaseMLEngine):
    """
    This is a MindsDB integration for the LangChain library, which provides a unified interface for interacting with
    various large language models (LLMs).

    Currently, this integration supports exposing OpenAI's LLMs with normal text completion support. They are then
    wrapped in a zero shot react description agent that offers a few third party tools out of the box, with support
    for additional ones if an API key is provided. Ongoing memory is also provided.

    Full tool support list:
        - wikipedia
        - python_repl
        - serper.dev search

    This integration inherits from the OpenAI engine, so it shares a lot of the requirements, features (e.g. prompt
    templating) and limitations.
    """
    name = 'writer'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if 'using' not in args:
            raise Exception("LangChain engine requires a USING clause! Refer to its documentation for more details.")
        else:
            args = args['using']

        if len(set(args.keys()) & {'prompt_template'}) == 0:
            raise Exception('Please provide a `prompt_template` for this engine.')

    def create(self, target: str, df: pd.DataFrame = None, args: Optional[Dict] = None):
        """
        Dispatch is running embeddings unless, user already has embeddings persisted
        """
        args = args['using']

        if df is not None:
            if 'context_column' in args:
                #run embeddings and ingest into Chroma VectorDB only if context column(s) provided
                #NB you can update PERSIST_DIRECTORY in settings.py, this is where chroma vector db is stored
                ingestor = Ingestor(df=df, args=args)
                ingestor.embeddings_to_vectordb()

            else:
                logger.debug(
                    'No context column(s) provided, '
                    'please provide a list of columns that are providing context.'
                )
                logger.debug('skipping embeddings and ingestion into Chroma VectorDB')

        self.model_storage.json_set('args', args)

    def predict(self,df,args=None):
        """
        Dispatch is performed depending on the underlying model type. Currently, only the default text completion
        is supported.
        """

        # get model parameters if defined by user - else use default values

        args = self.model_storage.json_get('args')

        user_defined_model_params = list(filter(lambda x: x in args, USER_DEFINED_MODEL_PARAMS))
        args['model_params'] = {model_param: args[model_param] for model_param in user_defined_model_params}
        model_parameters = ModelParameters(**args['model_params'])

        question_answerer = QuestionAnswerer(embeddings_model_name=DEFAULT_EMBEDDING_MODEL, model_parameters=model_parameters)

        question_answerer.query(df['question'].tolist()[0])

        return question_answerer.results_df

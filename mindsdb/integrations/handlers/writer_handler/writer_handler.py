from typing import Optional, Dict

import pandas as pd

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities.log import get_log
from .ingest import Ingestor
from .question_answer import QuestionAnswerer
from .settings import ModelParameters, \
    USER_DEFINED_MODEL_PARAMS, DEFAULT_EMBEDDINGS_MODEL

# these require no additional arguments

logger = get_log(logger_name=__name__)


class WriterHandler(BaseMLEngine):
    """
    WriterHandler is a MindsDB integration with Writer API LLMs that allows users to run question answering
    on their data by providing a question.

    The User is able to provide data that provides context for the questions, see create() method for more details.

    """
    name = 'writer'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def create_validation(target, args = None, **kwargs):
        if 'using' not in args:
            raise Exception("LangChain engine requires a USING clause! Refer to its documentation for more details.")
        else:
            args = args['using']

        if len(set(args.keys()) & {'prompt_template'}) == 0:
            raise Exception('Please provide a `prompt_template` for this engine.')

    def create(self, target: str, df: pd.DataFrame = None, args: Optional[Dict] = None):
        """
        Dispatch is running embeddings and storing in Chroma VectorDB, unless user already has embeddings persisted
        see PERSIST_DIRECTORY in settings.py for default location of Chroma VectorDB indexes
        """
        args = args['using']

        if df and not df.empty:
            if 'context_columns' in args:
                if 'embeddings_model_name' not in args:
                    logger.info(
                        f"No embeddings model provided in query, using default model: {DEFAULT_EMBEDDINGS_MODEL}"
                    )

                # run embeddings and ingest into Chroma VectorDB only if context column(s) provided
                # NB you can update PERSIST_DIRECTORY in settings.py, this is where chroma vector db is stored
                ingestor = Ingestor(df=df, args=args)
                ingestor.embeddings_to_vectordb()

            else:
                logger.info(
                    'No context column(s) provided, '
                    'please provide a list of columns that are providing context.'
                )
                logger.info('skipping embeddings and ingestion into Chroma VectorDB')

        self.model_storage.json_set('args', args)

    def predict(self, df: pd.DataFrame = None, args:dict = None):
        """
        Dispatch is performed depending on the underlying model type. Currently, only question answering
        is supported.
        """

        # get model parameters if defined by user - else use default values

        args = self.model_storage.json_get('args')

        user_defined_model_params = list(filter(lambda x: x in args, USER_DEFINED_MODEL_PARAMS))
        args['model_params'] = {model_param: args[model_param] for model_param in user_defined_model_params}
        model_parameters = ModelParameters(**args['model_params'])

        # get question answering results

        question_answerer = QuestionAnswerer(
            args=args,
            model_parameters=model_parameters
        )

        # get question from sql query e.g. where question = 'What is the capital of France?'
        question_answerer.query(df['question'].tolist()[0])

        # return results

        return question_answerer.results_df
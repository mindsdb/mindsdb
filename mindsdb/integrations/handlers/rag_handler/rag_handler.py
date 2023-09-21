from typing import Dict, Optional

import pandas as pd

from mindsdb.integrations.handlers.rag_handler.ingest import Ingestor
from mindsdb.integrations.handlers.rag_handler.rag import QuestionAnswerer
from mindsdb.integrations.handlers.rag_handler.settings import (
    DEFAULT_EMBEDDINGS_MODEL,
    OpenAIParameters,
    RAGHandlerParameters,
    WriterLLMParameters,
)
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities.log import get_log

# these require no additional arguments

logger = get_log(logger_name=__name__)


def build_llm_params(args):
    """build llm params from input query args"""

    llm_config_class = (
        WriterLLMParameters if args["llm_type"] == "writer" else OpenAIParameters
    )

    if not args.get("llm_params"):
        # only run this on create, not predict

        llm_params = {}
        llm_params["llm_name"] = args["llm_type"]

        for param in llm_config_class.__fields__.keys():
            if param in args:
                llm_params[param] = args.pop(param)
    else:
        llm_params = args.pop("llm_params")

    args["llm_params"] = llm_config_class(**llm_params)

    return args


class RAGHandler(BaseMLEngine):
    """
    RAGHandler is a MindsDB integration with Writer API LLMs that allows users to run question answering
    on their data by providing a question.

    The User is able to provide data that provides context for the questions, see create() method for more details.

    """

    name = "rag"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generative = True

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if "using" not in args:
            raise Exception(
                "Writer engine requires a USING clause! Refer to its documentation for more details."
            )

    def create(
        self,
        target: str,
        df: pd.DataFrame = None,
        args: Optional[Dict] = None,
    ):
        """
        Dispatch is running embeddings and storing in a VectorDB, unless user already has embeddings persisted
        """
        # get api key from user input on create ML_ENGINE or create MODEL
        args = args["using"]

        ml_engine_args = self.engine_storage.get_connection_args()

        # for a model created with USING, only get api for that specific llm type
        args.update({k: v for k, v in ml_engine_args.items() if args["llm_type"] in k})

        input_args = build_llm_params(args)

        args = RAGHandlerParameters(**input_args)

        # create folder for vector store to persist embeddings or load from existing folder
        args.vector_store_storage_path = self.engine_storage.folder_get(
            args.vector_store_folder_name, update=True if args.run_embeddings else False
        )

        if args.run_embeddings:
            if "context_columns" not in args and df:

                # if no context columns provided, use all columns in df
                logger.info("No context columns provided, using all columns in df")
                args.context_columns = df.columns.tolist()

            if "embeddings_model_name" not in args:
                logger.info(
                    f"No embeddings model provided in query, using default model: {DEFAULT_EMBEDDINGS_MODEL}"
                )

            ingestor = Ingestor(args=args, df=df)
            ingestor.embeddings_to_vectordb()

        else:
            # Note this should only be run if run_embeddings is false

            logger.info("Skipping embeddings and ingestion into Chroma VectorDB")

        export_args = args.dict(exclude={"llm_params"})
        # 'callbacks' aren't json serializable, we do this to avoid errors
        export_args["llm_params"] = args.llm_params.dict(exclude={"callbacks"})

        # for mindsdb cloud, store data in shared file system
        # for cloud version of mindsdb to make it be usable by all mindsdb nodes
        self.engine_storage.folder_sync(args.vector_store_folder_name)

        self.model_storage.json_set("args", export_args)

    def predict(self, df: pd.DataFrame = None, args: dict = None):
        """
        Dispatch is performed depending on the underlying model type. Currently, only question answering
        is supported.
        """

        input_args = build_llm_params(self.model_storage.json_get("args"))

        args = RAGHandlerParameters(**input_args)

        args.vector_store_storage_path = self.engine_storage.folder_get(
            args.vector_store_folder_name, update=False
        )

        # get question answering results
        question_answerer = QuestionAnswerer(args=args)

        # get question from sql query
        # e.g. where question = 'What is the capital of France?'
        response = question_answerer(df["question"].tolist()[0])

        return pd.DataFrame(response)

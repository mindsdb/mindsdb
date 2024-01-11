from typing import Dict, Optional

import pandas as pd

from mindsdb.integrations.handlers.rag_handler.ingest import RAGIngestor
from mindsdb.integrations.handlers.rag_handler.rag import RAGQuestionAnswerer
from mindsdb.integrations.handlers.rag_handler.settings import (
    DEFAULT_EMBEDDINGS_MODEL,
    RAGHandlerParameters,
    build_llm_params,
)
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log

# these require no additional arguments

logger = log.getLogger(__name__)


class RAGHandler(BaseMLEngine):
    """
    RAGHandler is a MindsDB integration with supported LLM APIs allows users to run question answering
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
                "RAG engine requires a USING clause! Refer to its documentation for more details."
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
        self._get_api_key(args)
        self._get_input_args(args)
        self._create_vector_store(args, df)
        self._run_embeddings(args, df)
        self._export_args(args)
        self._sync_vector_store(args)

    def _get_api_key(self, args):
        ml_engine_args = self.engine_storage.get_connection_args()
        args.update({k: v for k, v in ml_engine_args.items() if args["llm_type"] in k})

    def _get_input_args(self, args):
        input_args = build_llm_params(args)
        args = RAGHandlerParameters(**input_args)

    def _create_vector_store(self, args, df):
        args.vector_store_storage_path = self.engine_storage.folder_get(
            args.vector_store_folder_name
        )

    def _run_embeddings(self, args, df):
        if args.run_embeddings:
            self._handle_context_columns(args, df)
            self._handle_embeddings_model(args)
            self._run_embeddings_and_store(args, df)
        else:
            logger.info("Skipping embeddings and ingestion into Chroma VectorDB")

    def _handle_context_columns(self, args, df):
        if "context_columns" not in args and df is not None:
            logger.info("No context columns provided, using all columns in df")
            args.context_columns = df.columns.tolist()

    def _handle_embeddings_model(self, args):
        if "embeddings_model_name" not in args:
            logger.info(
                f"No embeddings model provided in query, using default model: {DEFAULT_EMBEDDINGS_MODEL}"
            )

    def _run_embeddings_and_store(self, args, df):
        if df is not None or args.url is not None:
            ingestor = RAGIngestor(args=args, df=df)
            ingestor.embeddings_to_vectordb()

    def _export_args(self, args):
        export_args = args.dict(exclude={"llm_params"})
        export_args["llm_params"] = args.llm_params.dict(exclude={"callbacks"})
        self.model_storage.json_set("args", export_args)

    def _sync_vector_store(self, args):
        self.engine_storage.folder_sync(args.vector_store_folder_name)

    def update(self, args) -> None:

        # build llm params from user input args in update query
        updated_args = build_llm_params(args["using"], update=True)

        # get current model args
        current_model_args = self.model_storage.json_get("args")["using"]

        # update current args with new args
        current_model_args.update(updated_args)

        # validate updated args are valid
        RAGHandlerParameters(**build_llm_params(current_model_args))

        # if valid, update model args
        self.model_storage.json_set("args", current_model_args)

    def predict(self, df: pd.DataFrame = None, args: dict = None):
        """
        Dispatch is performed depending on the underlying model type. Currently, only question answering
        is supported.
        """

        input_args = build_llm_params(self.model_storage.json_get("args"))

        args = RAGHandlerParameters(**input_args)

        args.vector_store_storage_path = self.engine_storage.folder_get(
            args.vector_store_folder_name
        )

        # get question answering results
        question_answerer = RAGQuestionAnswerer(args=args)

        # get question from sql query
        # e.g. where question = 'What is the capital of France?'
        response = question_answerer(df["question"].tolist()[0])

        return pd.DataFrame(response)

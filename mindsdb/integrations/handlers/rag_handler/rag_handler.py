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


logger = log.getLogger(__name__)

logger.warning("\nThe RAG handler has been deprecated and is no longer being actively supported. \n"
               "It will be fully removed in v24.8.x.x, "
               "for RAG workflows, please migrate to "
               "Agents + Retrieval skill. \n"
               "Example usage can be found here: \n"
               "https://github.com/mindsdb/mindsdb_python_sdk/blob/staging/examples"
               "/using_agents_with_retrieval.py")


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
        # get api key from user input on create ML_ENGINE or create MODEL
        args = args["using"]

        ml_engine_args = self.engine_storage.get_connection_args()

        # for a model created with USING, only get api for that specific llm type
        args.update({k: v for k, v in ml_engine_args.items() if args["llm_type"] in k})

        input_args = build_llm_params(args)

        args = RAGHandlerParameters(**input_args)

        # create folder for vector store to persist embeddings or load from existing folder
        args.vector_store_storage_path = self.engine_storage.folder_get(
            args.vector_store_folder_name
        )

        if args.run_embeddings:
            if "context_columns" not in args and df is not None:
                # if no context columns provided, use all columns in df
                logger.info("No context columns provided, using all columns in df")
                args.context_columns = df.columns.tolist()

            if "embeddings_model_name" not in args:
                logger.info(
                    f"No embeddings model provided in query, using default model: {DEFAULT_EMBEDDINGS_MODEL}"
                )

            if df is not None or args.url is not None:
                # if user provides a dataframe or url, run embeddings and store in vector store

                ingestor = RAGIngestor(args=args, df=df)
                ingestor.embeddings_to_vectordb()

        else:
            # Note this should only be run if run_embeddings is false or if no data is provided in query
            logger.info("Skipping embeddings and ingestion into Chroma VectorDB")

        export_args = args.dict(exclude={"llm_params"})
        # 'callbacks' aren't json serializable, we do this to avoid errors
        export_args["llm_params"] = args.llm_params.dict(exclude={"callbacks"})

        # for mindsdb cloud, store data in shared file system
        # for cloud version of mindsdb to make it be usable by all mindsdb nodes
        self.engine_storage.folder_sync(args.vector_store_folder_name)

        self.model_storage.json_set("args", export_args)

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
        response = question_answerer(df[args.input_column].tolist()[0])

        return pd.DataFrame(response)

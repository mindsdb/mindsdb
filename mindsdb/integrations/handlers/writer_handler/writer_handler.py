from typing import Dict, Optional

import pandas as pd

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log

from .ingest import Ingestor
from .question_answer import QuestionAnswerer
from .settings import (
    DEFAULT_EMBEDDINGS_MODEL,
    USER_DEFINED_MODEL_PARAMS,
    ModelParameters,
)

# these require no additional arguments

logger = log.getLogger(__name__)

class WriterHandler(BaseMLEngine):
    """
    WriterHandler is a MindsDB integration with Writer API LLMs that allows users to run question answering
    on their data by providing a question.

    The User is able to provide data that provides context for the questions, see create() method for more details.

    """

    name = "writer"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generative = True

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if "using" not in args:
            raise Exception(
                "Writer engine requires a USING clause! Refer to its documentation for more details."
            )
        else:
            args = args["using"]

        if "prompt_template" not in args:
            raise Exception("Please provide a `prompt_template` for this engine.")

    def create(
        self,
        target: str,
        df: pd.DataFrame = pd.DataFrame(),
        args: Optional[Dict] = None,
    ):
        """
        Dispatch is running embeddings and storing in Chroma VectorDB, unless user already has embeddings persisted
        """
        args = args["using"]

        if not df.empty and args["run_embeddings"]:
            if "context_columns" not in args:
                # if no context columns provided, use all columns in df
                args["context_columns"] = df.columns.tolist()

            if "embeddings_model_name" not in args:
                logger.info(
                    f"No embeddings model provided in query, using default model: {DEFAULT_EMBEDDINGS_MODEL}"
                )

            chromadb_folder_name = args["chromadb_folder_name"]
            # create folder for chromadb to persist embeddings
            args["chromadb_storage_path"] = self.engine_storage.folder_get(
                chromadb_folder_name
            )
            ingestor = Ingestor(df=df, args=args)
            ingestor.embeddings_to_vectordb()

            # for mindsdb cloud, store data in shared file system for cloud version of mindsdb to make it be usable by all mindsdb nodes
            self.engine_storage.folder_sync(chromadb_folder_name)

        else:
            logger.info("Skipping embeddings and ingestion into Chroma VectorDB")

        self.model_storage.json_set("args", args)

    def predict(self, df: pd.DataFrame = None, args: dict = None):
        """
        Dispatch is performed depending on the underlying model type. Currently, only question answering
        is supported.
        """

        # get model parameters if defined by user - else use default values

        args = self.model_storage.json_get("args")
        args["chromadb_storage_path"] = self.engine_storage.folder_get(
            args["chromadb_folder_name"], update=False
        )

        user_defined_model_params = list(
            filter(lambda x: x in args, USER_DEFINED_MODEL_PARAMS)
        )
        args["model_params"] = {
            model_param: args[model_param] for model_param in user_defined_model_params
        }
        model_parameters = ModelParameters(**args["model_params"])

        # get question answering results

        question_answerer = QuestionAnswerer(
            args=args, model_parameters=model_parameters
        )

        # get question from sql query e.g. where question = 'What is the capital of France?'
        question_answerer.query(df["question"].tolist()[0])

        # return results

        return question_answerer.results_df

import json
import random
from typing import Dict, Optional

import pandas as pd
from datasets import load_dataset
from sklearn.metrics import average_precision_score

from mindsdb.integrations.handlers.writer_handler.ingest import Ingestor
from mindsdb.integrations.handlers.writer_handler.question_answer import (
    QuestionAnswerer,
)
from mindsdb.integrations.handlers.writer_handler.settings import (
    DEFAULT_EMBEDDINGS_MODEL,
    SUPPORTED_VECTOR_STORES,
    USER_DEFINED_WRITER_LLM_PARAMS,
    WriterHandlerParameters,
)
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities.log import get_log

# these require no additional arguments

logger = get_log(logger_name=__name__)


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

        vector_store_name = args.get("vector_store_name", "chroma")

        if vector_store_name not in SUPPORTED_VECTOR_STORES:
            raise ValueError(
                f"currently we only support {', '.join(str(v) for v in SUPPORTED_VECTOR_STORES)} vector store"
            )

    def extract_llm_params(self, args):
        """extract llm params from input query args"""

        llm_params = {}
        for param in USER_DEFINED_WRITER_LLM_PARAMS:
            if param in args:
                llm_params[param] = args.pop(param)

        args["llm_params"] = llm_params

        return args

    def create(
        self,
        target: str,
        df: pd.DataFrame = pd.DataFrame(),
        args: Optional[Dict] = None,
    ):
        """
        Dispatch is running embeddings and storing in a VectorDB, unless user already has embeddings persisted
        """

        input_args = self.extract_llm_params(args["using"])

        args = WriterHandlerParameters(**input_args)

        if not df.empty and args.run_embeddings:
            if "context_columns" not in args:
                # if no context columns provided, use all columns in df
                args.context_columns = df.columns.tolist()

            if "embeddings_model_name" not in args:
                logger.info(
                    f"No embeddings model provided in query, using default model: {DEFAULT_EMBEDDINGS_MODEL}"
                )

            # create folder for vector store to persist embeddings
            args.vector_store_storage_path = self.engine_storage.folder_get(
                args.vector_store_folder_name
            )

            ingestor = Ingestor(df=df, args=args)
            ingestor.embeddings_to_vectordb()

            # for mindsdb cloud, store data in shared file system
            # for cloud version of mindsdb to make it be usable by all mindsdb nodes
            self.engine_storage.folder_sync(args.vector_store_folder_name)

        else:
            logger.info("Skipping embeddings and ingestion into Chroma VectorDB")

        export_args = args.dict(exclude={"llm_params"})
        # 'callbacks' aren't json serializable, we do this to avoid errors
        export_args["llm_params"] = args.llm_params.dict(exclude={"callbacks"})

        self.model_storage.json_set("args", export_args)

    def predict(self, df: pd.DataFrame = None, args: dict = None):
        """
        Dispatch is performed depending on the underlying model type. Currently, only question answering
        is supported.
        """

        # get model parameters if defined by user - else use default values

        input_args = self.model_storage.json_get("args")
        args = WriterHandlerParameters(**input_args)

        args.vector_store_storage_path = self.engine_storage.folder_get(
            args.vector_store_folder_name, update=False
        )

        # user_defined_model_params = list(
        #     filter(lambda x: x in args, USER_DEFINED_WRITER_LLM_PARAMS)
        # )
        # args["llm_params"] = {
        #     model_param: args[model_param] for model_param in user_defined_model_params
        # }
        #
        # model_parameters = WriterLLMParameters(**args["llm_params"])

        # get question answering results
        question_answerer = QuestionAnswerer(args=args)

        # get question from sql query
        # e.g. where question = 'What is the capital of France?'
        response = question_answerer.query(df["question"].tolist()[0])

        return pd.DataFrame(response)

    # todo evaluation method on standardised 100-200 sample of squad_v2 - probs move to mindsdb_evaluator library
    def evaluate(self):

        dataset_squad_v2 = load_dataset("squad_v2")
        val_df = pd.DataFrame(dataset_squad_v2["validation"])
        val_df["answers"] = val_df["answers"].apply(json.dumps)

        random.seed(53)

        sample_size = 100
        sample_queries = random.sample(val_df["question"].tolist(), sample_size)

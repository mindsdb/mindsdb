import json
import random
from typing import Dict, Optional

import pandas as pd

from mindsdb.integrations.handlers.writer_handler.evaluator import (
    accuracy,
    calculate_cosine_similarity,
)
from mindsdb.integrations.handlers.writer_handler.ingestor import Ingestor
from mindsdb.integrations.handlers.writer_handler.question_answer import (
    QuestionAnswerer,
)
from mindsdb.integrations.handlers.writer_handler.settings import (
    DEFAULT_EMBEDDINGS_MODEL,
    EVAL_COLUMN_NAMES,
    USER_DEFINED_WRITER_LLM_PARAMS,
    WriterHandlerParameters,
)
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.utilities.datasets.dataset import (
    load_dataset,
    validate_dataframe,
)
from mindsdb.utilities.log import get_log

# these require no additional arguments

logger = get_log(logger_name=__name__)


def extract_llm_params(args):
    """extract llm params from input query args"""

    llm_params = {}
    for param in USER_DEFINED_WRITER_LLM_PARAMS:
        if param in args:
            llm_params[param] = args.pop(param)

    args["llm_params"] = llm_params

    return args


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

    def create(
        self,
        target: str,
        df: pd.DataFrame = pd.DataFrame(),
        args: Optional[Dict] = None,
    ):
        """
        Dispatch is running embeddings and storing in a VectorDB, unless user already has embeddings persisted
        """

        input_args = extract_llm_params(args["using"])
        args = WriterHandlerParameters(**input_args)

        # create folder for vector store to persist embeddings
        args.vector_store_storage_path = self.engine_storage.folder_get(
            args.vector_store_folder_name
        )

        if not df.empty and args.run_embeddings and not args.evaluation:
            if "context_columns" not in args:
                # if no context columns provided, use all columns in df
                logger.info("No context columns provided, using all columns in df")
                args.context_columns = df.columns.tolist()

            if "embeddings_model_name" not in args:
                logger.info(
                    f"No embeddings model provided in query, using default model: {DEFAULT_EMBEDDINGS_MODEL}"
                )

            ingestor = Ingestor(df=df, args=args)
            ingestor.embeddings_to_vectordb()

        else:
            logger.info("Skipping embeddings and ingestion into Chroma VectorDB")

        if args.evaluation:
            args = self.evaluate(args)

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

        # get model parameters if defined by user - else use default values

        input_args = self.model_storage.json_get("args")
        args = WriterHandlerParameters(**input_args)

        args.vector_store_storage_path = self.engine_storage.folder_get(
            args.vector_store_folder_name, update=False
        )

        # get question answering results
        question_answerer = QuestionAnswerer(args=args)

        # get question from sql query
        # e.g. where question = 'What is the capital of France?'
        response = question_answerer.query(df["question"].tolist()[0])

        return pd.DataFrame(response)

    def evaluate(self, args: WriterHandlerParameters):

        if isinstance(args.evaluate_dataset, pd.DataFrame):
            evaluate_df = validate_dataframe(args.evaluate_dataset, EVAL_COLUMN_NAMES)
        else:
            evaluate_df = load_dataset(
                ml_task_type="question_answering", dataset_name=args.evaluate_dataset
            )

        ingestor = Ingestor(df=evaluate_df, args=args)
        ingestor.embeddings_to_vectordb()

        question_answerer = QuestionAnswerer(args=args)

        evaluate_df["retrieved_context"] = evaluate_df.apply(
            lambda x: question_answerer.extract_returned_text(x["question"]), axis=1
        )

        # embed context and retrieved context
        context_embeddings = question_answerer.embeddings_model.embed_documents(
            evaluate_df["context"].tolist()
        )
        retrieved_context_embeddings = (
            question_answerer.embeddings_model.embed_documents(
                evaluate_df["retrieved_context"].tolist()
            )
        )

        # calculate cosine similarity for each context and retrieved context pair for a given question
        cosine_similarities = [
            calculate_cosine_similarity(context_embedding, retrieved_context_embedding)
            for context_embedding, retrieved_context_embedding in zip(
                context_embeddings, retrieved_context_embeddings
            )
        ]

        evaluate_df["cosine_similarity"] = cosine_similarities

        # calculate accuracy
        evaluate_df["accuracy"] = evaluate_df.apply(
            lambda x: accuracy(
                x["cosine_similarity"], threshold=args.accuracy_threshold
            ),
            axis=1,
        )

        _accuracy = evaluate_df["accuracy"].mean()
        _cosine_similarity = evaluate_df["cosine_similarity"].mean()

        evaluation_metrics = {
            "accuracy": _accuracy,
            "cosine_similarity": _cosine_similarity,
        }
        logger.info(f"Accuracy: {_accuracy}")
        logger.info(f"Cosine Similarity: {_cosine_similarity}")

        self.model_storage.json_set("evaluation_metrics", evaluation_metrics)
        self.model_storage.json_set("evaluation_df", evaluate_df.to_dict())

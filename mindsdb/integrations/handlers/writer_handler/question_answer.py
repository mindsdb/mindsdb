from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, List

import pandas as pd
from langchain import PromptTemplate
from langchain.chains import RetrievalQA
from langchain.docstore.document import Document
from langchain.llms import Writer
from langchain.schema import format_document

from mindsdb.utilities.log import get_log

from .settings import (
    DEFAULT_EMBEDDINGS_MODEL,
    ModelParameters,
    get_chroma_client,
    get_retriever,
)

logger = get_log(logger_name=__name__)


# class LangChainQuestionAnswerer:
#     def __init__(self, args: dict, model_parameters: ModelParameters):
#         self.output_data = defaultdict(list)
#
#         self.embeddings_model_name = args.get(
#             "embeddings_model_name", DEFAULT_EMBEDDINGS_MODEL
#         )
#         self.persist_directory = args["chromadb_storage_path"]
#
#         self.collection_name = args.get("collection_name", "langchain")
#
#         llm = Writer(**model_parameters.dict())
#
#         retriever = get_retriever(
#             embeddings_model_name=self.embeddings_model_name,
#             persist_directory=self.persist_directory,
#             collection_name=self.collection_name,
#         )
#
#         # Custom prompt
#         prompt = args.get("prompt_template", None)
#
#         if prompt:
#             logger.info(f"Using custom prompt: {prompt}")
#         else:
#             logger.info(f"No custom prompt provided, using default prompt for 'stuff' QA chain")
#
#         # Create template
#         self.prompt_template = PromptTemplate(
#             input_variables=["question", "context"],
#             template=prompt
#         ) if prompt else None
#
#         self.qa = RetrievalQA.from_chain_type(
#             llm=llm,
#             chain_type="stuff",
#             retriever=retriever,
#             return_source_documents=True,
#             callbacks=model_parameters.callbacks,
#             chain_type_kwargs={
#                 "prompt": self.prompt_template
#             }
#         )
#
#     @property
#     def results_df(self):
#         return pd.DataFrame(self.output_data)
#
#     def query(self, input_query: str):
#         logger.debug(f"Querying: {input_query}")
#
#         res = self.qa(input_query)
#
#
#         self.output_data["question"].append(input_query)
#         self.output_data["answer"].append(res["result"])
#
#         sources = defaultdict(list)
#
#         for idx, document in enumerate(res["source_documents"]):
#             sources["sources_document"].append(document.metadata["source"])
#             sources["column"].append(document.metadata.get("column"))
#             sources["sources_row"].append(document.metadata.get("row"))
#             sources["sources_content"].append(document.page_content)
#
#         self.output_data["source_documents"].append(dict(sources))
#
#     def output(self, output_path: Path):
#         # output results
#         date_time_now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
#
#         self.results_df.to_csv(
#             output_path / f"answers-{date_time_now}-output.csv", index=False
#         )
#


class QuestionAnswerer:
    def __init__(self, args: dict, model_parameters: ModelParameters):

        self.output_data = defaultdict(list)

        self.embeddings_model_name = args.get(
            "embeddings_model_name", DEFAULT_EMBEDDINGS_MODEL
        )
        self.persist_directory = args["chromadb_storage_path"]

        self.collection_name = args.get("collection_name", "langchain")

        self.vector_store_map = {
            "chroma": get_chroma_client(
                embeddings_model_name=self.embeddings_model_name,
                persist_directory=self.persist_directory,
                collection_name=self.collection_name,
            ),
        }

        self.prompt_template = args["prompt_template"]

        self.vector_store_name = args.get("vector_store_name", "chroma")

        self.vector_store = self.vector_store_map[self.vector_store_name]

        self.llm = Writer(**model_parameters.dict())

    def _prepare_prompt(self, vector_store_response, question):

        # todo ensure contexts don't exceed max length
        # todo maybe use a selector model, otherwise truncate contexts
        context = [doc.page_content for doc in vector_store_response]

        combined_context = " ".join(context)

        return self.prompt_template.format(question=question, context=combined_context)

    def query(self, question: str):
        logger.debug(f"Querying: {question}")

        vector_store_response = self.vector_store.similarity_search(query=question)

        formatted_prompt = self._prepare_prompt(vector_store_response, question)

        llm_response = self.llm(prompt=formatted_prompt)

        result = defaultdict(list)

        result["question"].append(question)
        result["answer"].append(llm_response["result"])

        sources = defaultdict(list)

        for idx, document in enumerate(llm_response["source_documents"]):
            sources["sources_document"].append(document.metadata["source"])
            sources["column"].append(document.metadata.get("column"))
            sources["sources_row"].append(document.metadata.get("row"))
            sources["sources_content"].append(document.page_content)

        result["source_documents"].append(dict(sources))

        return result

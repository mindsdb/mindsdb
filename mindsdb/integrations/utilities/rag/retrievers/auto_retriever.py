from typing import List
import json


from langchain.retrievers.self_query.base import SelfQueryRetriever

import pandas as pd

from mindsdb.integrations.utilities.rag.retrievers.base import BaseRetriever

from mindsdb.integrations.utilities.rag.utils import documents_to_df
from mindsdb.integrations.utilities.rag.vector_store import VectorStoreOperator

from mindsdb.integrations.utilities.rag.settings import RAGPipelineModel


class AutoRetriever(BaseRetriever):
    """
    AutoRetrieval is a class that uses langchain to extract metadata from a List of Document and query it using self retrievers.

    """

    def __init__(
            self,
            config: RAGPipelineModel
    ):
        """

        :param config: RAGPipelineModel


        """

        self.documents = config.documents
        self.content_column_name = config.content_column_name
        self.vectorstore = config.vector_store
        self.filter_columns = config.auto_retriever_filter_columns
        self.document_description = config.dataset_description
        self.llm = config.llm
        self.embedding_model = config.embedding_model
        self.prompt_template = config.retriever_prompt_template
        self.cardinality_threshold = config.cardinality_threshold

    def _get_low_cardinality_columns(self, data: pd.DataFrame):
        """
        Given a dataframe, return a list of columns with low cardinality if datatype is not bool.
        :return:
        """
        low_cardinality_columns = []
        columns = data.columns if self.filter_columns is None else self.filter_columns
        for column in columns:
            if data[column].dtype != "bool":
                if data[column].nunique() < self.cardinality_threshold:
                    low_cardinality_columns.append(column)
        return low_cardinality_columns

    def get_metadata_field_info(self):
        """
        Given a list of Document, use llm to extract metadata from it.
        :return:
        """

        def _alter_description(data: pd.DataFrame,
                               low_cardinality_columns: list,
                               result: List[dict]):
            """
            For low cardinality columns, alter the description to include the sorted valid values.
            :param data: pd.DataFrame
            :param low_cardinality_columns: list
            :param result: List[dict]
            """
            for column_name in low_cardinality_columns:
                valid_values = sorted(data[column_name].unique())
                for entry in result:
                    if entry["name"] == column_name:
                        entry["description"] += f". Valid values: {valid_values}"

        data = documents_to_df(
            self.content_column_name,
            self.documents
        )

        prompt = self.prompt_template.format(dataframe=data.head().to_json(),
                                             description=self.document_description)
        result: List[dict] = json.loads(self.llm.invoke(input=prompt).content)

        _alter_description(
            data,
            self._get_low_cardinality_columns(data),
            result
        )

        return result

    def get_vectorstore(self):
        """

        :return:
        """
        return VectorStoreOperator(vector_store=self.vectorstore,
                                   documents=self.documents,
                                   embedding_model=self.embedding_model).vector_store

    def as_runnable(self) -> BaseRetriever:
        """
        return the self-query retriever
        :return:
        """
        vectorstore = self.get_vectorstore()

        return SelfQueryRetriever.from_llm(
            llm=self.llm,
            vectorstore=vectorstore,
            document_contents=self.document_description,
            metadata_field_info=self.get_metadata_field_info(),
            verbose=True
        )

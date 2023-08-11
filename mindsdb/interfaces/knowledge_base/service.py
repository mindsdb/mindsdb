"""
Tasks related to the knowledge base
"""

from enum import Enum
from typing import Iterable, Optional

import pandas as pd
from pydantic import BaseModel

from mindsdb.integrations.libs.base import BaseMLEngine, VectorStoreHandler
from mindsdb.interfaces.storage.db import KnowledgeBase


class FilterOP(Enum):
    """
    Filter operators
    """

    EQUAL = "="
    NOT_EQUAL = "!="
    GREATER_THAN = ">"
    GREATER_THAN_OR_EQUAL = ">="
    LESS_THAN = "<"
    LESS_THAN_OR_EQUAL = "<="
    IN = "IN"
    NOT_IN = "NOT IN"
    LIKE = "LIKE"
    NOT_LIKE = "NOT LIKE"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"


class FilterCondition(BaseModel):
    """
    Filter condition
    """

    column: str
    operator: FilterOP
    value: str


class KnowledgeBaseService:
    def __init__(self, kb: KnowledgeBase) -> None:
        self.kb = kb
        # provision the embedding model and vector database
        # to be used across the service
        self.vector_database_handler: VectorStoreHandler = (
            self._get_vector_database_handler()
        )
        self.embedding_model_handler: BaseMLEngine = self._get_embedding_model_handler()
        self.table_name = kb.vector_database_table_name

    def _get_embedding_model_handler(self):
        """
        Get the embedding model handler given the integration id
        """
        ...

    def _get_vector_database_handler(self):
        """
        Get the vector database handler given the integration id
        """
        ...

    def _get_embedding_column(self) -> str:
        # get the column that stores the embedding vectors in the
        # returned dataframe from the embedding model
        # get the args from the embedding model
        df = self.embedding_model_handler.describe(attribute="args")
        # check if the dataframe contains two columns key and value
        if "key" in df.columns and "value" in df.columns:
            df = df.set_index("key")
            if "target" in df.index:
                return df.loc["target"]["value"]
            elif "output_column" in df.index:
                return df.loc["output_column"]["value"]
            else:
                raise Exception(
                    "Could not find target or output_column in the embedding model args"
                )
        else:
            # try to get the args object from the handler directly
            args = self.embedding_model_handler.model_storage.json_get("args")
            if "target" in args:
                return args["target"]
            elif "output_column" in args:
                return args["output_column"]
            else:
                raise Exception(
                    "Could not find target or output_column in the embedding model args"
                )

    def select(
        self,
        search_texts: Optional[Iterable[FilterCondition]] = None,
        metadata_filters: Optional[Iterable[FilterCondition]] = None,
        limit: int = 10,
    ) -> pd.DataFrame:
        """
        Select all the rows from the knowledge base that match the given filters
        """
        if search_texts is not None:
            # construct a dataframe to pass to the embedding model
            data = {}
            for search_text in search_texts:
                data[search_text.column] = [search_text.value]

            df = pd.DataFrame(data)

            # get a search vector from the embedding model
            result_df = self.embedding_model_handler.predict(df)
            vector_column_name = self._get_embedding_column()
            # check if the vector column exists in the dataframe
            if vector_column_name not in result_df.columns:
                raise Exception(
                    f"Could not find vector column {vector_column_name} in the result dataframe"
                )

            # get the vector column
            search_vector = result_df.iloc[0][vector_column_name]
        else:
            search_vector = None

        # pass the search vector to the vector database to get the results
        result = self.vector_database_handler.select(
            table_name=self.table_name,
            search_vector=search_vector,
            metadata_filters=metadata_filters,
            limit=limit,
        )

        return result

    def insert(self, df: pd.DataFrame, columns: Iterable[str]) -> None:
        """
        Insert the given data into the knowledge base
        """

        # pass the dataframe to the embedding model to get the embedding vectors
        result_df = self.embedding_model_handler.predict(df)
        vector_column_name = self._get_embedding_column()
        # check if the vector column exists in the dataframe
        if vector_column_name not in result_df.columns:
            raise Exception(
                f"Could not find vector column {vector_column_name} in the result dataframe"
            )

        # get the vector column
        vector_col = result_df[vector_column_name]
        # add the vector column to the dataframe
        df["embeddings"] = vector_col

        # pass the dataframe + embedding vectors to the vector database to store the vectors
        self.vector_database_handler.insert(
            table_name=self.table_name,
            df=df,
            columns=columns,
        )

    def update(self, df: pd.DataFrame, columns: Iterable[str]) -> None:
        """
        Update the given data in the knowledge base
        """
        return self.insert(
            df, columns
        )  # this assumes the vector database will handle the upsert

    def delete(
        self, metadata_filters: Optional[Iterable[FilterCondition]] = None
    ) -> None:
        """
        Delete all the rows from the knowledge base that match the given filters
        """
        # pass the metadata filters to the vector database to delete the vectors
        self.vector_database_handler.delete(
            table_name=self.table_name, filters=metadata_filters
        )

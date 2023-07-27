import re
from functools import lru_cache
from typing import List, Union

import pandas as pd
import torch
from langchain.document_loaders import DataFrameLoader
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter

from mindsdb.utilities import log


def extract_collection_name(sql_query):
    # Regular expression pattern to match the collection name from the FROM clause
    pattern = r"FROM\s+\w+\.(\w+)"

    # Find the table name using regular expression
    match = re.search(pattern, sql_query, re.IGNORECASE)

    # If match found then extract the table name

    if match:
        table_name = match.group(1)
        where_condition = None

        # Regular expression pattern to match the where condition from the WHERE clause
        pattern = r"WHERE\s+(.*)"
        # Find the where condition using regular expression
        match = re.search(pattern, sql_query, re.IGNORECASE)
        if match:
            where_condition = match.group(1)

        return table_name, where_condition
    else:
        return None, None


def get_metadata_filter(metadata_filter: str):
    """convert metadata filter string to dict"""
    dict_from_string = {}

    for item in metadata_filter.split(","):
        if ":" in item:
            key, value = item.split(":")
            dict_from_string[key] = value
        else:
            key = item
            value = True
            dict_from_string[key] = value

    return dict_from_string


# Todo move all below classes methods to dataprep ML
class DfLoader(DataFrameLoader):

    """
    override the load method of langchain.document_loaders.DataFrameLoaders to ignore rows with 'None' values
    """

    def __init__(self, data_frame: pd.DataFrame, page_content_column: str):
        super().__init__(data_frame=data_frame, page_content_column=page_content_column)
        self._data_frame = data_frame
        self._page_content_column = page_content_column

    def load(self) -> List[Document]:
        """Loads the dataframe as a list of documents"""
        documents = []
        for n_row, frame in self._data_frame[self._page_content_column].iteritems():
            if pd.notnull(frame):
                # ignore rows with None values
                column_name = self._page_content_column

                document_contents = frame

                documents.append(
                    Document(
                        page_content=document_contents,
                        metadata={
                            "source": "dataframe",
                            "row": n_row,
                            "column": column_name,
                        },
                    )
                )
        return documents


def df_to_documents(
    df: pd.DataFrame, page_content_columns: Union[List[str], str]
) -> List[Document]:
    """Converts a given dataframe to a list of documents"""
    documents = []

    if isinstance(page_content_columns, str):
        page_content_columns = [page_content_columns]

    for _, page_content_column in enumerate(page_content_columns):
        if page_content_column not in df.columns.tolist():
            raise ValueError(
                f"page_content_column {page_content_column} not in dataframe columns"
            )

        loader = DfLoader(data_frame=df, page_content_column=page_content_column)
        documents.extend(loader.load())

    return documents


def split_documents(df, columns):
    # Load documents and split in chunks
    log.logger.info(f"Loading documents from input data")

    text_splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)
    documents = df_to_documents(df=df, page_content_columns=columns)
    texts = text_splitter.split_documents(documents)
    log.logger.info(f"Loaded {len(documents)} documents from input data")
    log.logger.info(f"Split into {len(texts)} chunks of text (max. 500 tokens each)")

    return texts


@lru_cache()
def load_embeddings_model(embeddings_model_name):
    try:
        model_kwargs = {"device": "gpu" if torch.cuda.is_available() else "cpu"}
        embedding_model = HuggingFaceEmbeddings(
            model_name=embeddings_model_name, model_kwargs=model_kwargs
        )
    except ValueError:
        raise ValueError(
            f"The {embeddings_model_name}  is not supported, please select a valid option from Hugging Face Hub!"
        )
    return embedding_model


def documents_to_df(documents: list):
    """Converts a list of documents to a dataframe"""
    df = pd.DataFrame()
    df["page_content"] = [document.page_content for document in documents]
    df["metadata"] = [document.metadata for document in documents]

    return df

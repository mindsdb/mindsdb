from functools import lru_cache
from typing import List, Union

import pandas as pd
import torch
from chromadb import Settings
from langchain.callbacks.streaming_stdout import StreamingStdOutCallbackHandler
from langchain.docstore.document import Document
from langchain.document_loaders import DataFrameLoader
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.vectorstores import Chroma
from pydantic import BaseModel

DEFAULT_EMBEDDINGS_MODEL = "sentence-transformers/all-mpnet-base-v2"
USER_DEFINED_MODEL_PARAMS = (
    "model_name",
    "max_tokens",
    "temperature",
    "top_p",
    "stop",
    "best_of",
    "verbose",
    "writer_org_id",
    "writer_api_key",
    "base_url",
)


class ModelParameters(BaseModel):
    """Model parameters for the Writer LLM API interface"""

    writer_api_key: str = None
    writer_org_id: str = None
    base_url: str = None
    model_id: str = "palmyra-x"
    callbacks: List[StreamingStdOutCallbackHandler] = [StreamingStdOutCallbackHandler()]
    max_tokens: int = 1024
    temperature: float = 0.0
    top_p: float = 1
    stop: List[str] = []
    best_of: int = 5
    verbose: bool = False

    class Config:
        arbitrary_types_allowed = True


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


def load_chroma(
    embeddings_model_name, persist_directory, collection_name, chroma_settings
):
    return Chroma(
        collection_name=collection_name,
        persist_directory=persist_directory,
        embedding_function=load_embeddings_model(embeddings_model_name),
        client_settings=chroma_settings,
    )


def get_chroma_settings(persist_directory):
    return Settings(
        chroma_db_impl="duckdb+parquet",
        persist_directory=persist_directory,
        anonymized_telemetry=False,
    )


def get_retriever(embeddings_model_name, persist_directory, collection_name):
    chroma_settings = get_chroma_settings(persist_directory)
    db = load_chroma(
        embeddings_model_name, persist_directory, collection_name, chroma_settings
    )
    retriever = db.as_retriever()
    return retriever

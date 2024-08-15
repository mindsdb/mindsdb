from typing import List

import pandas as pd
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings


def df_to_documents(df: pd.DataFrame, content_column_name: str) -> List[Document]:
    """
    Given a dataframe, convert it to a list of documents.

    :param df: pd.DataFrame
    :param content_column_name: str

    :return: List[Document]
    """
    documents = []
    for _, row in df.iterrows():
        metadata = row.to_dict()
        page_content = metadata.pop(content_column_name)
        documents.append(Document(page_content=page_content, metadata=metadata))
    return documents


def documents_to_df(content_column_name: str,
                    documents: List[Document],
                    embedding_model: Embeddings = None,
                    with_embeddings: bool = False) -> pd.DataFrame:
    """
    Given a list of documents, convert it to a dataframe.

    :param content_column_name: str
    :param documents: List[Document]
    :param embedding_model: Embeddings
    :param with_embeddings: bool

    :return: pd.DataFrame
    """
    df = pd.DataFrame([doc.metadata for doc in documents])

    df[content_column_name] = [doc.page_content for doc in documents]

    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # Reordering the columns to have the content column first.
    df = df[[content_column_name] + [col for col in df.columns if col != content_column_name]]

    if with_embeddings:
        df["embeddings"] = embedding_model.embed_documents(df[content_column_name].tolist())

    return df

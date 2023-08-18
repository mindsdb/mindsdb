from functools import lru_cache
from typing import List, Union

import pandas as pd
import torch
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer


class Parameters(BaseModel):
    embeddings_model_name: str


@lru_cache()
def load_embeddings_model(embeddings_model_name):
    try:
        device = "cuda" if torch.cuda.is_available() else "cpu"
        embedding_model = SentenceTransformer(
            model_name_or_path=embeddings_model_name, device=device
        )
    except ValueError:
        raise ValueError(
            f"The {embeddings_model_name}  is not supported, please select a valid option from Hugging Face Hub!"
        )
    return embedding_model


class DfLoader:
    def __init__(self, data_frame: pd.DataFrame, page_content_column: str):
        self._data_frame = data_frame
        self._page_content_column = page_content_column

    def load(self) -> List[str]:
        """takes a Dataframe column and loads it as a list of texts"""
        texts = []
        for n_row, frame in self._data_frame[self._page_content_column].iteritems():
            if pd.notnull(frame):
                # ignore rows with None values
                texts.append(frame)
        return texts


def df_to_documents(
    df: pd.DataFrame, page_content_columns: Union[List[str], str]
) -> List[str]:
    """Given a subset of columns, converts a dataframe into a list of Documents"""
    texts = []

    if isinstance(page_content_columns, str):
        page_content_columns = [page_content_columns]

    for _, page_content_column in enumerate(page_content_columns):
        if page_content_column not in df.columns.tolist():
            raise ValueError(
                f"page_content_column {page_content_column} not in dataframe columns"
            )

        loader = DfLoader(data_frame=df, page_content_column=page_content_column)
        texts.extend(loader.load())

    return texts

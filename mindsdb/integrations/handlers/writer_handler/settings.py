from functools import lru_cache
from typing import Union, List

import pandas as pd
from langchain.embeddings import HuggingFaceEmbeddings
from chromadb.config import Settings
from langchain.vectorstores import Chroma
import torch

from langchain.callbacks.streaming_stdout import StreamingStdOutCallbackHandler
from langchain.docstore.document import Document
from langchain.document_loaders import DataFrameLoader
from pydantic import BaseModel
from pathlib import Path

HANDLER_PATH = Path(__file__).parent.resolve()
DEFAULT_EMBEDDING_MODEL = "sentence-transformers/all-mpnet-base-v2"
PERSIST_DIRECTORY = (HANDLER_PATH / 'db').as_posix()
USER_DEFINED_MODEL_PARAMS = ('model_name', 'max_tokens', 'temperature', 'top_p', 'stop', 'best_of','verbose','writer_org_id','writer_api_key')

CHROMA_SETTINGS = Settings(
	chroma_db_impl='duckdb+parquet',
	persist_directory=PERSIST_DIRECTORY,
	anonymized_telemetry=False
)


class ModelParameters(BaseModel):
	"""Model parameters for the Writer LLM API interface"""
	writer_api_key: str = None
	writer_org_id: str = None
	model_id: str = 'palmyra-x'
	callbacks: List[StreamingStdOutCallbackHandler] = [StreamingStdOutCallbackHandler()]
	max_tokens: int = 1024
	temperature: float = 0.0
	top_p: float = 1
	stop: List[str] = []
	best_of: int = 5
	verbose: bool = False

	class Config:
		arbitrary_types_allowed = True


@lru_cache()
def df_to_documents(df:pd.DataFrame, page_content_columns:Union[List[str],str]) -> List[Document]:
	"""Converts a given dataframe to a list of documents"""
	documents = []

	if isinstance(page_content_columns,str):

		page_content_columns = [page_content_columns]

	for page_content_column in enumerate(page_content_columns):
		if page_content_column not in df.columns:
			raise ValueError(f"page_content_column {page_content_column} not in dataframe columns")

		loader = DataFrameLoader(df=df,page_content_column=page_content_column)
		documents.extend(loader.load())

	return documents


@lru_cache()
def load_embeddings_model(embeddings_model_name):
	try:
		model_kwargs = {'device': "gpu" if torch.cuda.is_available() else "cpu"}
		embedding_model = HuggingFaceEmbeddings(model_name=embeddings_model_name, model_kwargs=model_kwargs)
	except ValueError:
		raise ValueError(
			f"The {embeddings_model_name}  is not supported, please select a valid option from Hugging Face Hub!")
	return embedding_model


@lru_cache()
def load_chroma(embeddings_model_name):
	return Chroma(
		persist_directory=PERSIST_DIRECTORY,
		embedding_function=load_embeddings_model(embeddings_model_name),
		client_settings=CHROMA_SETTINGS)


def get_retriever(embeddings_model_name):
	db = load_chroma(embeddings_model_name)
	retriever = db.as_retriever()
	return retriever

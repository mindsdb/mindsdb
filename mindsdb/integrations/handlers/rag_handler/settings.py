from dataclasses import dataclass
from functools import lru_cache, partial
from typing import List, Union

import html2text
import openai
import pandas as pd
import requests
from chromadb import Settings
from langchain import Writer
from langchain.callbacks.streaming_stdout import StreamingStdOutCallbackHandler
from langchain.docstore.document import Document
from langchain.document_loaders import DataFrameLoader
from langchain.embeddings.base import Embeddings
from langchain.embeddings.huggingface import HuggingFaceEmbeddings
from langchain.vectorstores import FAISS, Chroma, VectorStore
from pydantic import BaseModel, Extra, Field, validator

DEFAULT_EMBEDDINGS_MODEL = "BAAI/bge-base-en"

SUPPORTED_VECTOR_STORES = ("chroma", "faiss")

SUPPORTED_LLMS = ("writer", "openai")

SUMMARIZATION_PROMPT_TEMPLATE = """
Summarize the following texts for me:
{context}

When summarizing, please keep the following in mind the following question:
{question}
"""


def is_valid_store(name):
    return name in SUPPORTED_VECTOR_STORES


class VectorStoreFactory:
    @staticmethod
    def get_vectorstore_class(name):

        if not isinstance(name, str):
            raise TypeError("name must be a string")

        if not is_valid_store(name):
            raise ValueError(f"Invalid vector store {name}")

        if name == "faiss":
            return FAISS

        if name == "chroma":
            return Chroma


def get_chroma_settings(persist_directory: str = "chromadb") -> Settings:
    return Settings(
        chroma_db_impl="duckdb+parquet",
        persist_directory=persist_directory,
        anonymized_telemetry=False,
    )


@dataclass
class PersistedVectorStoreSaverConfig:
    vector_store_name: str
    persist_directory: str
    collection_name: str
    vector_store: VectorStore


@dataclass
class PersistedVectorStoreLoaderConfig:
    vector_store_name: str
    embeddings_model: Embeddings
    persist_directory: str
    collection_name: str


class PersistedVectorStoreSaver:
    def __init__(self, config: PersistedVectorStoreSaverConfig):
        self.config = config

    def save_vector_store(self, vector_store: VectorStore):
        method_name = f"save_{self.config.vector_store_name}"
        getattr(self, method_name)(vector_store)

    def save_chroma(self, vector_store: Chroma):
        vector_store.persist()

    def save_faiss(self, vector_store: FAISS):
        vector_store.save_local(
            folder_path=self.config.persist_directory,
            index_name=self.config.collection_name,
        )


class PersistedVectorStoreLoader:
    def __init__(self, config: PersistedVectorStoreLoaderConfig):
        self.config = config

    def load_vector_store_client(
        self,
        vector_store: str,
    ):
        """Load vector store client from the persisted vector store"""

        if vector_store == "chroma":

            return Chroma(
                collection_name=self.config.collection_name,
                embedding_function=self.config.embeddings_model,
                client_settings=get_chroma_settings(
                    persist_directory=self.config.persist_directory
                ),
            )

        elif vector_store == "faiss":

            return FAISS.load_local(
                folder_path=self.config.persist_directory,
                embeddings=self.config.embeddings_model,
                index_name=self.config.collection_name,
            )

        else:
            raise NotImplementedError(f"{vector_store} client is not yet supported")

    def load_vector_store(self):
        method_name = f"load_{self.config.vector_store_name}"
        return getattr(self, method_name)()

    def load_chroma(self) -> Chroma:
        return self.load_vector_store_client(vector_store="chroma")

    def load_faiss(self) -> FAISS:
        return self.load_vector_store_client(vector_store="faiss")


class LLMParameters(BaseModel):
    """Model parameters for the LLM API interface"""

    llm_name: str = Field(default_factory=str, title="LLM API name")
    max_tokens: int = Field(default=100, title="max tokens in response")
    temperature: float = Field(default=0.0, title="temperature")
    top_p: float = 1
    best_of: int = 5
    stop: List[str] = None

    class Config:
        extra = Extra.forbid
        arbitrary_types_allowed = True
        use_enum_values = True


class OpenAIParameters(LLMParameters):
    """Model parameters for the LLM API interface"""

    openai_api_key: str
    model_id: str = Field(default="text-davinci-003", title="model name")
    n: int = Field(default=1, title="number of responses to return")


class WriterLLMParameters(LLMParameters):
    """Model parameters for the Writer LLM API interface"""

    writer_api_key: str
    writer_org_id: str = None
    base_url: str = None
    model_id: str = "palmyra-x"
    callbacks: List[StreamingStdOutCallbackHandler] = [StreamingStdOutCallbackHandler()]
    verbose: bool = False


class LLMLoader(BaseModel):
    llm_config: Union[WriterLLMParameters, OpenAIParameters]
    config_dict: dict = None

    def load_llm(self):
        method_name = f"load_{self.llm_config.llm_name}_llm"
        self.config_dict = self.llm_config.dict()
        self.config_dict.pop("llm_name")
        return getattr(self, method_name)()

    def load_writer_llm(self):
        return Writer(**self.config_dict)

    def load_openai_llm(self):
        openai.api_key = self.llm_config.openai_api_key
        config = self.config_dict
        config.pop("openai_api_key")
        config["model"] = config.pop("model_id")

        return partial(openai.Completion.create, **config)


class MissingPromptTemplate(Exception):
    pass


class UnsupportedVectorStore(Exception):
    pass


class MissingUseIndex(Exception):
    pass


class RAGHandlerParameters(BaseModel):
    """Model parameters for create model"""

    prompt_template: str
    llm_type: str
    llm_params: LLMParameters
    chunk_size: int = 500
    chunk_overlap: int = 50
    url: Union[str, List[str]] = None
    run_embeddings: bool = True
    external_index_name: str = None
    top_k: int = 4
    embeddings_model_name: str = DEFAULT_EMBEDDINGS_MODEL
    context_columns: Union[List[str], str] = None
    vector_store_name: str = "chroma"
    vector_store: VectorStore = None
    collection_name: str = "langchain"
    summarize_context: bool = False
    summarization_prompt_template: str = SUMMARIZATION_PROMPT_TEMPLATE
    vector_store_folder_name: str = "persisted_vector_db"
    vector_store_storage_path: str = None

    class Config:
        extra = Extra.forbid
        arbitrary_types_allowed = True
        use_enum_values = True

    @validator("llm_type")
    def llm_type_must_be_supported(cls, v):
        if v not in SUPPORTED_LLMS:
            raise ValueError(f"llm_type must be one of `writer` or `openai`, got {v}")
        return v

    @validator("vector_store_name")
    def name_must_be_lower(cls, v):
        return v.lower()

    @validator("prompt_template")
    def prompt_template_must_be_provided(cls, v):
        if not v:
            raise MissingPromptTemplate(
                "Please provide a `prompt_template` for this engine."
            )
        return v

    @validator("vector_store_name")
    def vector_store_must_be_supported(cls, v):
        if not is_valid_store(v):
            raise UnsupportedVectorStore(
                f"currently we only support {', '.join(str(v) for v in SUPPORTED_VECTOR_STORES)} vector store"
            )
        return v


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
        for n_row, frame in self._data_frame[self._page_content_column].items():
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


def url_to_documents(urls: Union[List[str], str]) -> List[Document]:
    """Converts a given url to a document"""
    documents = []
    if isinstance(urls, str):
        urls = [urls]

    for url in urls:
        response = requests.get(url, headers=None).text
        html_to_text = html2text.html2text(response)
        documents.append(Document(page_content=html_to_text, metadata={"source": url}))

    return documents


# todo issue#7361 hard coding device to cpu, add support for gpu later on
# e.g. {"device": "gpu" if torch.cuda.is_available() else "cpu"}
@lru_cache()
def load_embeddings_model(embeddings_model_name):
    try:
        model_kwargs = {"device": "cpu"}
        embedding_model = HuggingFaceEmbeddings(
            model_name=embeddings_model_name, model_kwargs=model_kwargs
        )
    except ValueError:
        raise ValueError(
            f"The {embeddings_model_name}  is not supported, please select a valid option from Hugging Face Hub!"
        )
    return embedding_model

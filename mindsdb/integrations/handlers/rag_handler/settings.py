import json
from dataclasses import dataclass
from functools import lru_cache, partial
from typing import Any, Dict, List, Union, Optional

import html2text
import openai
import pandas as pd
import requests
import writer
from langchain_community.embeddings.huggingface import HuggingFaceEmbeddings
from langchain_community.llms import Writer
from langchain_community.document_loaders import DataFrameLoader
from langchain_community.vectorstores import FAISS, Chroma
from pydantic import BaseModel, Extra, Field, field_validator, ValidationInfo


from mindsdb.integrations.handlers.chromadb_handler.chromadb_handler import get_chromadb
from mindsdb.integrations.handlers.rag_handler.exceptions import (
    InvalidOpenAIModel,
    InvalidPromptTemplate,
    InvalidWriterModel,
    UnsupportedLLM,
    UnsupportedVectorStore,
)
from langchain_core.callbacks import StreamingStdOutCallbackHandler
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.vectorstores import VectorStore

DEFAULT_EMBEDDINGS_MODEL = "BAAI/bge-base-en"

SUPPORTED_VECTOR_STORES = ("chromadb", "faiss")

SUPPORTED_LLMS = ("writer", "openai")

# Default parameters for RAG Handler

# this is the default prompt template for qa
DEFAULT_QA_PROMPT_TEMPLATE = """
Use the following pieces of context to answer the question at the end. If you do not know the answer,
just say that you do not know, do not try to make up an answer.
Context: {context}
Question: {question}
Helpful Answer:"""

# this is the default prompt template for if the user wants to summarize the context before qa prompt
DEFAULT_SUMMARIZATION_PROMPT_TEMPLATE = """
Summarize the following texts for me:
{context}

When summarizing, please keep the following in mind the following question:
{question}
"""

DEFAULT_CHUNK_SIZE = 750
DEFAULT_CHUNK_OVERLAP = 250
DEFAULT_VECTOR_STORE_NAME = "chromadb"
DEFAULT_VECTOR_STORE_COLLECTION_NAME = "collection"
MAX_EMBEDDINGS_BATCH_SIZE = 2000

chromadb = get_chromadb()


def is_valid_store(name) -> bool:
    return name in SUPPORTED_VECTOR_STORES


class VectorStoreFactory:
    """Factory class for vector stores"""

    @staticmethod
    def get_vectorstore_class(name) -> Union[FAISS, Chroma, VectorStore]:

        if not isinstance(name, str):
            raise TypeError("name must be a string")

        if not is_valid_store(name):
            raise ValueError(f"Invalid vector store {name}")

        if name == "faiss":
            return FAISS

        if name == "chromadb":
            return Chroma


def get_chroma_client(persist_directory: str) -> chromadb.PersistentClient:
    """Get Chroma client"""
    return chromadb.PersistentClient(path=persist_directory)


def get_available_writer_model_ids(args: ValidationInfo) -> list:
    """Get available writer LLM model ids"""

    args = args.data

    writer_client = writer.Writer(
        api_key=args["writer_api_key"],
        organization_id=args["writer_org_id"],
    )

    res = writer_client.models.list(organization_id=args["writer_org_id"])

    available_models_dict = json.loads(res.raw_response.text)

    return [model["id"] for model in available_models_dict["models"]]


def get_available_openai_model_ids(args: ValidationInfo) -> list:
    """Get available openai LLM model ids"""

    args = args.data

    models = openai.OpenAI(api_key=args["openai_api_key"], base_url=args.get("base_url")).models.list().data

    return [models.id for models in models]


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
    """Saves vector store to disk"""

    def __init__(self, config: PersistedVectorStoreSaverConfig):
        self.config = config

    def save_vector_store(self, vector_store: VectorStore):
        method_name = f"save_{self.config.vector_store_name}"
        getattr(self, method_name)(vector_store)

    def save_chromadb(self, vector_store: Chroma):
        """Save Chroma vector store to disk"""
        # no need to save chroma vector store to disk, auto save
        pass

    def save_faiss(self, vector_store: FAISS):
        vector_store.save_local(
            folder_path=self.config.persist_directory,
            index_name=self.config.collection_name,
        )


class PersistedVectorStoreLoader:
    """Loads vector store from disk"""

    def __init__(self, config: PersistedVectorStoreLoaderConfig):
        self.config = config

    def load_vector_store_client(
            self,
            vector_store: str,
    ):
        """Load vector store from the persisted vector store"""

        if vector_store == "chromadb":

            return Chroma(
                collection_name=self.config.collection_name,
                embedding_function=self.config.embeddings_model,
                client=get_chroma_client(self.config.persist_directory),
            )

        elif vector_store == "faiss":

            return FAISS.load_local(
                folder_path=self.config.persist_directory,
                embeddings=self.config.embeddings_model,
                index_name=self.config.collection_name,
                allow_dangerous_deserialization=True
            )

        else:
            raise NotImplementedError(f"{vector_store} client is not yet supported")

    def load_vector_store(self) -> VectorStore:
        """Load vector store from the persisted vector store"""
        method_name = f"load_{self.config.vector_store_name}"
        return getattr(self, method_name)()

    def load_chromadb(self) -> Chroma:
        """Load Chroma vector store from the persisted vector store"""
        return self.load_vector_store_client(vector_store="chromadb")

    def load_faiss(self) -> FAISS:
        """Load FAISS vector store from the persisted vector store"""
        return self.load_vector_store_client(vector_store="faiss")


class LLMParameters(BaseModel):
    """Model parameters for the LLM API interface"""

    llm_name: str = Field(default_factory=str, title="LLM API name")
    max_tokens: int = Field(default=100, title="max tokens in response")
    temperature: float = Field(default=0.0, title="temperature")
    base_url: Optional[str] = None
    top_p: float = 1
    best_of: int = 5
    stop: Optional[List[str]] = None

    class Config:
        extra = Extra.forbid
        arbitrary_types_allowed = True
        use_enum_values = True
        protected_namespaces = ()


class OpenAIParameters(LLMParameters):
    """Model parameters for the LLM API interface"""

    openai_api_key: str
    model_id: str = Field(default="gpt-3.5-turbo-instruct", title="model name")
    n: int = Field(default=1, title="number of responses to return")

    @field_validator("model_id", mode="after")
    def openai_model_must_be_supported(cls, v, values):
        supported_models = get_available_openai_model_ids(values)
        if v not in supported_models:
            raise InvalidOpenAIModel(
                f"'model_id' must be one of {supported_models}, got {v}"
            )
        return v


class WriterLLMParameters(LLMParameters):
    """Model parameters for the Writer LLM API interface"""

    writer_api_key: str
    writer_org_id: Optional[str] = None
    model_id: str = "palmyra-x"
    callbacks: List[StreamingStdOutCallbackHandler] = [StreamingStdOutCallbackHandler()]
    verbose: bool = False

    @field_validator("model_id")
    def writer_model_must_be_supported(cls, v, values):
        supported_models = get_available_writer_model_ids(values)
        if v not in supported_models:
            raise InvalidWriterModel(
                f"'model_id' must be one of {supported_models}, got {v}"
            )
        return v


class LLMLoader(BaseModel):
    llm_config: dict
    config_dict: dict = None

    def load_llm(self) -> Union[Writer, partial]:
        """Load LLM"""
        method_name = f"load_{self.llm_config['llm_name']}_llm"
        self.config_dict = self.llm_config.copy()
        self.config_dict.pop("llm_name")
        return getattr(self, method_name)()

    def load_writer_llm(self) -> Writer:
        """Load Writer LLM API interface"""
        return Writer(**self.config_dict)

    def load_openai_llm(self) -> partial:
        """Load OpenAI LLM API interface"""
        client = openai.OpenAI(api_key=self.config_dict["openai_api_key"], base_url=self.config_dict["base_url"])
        config = self.config_dict.copy()
        keys_to_remove = ["openai_api_key", "base_url"]
        for key in keys_to_remove:
            config.pop(key)
        config["model"] = config.pop("model_id")

        return partial(client.completions.create, **config)


class RAGBaseParameters(BaseModel):
    """Base model parameters for RAG Handler"""

    llm_params: Any
    vector_store_folder_name: str
    input_column: str
    use_gpu: bool = False
    embeddings_batch_size: int = MAX_EMBEDDINGS_BATCH_SIZE  # not used, leaving in place to prevent breaking changes
    prompt_template: str = DEFAULT_QA_PROMPT_TEMPLATE
    chunk_size: int = DEFAULT_CHUNK_SIZE
    chunk_overlap: int = DEFAULT_CHUNK_OVERLAP
    url: Optional[Union[str, List[str]]] = None
    url_column_name: Optional[str] = None
    run_embeddings: Optional[bool] = True
    top_k: int = 4
    embeddings_model: Optional[Embeddings] = None
    embeddings_model_name: str = DEFAULT_EMBEDDINGS_MODEL
    context_columns: Optional[Union[List[str], str]] = None
    vector_store_name: str = DEFAULT_VECTOR_STORE_NAME
    vector_store: Optional[VectorStore] = None
    collection_name: str = DEFAULT_VECTOR_STORE_COLLECTION_NAME
    summarize_context: bool = True
    summarization_prompt_template: str = DEFAULT_SUMMARIZATION_PROMPT_TEMPLATE
    vector_store_storage_path: Optional[str] = Field(
        default=None, title="don't use this field, it's for internal use only"
    )

    class Config:
        extra = Extra.forbid
        arbitrary_types_allowed = True
        use_enum_values = True

    @field_validator("prompt_template")
    def prompt_format_must_be_valid(cls, v):
        if "{context}" not in v or "{question}" not in v:
            raise InvalidPromptTemplate(
                "prompt_template must contain {context} and {question}"
                f"\n For example, {DEFAULT_QA_PROMPT_TEMPLATE}"
            )
        return v

    @field_validator("vector_store_name")
    def name_must_be_lower(cls, v):
        return v.lower()

    @field_validator("vector_store_name")
    def vector_store_must_be_supported(cls, v):
        if not is_valid_store(v):
            raise UnsupportedVectorStore(
                f"we don't support {v}. currently we only support {', '.join(str(v) for v in SUPPORTED_VECTOR_STORES)} vector store"
            )
        return v


class RAGHandlerParameters(RAGBaseParameters):
    """Model parameters for create model"""

    llm_type: str
    llm_params: LLMParameters

    @field_validator("llm_type")
    def llm_type_must_be_supported(cls, v):
        if v not in SUPPORTED_LLMS:
            raise UnsupportedLLM(f"'llm_type' must be one of {SUPPORTED_LLMS}, got {v}")
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
        df: pd.DataFrame,
        page_content_columns: Union[List[str], str],
        url_column_name: str = None,
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
        if url_column_name is not None and page_content_column == url_column_name:
            documents.extend(url_to_documents(df[page_content_column].tolist()))
            continue

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


@lru_cache()
def load_embeddings_model(embeddings_model_name, use_gpu=False):
    """Load embeddings model from Hugging Face Hub"""
    try:
        model_kwargs = dict(device="cuda" if use_gpu else "cpu")
        embedding_model = HuggingFaceEmbeddings(
            model_name=embeddings_model_name, model_kwargs=model_kwargs
        )
    except ValueError:
        raise ValueError(
            f"The {embeddings_model_name}  is not supported, please select a valid option from Hugging Face Hub!"
        )
    return embedding_model


def on_create_build_llm_params(
        args: dict, llm_config_class: Union[WriterLLMParameters, OpenAIParameters]
) -> Dict:
    """build llm params from create args"""

    llm_params = {"llm_name": args["llm_type"]}

    for param in llm_config_class.model_fields.keys():
        if param in args:
            llm_params[param] = args.pop(param)

    return llm_params


def build_llm_params(args: dict, update=False) -> Dict:
    """build llm params from args"""

    if args["llm_type"] == "writer":
        llm_config_class = WriterLLMParameters
    elif args["llm_type"] == "openai":
        llm_config_class = OpenAIParameters
    else:
        raise UnsupportedLLM(
            f"'llm_type' must be one of {SUPPORTED_LLMS}, got {args['llm_type']}"
        )

    if not args.get("llm_params"):
        # for create method only
        llm_params = on_create_build_llm_params(args, llm_config_class)
    else:
        # for predict method only
        llm_params = args.pop("llm_params")
    if update:
        # for update method only
        args["llm_params"] = llm_params
        return args

    args["llm_params"] = llm_config_class(**llm_params)

    return args

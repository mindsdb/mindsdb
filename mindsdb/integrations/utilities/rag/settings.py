from enum import Enum
from typing import List, Union, Any

from langchain_community.vectorstores.chroma import Chroma
from langchain_community.vectorstores.pgvector import PGVector
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.language_models import BaseChatModel
from langchain_core.vectorstores import VectorStore
from langchain_core.stores import BaseStore
from langchain.text_splitter import TextSplitter
from pydantic import BaseModel

DEFAULT_COLLECTION_NAME = 'default_collection'

# Multi retriever specific
DEFAULT_ID_KEY = "doc_id"
DEFAULT_MAX_CONCURRENCY = 5

DEFAULT_CARDINALITY_THRESHOLD = 40
DEFAULT_CHUNK_SIZE = 1000
DEFAULT_CHUNK_OVERLAP = 200
DEFAULT_POOL_RECYCLE = 3600
DEFAULT_LLM_MODEL = "gpt-4o"
DEFAULT_CONTENT_COLUMN_NAME = "body"
DEFAULT_DATASET_DESCRIPTION = "email inbox"
DEFAULT_TEST_TABLE_NAME = "test_email"
DEFAULT_VECTOR_STORE = Chroma
DEFAULT_RERANKER_FLAG = False
DEFAULT_RERANKING_MODEL = "gpt-4o"
DEFAULT_AUTO_META_PROMPT_TEMPLATE = """
Below is a json representation of a table with information about {description}.
Return a JSON list with an entry for each column. Each entry should have
{{"name": "column name", "description": "column description", "type": "column data type"}}
\n\n{dataframe}\n\nJSON:\n
"""
DEFAULT_RAG_PROMPT_TEMPLATE = '''You are an assistant for
question-answering tasks. Use the following pieces of retrieved context
to answer the question. If you don't know the answer, just say that you
don't know. Use two sentences maximum and keep the answer concise.
Question: {question}
Context: {context}
Answer:'''

DEFAULT_QA_GENERATION_PROMPT_TEMPLATE = '''You are an assistant for
generating sample questions and answers from the given document and metadata. Given
a document and its metadata as context, generate a question and answer from that document and its metadata.

The document will be a string. The metadata will be a JSON string. You need
to parse the JSON to understand it.

Generate a question that requires BOTH the document and metadata to answer, if possible.
Otherwise, generate a question that requires ONLY the document to answer.

Return a JSON dictionary with the question and answer like this:
{{ "question": <the full generated question>, "answer": <the full generated answer> }}

Make sure the JSON string is valid before returning it. You must return the question and answer
in the specified JSON format no matter what.

Document: {document}
Metadata: {metadata}
Answer:'''


class MultiVectorRetrieverMode(Enum):
    """
    Enum for MultiVectorRetriever types.
    """
    SPLIT = "split"
    SUMMARIZE = "summarize"
    BOTH = "both"


class VectorStoreType(Enum):
    CHROMA = 'chromadb'
    PGVECTOR = 'pgvector'


vector_store_map = {
    VectorStoreType.CHROMA: Chroma,
    VectorStoreType.PGVECTOR: PGVector
}


class RetrieverType(Enum):
    VECTOR_STORE = 'vector_store'
    AUTO = 'auto'
    MULTI = 'multi'


class VectorStoreConfig(BaseModel):
    vector_store_type: VectorStoreType = VectorStoreType.CHROMA
    persist_directory: str = None
    collection_name: str = DEFAULT_COLLECTION_NAME
    connection_string: str = None
    kb_table: Any = None

    class Config:
        arbitrary_types_allowed = True
        extra = "forbid"


class RAGPipelineModel(BaseModel):
    documents: List[Document] = None  # List of documents

    # used for loading an existing vector store
    vector_store_config: VectorStoreConfig = VectorStoreConfig()  # Vector store configuration

    # used for llm generation
    llm: BaseChatModel = None  # Language model
    llm_model_name: str = DEFAULT_LLM_MODEL  # Language model name
    llm_provider: str = None  # Language model provider

    vector_store: VectorStore = vector_store_map[vector_store_config.vector_store_type]  # Vector store
    db_connection_string: str = None  # Database connection string
    table_name: str = DEFAULT_TEST_TABLE_NAME  # table name
    embedding_model: Embeddings = None  # Embedding model
    rag_prompt_template: str = DEFAULT_RAG_PROMPT_TEMPLATE  # RAG prompt template
    retriever_prompt_template: Union[str, dict] = None  # Retriever prompt template
    retriever_type: RetrieverType = RetrieverType.VECTOR_STORE  # Retriever type

    # Multi retriever specific
    multi_retriever_mode: MultiVectorRetrieverMode = MultiVectorRetrieverMode.BOTH  # Multi retriever mode
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY  # Maximum concurrency
    id_key: int = DEFAULT_ID_KEY  # ID key
    parent_store: BaseStore = None  # Parent store
    text_splitter: TextSplitter = None  # Text splitter
    chunk_size: int = DEFAULT_CHUNK_SIZE  # Chunk size
    chunk_overlap: int = DEFAULT_CHUNK_OVERLAP  # Chunk overlap

    # Auto retriever specific
    auto_retriever_filter_columns: List[str] = None  # Filter columns
    cardinality_threshold: int = DEFAULT_CARDINALITY_THRESHOLD  # Cardinality threshold
    content_column_name: str = DEFAULT_CONTENT_COLUMN_NAME  # content column name (the column we will get embeddings)
    dataset_description: str = DEFAULT_DATASET_DESCRIPTION  # Description of the dataset
    reranker: bool = DEFAULT_RERANKER_FLAG

    class Config:
        arbitrary_types_allowed = True
        extra = "forbid"

    @classmethod
    def get_field_names(cls):
        return list(cls.model_fields.keys())

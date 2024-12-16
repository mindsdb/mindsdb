from enum import Enum
from typing import List, Union, Any, Optional, Dict

from langchain_community.vectorstores.chroma import Chroma
from langchain_community.vectorstores.pgvector import PGVector
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.language_models import BaseChatModel
from langchain_core.vectorstores import VectorStore
from langchain_core.stores import BaseStore
from pydantic import BaseModel, Field, field_validator
from langchain_text_splitters import TextSplitter

DEFAULT_COLLECTION_NAME = 'default_collection'

# Multi retriever specific
DEFAULT_ID_KEY = "doc_id"
DEFAULT_MAX_CONCURRENCY = 5
DEFAULT_K = 20

DEFAULT_CARDINALITY_THRESHOLD = 40
DEFAULT_MAX_SUMMARIZATION_TOKENS = 4000
DEFAULT_CHUNK_SIZE = 1000
DEFAULT_CHUNK_OVERLAP = 200
DEFAULT_POOL_RECYCLE = 3600
DEFAULT_LLM_MODEL = "gpt-4o"
DEFAULT_LLM_MODEL_PROVIDER = "openai"
DEFAULT_CONTENT_COLUMN_NAME = "body"
DEFAULT_DATASET_DESCRIPTION = "email inbox"
DEFAULT_TEST_TABLE_NAME = "test_email"
DEFAULT_VECTOR_STORE = Chroma
DEFAULT_RERANKER_FLAG = False
DEFAULT_RERANKING_MODEL = "gpt-4o"
DEFAULT_LLM_ENDPOINT = "https://api.openai.com/v1"
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

DEFAULT_MAP_PROMPT_TEMPLATE = '''The following is a set of documents
{docs}
Based on this list of docs, please summarize based on the user input.

User input: {input}

Helpful Answer:'''

DEFAULT_REDUCE_PROMPT_TEMPLATE = '''The following is set of summaries:
{docs}
Take these and distill it into a final, consolidated summary related to the user input.

User input: {input}

Helpful Answer:'''


class LLMConfig(BaseModel):
    model_name: str = Field(default=DEFAULT_LLM_MODEL, description='LLM model to use for generation')
    provider: str = Field(default=DEFAULT_LLM_MODEL_PROVIDER, description='LLM model provider to use for generation')
    params: Dict[str, Any] = {}


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


class VectorStoreConfig(BaseModel):
    vector_store_type: VectorStoreType = VectorStoreType.CHROMA
    persist_directory: str = None
    collection_name: str = DEFAULT_COLLECTION_NAME
    connection_string: str = None
    kb_table: Any = None

    class Config:
        arbitrary_types_allowed = True
        extra = "forbid"


class RetrieverType(Enum):
    VECTOR_STORE = 'vector_store'
    AUTO = 'auto'
    MULTI = 'multi'


class SearchType(Enum):
    """
    Enum for vector store search types.
    """
    SIMILARITY = "similarity"
    MMR = "mmr"
    SIMILARITY_SCORE_THRESHOLD = "similarity_score_threshold"


class SearchKwargs(BaseModel):
    k: int = Field(
        default=DEFAULT_K,
        description="Amount of documents to return",
        ge=1
    )
    filter: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Filter by document metadata"
    )
    # For similarity_score_threshold search type
    score_threshold: Optional[float] = Field(
        default=None,
        description="Minimum relevance threshold for similarity_score_threshold search",
        ge=0.0,
        le=1.0
    )
    # For MMR search type
    fetch_k: Optional[int] = Field(
        default=None,
        description="Amount of documents to pass to MMR algorithm",
        ge=1
    )
    lambda_mult: Optional[float] = Field(
        default=None,
        description="Diversity of results returned by MMR (1=min diversity, 0=max)",
        ge=0.0,
        le=1.0
    )

    def model_dump(self, *args, **kwargs):
        # Override model_dump to exclude None values by default
        kwargs['exclude_none'] = True
        return super().model_dump(*args, **kwargs)


class SummarizationConfig(BaseModel):
    llm_config: LLMConfig = Field(
        default_factory=LLMConfig,
        description="LLM configuration to use for summarization"
    )
    map_prompt_template: str = Field(
        default=DEFAULT_MAP_PROMPT_TEMPLATE,
        description="Prompt for an LLM to summarize a single document"
    )
    reduce_prompt_template: str = Field(
        default=DEFAULT_REDUCE_PROMPT_TEMPLATE,
        description="Prompt for an LLM to summarize a set of summaries of documents into one"
    )
    max_summarization_tokens: int = Field(
        default=DEFAULT_MAX_SUMMARIZATION_TOKENS,
        description="Max number of tokens for summarized documents"
    )


class RerankerConfig(BaseModel):
    model: str = DEFAULT_RERANKING_MODEL
    base_url: str = DEFAULT_LLM_ENDPOINT
    filtering_threshold: float = 0.99
    num_docs_to_keep: Optional[int] = None


class RAGPipelineModel(BaseModel):
    documents: Optional[List[Document]] = Field(
        default=None,
        description="List of documents"
    )

    vector_store_config: VectorStoreConfig = Field(
        default_factory=VectorStoreConfig,
        description="Vector store configuration"
    )

    llm: Optional[BaseChatModel] = Field(
        default=None,
        description="Language model"
    )
    llm_model_name: str = Field(
        default=DEFAULT_LLM_MODEL,
        description="Language model name"
    )
    llm_provider: Optional[str] = Field(
        default=None,
        description="Language model provider"
    )

    vector_store: VectorStore = Field(
        default_factory=lambda: vector_store_map[VectorStoreConfig().vector_store_type],
        description="Vector store"
    )
    db_connection_string: Optional[str] = Field(
        default=None,
        description="Database connection string"
    )
    table_name: str = Field(
        default=DEFAULT_TEST_TABLE_NAME,
        description="Table name"
    )
    embedding_model: Optional[Embeddings] = Field(
        default=None,
        description="Embedding model"
    )
    rag_prompt_template: str = Field(
        default=DEFAULT_RAG_PROMPT_TEMPLATE,
        description="RAG prompt template"
    )
    retriever_prompt_template: Optional[Union[str, dict]] = Field(
        default=None,
        description="Retriever prompt template"
    )
    retriever_type: RetrieverType = Field(
        default=RetrieverType.VECTOR_STORE,
        description="Retriever type"
    )
    search_type: SearchType = Field(
        default=SearchType.SIMILARITY,
        description="Type of search to perform"
    )
    search_kwargs: SearchKwargs = Field(
        default_factory=SearchKwargs,
        description="Search configuration for the retriever"
    )
    summarization_config: Optional[SummarizationConfig] = Field(
        default=None,
        description="Configuration for summarizing retrieved documents as context"
    )

    # Multi retriever specific
    multi_retriever_mode: MultiVectorRetrieverMode = Field(
        default=MultiVectorRetrieverMode.BOTH,
        description="Multi retriever mode"
    )
    max_concurrency: int = Field(
        default=DEFAULT_MAX_CONCURRENCY,
        description="Maximum concurrency"
    )
    id_key: int = Field(
        default=DEFAULT_ID_KEY,
        description="ID key"
    )
    parent_store: Optional[BaseStore] = Field(
        default=None,
        description="Parent store"
    )
    text_splitter: Optional[TextSplitter] = Field(
        default=None,
        description="Text splitter"
    )
    chunk_size: int = Field(
        default=DEFAULT_CHUNK_SIZE,
        description="Chunk size"
    )
    chunk_overlap: int = Field(
        default=DEFAULT_CHUNK_OVERLAP,
        description="Chunk overlap"
    )

    # Auto retriever specific
    auto_retriever_filter_columns: Optional[List[str]] = Field(
        default=None,
        description="Filter columns"
    )
    cardinality_threshold: int = Field(
        default=DEFAULT_CARDINALITY_THRESHOLD,
        description="Cardinality threshold"
    )
    content_column_name: str = Field(
        default=DEFAULT_CONTENT_COLUMN_NAME,
        description="Content column name (the column we will get embeddings)"
    )
    dataset_description: str = Field(
        default=DEFAULT_DATASET_DESCRIPTION,
        description="Description of the dataset"
    )
    reranker: bool = Field(
        default=DEFAULT_RERANKER_FLAG,
        description="Whether to use reranker"
    )
    reranker_config: RerankerConfig = Field(
        default_factory=RerankerConfig,
        description="Reranker configuration"
    )

    class Config:
        arbitrary_types_allowed = True
        extra = "forbid"

        json_schema_extra = {
            "example": {
                "retriever_type": RetrieverType.VECTOR_STORE.value,
                "multi_retriever_mode": MultiVectorRetrieverMode.BOTH.value,
                # add more examples here
            }
        }

    @classmethod
    def get_field_names(cls):
        return list(cls.model_fields.keys())

    @field_validator('search_kwargs')
    @classmethod
    def validate_search_kwargs(cls, v: SearchKwargs, info) -> SearchKwargs:
        search_type = info.data.get('search_type', SearchType.SIMILARITY)

        # Validate MMR-specific parameters
        if search_type == SearchType.MMR:
            if v.fetch_k is not None and v.fetch_k <= v.k:
                raise ValueError("fetch_k must be greater than k")
            if v.lambda_mult is not None and (v.lambda_mult < 0 or v.lambda_mult > 1):
                raise ValueError("lambda_mult must be between 0 and 1")
            if v.fetch_k is None and v.lambda_mult is not None:
                raise ValueError("fetch_k is required when using lambda_mult with MMR search type")
            if v.lambda_mult is None and v.fetch_k is not None:
                raise ValueError("lambda_mult is required when using fetch_k with MMR search type")
        elif search_type != SearchType.MMR:
            if v.fetch_k is not None:
                raise ValueError("fetch_k is only valid for MMR search type")
            if v.lambda_mult is not None:
                raise ValueError("lambda_mult is only valid for MMR search type")

        # Validate similarity_score_threshold parameters
        if search_type == SearchType.SIMILARITY_SCORE_THRESHOLD:
            if v.score_threshold is not None and (v.score_threshold < 0 or v.score_threshold > 1):
                raise ValueError("score_threshold must be between 0 and 1")
            if v.score_threshold is None:
                raise ValueError("score_threshold is required for similarity_score_threshold search type")
        elif search_type != SearchType.SIMILARITY_SCORE_THRESHOLD and v.score_threshold is not None:
            raise ValueError("score_threshold is only valid for similarity_score_threshold search type")

        return v

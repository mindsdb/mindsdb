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

DEFAULT_SEMANTIC_PROMPT_TEMPLATE = '''Provide a better search query for web search engine to answer the given question.

<< EXAMPLES >>
1. Input: "Show me documents containing how to finetune a LLM please"
Output: "how to finetune a LLM"

Output only a single better search query and nothing else like in the example.

Here is the user input: {input}
'''

DEFAULT_METADATA_FILTERS_PROMPT_TEMPLATE = '''Construct a list of PostgreSQL metadata filters to filter documents in the database based on the user input.

<< INSTRUCTIONS >>
{format_instructions}

RETURN ONLY THE FINAL JSON. DO NOT EXPLAIN, JUST RETURN THE FINAL JSON.

<< TABLES YOU HAVE ACCESS TO >>

{schema}

<< EXAMPLES >>

{examples}

Here is the user input:
{input}
'''

DEFAULT_SQL_PROMPT_TEMPLATE = '''
Construct a valid {dialect} SQL query to select documents relevant to the user input.
Source documents are found in the {source_table} table. You may need to join with other tables to get additional document metadata.

The JSON col "metadata" in the {embeddings_table} has a string field called "original_row_id". This "original_row_id" string field in the
"metadata" col is the document ID associated with a row in the {embeddings_table} table.
You MUST always join with the {embeddings_table} table containing vector embeddings for the documents. For example, for a table named sd with an id column "Id":
JOIN {embeddings_table} v ON (v."metadata"->>'original_row_id')::int = sd."Id"

You MUST always order the embeddings by the {distance_function} comparator with '{{embeddings}}'.
You MUST always limit by {k} returned documents.
For example:
ORDER BY v.embeddings {distance_function} '{{embeddings}}' LIMIT {k};


<< TABLES YOU HAVE ACCESS TO >>
1. {embeddings_table} - Contains document chunks, vector embeddings, and metadata for documents.
You MUST always include the metadata column in your SELECT statement.
You MUST always join with the {embeddings_table} table containing vector embeddings for the documents.
You MUST always order by the provided embeddings vector using the {distance_function} comparator.
You MUST always limit by {k} returned documents.

Columns:
```json
{{
    "id": {{
        "type": "string",
        "description": "Unique ID for this document chunk"
    }},
    "content": {{
        "type": "string",
        "description": "A document chunk (subset of the original document)"
    }},
    "embeddings": {{
        "type": "vector",
        "description": "Vector embeddings for the document chunk. ALWAYS order by the provided embeddings vector using the {distance_function} comparator."
    }},
    "metadata": {{
        "type": "jsonb",
        "description": "Metadata for the document chunk. Always select metadata and always join with the {source_table} table on the string metadata field 'original_row_id'"
    }}
}}

{schema}

<< EXAMPLES >>

{examples}

Output the {dialect} SQL query that is ready to be executed only WITHOUT ANY DELIMITERS. Make sure to properly quote identifiers.

Here is the user input:
{input}
'''

DEFAULT_QUESTION_REFORMULATION_TEMPLATE = """Given the original question and the retrieved context,
analyze what additional information is needed for a complete, accurate answer.

Original Question: {question}

Retrieved Context:
{context}

Analysis Instructions:
1. Evaluate Context Coverage:
   - Identify key entities and concepts from the question
   - Check for temporal information (dates, periods, sequences)
   - Verify causal relationships are explained
   - Confirm presence of requested quantitative data
   - Assess if geographic or spatial context is sufficient

2. Quality Assessment:
   If the retrieved context is:
   - Irrelevant or tangential
   - Too general or vague
   - Potentially contradictory
   - Missing key perspectives
   - Lacking proper evidence
   Generate questions to address these specific gaps.

3. Follow-up Question Requirements:
   - Questions must directly contribute to answering the original query
   - Break complex relationships into simpler, sequential steps
   - Maintain specificity rather than broad inquiries
   - Avoid questions answerable from existing context
   - Ensure questions build on each other logically
   - Limit questions to 150 characters each
   - Each question must be self-contained
   - Questions must end with a question mark

4. Response Format:
   - Return a JSON array of strings
   - Use square brackets and double quotes
   - Questions must be unique (no duplicates)
   - If context is sufficient, return empty array []
   - Maximum 3 follow-up questions
   - Minimum length per question: 30 characters
   - No null values or empty strings

Example:
Original: "How did the development of antibiotics affect military casualties in WWII?"

Invalid responses:
{'questions': ['What are antibiotics?']}  // Wrong format
['What is WWII?']  // Too basic
['How did it impact things?']  // Too vague
['', 'Question 2']  // Contains empty string
['Same question?', 'Same question?']  // Duplicate

Valid response:
["What were military casualty rates from infections before widespread antibiotic use in 1942?",
 "How did penicillin availability change throughout different stages of WWII?",
 "What were the primary battlefield infections treated with antibiotics during WWII?"]

or [] if context fully answers the original question.

Your task: Based on the analysis of the original question and context,
output ONLY a JSON array of follow-up questions needed to provide a complete answer.
If no additional information is needed, output an empty array [].

Follow-up Questions:"""

DEFAULT_QUERY_RETRY_PROMPT_TEMPLATE = '''
{query}

The {dialect} query above failed with the error message: {error}.

<< TABLES YOU HAVE ACCESS TO >>
1. {embeddings_table} - Contains document chunks, vector embeddings, and metadata for documents.

Columns:
```json
{{
    "id": {{
        "type": "string",
        "description": "Unique ID for this document chunk"
    }},
    "content": {{
        "type": "string",
        "description": "A document chunk (subset of the original document)"
    }},
    "embeddings": {{
        "type": "vector",
        "description": "Vector embeddings for the document chunk."
    }},
    "metadata": {{
        "type": "jsonb",
        "description": "Metadata for the document chunk."
    }}
}}

{schema}

Rewrite the query so it works.

Output the final SQL query only.

SQL Query:
'''

DEFAULT_NUM_QUERY_RETRIES = 2

NAME_LOOKUP_TOOL_DESCRIPTION = '''This tool is designed to retrieve a SINGLE document using its name or title. Use this tool only when the user's request includes the name or title of a SINGLE document. The input provided to the tool must be the exact full name of the document.

When to Use:

The user's request clearly specifies the document name or title.
The query implies a singular, specific document is needed.
Valid Query Examples:

Please find the document NEI 2012-06, Revision 4
Please find the document NUREG/CR-7278
Show me the document OG- 11-68
Find the document NUREG/CR-6393
Find NUREG/CR-7116
Find document titled "Concentration Averaging and Encapsulation Branch Technical Position"
Find "BEAVER VALLEY 2 - LER 2022-001"
Find REGULATORY GUIDE 1.183 from 2000
Find Docket No. 52-033
Show Prairie Island 1/2 - Application to Revise Technical Specifications to Adopt TSTF-591, "Revise Risk Informed Completion Time (RICT) Program"

Use this tool when the document name is clearly specified to ensure precise and accurate lookup results.

If you can potentially use multiple of your tools to answer the user question, you should prefer this name lookup tool because it is faster.
'''

ID_LOOKUP_TOOL_DESCRIPTION = '''ONLY use this tool when the user specifies a document ID to get after using the name_lookup tool. Never use this tool unless the name_lookup tool was used and returned a list of document IDs for the user to choose from.

The input provided must be the exact document ID and nothing else (e.g. 523093).
'''

DEFAULT_DOCUMENT_IDENTIFIER_PROMPT_TEMPLATE = '''Construct a DocumentIdentifier.

A single document usually has usually the following format:

<type_key>[separator /-_]<must have document_identifier can like number[possible separator and other numbers]> <possibly rev revision number]>, also there are location_keywords that we can try to find independently from title, like name of plant or location.

Here are all the type keys and what they mean:

{type_descriptions}

<< INSTRUCTIONS FOR HOW TO FORMAT FINAL OUTPUT >>
{format_instructions}

RETURN ONLY THE FINAL JSON. DO NOT EXPLAIN, JUST RETURN THE FINAL JSON.

Here is the user input:
{input}
'''

DEFAULT_DOCUMENT_TYPE_IDENTIFIER = 5

RETRIEVAL_TOOL_PROMPT_TEMPLATE = '''This tool is designed to fetch in-depth context, background information, and supporting details about {description}. It should be used when the user's inquiry requires an explanation, analysis, or synthesis of information rather than a simple document lookup. The input should be the full question as posed by the user.

When to Use This Tool:

The query asks for detailed context or an explanation about a subject related to {description} that CANNOT be found by looking up a SINGLE document.
The user is requesting background information, definitions, or analysis rather than a specific document.
The input question implies a need for understanding processes, systems, or key details within the subject.
Examples of Suitable Queries:

"What is the Significance Determination Process?"
"What is the docket number for the Kairos Hermes reactor?"
'''


class LLMConfig(BaseModel):
    model_name: str = Field(default=DEFAULT_LLM_MODEL, description='LLM model to use for generation')
    provider: str = Field(default=DEFAULT_LLM_MODEL_PROVIDER, description='LLM model provider to use for generation')
    params: Dict[str, Any] = Field(default_factory=dict)


class DocumentIdentifier(BaseModel):
    type_key: str = Field(
        description='Key for the type of document (e.g. LER, SECY, NUREG)'
    )
    document_number: Optional[str] = Field(
        description='Number to further identify document',
        default=None
    )
    revision: Optional[str] = Field(
        description='Some documents have revisions (e.g. 4, 3.H). Only include this if specifically asked for a revision.'
    )


class DocumentTypeMetadata(BaseModel):
    key: str = Field(
        description='Key for the type of document (e.g. LER, SECY, NUREG)'
    )
    id: Optional[int] = Field(
        default=None,
        description='Identifier for the document type'
    )
    type_pattern: Optional[str] = Field(
        default=None,
        description='Postgres pattern that matches this document type (e.g. %LER%)'
    )
    name: str = Field(
        description='Human readable name for this document type'
    )
    description: Optional[str] = Field(
        default=None,
        description='Description for what this document type is'
    )


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
    is_sparse: bool = False
    vector_size: Optional[int] = None

    class Config:
        arbitrary_types_allowed = True
        extra = "forbid"


class RetrieverType(str, Enum):
    """Retriever type for RAG pipeline"""
    VECTOR_STORE = "vector_store"
    AUTO = "auto"
    MULTI = "multi"
    SQL = "sql"
    MULTI_HOP = "multi_hop"


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


class ColumnSchema(BaseModel):
    name: str = Field(
        description="Name of the column in the database"
    )
    type: str = Field(
        description="Type of the column (e.g. int, string, datetime)"
    )
    description: str = Field(
        description="Description of what the column represents"
    )
    values: Optional[Dict[Any, Any]] = Field(
        default=None,
        description="Mapping of values the column can be with the description of what the value means"
    )


class MetadataConfig(BaseModel):
    """Class to configure metadata for retrieval. Only supports very basic document name lookup at the moment."""
    table: str = Field(
        description="Source table for metadata."
    )
    max_document_context: int = Field(
        # To work well with models with context window of 32768.
        default=16384,
        description="Truncate a document before using as context with an LLM if it exceeds this amount of tokens"
    )
    embeddings_table: str = Field(
        default="embeddings",
        description="Source table for embeddings"
    )
    id_column: str = Field(
        default="Id",
        description="Name of ID column in metadata table"
    )
    name_column: str = Field(
        default="Title",
        description="Name of column containing name or title of document"
    )
    name_column_index: Optional[str] = Field(
        default=None,
        description="Name of GIN index to use when looking up name."
    )
    type_column: str = Field(
        default="Type",
        description="Name of column containing type of document"
    )
    type_pattern_column: str = Field(
        default="ReportNumber",
        description="Name of column to match document type pattern against"
    )
    date_column: str = Field(
        default="CreatedOnDate",
        description="Name of column containing date when document was created or modified last"
    )
    content_column: str = Field(
        default="content",
        description="Name of column in embeddings table containing chunk content"
    )
    embeddings_metadata_column: str = Field(
        default="metadata",
        description="Name of column in embeddings table containing chunk metadata"
    )
    doc_id_key: str = Field(
        default="original_row_id",
        description="Metadata field that links an embedded chunk back to source document ID"
    )
    llm_config: LLMConfig = Field(
        default_factory=LLMConfig,
        description="LLM configuration to use for generating the final SQL query for retrieval"
    )
    document_types: List[DocumentTypeMetadata] = Field(
        description="Document types to include when searching by name"
    )
    document_identifier_prompt_template: str = Field(
        default=DEFAULT_DOCUMENT_IDENTIFIER_PROMPT_TEMPLATE,
        description="Prompt template to generate DocumentIdentifier. Has 'format_instructions', 'type_descriptions', and 'input' input variables"
    )


class MetadataSchema(BaseModel):
    table: str = Field(
        description="Name of table in the database"
    )
    description: str = Field(
        description="Description of what the table represents"
    )
    columns: List[ColumnSchema] = Field(
        description="List of column schemas describing the metadata columns available for the table"
    )
    join: str = Field(
        description="SQL join string to join this table with source documents table",
        default=''
    )

    class Config:
        frozen = True


class LLMExample(BaseModel):
    input: str = Field(
        description="User input for the example"
    )
    output: str = Field(
        description="What the LLM should generate for this example's input"
    )


class SQLRetrieverConfig(BaseModel):
    llm_config: LLMConfig = Field(
        default_factory=LLMConfig,
        description="LLM configuration to use for generating the final SQL query for retrieval"
    )
    metadata_filters_prompt_template: str = Field(
        default=DEFAULT_METADATA_FILTERS_PROMPT_TEMPLATE,
        description="Prompt template to generate PostgreSQL metadata filters. Has 'format_instructions', 'schema', 'examples', and 'input' input variables"
    )
    num_retries: int = Field(
        default=DEFAULT_NUM_QUERY_RETRIES,
        description="How many times for an LLM to try rewriting a failed SQL query before using the fallback retriever."
    )
    rewrite_prompt_template: str = Field(
        default=DEFAULT_SEMANTIC_PROMPT_TEMPLATE,
        description="Prompt template to rewrite user input to be better suited for retrieval. Has 'input' input variable."
    )
    source_table: str = Field(
        description="Name of the source table containing the original documents that were embedded"
    )
    metadata_schemas: Optional[List[MetadataSchema]] = Field(
        default=None,
        description="Optional list of table schemas containing document metadata to potentially join with."
    )
    examples: Optional[List[LLMExample]] = Field(
        default=None,
        description="Optional examples of final generated pgvector queries based on user input."
    )


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
    filtering_threshold: float = 0.5
    num_docs_to_keep: Optional[int] = None
    max_concurrent_requests: int = 20
    max_retries: int = 3
    retry_delay: float = 1.0
    early_stop: bool = True  # Whether to enable early stopping
    early_stop_threshold: float = 0.8  # Confidence threshold for early stopping


class MultiHopRetrieverConfig(BaseModel):
    """Configuration for multi-hop retrieval"""
    base_retriever_type: RetrieverType = Field(
        default=RetrieverType.VECTOR_STORE,
        description="Type of base retriever to use for multi-hop retrieval"
    )
    max_hops: int = Field(
        default=3,
        description="Maximum number of follow-up questions to generate",
        ge=1
    )
    reformulation_template: str = Field(
        default=DEFAULT_QUESTION_REFORMULATION_TEMPLATE,
        description="Template for reformulating questions"
    )
    llm_config: LLMConfig = Field(
        default_factory=LLMConfig,
        description="LLM configuration to use for generating follow-up questions"
    )


class RAGPipelineModel(BaseModel):
    enable_retrieval: bool = Field(
        default=True,
        description="Whether or not to enable base retrieval pipeline as a tool for semantic search"
    )
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
    metadata_config: Optional[MetadataConfig] = Field(
        default=None,
        description="Configuration for metadata to be used for retrieval"
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
    # SQL retriever specific.
    sql_retriever_config: Optional[SQLRetrieverConfig] = Field(
        default=None,
        description="Configuration for retrieving documents by generating SQL to filter by metadata & order by distance function"
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

    multi_hop_config: Optional[MultiHopRetrieverConfig] = Field(
        default=None,
        description="Configuration for multi-hop retrieval. Required when retriever_type is MULTI_HOP."
    )

    @field_validator("multi_hop_config")
    @classmethod
    def validate_multi_hop_config(cls, v: Optional[MultiHopRetrieverConfig], info):
        """Validate that multi_hop_config is set when using multi-hop retrieval."""
        values = info.data
        if values.get("retriever_type") == RetrieverType.MULTI_HOP and v is None:
            raise ValueError("multi_hop_config must be set when using multi-hop retrieval")
        return v

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

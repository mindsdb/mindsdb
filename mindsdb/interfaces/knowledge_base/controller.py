import os
import copy
from typing import Dict, List, Optional, Any, Text, Tuple, Union
import json
import decimal

import pandas as pd
import numpy as np
from pydantic import BaseModel, ValidationError
from sqlalchemy.orm.attributes import flag_modified

from mindsdb_sql_parser.ast import BinaryOperation, Constant, Identifier, Select, Update, Delete, Star
from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.libs.keyword_search_base import KeywordSearchBase
from mindsdb.integrations.utilities.query_traversal import query_traversal

import mindsdb.interfaces.storage.db as db
from mindsdb.integrations.libs.vectordatabase_handler import (
    DistanceFunction,
    TableField,
    VectorStoreHandler,
)
from mindsdb.integrations.utilities.handler_utils import get_api_key
from mindsdb.integrations.utilities.handlers.auth_utilities.snowflake import get_validated_jwt

from mindsdb.integrations.utilities.rag.settings import RerankerMode

from mindsdb.interfaces.agents.constants import get_default_embeddings_model_class, MAX_INSERT_BATCH_SIZE
from mindsdb.interfaces.agents.provider_utils import get_llm_provider

try:
    from mindsdb.interfaces.agents.langchain_agent import create_chat_model
except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
    if getattr(exc, "name", "") and "langchain" in exc.name:
        create_chat_model = None
        _LANGCHAIN_IMPORT_ERROR = exc
    else:  # Unknown import error, surface it
        raise
else:
    _LANGCHAIN_IMPORT_ERROR = None
from mindsdb.interfaces.database.projects import ProjectController
from mindsdb.interfaces.knowledge_base.preprocessing.models import PreprocessingConfig, Document
from mindsdb.interfaces.knowledge_base.preprocessing.document_preprocessor import PreprocessorFactory
from mindsdb.interfaces.knowledge_base.evaluate import EvaluateBase
from mindsdb.interfaces.knowledge_base.executor import KnowledgeBaseQueryExecutor
from mindsdb.interfaces.model.functions import PredictorRecordNotFound
from mindsdb.utilities.exception import EntityExistsError, EntityNotExistsError
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator, KeywordSearchArgs
from mindsdb.utilities.config import config
from mindsdb.utilities.context import context as ctx

from mindsdb.api.executor.command_executor import ExecuteCommands
from mindsdb.api.executor.utilities.sql import query_df
from mindsdb.utilities import log
from mindsdb.integrations.utilities.rag.rerankers.base_reranker import BaseLLMReranker, ListwiseLLMReranker
from mindsdb.interfaces.knowledge_base.llm_client import LLMClient

logger = log.getLogger(__name__)


def _require_agent_extra(feature: str):
    if create_chat_model is None:
        raise ImportError(
            f"{feature} requires the optional agent dependencies. Install them via `pip install mindsdb[kb]`."
        ) from _LANGCHAIN_IMPORT_ERROR


class KnowledgeBaseInputParams(BaseModel):
    metadata_columns: List[str] | None = None
    content_columns: List[str] | None = None
    id_column: str | None = None
    kb_no_upsert: bool = False
    kb_skip_existing: bool = False
    embedding_model: Dict[Text, Any] | None = None
    is_sparse: bool = False
    vector_size: int | None = None
    reranking_model: Union[Dict[Text, Any], bool] | None = None
    preprocessing: Dict[Text, Any] | None = None

    class Config:
        extra = "forbid"


def get_model_params(model_params: dict, default_config_key: str):
    """
    Get model parameters by combining default config with user provided parameters.
    """
    combined_model_params = copy.deepcopy(config.get(default_config_key, {}))

    if model_params:
        if not isinstance(model_params, dict):
            raise ValueError("Model parameters must be passed as a JSON object")

        # if provider mismatches - don't use default values
        if "provider" in model_params and model_params["provider"] != combined_model_params.get("provider"):
            return model_params

        combined_model_params.update(model_params)

    combined_model_params.pop("use_default_llm", None)

    return combined_model_params


def adapt_embedding_model_params(embedding_model_params: dict):
    """
    Prepare parameters for embedding model.
    """
    params_copy = copy.deepcopy(embedding_model_params)
    provider = params_copy.pop("provider", None).lower()
    api_key = get_api_key(provider, params_copy, strict=False) or params_copy.get("api_key")
    # Underscores are replaced because the provider name ultimately gets mapped to a class name.
    # This is mostly to support Azure OpenAI (azure_openai); the mapped class name is 'AzureOpenAIEmbeddings'.
    params_copy["class"] = provider.replace("_", "")
    if provider == "azure_openai":
        # Azure OpenAI expects the api_key to be passed as 'openai_api_key'.
        params_copy["openai_api_key"] = api_key
        params_copy["azure_endpoint"] = params_copy.pop("base_url")
        if "chunk_size" not in params_copy:
            params_copy["chunk_size"] = 2048
        if "api_version" in params_copy:
            params_copy["openai_api_version"] = params_copy["api_version"]
    else:
        params_copy[f"{provider}_api_key"] = api_key
    params_copy.pop("api_key", None)
    params_copy["model"] = params_copy.pop("model_name", None)

    return params_copy


def get_reranking_model_from_params(reranking_model_params: dict):
    """
    Create reranking model from parameters.
    """
    from mindsdb.integrations.utilities.rag.settings import RerankerConfig

    # Work on a copy; do not mutate caller's dict
    params_copy = copy.deepcopy(reranking_model_params)

    # Handle API key if not provided
    provider = params_copy.get("provider", "openai").lower()
    if "api_key" not in params_copy:
        params_copy["api_key"] = get_api_key(provider, params_copy, strict=False)

    # Handle model_name -> model alias for backward compatibility
    if "model_name" in params_copy and "model" not in params_copy:
        params_copy["model"] = params_copy.pop("model_name")

    # Validate core fields (e.g. mode) via Pydantic
    try:
        cfg = RerankerConfig(**params_copy)
    except ValueError as e:
        raise ValueError(f"Invalid reranker configuration: {str(e)}")

    # Merge validated fields back, preserving any extra user fields
    validated = cfg.model_dump()
    reranker_params = {**params_copy, **validated}

    # Choose reranker class based on validated mode
    if cfg.mode == RerankerMode.LISTWISE:
        return ListwiseLLMReranker(**reranker_params)
    return BaseLLMReranker(**reranker_params)


def safe_pandas_is_datetime(value: str) -> bool:
    """
    Check if the value can be parsed as a datetime.
    """
    try:
        result = pd.api.types.is_datetime64_any_dtype(value)
        return result
    except ValueError:
        return False


def to_json(obj):
    if obj is None:
        return None
    try:
        return json.dumps(obj)
    except TypeError:
        return obj


def rotate_provider_api_key(params):
    """
    Check api key for specific providers. At the moment it checks and updated jwt token of snowflake provider
    :param params: input params, can be modified by this function
    :return: a new api key if it is refreshed
    """
    provider = params.get("provider").lower()

    if provider == "snowflake":
        api_key = params.get("api_key")
        api_key2 = get_validated_jwt(
            api_key,
            account=params.get("snowflake_account_id"),
            user=params.get("user"),
            private_key=params.get("private_key"),
        )
        if api_key2 != api_key:
            # update keys
            params["api_key"] = api_key2
            return api_key2


class KnowledgeBaseTable:
    """
    Knowledge base table interface
    Handlers requests to KB table and modifies data in linked vector db table
    """

    def __init__(self, kb: db.KnowledgeBase, session):
        self._kb = kb
        self._vector_db = None
        self.session = session
        self.document_preprocessor = None
        self.document_loader = None
        self.model_params = None

        self.kb_to_vector_columns = {"id": "_original_doc_id", "chunk_id": "id", "chunk_content": "content"}
        if self._kb.params.get("version", 0) < 2:
            self.kb_to_vector_columns["id"] = "original_doc_id"

    def configure_preprocessing(self, config: Optional[dict] = None):
        """Configure preprocessing for the knowledge base table"""
        logger.debug(f"Configuring preprocessing with config: {config}")
        self.document_preprocessor = None  # Reset existing preprocessor
        if config is None:
            config = {}

        # Ensure content_column is set for JSON chunking if not already specified
        if config.get("type") == "json_chunking" and config.get("json_chunking_config"):
            if "content_column" not in config["json_chunking_config"]:
                config["json_chunking_config"]["content_column"] = "content"

        preprocessing_config = PreprocessingConfig(**config)
        self.document_preprocessor = PreprocessorFactory.create_preprocessor(preprocessing_config)

        # set doc_id column name
        self.document_preprocessor.config.doc_id_column_name = self.kb_to_vector_columns["id"]

        logger.debug(f"Created preprocessor of type: {type(self.document_preprocessor)}")

    def select_query(self, query: Select) -> pd.DataFrame:
        """
        Handles select from KB table.
        Replaces content values with embeddings in where clause. Sends query to vector db
        :param query: query to KB table
        :return: dataframe with the result table
        """

        # Copy query for complex execution via DuckDB: DISTINCT, GROUP BY etc.
        query_copy = copy.deepcopy(query)

        executor = KnowledgeBaseQueryExecutor(self)
        df = executor.run(query)

        # copy metadata to columns
        if "metadata" in df.columns:
            meta_columns = self._get_allowed_metadata_columns()
            if meta_columns:
                meta_data = pd.json_normalize(df["metadata"])
                # exclude absent columns and used colunns
                df_columns = list(df.columns)
                meta_columns = list(set(meta_columns).intersection(meta_data.columns).difference(df_columns))

                # add columns
                df = df.join(meta_data[meta_columns])

                # put metadata in the end
                df_columns.remove("metadata")
                df = df[df_columns + meta_columns + ["metadata"]]

        if (
            query_copy.group_by is not None
            or query_copy.order_by is not None
            or query_copy.having is not None
            or query_copy.distinct is True
            or len(query_copy.targets) != 1
            or not isinstance(query_copy.targets[0], Star)
        ):
            query_copy.where = None
            if "metadata" in df.columns:
                df["metadata"] = df["metadata"].apply(to_json)

            if query_copy.from_table is None:
                query_copy.from_table = Identifier(parts=[self._kb.name])

            df = query_df(df, query_copy, session=self.session)

        return df

    def select(self, query, disable_reranking=False):
        logger.debug(f"Processing select query: {query}")

        # Extract the content query text for potential reranking

        db_handler = self.get_vector_db()

        logger.debug("Replaced content with embeddings in where clause")
        # set table name
        query.from_table = Identifier(parts=[self._kb.vector_database_table])
        logger.debug(f"Set table name to: {self._kb.vector_database_table}")

        query.targets = [
            Identifier(TableField.ID.value),
            Identifier(TableField.CONTENT.value),
            Identifier(TableField.METADATA.value),
            Identifier(TableField.DISTANCE.value),
        ]

        # Get response from vector db
        logger.debug(f"Using vector db handler: {type(db_handler)}")

        # extract values from conditions and prepare for vectordb
        conditions = []
        keyword_search_conditions = []
        keyword_search_cols_and_values = []
        query_text = None
        relevance_threshold = None
        relevance_threshold_allowed_operators = [
            FilterOperator.GREATER_THAN_OR_EQUAL.value,
            FilterOperator.GREATER_THAN.value,
        ]
        gt_filtering = False
        query_conditions = db_handler.extract_conditions(query.where)
        hybrid_search_alpha = None
        if query_conditions is not None:
            for item in query_conditions:
                if (item.column == "relevance") and (item.op.value in relevance_threshold_allowed_operators):
                    try:
                        relevance_threshold = float(item.value)
                        # Validate range: must be between 0 and 1
                        if not (0 <= relevance_threshold <= 1):
                            raise ValueError(f"relevance_threshold must be between 0 and 1, got: {relevance_threshold}")
                        if item.op.value == FilterOperator.GREATER_THAN.value:
                            gt_filtering = True
                        logger.debug(f"Found relevance_threshold in query: {relevance_threshold}")
                    except (ValueError, TypeError) as e:
                        error_msg = f"Invalid relevance_threshold value: {item.value}. {e}"
                        logger.error(error_msg)
                        raise ValueError(error_msg) from e
                elif (item.column == "relevance") and (item.op.value not in relevance_threshold_allowed_operators):
                    raise ValueError(
                        f"Invalid operator for relevance: {item.op.value}. Only the following operators are allowed: "
                        f"{','.join(relevance_threshold_allowed_operators)}."
                    )
                elif item.column == "reranking":
                    if item.value is False or (isinstance(item.value, str) and item.value.lower() == "false"):
                        disable_reranking = True
                elif item.column == "hybrid_search":
                    if item.value:
                        if hybrid_search_alpha is None:
                            hybrid_search_alpha = 0.5
                elif item.column == "hybrid_search_alpha":
                    # validate item.value is a float
                    if not isinstance(item.value, (float, int)):
                        raise ValueError(f"Invalid hybrid_search_alpha value: {item.value}. Must be a float or int.")
                    # validate hybrid search alpha is between 0 and 1
                    if not (0 <= item.value <= 1):
                        raise ValueError(f"Invalid hybrid_search_alpha value: {item.value}. Must be between 0 and 1.")
                    hybrid_search_alpha = item.value
                elif item.column == TableField.CONTENT.value:
                    query_text = item.value

                    # replace content with embeddings
                    conditions.append(
                        FilterCondition(
                            column=TableField.EMBEDDINGS.value,
                            value=self._content_to_embeddings(item.value),
                            op=FilterOperator.EQUAL,
                        )
                    )
                    keyword_search_cols_and_values.append((TableField.CONTENT.value, item.value))
                else:
                    conditions.append(item)
                    keyword_search_conditions.append(item)  # keyword search conditions do not use embeddings

        if len(keyword_search_cols_and_values) > 1:
            raise ValueError(
                "Multiple content columns found in query conditions. "
                "Only one content column is allowed for keyword search."
            )

        logger.debug(f"Extracted query text: {query_text}")

        self.addapt_conditions_columns(conditions)

        # Set default limit if query is present
        limit = query.limit.value if query.limit is not None else None
        if query_text is not None:
            if limit is None:
                limit = 10
            elif limit > 100:
                limit = 100

            if not disable_reranking:
                # expand limit, get more records before reranking usage:
                #   get twice size of input but not greater than 30
                query_limit = min(limit * 2, limit + 30)
            else:
                query_limit = limit
            query.limit = Constant(query_limit)

        allowed_metadata_columns = self._get_allowed_metadata_columns()

        if hybrid_search_alpha is None:
            hybrid_search_alpha = 1

        if hybrid_search_alpha > 0:
            df = db_handler.dispatch_select(query, conditions, allowed_metadata_columns=allowed_metadata_columns)
            df = self.addapt_result_columns(df)
            logger.debug(f"Query returned {len(df)} rows")
            logger.debug(f"Columns in response: {df.columns.tolist()}")
        else:
            df = pd.DataFrame([], columns=["id", "chunk_id", "chunk_content", "metadata", "distance"])

        # check if db_handler inherits from KeywordSearchBase
        if hybrid_search_alpha < 1:
            if not isinstance(db_handler, KeywordSearchBase):
                raise ValueError(
                    f"Hybrid search is enabled but the db_handler {type(db_handler)} does not support it. "
                )

            # If query_text is present, use it for keyword search
            logger.debug(f"Performing keyword search with query text: {query_text}")
            keyword_search_args = KeywordSearchArgs(query=query_text, column=TableField.CONTENT.value)
            keyword_query_obj = copy.deepcopy(query)

            keyword_query_obj.targets = [
                Identifier(TableField.ID.value),
                Identifier(TableField.CONTENT.value),
                Identifier(TableField.METADATA.value),
            ]

            df_keyword = db_handler.dispatch_select(
                keyword_query_obj,
                keyword_search_conditions,
                allowed_metadata_columns=allowed_metadata_columns,
                keyword_search_args=keyword_search_args,
            )
            df_keyword = self.addapt_result_columns(df_keyword)
            logger.debug(f"Keyword search returned {len(df_keyword)} rows")
            logger.debug(f"Columns in keyword search response: {df_keyword.columns.tolist()}")
            # ensure df and df_keyword_select have exactly the same columns
            if not df_keyword.empty:
                if df.empty:
                    df = df_keyword
                else:
                    df_keyword[TableField.DISTANCE.value] = hybrid_search_alpha * df_keyword[TableField.DISTANCE.value]
                    df[TableField.DISTANCE.value] = (1 - hybrid_search_alpha) * df[TableField.DISTANCE.value]

                    df = pd.concat([df, df_keyword], ignore_index=True)
                    # sort by distance if distance column exists
                    if TableField.DISTANCE.value in df.columns:
                        df = df.sort_values(by=TableField.DISTANCE.value, ascending=True)
                    # if chunk_id column exists remove duplicates based on chunk_id
                    if "chunk_id" in df.columns:
                        df = df.drop_duplicates(subset=["chunk_id"])

        # Check if we have a rerank_model configured in KB params
        df = self.add_relevance(df, query_text, relevance_threshold, disable_reranking)
        if limit is not None:
            df = df[:limit]

        # if relevance filtering method is strictly GREATER THAN we filter the df
        if gt_filtering:
            relevance_scores = TableField.RELEVANCE.value
            df = df[df[relevance_scores] > relevance_threshold]

        return df

    def _get_allowed_metadata_columns(self) -> List[str] | None:
        # Return list of KB columns to restrict querying, if None: no restrictions

        if self._kb.params.get("version", 0) < 2:
            # disable for old version KBs
            return None

        user_columns = self._kb.params.get("metadata_columns", [])
        dynamic_columns = self._kb.params.get("inserted_metadata", [])

        columns = set(user_columns) | set(dynamic_columns)
        return [col.lower() for col in columns]

    def score_documents(self, query_text, documents, reranking_model_params):
        rotate_provider_api_key(reranking_model_params)
        reranker = get_reranking_model_from_params(reranking_model_params)
        return reranker.get_scores(query_text, documents)

    def add_relevance(self, df, query_text, relevance_threshold=None, disable_reranking=False):
        relevance_column = TableField.RELEVANCE.value

        reranking_model_params = get_model_params(self._kb.params.get("reranking_model"), "default_reranking_model")
        if reranking_model_params and query_text and len(df) > 0 and not disable_reranking:
            # Use reranker for relevance score

            new_api_key = rotate_provider_api_key(reranking_model_params)
            if new_api_key:
                # update key
                if "reranking_model" not in self._kb.params:
                    self._kb.params["reranking_model"] = {}
                self._kb.params["reranking_model"]["api_key"] = new_api_key
                flag_modified(self._kb, "params")
                db.session.commit()

            # Apply custom filtering threshold if provided
            if relevance_threshold is not None:
                reranking_model_params["filtering_threshold"] = relevance_threshold
                logger.info(f"Using custom filtering threshold: {relevance_threshold}")

            reranker = get_reranking_model_from_params(reranking_model_params)
            # Get documents to rerank
            documents = df["chunk_content"].tolist()
            # Use the get_scores method with disable_events=True
            scores = reranker.get_scores(query_text, documents)
            # Add scores as the relevance column
            df[relevance_column] = scores

            # Filter by threshold
            scores_array = np.array(scores)
            df = df[scores_array >= reranker.filtering_threshold]

        elif "distance" in df.columns:
            # Calculate relevance from distance
            logger.info("Calculating relevance from vector distance")
            df[relevance_column] = 1 / (1 + df["distance"])
            if relevance_threshold is not None:
                df = df[df[relevance_column] > relevance_threshold]

        else:
            df[relevance_column] = None
            df["distance"] = None
        # Sort by relevance
        df = df.sort_values(by=relevance_column, ascending=False)
        return df

    def addapt_conditions_columns(self, conditions):
        if conditions is None:
            return
        for condition in conditions:
            if condition.column in self.kb_to_vector_columns:
                condition.column = self.kb_to_vector_columns[condition.column]

    def addapt_result_columns(self, df):
        col_update = {}
        for kb_col, vec_col in self.kb_to_vector_columns.items():
            if vec_col in df.columns:
                col_update[vec_col] = kb_col

        df = df.rename(columns=col_update)

        columns = list(df.columns)
        # update id, get from metadata
        df[TableField.ID.value] = df[TableField.METADATA.value].apply(
            lambda m: None if m is None else m.get(self.kb_to_vector_columns["id"])
        )

        # id on first place
        return df[[TableField.ID.value] + columns]

    def insert_files(self, file_names: List[str]):
        """Process and insert files"""
        if not self.document_loader:
            raise ValueError("Document loader not configured")

        documents = list(self.document_loader.load_files(file_names))
        if documents:
            self.insert_documents(documents)

    def insert_web_pages(self, urls: List[str], crawl_depth: int, limit: int, filters: List[str] = None):
        """Process and insert web pages"""
        if not self.document_loader:
            raise ValueError("Document loader not configured")

        documents = list(
            self.document_loader.load_web_pages(urls, limit=limit, crawl_depth=crawl_depth, filters=filters)
        )
        if documents:
            self.insert_documents(documents)

    def insert_query_result(self, query: str, project_name: str):
        """Process and insert SQL query results"""
        ast_query = parse_sql(query)

        command_executor = ExecuteCommands(self.session)
        response = command_executor.execute_command(ast_query, project_name)

        if response.error_code is not None:
            raise ValueError(f"Error executing query: {response.error_message}")

        if response.data is None:
            raise ValueError("Query returned no data")

        records = response.data.records
        df = pd.DataFrame(records)

        self.insert(df)

    def insert_rows(self, rows: List[Dict]):
        """Process and insert raw data rows"""
        if not rows:
            return

        df = pd.DataFrame(rows)

        self.insert(df)

    def insert_documents(self, documents: List[Document]):
        """Process and insert documents with preprocessing if configured"""
        df = pd.DataFrame([doc.model_dump() for doc in documents])

        self.insert(df)

    def update_query(self, query: Update):
        # add embeddings to content in updated collumns
        query = copy.deepcopy(query)

        emb_col = TableField.EMBEDDINGS.value
        cont_col = TableField.CONTENT.value

        db_handler = self.get_vector_db()
        conditions = db_handler.extract_conditions(query.where)
        doc_id = None
        for condition in conditions:
            if condition.column == "chunk_id" and condition.op == FilterOperator.EQUAL:
                doc_id = condition.value

        if cont_col in query.update_columns:
            content = query.update_columns[cont_col]

            # Apply preprocessing to content if configured
            if self.document_preprocessor:
                doc = Document(
                    id=doc_id,
                    content=content.value,
                    metadata={},  # Empty metadata for content-only updates
                )
                processed_chunks = self.document_preprocessor.process_documents([doc])
                if processed_chunks:
                    content.value = processed_chunks[0].content

            query.update_columns[emb_col] = Constant(self._content_to_embeddings(content.value))

        if "metadata" not in query.update_columns:
            query.update_columns["metadata"] = Constant({})

        # TODO search content in where clause?

        # set table name
        query.table = Identifier(parts=[self._kb.vector_database_table])

        # send to vectordb
        self.addapt_conditions_columns(conditions)
        db_handler.dispatch_update(query, conditions)

    def delete_query(self, query: Delete):
        """
        Handles delete query to KB table.
        Replaces content values with embeddings in WHERE clause. Sends query to vector db
        :param query: query to KB table
        """
        query_traversal(query.where, self._replace_query_content)

        # set table name
        query.table = Identifier(parts=[self._kb.vector_database_table])

        # send to vectordb
        db_handler = self.get_vector_db()
        conditions = db_handler.extract_conditions(query.where)
        self.addapt_conditions_columns(conditions)
        db_handler.dispatch_delete(query, conditions)

    def hybrid_search(
        self,
        query: str,
        keywords: List[str] = None,
        metadata: Dict[str, str] = None,
        distance_function=DistanceFunction.COSINE_DISTANCE,
    ) -> pd.DataFrame:
        query_df = pd.DataFrame.from_records([{TableField.CONTENT.value: query}])
        embeddings_df = self._df_to_embeddings(query_df)
        if embeddings_df.empty:
            return pd.DataFrame([])
        embeddings = embeddings_df.iloc[0][TableField.EMBEDDINGS.value]
        keywords_query = None
        if keywords is not None:
            keywords_query = " ".join(keywords)
        db_handler = self.get_vector_db()
        return db_handler.hybrid_search(
            self._kb.vector_database_table,
            embeddings,
            query=keywords_query,
            metadata=metadata,
            distance_function=distance_function,
        )

    def clear(self):
        """
        Clear data in KB table
        Sends delete to vector db table
        """
        db_handler = self.get_vector_db()
        db_handler.delete(self._kb.vector_database_table)

    def insert(self, df: pd.DataFrame, params: dict = None):
        """Insert dataframe to KB table.

        Args:
            df: DataFrame to insert
            params: User parameters of insert
        """
        if df.empty:
            return

        if len(df) > MAX_INSERT_BATCH_SIZE:
            raise ValueError("Input data is too large, please load data in batches")

        try:
            run_query_id = ctx.run_query_id
            # Link current KB to running query (where KB is used to insert data)
            if run_query_id is not None:
                self._kb.query_id = run_query_id
                db.session.commit()

        except AttributeError:
            ...

        # First adapt column names to identify content and metadata columns
        adapted_df, normalized_columns = self._adapt_column_names(df)
        content_columns = normalized_columns["content_columns"]

        # Convert DataFrame rows to documents, creating separate documents for each content column
        raw_documents = []
        for idx, row in adapted_df.iterrows():
            base_metadata = self._parse_metadata(row.get(TableField.METADATA.value, {}))
            provided_id = row.get(TableField.ID.value)

            for col in content_columns:
                content = row.get(col)
                if content and str(content).strip():
                    content_str = str(content)

                    # Use provided_id directly if it exists, otherwise generate one
                    doc_id = self._generate_document_id(content_str, col, provided_id)

                    metadata = {
                        **base_metadata,
                        "_original_row_index": str(idx),  # provide link to original row index
                        "_content_column": col,
                    }

                    raw_documents.append(Document(content=content_str, id=doc_id, metadata=metadata))

        # Apply preprocessing to all documents if preprocessor exists
        if self.document_preprocessor:
            processed_chunks = self.document_preprocessor.process_documents(raw_documents)
        else:
            processed_chunks = raw_documents  # Use raw documents if no preprocessing

        # Convert processed chunks back to DataFrame with standard structure
        df = pd.DataFrame(
            [
                {
                    TableField.CONTENT.value: chunk.content,
                    TableField.ID.value: chunk.id,
                    TableField.METADATA.value: chunk.metadata,
                }
                for chunk in processed_chunks
            ]
        )

        if df.empty:
            logger.warning("No valid content found in any content columns")
            return

        # Check if we should skip existing items (before calculating embeddings)
        if params is not None and params.get("kb_skip_existing", False):
            logger.debug(f"Checking for existing items to skip before processing {len(df)} items")
            db_handler = self.get_vector_db()

            # Get list of IDs from current batch
            current_ids = df[TableField.ID.value].dropna().astype(str).tolist()
            if current_ids:
                # Check which IDs already exist
                existing_ids = db_handler.check_existing_ids(self._kb.vector_database_table, current_ids)
                if existing_ids:
                    # Filter out existing items
                    df = df[~df[TableField.ID.value].astype(str).isin(existing_ids)]
                    logger.info(f"Skipped {len(existing_ids)} existing items, processing {len(df)} new items")

                    if df.empty:
                        logger.info("All items already exist, nothing to insert")
                        return

        # add embeddings and send to vector db
        df_emb = self._df_to_embeddings(df)
        df = pd.concat([df, df_emb], axis=1)
        db_handler = self.get_vector_db()

        if params is not None and params.get("kb_no_upsert", False):
            # speed up inserting by disable checking existing records
            db_handler.insert(self._kb.vector_database_table, df)
        else:
            db_handler.do_upsert(self._kb.vector_database_table, df)

    def _adapt_column_names(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, List[str]]]:
        """
        Convert input columns for vector db input
        - id, content and metadata
        """
        # Debug incoming data
        logger.debug(f"Input DataFrame columns: {df.columns}")
        logger.debug(f"Input DataFrame first row: {df.iloc[0].to_dict()}")

        params = self._kb.params
        columns = list(df.columns)

        # -- prepare id --
        id_column = params.get("id_column")
        if id_column is not None and id_column not in columns:
            id_column = None

        if id_column is None and TableField.ID.value in columns:
            id_column = TableField.ID.value

        # Also check for case-insensitive 'id' column
        if id_column is None:
            column_map = {col.lower(): col for col in columns}
            if "id" in column_map:
                id_column = column_map["id"]

        if id_column is not None:
            columns.remove(id_column)
            logger.debug(f"Using ID column: {id_column}")

        # Create output dataframe
        df_out = pd.DataFrame()

        # Add ID if present
        if id_column is not None:
            df_out[TableField.ID.value] = df[id_column]
            logger.debug(f"Added IDs: {df_out[TableField.ID.value].tolist()}")

        # -- prepare content and metadata --
        content_columns = params.get("content_columns", [TableField.CONTENT.value])
        metadata_columns = params.get("metadata_columns")

        logger.debug(f"Processing with: content_columns={content_columns}, metadata_columns={metadata_columns}")

        # Handle SQL query result columns
        if content_columns:
            # Ensure content columns are case-insensitive
            column_map = {col.lower(): col for col in columns}
            content_columns = [column_map.get(col.lower(), col) for col in content_columns]
            logger.debug(f"Mapped content columns: {content_columns}")

        if metadata_columns:
            # Ensure metadata columns are case-insensitive
            column_map = {col.lower(): col for col in columns}
            metadata_columns = [column_map.get(col.lower(), col) for col in metadata_columns]
            logger.debug(f"Mapped metadata columns: {metadata_columns}")

        content_columns = list(set(content_columns).intersection(columns))
        if len(content_columns) == 0:
            raise ValueError(f"Content columns {params.get('content_columns')} not found in dataset: {columns}")

        if metadata_columns is not None:
            metadata_columns = list(set(metadata_columns).intersection(columns))
        else:
            # all the rest columns
            metadata_columns = list(set(columns).difference(content_columns))

            # update list of used columns
            inserted_metadata = set(self._kb.params.get("inserted_metadata", []))
            inserted_metadata.update(metadata_columns)
            self._kb.params["inserted_metadata"] = list(inserted_metadata)
            flag_modified(self._kb, "params")
            db.session.commit()

        # Add content columns directly (don't combine them)
        for col in content_columns:
            df_out[col] = df[col]

        # Add metadata
        if metadata_columns and len(metadata_columns) > 0:

            def convert_row_to_metadata(row):
                metadata = {}
                for col in metadata_columns:
                    value = row[col]
                    value_type = type(value)
                    # Convert numpy/pandas types to Python native types
                    if safe_pandas_is_datetime(value) or isinstance(value, pd.Timestamp):
                        value = str(value)
                    elif pd.api.types.is_integer_dtype(value_type):
                        value = int(value)
                    elif pd.api.types.is_float_dtype(value_type) or isinstance(value, decimal.Decimal):
                        value = float(value)
                    elif pd.api.types.is_bool_dtype(value_type):
                        value = bool(value)
                    elif isinstance(value, dict):
                        metadata.update(value)
                        continue
                    elif value is not None:
                        value = str(value)
                    metadata[col] = value
                return metadata

            metadata_dict = df[metadata_columns].apply(convert_row_to_metadata, axis=1)
            df_out[TableField.METADATA.value] = metadata_dict

        logger.debug(f"Output DataFrame columns: {df_out.columns}")
        logger.debug(f"Output DataFrame first row: {df_out.iloc[0].to_dict() if not df_out.empty else 'Empty'}")

        return df_out, {"content_columns": content_columns, "metadata_columns": metadata_columns}

    def _replace_query_content(self, node, **kwargs):
        if isinstance(node, BinaryOperation):
            if isinstance(node.args[0], Identifier) and isinstance(node.args[1], Constant):
                col_name = node.args[0].parts[-1]
                if col_name.lower() == TableField.CONTENT.value:
                    # replace
                    node.args[0].parts = [TableField.EMBEDDINGS.value]
                    node.args[1].value = [self._content_to_embeddings(node.args[1].value)]

    def get_vector_db(self) -> VectorStoreHandler:
        """
        helper to get vector db handler
        """
        if self._vector_db is None:
            database = db.Integration.query.get(self._kb.vector_database_id)
            if database is None:
                raise ValueError("Vector database not found. Is it deleted?")
            database_name = database.name
            self._vector_db = self.session.integration_controller.get_data_handler(database_name)
        return self._vector_db

    def get_vector_db_table_name(self) -> str:
        """
        helper to get underlying table name used for embeddings
        """
        return self._kb.vector_database_table

    def _df_to_embeddings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Returns embeddings for input dataframe.
        Uses model embedding model to convert content to embeddings.
        Automatically detects input and output of model using model description
        :param df:
        :return: dataframe with embeddings
        """

        if df.empty:
            return pd.DataFrame([], columns=[TableField.EMBEDDINGS.value])

        model_id = self._kb.embedding_model_id

        if model_id is None:
            messages = list(df[TableField.CONTENT.value])
            embedding_params = get_model_params(self._kb.params.get("embedding_model", {}), "default_embedding_model")

            llm_client = LLMClient(embedding_params, session=self.session)
            results = llm_client.embeddings(messages)

            results = [[val] for val in results]
            return pd.DataFrame(results, columns=[TableField.EMBEDDINGS.value])

        # get the input columns
        model_rec = db.session.query(db.Predictor).filter_by(id=model_id).first()

        assert model_rec is not None, f"Model not found: {model_id}"
        model_project = db.session.query(db.Project).filter_by(id=model_rec.project_id).first()

        project_datanode = self.session.datahub.get(model_project.name)

        model_using = model_rec.learn_args.get("using", {})
        input_col = model_using.get("question_column")
        if input_col is None:
            input_col = model_using.get("input_column")

        if input_col is not None and input_col != TableField.CONTENT.value:
            df = df.rename(columns={TableField.CONTENT.value: input_col})

        df_out = project_datanode.predict(model_name=model_rec.name, df=df, params=self.model_params)

        target = model_rec.to_predict[0]
        if target != TableField.EMBEDDINGS.value:
            # adapt output for vectordb
            df_out = df_out.rename(columns={target: TableField.EMBEDDINGS.value})

        df_out = df_out[[TableField.EMBEDDINGS.value]]

        return df_out

    def _content_to_embeddings(self, content: str) -> List[float]:
        """
        Converts string to embeddings
        :param content: input string
        :return: embeddings
        """
        df = pd.DataFrame([[content]], columns=[TableField.CONTENT.value])
        res = self._df_to_embeddings(df)
        return res[TableField.EMBEDDINGS.value][0]

    @staticmethod
    def call_litellm_embedding(session, model_params, messages):
        args = copy.deepcopy(model_params)

        if "model_name" not in args:
            raise ValueError("'model_name' must be provided for embedding model")

        llm_model = args.pop("model_name")
        engine = args.pop("provider")

        module = session.integration_controller.get_handler_module("litellm")
        if module is None or module.Handler is None:
            raise ValueError(f'Unable to use "{engine}" provider. Litellm handler is not installed')
        return module.Handler.embeddings(engine, llm_model, messages, args)

    def build_rag_pipeline(self, retrieval_config: dict):
        """
        Builds a RAG pipeline with returned sources

        Args:
            retrieval_config: dict with retrieval config

        Returns:
            RAG: Configured RAG pipeline instance

        Raises:
            ValueError: If the configuration is invalid or required components are missing
        """
        # Get embedding model from knowledge base
        from mindsdb.integrations.handlers.langchain_embedding_handler.langchain_embedding_handler import (
            construct_model_from_args,
        )
        from mindsdb.integrations.utilities.rag.rag_pipeline_builder import RAG
        from mindsdb.integrations.utilities.rag.config_loader import load_rag_config

        embedding_model_params = get_model_params(self._kb.params.get("embedding_model", {}), "default_embedding_model")
        if self._kb.embedding_model:
            # Extract embedding model args from knowledge base table
            embedding_args = self._kb.embedding_model.learn_args.get("using", {})
            # Construct the embedding model directly
            embeddings_model = construct_model_from_args(embedding_args)
            logger.debug(f"Using knowledge base embedding model with args: {embedding_args}")
        elif embedding_model_params:
            embeddings_model = construct_model_from_args(adapt_embedding_model_params(embedding_model_params))
            logger.debug(f"Using knowledge base embedding model from params: {self._kb.params['embedding_model']}")
        else:
            embeddings_model_class = get_default_embeddings_model_class()
            embeddings_model = embeddings_model_class()
            logger.debug("Using default embedding model as knowledge base has no embedding model")

        # Update retrieval config with knowledge base parameters
        kb_params = {"vector_store_config": {"kb_table": self}}

        # Load and validate config
        try:
            rag_config = load_rag_config(retrieval_config, kb_params, embeddings_model)

            # Build LLM if specified
            if "llm_model_name" in rag_config:
                llm_args = {"model_name": rag_config.llm_model_name}
                if not rag_config.llm_provider:
                    llm_args["provider"] = get_llm_provider(llm_args)
                else:
                    llm_args["provider"] = rag_config.llm_provider
                _require_agent_extra("Building knowledge base retrieval pipelines")
                rag_config.llm = create_chat_model(llm_args)

            # Create RAG pipeline
            rag = RAG(rag_config)
            logger.debug(f"RAG pipeline created with config: {rag_config}")
            return rag

        except Exception as e:
            logger.exception("Error building RAG pipeline:")
            raise ValueError(f"Failed to build RAG pipeline: {str(e)}") from e

    def _parse_metadata(self, base_metadata):
        """Helper function to robustly parse metadata string to dict"""
        if isinstance(base_metadata, dict):
            return base_metadata
        if isinstance(base_metadata, str):
            try:
                import ast

                return ast.literal_eval(base_metadata)
            except (SyntaxError, ValueError):
                logger.warning(f"Could not parse metadata: {base_metadata}. Using empty dict.")
                return {}
        return {}

    def _generate_document_id(self, content: str, content_column: str, provided_id: str = None) -> str:
        """Generate a deterministic document ID using the utility function."""
        from mindsdb.interfaces.knowledge_base.utils import generate_document_id

        return generate_document_id(content=content, provided_id=provided_id)

    def _convert_metadata_value(self, value):
        """
        Convert metadata value to appropriate Python type.

        Args:
            value: The value to convert

        Returns:
            Converted value in appropriate Python type
        """
        if pd.isna(value):
            return None

        # Handle pandas/numpy types
        if pd.api.types.is_datetime64_any_dtype(value) or isinstance(value, pd.Timestamp):
            return str(value)
        elif pd.api.types.is_integer_dtype(type(value)):
            return int(value)
        elif pd.api.types.is_float_dtype(type(value)):
            return float(value)
        elif pd.api.types.is_bool_dtype(type(value)):
            return bool(value)

        # Handle basic Python types
        if isinstance(value, (int, float, bool)):
            return value

        # Convert everything else to string
        return str(value)

    def create_index(self):
        """
        Create an index on the knowledge base table
        :param index_name: name of the index
        :param params: parameters for the index
        """
        db_handler = self.get_vector_db()
        db_handler.create_index(self._kb.vector_database_table)


class KnowledgeBaseController:
    """
    Knowledge base controller handles all
    manages knowledge bases
    """

    KB_VERSION = 2

    def __init__(self, session) -> None:
        self.session = session

    def _check_kb_input_params(self, params):
        # check names and types KB params
        try:
            KnowledgeBaseInputParams.model_validate(params)
        except ValidationError as e:
            problems = []
            for error in e.errors():
                parameter = ".".join([str(i) for i in error["loc"]])
                param_type = error["type"]
                if param_type == "extra_forbidden":
                    msg = f"Parameter '{parameter}' is not allowed"
                else:
                    msg = f"Error in '{parameter}' (type: {param_type}): {error['msg']}. Input: {repr(error['input'])}"
                problems.append(msg)

            msg = "\n".join(problems)
            if len(problems) > 1:
                msg = "\n" + msg
            raise ValueError(f"Problem with knowledge base parameters: {msg}") from e

    def add(
        self,
        name: str,
        project_name: str,
        storage: Identifier,
        params: dict,
        preprocessing_config: Optional[dict] = None,
        if_not_exists: bool = False,
        keyword_search_enabled: bool = False,
        # embedding_model: Identifier = None, # Legacy: Allow MindsDB models to be passed as embedding_model.
    ) -> db.KnowledgeBase:
        """
        Add a new knowledge base to the database
        :param preprocessing_config: Optional preprocessing configuration to validate and store
        :param is_sparse: Whether to use sparse vectors for embeddings
        :param vector_size: Optional size specification for vectors, required when is_sparse=True
        """

        # Validate preprocessing config first if provided
        if preprocessing_config is not None:
            PreprocessingConfig(**preprocessing_config)  # Validate before storing
            params = params or {}
            params["preprocessing"] = preprocessing_config

        self._check_kb_input_params(params)

        # Check if vector_size is provided when using sparse vectors
        is_sparse = params.get("is_sparse")
        vector_size = params.get("vector_size")
        if is_sparse and vector_size is None:
            raise ValueError("vector_size is required when is_sparse=True")

        # get project id
        project = self.session.database_controller.get_project(project_name)
        project_id = project.id

        # check if knowledge base already exists
        kb = self.get(name, project_id)
        if kb is not None:
            if if_not_exists:
                return kb
            raise EntityExistsError("Knowledge base already exists", name)

        embedding_params = get_model_params(params.get("embedding_model", {}), "default_embedding_model")
        params["embedding_model"] = embedding_params

        # if model_name is None:  # Legacy
        embed_info = self._check_embedding_model(
            project.name,
            params=embedding_params,
            kb_name=name,
        )

        # if params.get("reranking_model", {}) is bool and False we evaluate it to empty dictionary
        reranking_model_params = params.get("reranking_model", {})

        if isinstance(reranking_model_params, bool) and not reranking_model_params:
            params["reranking_model"] = {}
        else:
            reranking_model_params = get_model_params(reranking_model_params, "default_reranking_model")

        params["reranking_model"] = reranking_model_params
        if reranking_model_params:
            # Get reranking model from params.
            # This is called here to check validaity of the parameters.
            rotate_provider_api_key(reranking_model_params)
            self._test_reranking(reranking_model_params)

        # search for the vector database table
        if storage is None:
            cloud_pg_vector = os.environ.get("KB_PGVECTOR_URL")
            if cloud_pg_vector:
                vector_table_name = name
                # Add sparse vector support for pgvector
                vector_db_params = {}
                # Check both explicit parameter and model configuration
                if is_sparse:
                    vector_db_params["is_sparse"] = True
                    if vector_size is not None:
                        vector_db_params["vector_size"] = vector_size
                vector_db_name = self._create_persistent_pgvector(vector_db_params)
                params["default_vector_storage"] = vector_db_name
            else:
                # create chroma db with same name
                vector_table_name = "default_collection"
                vector_db_name = self._create_persistent_chroma(name)
                # memorize to remove it later
                params["default_vector_storage"] = vector_db_name
        elif len(storage.parts) != 2:
            raise ValueError("Storage param has to be vector db with table")
        else:
            vector_db_name, vector_table_name = storage.parts

        data_node = self.session.datahub.get(vector_db_name)
        if data_node:
            vector_store_handler = data_node.integration_handler
        else:
            raise ValueError(
                f"Unable to find database named {vector_db_name}, please make sure {vector_db_name} is defined"
            )
        # create table in vectordb before creating KB
        if "default_vector_storage" in params:
            # if vector db is a default - drop previous table, if exists
            try:
                vector_store_handler.drop_table(vector_table_name)
            except Exception:
                ...
        vector_store_handler.create_table(vector_table_name)
        self._check_vector_table(embed_info, vector_store_handler, vector_table_name)

        if keyword_search_enabled:
            vector_store_handler.add_full_text_index(vector_table_name, TableField.CONTENT.value)
        vector_database_id = self.session.integration_controller.get(vector_db_name)["id"]

        # Store sparse vector settings in params if specified
        if is_sparse:
            params = params or {}
            params["vector_config"] = {"is_sparse": is_sparse}
            if vector_size is not None:
                params["vector_config"]["vector_size"] = vector_size

        params["version"] = self.KB_VERSION
        kb = db.KnowledgeBase(
            name=name,
            project_id=project_id,
            vector_database_id=vector_database_id,
            vector_database_table=vector_table_name,
            embedding_model_id=None,
            params=params,
        )
        db.session.add(kb)
        db.session.commit()
        return kb

    def _check_vector_table(self, embed_info, vector_store_handler, vector_table_name):
        query = Select(
            targets=[Identifier(TableField.EMBEDDINGS.value)],
            from_table=Identifier(parts=[vector_table_name]),
            limit=Constant(1),
        )
        df = vector_store_handler.dispatch_select(query, [])
        if len(df) > 0:
            value = df[TableField.EMBEDDINGS.value][0]
            if isinstance(value, str):
                value = json.loads(value)
            if len(value) != embed_info["dimension"]:
                raise ValueError(
                    f"Dimension of embedding model doesn't match to dimension of vector table: {embed_info['dimension']} != {len(value)}"
                )

    def update(
        self,
        name: str,
        project_name: str,
        params: dict,
        preprocessing_config: Optional[dict] = None,
    ) -> db.KnowledgeBase:
        """
        Update the knowledge base
        :param name: The name of the knowledge base
        :param project_name: Current project name
        :param params: The parameters to update
        :param preprocessing_config: Optional preprocessing configuration to validate and store
        """

        # Validate preprocessing config first if provided
        if preprocessing_config is not None:
            PreprocessingConfig(**preprocessing_config)  # Validate before storing
            params = params or {}
            params["preprocessing"] = preprocessing_config

        self._check_kb_input_params(params)

        # get project id
        project = self.session.database_controller.get_project(project_name)
        project_id = project.id

        # get existed KB
        kb = self.get(name.lower(), project_id)
        if kb is None:
            raise EntityNotExistsError("Knowledge base doesn't exists", name)

        if "embedding_model" in params:
            new_config = params["embedding_model"]
            # update embedding
            embed_params = kb.params.get("embedding_model", {})
            if not embed_params:
                # maybe old version of KB
                raise ValueError("No embedding config to update")

            # some parameters are not allowed to update
            for key in ("provider", "model_name"):
                if key in new_config and new_config[key] != embed_params.get(key):
                    raise ValueError(f"You can't update '{key}' setting")

            embed_params.update(new_config)

            self._check_embedding_model(
                project.name,
                params=embed_params,
                kb_name=name,
            )
            kb.params["embedding_model"] = embed_params

        if "reranking_model" in params:
            new_config = params["reranking_model"]
            # update embedding
            rerank_params = kb.params.get("reranking_model", {})

            if new_config is False:
                # disable reranking
                rerank_params = {}
            elif "provider" in new_config and new_config["provider"] != rerank_params.get("provider"):
                # use new config (and include default config)
                rerank_params = get_model_params(new_config, "default_reranking_model")
            else:
                # update current config
                rerank_params.update(new_config)

            if rerank_params:
                self._test_reranking(rerank_params)

            kb.params["reranking_model"] = rerank_params

        # update other keys
        for key in ["id_column", "metadata_columns", "content_columns", "preprocessing"]:
            if key in params:
                kb.params[key] = params[key]

        flag_modified(kb, "params")
        db.session.commit()

        return self.get(name.lower(), project_id)

    def _test_reranking(self, params):
        try:
            reranker = get_reranking_model_from_params(params)
            reranker.get_scores("test", ["test"])
        except (ValueError, RuntimeError) as e:
            if params["provider"] in ("azure_openai", "openai") and params.get("method") != "no-logprobs":
                # check with no-logprobs
                params["method"] = "no-logprobs"
                self._test_reranking(params)
                logger.warning(
                    f"logprobs is not supported for this model: {params.get('model_name')}. using no-logprobs mode"
                )
            else:
                raise RuntimeError(f"Problem with reranker config: {e}") from e

    def _create_persistent_pgvector(self, params=None):
        """Create default vector database for knowledge base, if not specified"""
        vector_store_name = "kb_pgvector_store"

        # check if exists
        if self.session.integration_controller.get(vector_store_name):
            return vector_store_name

        self.session.integration_controller.add(vector_store_name, "pgvector", params or {})
        return vector_store_name

    def _create_persistent_chroma(self, kb_name, engine="chromadb"):
        """Create default vector database for knowledge base, if not specified"""

        vector_store_name = f"{kb_name}_{engine}"

        vector_store_folder_name = f"{vector_store_name}"
        connection_args = {"persist_directory": vector_store_folder_name}

        # check if exists
        if self.session.integration_controller.get(vector_store_name):
            return vector_store_name

        self.session.integration_controller.add(vector_store_name, engine, connection_args)
        return vector_store_name

    def _check_embedding_model(self, project_name, params: dict = None, kb_name="") -> dict:
        """check embedding model for knowledge base, return embedding model info"""

        # if mindsdb model from old KB exists - drop it
        model_name = f"kb_embedding_{kb_name}"
        try:
            model = self.session.model_controller.get_model(model_name, project_name=project_name)
            if model is not None:
                self.session.model_controller.delete_model(model_name, project_name)
        except PredictorRecordNotFound:
            pass

        if "provider" not in params:
            raise ValueError("'provider' parameter is required for embedding model")

        # check available providers
        avail_providers = ("openai", "azure_openai", "bedrock", "gemini", "google", "ollama")
        if params["provider"] not in avail_providers:
            raise ValueError(
                f"Wrong embedding provider: {params['provider']}. Available providers: {', '.join(avail_providers)}"
            )

        llm_client = LLMClient(params, session=self.session)

        try:
            resp = llm_client.embeddings(["test"])
            return {"dimension": len(resp[0])}
        except Exception as e:
            raise RuntimeError(f"Problem with embedding model config: {e}") from e

    def delete(self, name: str, project_name: int, if_exists: bool = False) -> None:
        """
        Delete a knowledge base from the database
        """
        try:
            project = self.session.database_controller.get_project(project_name)
        except ValueError as e:
            raise ValueError(f"Project not found: {project_name}") from e
        project_id = project.id

        # check if knowledge base exists
        kb = self.get(name, project_id)
        if kb is None:
            # knowledge base does not exist
            if if_exists:
                return
            else:
                raise EntityNotExistsError("Knowledge base does not exist", name)

        # kb exists
        db.session.delete(kb)
        db.session.commit()

        # drop objects if they were created automatically
        if "default_vector_storage" in kb.params:
            try:
                dn = self.session.datahub.get(kb.params["default_vector_storage"])
                dn.integration_handler.drop_table(kb.vector_database_table)
                if dn.ds_type != "pgvector":
                    self.session.integration_controller.delete(kb.params["default_vector_storage"])
            except EntityNotExistsError:
                pass
        if "created_embedding_model" in kb.params:
            try:
                self.session.model_controller.delete_model(kb.params["created_embedding_model"], project_name)
            except EntityNotExistsError:
                pass

    def get(self, name: str, project_id: int) -> db.KnowledgeBase:
        """
        Get a knowledge base from the database
        by name + project_id
        """
        kb = (
            db.session.query(db.KnowledgeBase)
            .filter_by(
                name=name,
                project_id=project_id,
            )
            .first()
        )
        return kb

    def get_table(self, name: str, project_id: int, params: dict = None) -> KnowledgeBaseTable:
        """
        Returns kb table object with properly configured preprocessing
        :param name: table name
        :param project_id: project id
        :param params: runtime parameters for KB. Keys: 'model' - parameters for embedding model
        :return: kb table object
        """
        kb = self.get(name, project_id)
        if kb is not None:
            table = KnowledgeBaseTable(kb, self.session)
            if params:
                table.model_params = params.get("model")

            # Always configure preprocessing - either from params or default
            if kb.params and "preprocessing" in kb.params:
                table.configure_preprocessing(kb.params["preprocessing"])
            else:
                table.configure_preprocessing(None)  # This ensures default preprocessor is created

            return table

    def list(self, project_name: str = None) -> List[dict]:
        """
        List all knowledge bases from the database
        belonging to a project
        """
        project_controller = ProjectController()
        projects = project_controller.get_list()
        if project_name is not None:
            projects = [p for p in projects if p.name == project_name]

        query = db.session.query(db.KnowledgeBase).filter(
            db.KnowledgeBase.project_id.in_(list([p.id for p in projects]))
        )

        data = []
        project_names = {i.id: i.name for i in project_controller.get_list()}

        for record in query:
            kb = record.as_dict(with_secrets=self.session.show_secrets)
            kb["project_name"] = project_names[record.project_id]

            data.append(kb)

        return data

    def create_index(self, table_name, project_name):
        project_id = self.session.database_controller.get_project(project_name).id
        kb_table = self.get_table(table_name, project_id)
        kb_table.create_index()

    def evaluate(self, table_name: str, project_name: str, params: dict = None) -> pd.DataFrame:
        """
        Run evaluate and/or create test data for evaluation
        :param table_name: name of KB
        :param project_name: project of KB
        :param params: evaluation parameters
        :return: evaluation results
        """
        project_id = self.session.database_controller.get_project(project_name).id
        kb_table = self.get_table(table_name, project_id)

        scores = EvaluateBase.run(self.session, kb_table, params)

        return scores

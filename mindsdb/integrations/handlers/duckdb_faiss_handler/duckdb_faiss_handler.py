import os
import json
import hashlib
import re
from typing import Dict, List, Optional, Any, Union
from pathlib import Path

import pandas as pd
import duckdb
from mindsdb_sql_parser.ast import (
    CreateTable,
    DropTables,
    Insert,
    Select,
    Delete,
    Update,
    Identifier,
    BinaryOperation,
    Constant,
    Tuple as AstTuple,
    TypeCast,
    OrderBy,
    Function,
)
from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.integrations.libs.response import RESPONSE_TYPE, HandlerResponse as Response
from mindsdb.integrations.libs.vectordatabase_handler import (
    FilterCondition,
    VectorStoreHandler,
    DistanceFunction,
    TableField,
    FilterOperator,
)
from mindsdb.integrations.libs.keyword_search_base import KeywordSearchBase
from mindsdb.integrations.utilities.sql_utils import KeywordSearchArgs
from mindsdb.utilities import log
from mindsdb.utilities.profiler import profiler
from mindsdb.utilities.context import context as ctx

from .faiss_index import FaissIndexWithFilter

logger = log.getLogger(__name__)


class DuckDBFaissHandler(VectorStoreHandler, KeywordSearchBase):
    """This handler handles connection and execution of DuckDB with Faiss vector indexing."""

    name = "duckdb_faiss"

    def __init__(self, name: str, **kwargs):
        super().__init__(name=name, **kwargs)
        
        # Extract configuration
        self.connection_data = kwargs.get("connection_data", {})
        self.handler_storage = kwargs.get("handler_storage")
        
        # Faiss configuration
        self.metric = self.connection_data.get("metric", "cosine")
        self.backend = self.connection_data.get("backend", "hnsw")
        self.use_gpu = self.connection_data.get("use_gpu", False)
        self.nlist = self.connection_data.get("nlist", 1024)
        self.nprobe = self.connection_data.get("nprobe", 32)
        self.hnsw_m = self.connection_data.get("hnsw_m", 32)
        self.hnsw_ef_search = self.connection_data.get("hnsw_ef_search", 64)
        
        # Storage paths
        self.persist_directory = self.connection_data.get("persist_directory")
        if self.persist_directory and not os.path.isabs(self.persist_directory):
            # Relative path - use handler storage
            self.persist_directory = self.handler_storage.folder_get(self.persist_directory)
        elif not self.persist_directory:
            # Use default handler storage
            self.persist_directory = self.handler_storage.folder_get("duckdb_faiss_data")
        
        # DuckDB connection
        self.connection = None
        self.is_connected = False
        
        # Table registry: {table_name: {faiss_index, vector_columns, dimensions}}
        self.table_registry = {}
        
        # Initialize storage paths
        self.duckdb_path = os.path.join(self.persist_directory, "duckdb.db")
        self.faiss_indices_path = os.path.join(self.persist_directory, "faiss_indices")
        os.makedirs(self.faiss_indices_path, exist_ok=True)
        
        self.connect()

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Connect to DuckDB database."""
        if self.is_connected:
            return self.connection
        
        try:
            self.connection = duckdb.connect(self.duckdb_path)
            self.is_connected = True
            
            # Initialize system tables
            self._initialize_system_tables()
            
            # Load existing table metadata
            self._load_table_registry()
            
            logger.info("Connected to DuckDB database")
            return self.connection
            
        except Exception as e:
            logger.error(f"Error connecting to DuckDB: {e}")
            raise

    def disconnect(self):
        """Close DuckDB connection."""
        if self.is_connected and self.connection:
            self.connection.close()
            self.is_connected = False

    def _initialize_system_tables(self):
        """Initialize system tables for tracking table metadata."""
        with self.connection.cursor() as cur:
            # Create system table for tracking table schemas
            cur.execute("""
                CREATE TABLE IF NOT EXISTS __mindsdb_tables__ (
                    table_name VARCHAR PRIMARY KEY,
                    vector_columns JSON,
                    dimensions JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

    def _load_table_registry(self):
        """Load existing table metadata into registry."""
        try:
            with self.connection.cursor() as cur:
                cur.execute("SELECT table_name, vector_columns, dimensions FROM __mindsdb_tables__")
                rows = cur.fetchall()
                
                for table_name, vector_columns, dimensions in rows:
                    self.table_registry[table_name] = {
                        "vector_columns": json.loads(vector_columns) if vector_columns else {},
                        "dimensions": json.loads(dimensions) if dimensions else {},
                        "faiss_index": None  # Lazy load
                    }
        except Exception as e:
            logger.warning(f"Could not load table registry: {e}")

    def _get_faiss_index(self, table_name: str) -> Optional[FaissIndexWithFilter]:
        """Lazy load Faiss index for a table."""
        if table_name not in self.table_registry:
            return None
            
        if self.table_registry[table_name]["faiss_index"] is None:
            index_path = os.path.join(self.faiss_indices_path, f"{table_name}.index")
            if os.path.exists(index_path):
                try:
                    self.table_registry[table_name]["faiss_index"] = FaissIndexWithFilter.load(index_path)
                except Exception as e:
                    logger.error(f"Failed to load Faiss index for {table_name}: {e}")
                    return None
            else:
                logger.warning(f"Faiss index not found for table {table_name}")
                return None
                
        return self.table_registry[table_name]["faiss_index"]

    def _parse_vector_columns(self, create_table_query: CreateTable) -> Dict[str, int]:
        """Parse CREATE TABLE query to extract VECTOR(dim) columns."""
        vector_columns = {}
        
        for column in create_table_query.columns:
            column_name = column.name
            column_type = str(column.type).upper()
            
            # Match VECTOR(dimension) pattern
            match = re.match(r'VECTOR\s*\(\s*(\d+)\s*\)', column_type)
            if match:
                dimension = int(match.group(1))
                vector_columns[column_name] = dimension
                
        return vector_columns

    def _generate_id(self, content: str) -> str:
        """Generate ID from content hash."""
        return hashlib.md5(content.encode()).hexdigest()

    def create_table(self, table_name: str, if_not_exists=True) -> Response:
        """Create table with DuckDB and Faiss structures."""
        try:
            # This will be called from the dispatcher with the actual CREATE TABLE query
            # For now, we'll implement the core logic
            logger.info(f"Creating table {table_name}")
            return Response(RESPONSE_TYPE.OK)
            
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

    def _dispatch_create_table(self, query: CreateTable):
        """Dispatch CREATE TABLE query."""
        table_name = query.name.parts[-1]
        if_not_exists = getattr(query, "if_not_exists", False)
        
        # Parse vector columns
        vector_columns = self._parse_vector_columns(query)
        
        # Extract regular columns
        regular_columns = []
        for column in query.columns:
            column_name = column.name
            column_type = str(column.type).upper()
            
            # Skip vector columns (handled separately)
            if not re.match(r'VECTOR\s*\(\s*\d+\s*\)', column_type):
                regular_columns.append(f"{column_name} {column_type}")
        
        # Create the table
        self._create_table_impl(table_name, vector_columns, regular_columns, if_not_exists)
        return Response(RESPONSE_TYPE.OK)

    def _create_table_impl(self, table_name: str, vector_columns: Dict[str, int], 
                           regular_columns: List[str], if_not_exists=True):
        """Internal implementation of table creation."""
        with self.connection.cursor() as cur:
            # Create DuckDB table
            columns_sql = ["id VARCHAR PRIMARY KEY", "content TEXT", "metadata JSON"]
            columns_sql.extend(regular_columns)
            
            # Add vector columns as JSON (we'll store the actual vectors in Faiss)
            for col_name, dim in vector_columns.items():
                columns_sql.append(f"{col_name} JSON")  # Store as JSON for metadata
            
            create_sql = f"CREATE TABLE {'IF NOT EXISTS' if if_not_exists else ''} {table_name} ({', '.join(columns_sql)})"
            cur.execute(create_sql)
            
            # Create Faiss index for each vector column
            for col_name, dim in vector_columns.items():
                faiss_index = FaissIndexWithFilter(
                    dim=dim,
                    metric=self.metric,
                    backend=self.backend,
                    use_gpu=self.use_gpu,
                    nlist=self.nlist,
                    nprobe=self.nprobe,
                    hnsw_m=self.hnsw_m,
                    hnsw_ef_search=self.hnsw_ef_search,
                )
                
                # Save initial empty index
                index_path = os.path.join(self.faiss_indices_path, f"{table_name}_{col_name}.index")
                faiss_index.save(index_path)
                
                # Store in registry
                if table_name not in self.table_registry:
                    self.table_registry[table_name] = {
                        "vector_columns": {},
                        "dimensions": {},
                        "faiss_index": None
                    }
                
                self.table_registry[table_name]["vector_columns"][col_name] = dim
                self.table_registry[table_name]["dimensions"][col_name] = dim
                
                # For now, use the first vector column as the main index
                if self.table_registry[table_name]["faiss_index"] is None:
                    self.table_registry[table_name]["faiss_index"] = faiss_index
            
            # Store metadata in system table
            cur.execute("""
                INSERT OR REPLACE INTO __mindsdb_tables__ 
                (table_name, vector_columns, dimensions) 
                VALUES (?, ?, ?)
            """, (
                table_name,
                json.dumps(vector_columns),
                json.dumps({col: dim for col, dim in vector_columns.items()})
            ))

    def drop_table(self, table_name: str, if_exists=True) -> Response:
        """Drop table from both DuckDB and Faiss."""
        try:
            with self.connection.cursor() as cur:
                # Drop DuckDB table
                drop_sql = f"DROP TABLE {'IF EXISTS' if if_exists else ''} {table_name}"
                cur.execute(drop_sql)
                
                # Remove from system table
                cur.execute("DELETE FROM __mindsdb_tables__ WHERE table_name = ?", (table_name,))
                
                # Delete Faiss index files
                if table_name in self.table_registry:
                    vector_columns = self.table_registry[table_name]["vector_columns"]
                    for col_name in vector_columns:
                        index_path = os.path.join(self.faiss_indices_path, f"{table_name}_{col_name}.index")
                        if os.path.exists(index_path):
                            os.remove(index_path)
                        if os.path.exists(index_path + ".meta"):
                            os.remove(index_path + ".meta")
                    
                    del self.table_registry[table_name]
                
                logger.info(f"Dropped table {table_name}")
                return Response(RESPONSE_TYPE.OK)
                
        except Exception as e:
            logger.error(f"Error dropping table {table_name}: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

    def insert(self, table_name: str, data: pd.DataFrame) -> Response:
        """Insert data into both DuckDB and Faiss."""
        try:
            if table_name not in self.table_registry:
                raise ValueError(f"Table {table_name} does not exist")
            
            vector_columns = self.table_registry[table_name]["vector_columns"]
            faiss_index = self._get_faiss_index(table_name)
            
            if faiss_index is None:
                raise ValueError(f"Faiss index not found for table {table_name}")
            
            # Process each row
            for _, row in data.iterrows():
                # Generate ID if not provided
                if "id" not in row or pd.isna(row["id"]):
                    content = str(row.get("content", ""))
                    row["id"] = self._generate_id(content)
                
                # Prepare DuckDB data (exclude vector columns)
                duckdb_data = {
                    "id": str(row["id"]),
                    "content": str(row.get("content", "")),
                    "metadata": json.dumps(row.get("metadata", {}))
                }
                
                # Add regular columns
                for col in row.index:
                    if col not in ["id", "content", "metadata"] and col not in vector_columns:
                        duckdb_data[col] = row[col]
                
                # Insert into DuckDB
                with self.connection.cursor() as cur:
                    columns = list(duckdb_data.keys())
                    values = list(duckdb_data.values())
                    placeholders = ", ".join(["?" for _ in values])
                    insert_sql = f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
                    cur.execute(insert_sql, values)
                
                # Add vectors to Faiss
                for col_name, dim in vector_columns.items():
                    if col_name in row and not pd.isna(row[col_name]):
                        vector = row[col_name]
                        if isinstance(vector, str):
                            vector = json.loads(vector)
                        elif isinstance(vector, list):
                            vector = vector
                        else:
                            continue
                        
                        # Convert to numpy array
                        vector_array = [[float(x) for x in vector]]
                        faiss_index.add(vector_array, [int(row["id"])])
            
            # Save Faiss index
            index_path = os.path.join(self.faiss_indices_path, f"{table_name}.index")
            faiss_index.save(index_path)
            
            # Sync to handler storage
            if self.handler_storage:
                self.handler_storage.folder_sync("duckdb_faiss_data")
            
            logger.info(f"Inserted {len(data)} rows into {table_name}")
            return Response(RESPONSE_TYPE.OK)
            
        except Exception as e:
            logger.error(f"Error inserting into table {table_name}: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

    def select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
    ) -> pd.DataFrame:
        """Select data with hybrid search logic."""
        try:
            if table_name not in self.table_registry:
                raise ValueError(f"Table {table_name} does not exist")
            
            # Check if this is a vector search by looking for distance column
            distance_condition = None
            embedding_condition = None
            metadata_conditions = []
            
            if conditions:
                for condition in conditions:
                    if condition.column == "distance":
                        # This is a distance threshold condition
                        distance_condition = condition
                    elif condition.column in self.table_registry[table_name]["vector_columns"]:
                        # Direct vector column condition
                        embedding_condition = condition
                    else:
                        metadata_conditions.append(condition)
            
            # If no vector search, use standard DuckDB query
            if embedding_condition is None and distance_condition is None:
                return self._select_duckdb_only(table_name, columns, metadata_conditions, offset, limit)
            
            # Enhanced vector search logic
            if distance_condition:
                # This is the new syntax: WHERE distance < threshold
                return self._select_with_distance_condition(table_name, columns, 
                                                          distance_condition, metadata_conditions, 
                                                          offset, limit)
            elif embedding_condition:
                # Legacy syntax: WHERE embeddings <-> vector < threshold
                if metadata_conditions:
                    return self._select_metadata_first(table_name, columns, metadata_conditions, 
                                                     embedding_condition, offset, limit)
                else:
                    return self._select_vector_first(table_name, columns, embedding_condition, offset, limit)
                
        except Exception as e:
            logger.error(f"Error selecting from table {table_name}: {e}")
            raise

    def _select_duckdb_only(self, table_name: str, columns: List[str], 
                           conditions: List[FilterCondition], offset: int, limit: int) -> pd.DataFrame:
        """Standard DuckDB SELECT query."""
        with self.connection.cursor() as cur:
            # Build WHERE clause
            where_clause = ""
            params = []
            
            if conditions:
                where_parts = []
                for condition in conditions:
                    if condition.op == FilterOperator.EQUAL:
                        where_parts.append(f"{condition.column} = ?")
                        params.append(condition.value)
                    elif condition.op == FilterOperator.IN:
                        placeholders = ", ".join(["?" for _ in condition.value])
                        where_parts.append(f"{condition.column} IN ({placeholders})")
                        params.extend(condition.value)
                    # Add more operators as needed
                
                if where_parts:
                    where_clause = "WHERE " + " AND ".join(where_parts)
            
            # Build query
            select_columns = ", ".join(columns) if columns else "*"
            query = f"SELECT {select_columns} FROM {table_name} {where_clause}"
            
            if limit:
                query += f" LIMIT {limit}"
            if offset:
                query += f" OFFSET {offset}"
            
            cur.execute(query, params)
            result = cur.fetchdf()
            return result

    def _select_metadata_first(self, table_name: str, columns: List[str], 
                              metadata_conditions: List[FilterCondition],
                              embedding_condition: FilterCondition, 
                              offset: int, limit: int) -> pd.DataFrame:
        """Filter metadata first, then search Faiss."""
        # First, get IDs from DuckDB based on metadata conditions
        with self.connection.cursor() as cur:
            where_parts = []
            params = []
            
            for condition in metadata_conditions:
                if condition.op == FilterOperator.EQUAL:
                    where_parts.append(f"{condition.column} = ?")
                    params.append(condition.value)
                elif condition.op == FilterOperator.IN:
                    placeholders = ", ".join(["?" for _ in condition.value])
                    where_parts.append(f"{condition.column} IN ({placeholders})")
                    params.extend(condition.value)
            
            where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""
            query = f"SELECT id FROM {table_name} {where_clause}"
            
            cur.execute(query, params)
            allowed_ids = [row[0] for row in cur.fetchall()]
        
        # Search Faiss with filtered IDs
        faiss_index = self._get_faiss_index(table_name)
        if faiss_index is None:
            raise ValueError(f"Faiss index not found for table {table_name}")
        
        # Convert embedding to numpy array
        embedding = embedding_condition.value
        if isinstance(embedding, str):
            embedding = json.loads(embedding)
        
        distances, indices, _ = faiss_index.search(
            [embedding], 
            k=limit or 100,
            allowed_ids=allowed_ids
        )
        
        # Fetch full data from DuckDB
        if len(indices[0]) > 0:
            ids = [str(idx) for idx in indices[0] if idx != -1]
            if ids:
                placeholders = ", ".join(["?" for _ in ids])
                select_columns = ", ".join(columns) if columns else "*"
                query = f"SELECT {select_columns} FROM {table_name} WHERE id IN ({placeholders})"
                
                with self.connection.cursor() as cur:
                    cur.execute(query, ids)
                    result = cur.fetchdf()
                    
                    # Add distance column
                    if "distance" in (columns or []):
                        result["distance"] = distances[0][:len(result)]
                    
                    return result
        
        return pd.DataFrame()

    def _select_vector_first(self, table_name: str, columns: List[str], 
                            embedding_condition: FilterCondition, 
                            offset: int, limit: int) -> pd.DataFrame:
        """Search Faiss first, then fetch from DuckDB."""
        faiss_index = self._get_faiss_index(table_name)
        if faiss_index is None:
            raise ValueError(f"Faiss index not found for table {table_name}")
        
        # Convert embedding to numpy array
        embedding = embedding_condition.value
        if isinstance(embedding, str):
            embedding = json.loads(embedding)
        
        distances, indices, _ = faiss_index.search(
            [embedding], 
            k=limit or 100
        )
        
        # Fetch full data from DuckDB
        if len(indices[0]) > 0:
            ids = [str(idx) for idx in indices[0] if idx != -1]
            if ids:
                placeholders = ", ".join(["?" for _ in ids])
                select_columns = ", ".join(columns) if columns else "*"
                query = f"SELECT {select_columns} FROM {table_name} WHERE id IN ({placeholders})"
                
                with self.connection.cursor() as cur:
                    cur.execute(query, ids)
                    result = cur.fetchdf()
                    
                    # Add distance column
                    if "distance" in (columns or []):
                        result["distance"] = distances[0][:len(result)]
                    
                    return result
        
        return pd.DataFrame()

    def _select_with_distance_condition(self, table_name: str, columns: List[str], 
                                      distance_condition: FilterCondition, 
                                      metadata_conditions: List[FilterCondition],
                                      offset: int, limit: int) -> pd.DataFrame:
        """Handle WHERE distance < threshold syntax - need to extract query vector from SELECT clause."""
        # For now, we'll need to extract the query vector from the original query
        # This is a limitation - we need access to the original SELECT query to parse vector operations
        # For this implementation, we'll assume the query vector is provided in a special way
        
        # This is a simplified implementation - in practice, we'd need to parse the original SQL
        # to extract the vector operation from the SELECT clause
        logger.warning("Distance-based filtering requires query vector extraction from SELECT clause")
        logger.warning("This is a simplified implementation - full support requires SQL parsing")
        
        # For now, return empty result - this would need the full SQL parser integration
        return pd.DataFrame()

    def dispatch_select(
        self,
        query: Select,
        conditions: Optional[List[FilterCondition]] = None,
        allowed_metadata_columns: List[str] = None,
        keyword_search_args: Optional[KeywordSearchArgs] = None,
    ):
        """
        Enhanced dispatcher that can parse vector operations from SELECT clause.
        """
        # Parse the SELECT query to extract vector operations
        vector_ops = self._parse_vector_operations(query)
        
        if vector_ops:
            # This is a vector search query with distance-based filtering
            return self._execute_enhanced_vector_search(query, vector_ops, conditions, 
                                                      allowed_metadata_columns, keyword_search_args)
        else:
            # Use the standard dispatcher
            return super().dispatch_select(query, conditions, allowed_metadata_columns, keyword_search_args)

    def _parse_vector_operations(self, query: Select) -> Dict[str, Any]:
        """Parse SELECT query to extract vector operations like embeddings <-> vector."""
        vector_ops = {}
        
        for target in query.targets:
            if isinstance(target, BinaryOperation) and target.op == "<->":
                # Extract: embeddings <-> '[0.1, 0.2, ...]' as distance
                vector_column = target.args[0].parts[-1]  # 'embeddings'
                query_vector = target.args[1].value       # '[0.1, 0.2, ...]'
                alias = target.alias.parts[-1] if target.alias else "distance"
                
                vector_ops[alias] = {
                    "column": vector_column,
                    "vector": query_vector,
                    "operation": target
                }
        
        return vector_ops

    def _execute_enhanced_vector_search(self, query: Select, vector_ops: Dict[str, Any], 
                                      conditions: List[FilterCondition], 
                                      allowed_metadata_columns: List[str],
                                      keyword_search_args: Optional[KeywordSearchArgs]) -> pd.DataFrame:
        """Execute enhanced vector search with distance-based filtering."""
        table_name = query.from_table.parts[-1]
        
        # Get the query vector (assuming single vector operation for now)
        distance_alias = list(vector_ops.keys())[0]
        vector_info = vector_ops[distance_alias]
        query_vector = vector_info["vector"]
        
        # Parse conditions to separate vector and metadata filters
        vector_conditions = []
        metadata_conditions = []
        
        if conditions:
            for condition in conditions:
                if condition.column == distance_alias:
                    # This is a distance threshold condition
                    vector_conditions.append(condition)
                else:
                    metadata_conditions.append(condition)
        
        # Get columns to select
        if isinstance(query.targets[0], Star):
            columns = ["id", "content", "metadata"]  # Default columns
        else:
            columns = [col.parts[-1] for col in query.targets]
        
        # Get offset and limit
        offset = query.offset.value if query.offset is not None else None
        limit = query.limit.value if query.limit is not None else None
        
        # Execute hybrid search
        if metadata_conditions:
            return self._select_metadata_first_enhanced(table_name, columns, query_vector, 
                                                      vector_conditions, metadata_conditions, 
                                                      offset, limit)
        else:
            return self._select_vector_first_enhanced(table_name, columns, query_vector, 
                                                    vector_conditions, offset, limit)

    def _select_vector_first_enhanced(self, table_name: str, columns: List[str], 
                                    query_vector: List[float], vector_conditions: List[FilterCondition],
                                    offset: int, limit: int) -> pd.DataFrame:
        """Enhanced vector-first search with distance filtering."""
        faiss_index = self._get_faiss_index(table_name)
        if faiss_index is None:
            raise ValueError(f"Faiss index not found for table {table_name}")
        
        # Convert query vector
        if isinstance(query_vector, str):
            query_vector = json.loads(query_vector)
        
        # Search Faiss
        distances, indices, _ = faiss_index.search(
            [query_vector], 
            k=limit or 100
        )
        
        # Apply distance threshold from WHERE clause
        distance_threshold = None
        for condition in vector_conditions:
            if condition.column == "distance" and condition.op == FilterOperator.LESS_THAN:
                distance_threshold = condition.value
                break
        
        # Filter by distance threshold
        if distance_threshold is not None:
            valid_indices = []
            valid_distances = []
            for i, (idx, dist) in enumerate(zip(indices[0], distances[0])):
                if idx != -1 and dist < distance_threshold:
                    valid_indices.append(idx)
                    valid_distances.append(dist)
        else:
            valid_indices = [idx for idx in indices[0] if idx != -1]
            valid_distances = [dist for i, dist in enumerate(distances[0]) 
                              if indices[0][i] != -1]
        
        # Fetch data from DuckDB
        if valid_indices:
            ids = [str(idx) for idx in valid_indices]
            placeholders = ", ".join(["?" for _ in ids])
            select_columns = ", ".join(columns) if columns else "*"
            db_query = f"SELECT {select_columns} FROM {table_name} WHERE id IN ({placeholders})"
            
            with self.connection.cursor() as cur:
                cur.execute(db_query, ids)
                result = cur.fetchdf()
                
                # Add distance column
                if "distance" in columns:
                    result["distance"] = valid_distances[:len(result)]
                
                return result
        
        return pd.DataFrame()

    def _select_metadata_first_enhanced(self, table_name: str, columns: List[str], 
                                       query_vector: List[float], vector_conditions: List[FilterCondition],
                                       metadata_conditions: List[FilterCondition], 
                                       offset: int, limit: int) -> pd.DataFrame:
        """Enhanced metadata-first search with distance filtering."""
        # First, get IDs from DuckDB based on metadata conditions
        with self.connection.cursor() as cur:
            where_parts = []
            params = []
            
            for condition in metadata_conditions:
                if condition.op == FilterOperator.EQUAL:
                    where_parts.append(f"{condition.column} = ?")
                    params.append(condition.value)
                elif condition.op == FilterOperator.IN:
                    placeholders = ", ".join(["?" for _ in condition.value])
                    where_parts.append(f"{condition.column} IN ({placeholders})")
                    params.extend(condition.value)
            
            where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""
            query = f"SELECT id FROM {table_name} {where_clause}"
            
            cur.execute(query, params)
            allowed_ids = [row[0] for row in cur.fetchall()]
        
        # Search Faiss with filtered IDs
        faiss_index = self._get_faiss_index(table_name)
        if faiss_index is None:
            raise ValueError(f"Faiss index not found for table {table_name}")
        
        # Convert query vector
        if isinstance(query_vector, str):
            query_vector = json.loads(query_vector)
        
        distances, indices, _ = faiss_index.search(
            [query_vector], 
            k=limit or 100,
            allowed_ids=allowed_ids
        )
        
        # Apply distance threshold
        distance_threshold = None
        for condition in vector_conditions:
            if condition.column == "distance" and condition.op == FilterOperator.LESS_THAN:
                distance_threshold = condition.value
                break
        
        # Filter by distance threshold
        if distance_threshold is not None:
            valid_indices = []
            valid_distances = []
            for i, (idx, dist) in enumerate(zip(indices[0], distances[0])):
                if idx != -1 and dist < distance_threshold:
                    valid_indices.append(idx)
                    valid_distances.append(dist)
        else:
            valid_indices = [idx for idx in indices[0] if idx != -1]
            valid_distances = [dist for i, dist in enumerate(distances[0]) 
                              if indices[0][i] != -1]
        
        # Fetch full data from DuckDB
        if valid_indices:
            ids = [str(idx) for idx in valid_indices]
            placeholders = ", ".join(["?" for _ in ids])
            select_columns = ", ".join(columns) if columns else "*"
            query = f"SELECT {select_columns} FROM {table_name} WHERE id IN ({placeholders})"
            
            with self.connection.cursor() as cur:
                cur.execute(query, ids)
                result = cur.fetchdf()
                
                # Add distance column
                if "distance" in columns:
                    result["distance"] = valid_distances[:len(result)]
                
                return result
        
        return pd.DataFrame()

    def update(self, table_name: str, data: pd.DataFrame, key_columns: List[str] = None) -> Response:
        """Update data in both DuckDB and Faiss."""
        try:
            if table_name not in self.table_registry:
                raise ValueError(f"Table {table_name} does not exist")
            
            vector_columns = self.table_registry[table_name]["vector_columns"]
            faiss_index = self._get_faiss_index(table_name)
            
            for _, row in data.iterrows():
                # Update DuckDB
                with self.connection.cursor() as cur:
                    set_clauses = []
                    params = []
                    
                    for col, value in row.items():
                        if col != "id":
                            set_clauses.append(f"{col} = ?")
                            params.append(value)
                    
                    params.append(row["id"])
                    update_sql = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE id = ?"
                    cur.execute(update_sql, params)
                
                # Update Faiss (delete old, add new)
                if faiss_index and vector_columns:
                    # Mark old ID as deleted
                    faiss_index.delete_ids([int(row["id"])])
                    
                    # Add new vector if present
                    for col_name in vector_columns:
                        if col_name in row and not pd.isna(row[col_name]):
                            vector = row[col_name]
                            if isinstance(vector, str):
                                vector = json.loads(vector)
                            
                            vector_array = [[float(x) for x in vector]]
                            faiss_index.add(vector_array, [int(row["id"])])
            
            # Save Faiss index
            if faiss_index:
                index_path = os.path.join(self.faiss_indices_path, f"{table_name}.index")
                faiss_index.save(index_path)
            
            return Response(RESPONSE_TYPE.OK)
            
        except Exception as e:
            logger.error(f"Error updating table {table_name}: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

    def delete(self, table_name: str, conditions: List[FilterCondition] = None) -> Response:
        """Delete data from both DuckDB and Faiss."""
        try:
            if table_name not in self.table_registry:
                raise ValueError(f"Table {table_name} does not exist")
            
            # Get IDs to delete from DuckDB
            with self.connection.cursor() as cur:
                where_parts = []
                params = []
                
                if conditions:
                    for condition in conditions:
                        if condition.op == FilterOperator.EQUAL:
                            where_parts.append(f"{condition.column} = ?")
                            params.append(condition.value)
                        elif condition.op == FilterOperator.IN:
                            placeholders = ", ".join(["?" for _ in condition.value])
                            where_parts.append(f"{condition.column} IN ({placeholders})")
                            params.extend(condition.value)
                
                where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""
                query = f"SELECT id FROM {table_name} {where_clause}"
                
                cur.execute(query, params)
                ids_to_delete = [row[0] for row in cur.fetchall()]
            
            # Delete from DuckDB
            if ids_to_delete:
                with self.connection.cursor() as cur:
                    placeholders = ", ".join(["?" for _ in ids_to_delete])
                    delete_sql = f"DELETE FROM {table_name} WHERE id IN ({placeholders})"
                    cur.execute(delete_sql, ids_to_delete)
                
                # Mark as deleted in Faiss
                faiss_index = self._get_faiss_index(table_name)
                if faiss_index:
                    faiss_index.delete_ids([int(id_val) for id_val in ids_to_delete])
                    
                    # Save updated index
                    index_path = os.path.join(self.faiss_indices_path, f"{table_name}.index")
                    faiss_index.save(index_path)
            
            return Response(RESPONSE_TYPE.OK)
            
        except Exception as e:
            logger.error(f"Error deleting from table {table_name}: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

    def get_tables(self) -> Response:
        """Get list of tables."""
        try:
            with self.connection.cursor() as cur:
                cur.execute("SELECT table_name FROM __mindsdb_tables__")
                tables = [row[0] for row in cur.fetchall()]
                
                data = [{"table_name": table} for table in tables]
                return Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame(data))
                
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

    def get_columns(self, table_name: str) -> Response:
        """Get table columns."""
        try:
            if table_name not in self.table_registry:
                raise ValueError(f"Table {table_name} does not exist")
            
            # Get DuckDB columns
            with self.connection.cursor() as cur:
                cur.execute(f"DESCRIBE {table_name}")
                columns = cur.fetchall()
            
            # Add vector columns to schema
            vector_columns = self.table_registry[table_name]["vector_columns"]
            
            data = []
            for col_name, col_type in columns:
                data.append({"COLUMN_NAME": col_name, "DATA_TYPE": col_type})
            
            # Add vector columns
            for col_name, dim in vector_columns.items():
                data.append({"COLUMN_NAME": col_name, "DATA_TYPE": f"VECTOR({dim})"})
            
            return Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame(data))
            
        except Exception as e:
            logger.error(f"Error getting columns for table {table_name}: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

    def check_connection(self) -> Response:
        """Check the connection to the database."""
        try:
            if not self.is_connected:
                self.connect()
            return Response(RESPONSE_TYPE.OK)
        except Exception as e:
            logger.error(f"Connection check failed: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

    def native_query(self, query: str) -> Response:
        """Execute a native SQL query."""
        try:
            with self.connection.cursor() as cur:
                cur.execute(query)
                result = cur.fetchdf()
                return Response(RESPONSE_TYPE.TABLE, data_frame=result)
        except Exception as e:
            logger.error(f"Error executing native query: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

    def __del__(self):
        """Cleanup on deletion."""
        if self.is_connected:
            self.disconnect()

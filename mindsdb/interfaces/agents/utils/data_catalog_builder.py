"""Data catalog builder for agents - constructs and caches data catalogs for tables and knowledge bases"""

import csv
import hashlib
from io import StringIO
from typing import Dict, List, Optional, Any
import pandas as pd

from mindsdb.utilities import log
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.cache import get_cache
from mindsdb.interfaces.agents.utils.sql_toolkit import list_to_csv_str, MindsDBQuery
from mindsdb.utilities.exception import QueryError

logger = log.getLogger(__name__)

_MAX_CACHE_SIZE = 100


def dataframe_to_markdown(df: pd.DataFrame) -> str:
    """
    Convert a pandas DataFrame to markdown table string.
    
    Args:
        df: DataFrame to convert
        
    Returns:
        Markdown-formatted table string
    """
    if df is None or df.empty:
        return ""
    
    # Try using pandas' built-in to_markdown if available (requires tabulate)
    try:
        return df.to_markdown(index=False)
    except (AttributeError, ImportError):
        # Fallback: manual markdown table generation
        lines = []
        
        # Get column names
        columns = df.columns.tolist()
        
        # Convert all values to strings and handle None/NaN/arrays
        def format_value(val):
            # Handle arrays/lists first - pd.isna() fails on arrays
            if isinstance(val, (list, tuple)):
                return str(val)
            # Handle pandas Series/Index - pd.isna() returns array for these
            if isinstance(val, (pd.Series, pd.Index)):
                return str(val.tolist())
            # Handle numpy arrays
            try:
                import numpy as np
                if isinstance(val, np.ndarray):
                    return str(val.tolist())
            except ImportError:
                pass
            # Handle scalar NaN/None values
            # Use try-except to catch ValueError when pd.isna() returns array
            try:
                is_na_result = pd.isna(val)
                # Check if result is array-like (would cause ambiguity error in if statement)
                if hasattr(is_na_result, '__len__') and not isinstance(is_na_result, (str, bool)):
                    # Result is array-like, treat as non-NA and convert to string
                    return str(val)
                # Only check boolean result if it's a scalar
                if isinstance(is_na_result, bool) and is_na_result:
                    return ""
            except (ValueError, TypeError):
                # pd.isna() can fail on some types (e.g., arrays), just convert to string
                pass
            return str(val)
        
        # Calculate column widths
        col_widths = {}
        for col in columns:
            col_widths[col] = max(
                len(str(col)),
                max([len(format_value(val)) for val in df[col]], default=0)
            )
        
        # Build header row
        header = "| " + " | ".join([str(col).ljust(col_widths[col]) for col in columns]) + " |"
        lines.append(header)
        
        # Build separator row
        separator = "| " + " | ".join(["-" * col_widths[col] for col in columns]) + " |"
        lines.append(separator)
        
        # Build data rows
        for _, row in df.iterrows():
            data_row = "| " + " | ".join([
                format_value(row[col]).ljust(col_widths[col]) for col in columns
            ]) + " |"
            lines.append(data_row)
        
        return "\n".join(lines)


class DataCatalogBuilder:
    """Builds and caches data catalogs for agent data sources"""

    def __init__(self, disable_cache: bool = True):
        """
        Initialize data catalog builder.

        Args:
            disable_cache: If True, disable caching for all catalog operations (default: False)
        """
        self.sql_toolkit =  MindsDBQuery()
        self.cache = get_cache("agent", max_size=_MAX_CACHE_SIZE)
        self.disable_cache = disable_cache

    def _get_cache_key(
        self, 
        tables: Optional[List[str]] = None, 
        knowledge_bases: Optional[List[str]] = None,
        suffix: str = "data_catalog"
    ) -> str:
        """
        Generate cache key for data catalog based on MD5 hash of tables and knowledge bases.
        
        Args:
            tables: List of table names
            knowledge_bases: List of knowledge base names
            suffix: Cache key suffix
            
        Returns:
            Cache key string
        """
        # Create a sorted string representation of tables and knowledge bases
        tables_str = ",".join(sorted(tables or []))
        kbs_str = ",".join(sorted(knowledge_bases or []))
        combined = f"{tables_str}|{kbs_str}"
        
        # Generate MD5 hash
        md5_hash = hashlib.md5(combined.encode('utf-8')).hexdigest()+'.2'
        
        return f"{ctx.company_id}_{md5_hash}_{suffix}"

    def _dataframe_to_csv(self, df: pd.DataFrame) -> str:
        """
        Convert a pandas DataFrame to CSV string.
        
        Args:
            df: DataFrame to convert
            
        Returns:
            CSV-formatted string
        """
        if df is None or df.empty:
            return ""
        
        output = StringIO()
        df.to_csv(output, index=False, lineterminator='\n')
        return output.getvalue()

    def build_table_catalog_entry(self, table_name: str, schema: Optional[str] = None) -> Dict[str, Any]:
        """
        Build catalog entry for a single table.

        Args:
            table_name: Name of the table
            schema: Schema name (defaults to 'mindsdb' if not provided)

        Returns:
            Dictionary with 'sample_data', 'metadata', 'sample_data_query', 'metadata_query', and 'table_name'
        """
        if schema is None:
            schema = "mindsdb"

        # Ensure lowercase
        schema = schema.lower()
        table_name = table_name.lower()

        # Get sample data
        sample_data_csv = ""
        sample_data_query = f"SELECT * FROM {schema}.{table_name} LIMIT 5"
        try:
            result = self.sql_toolkit.execute(sample_data_query)
            if isinstance(result, pd.DataFrame):
                sample_data_csv = self._dataframe_to_csv(result)
            else:
                sample_data_csv = f"Error retrieving sample data: Unexpected result type"

        except QueryError as e:
            logger.warning(f"Error getting sample data for table {schema}.{table_name}: {e}")
            sample_data_csv = f"Error retrieving sample data: {str(e)}"
        except Exception as e:
            logger.warning(f"Error getting sample data for table {schema}.{table_name}: {e}")
            sample_data_csv = f"Error retrieving sample data: {str(e)}"

        # Get metadata
        metadata_csv = None
        # Query information_schema.columns for table metadata
        metadata_query = f"""
            SELECT 
                table_schema, table_name, column_name, column_type, original_type
            FROM information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = '{table_name}'
        """
        try:
            result = self.sql_toolkit.execute(metadata_query)
            logger.debug(f"result: {result}")
            if isinstance(result, pd.DataFrame):
                metadata_csv = self._dataframe_to_csv(result)
            else:
                metadata_csv = None
        except QueryError as e:
            logger.warning(f"Error getting metadata for table {schema}.{table_name}: {e}")
            metadata_csv = None
        except Exception as e:
            logger.warning(f"Error getting metadata for table {schema}.{table_name}: {e}")
            metadata_csv = None

        return {
            "sample_data": sample_data_csv,
            "metadata": metadata_csv,
            "sample_data_query": sample_data_query.strip(),
            "table_name": f"{schema}.{table_name}",
        }

    def build_knowledge_base_catalog_entry(
        self, kb_name: str, project: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Build catalog entry for a single knowledge base.

        Args:
            kb_name: Name of the knowledge base
            project: Project name (defaults to 'mindsdb' if not provided)

        Returns:
            Dictionary with 'sample_data', 'metadata', 'sample_data_query', 'metadata_query', and 'kb_name'
        """
        if project is None:
            project = "mindsdb"

        # Get sample data
        sample_data_csv = ""
        sample_data_query = f"SELECT * FROM {project}.{kb_name} LIMIT 3"
        try:
            result = self.sql_toolkit.execute(sample_data_query)
            if isinstance(result, pd.DataFrame):
                sample_data_csv = self._dataframe_to_csv(result)
            else:
                sample_data_csv = f"Error retrieving sample data: Unexpected result type"
        except QueryError as e:
            logger.warning(f"Error getting sample data for KB {project}.{kb_name}: {e}")
            sample_data_csv = f"Error retrieving sample data: {str(e)}"
        except Exception as e:
            logger.warning(f"Error getting sample data for KB {project}.{kb_name}: {e}")
            sample_data_csv = f"Error retrieving sample data: {str(e)}"

        # Get metadata
        metadata_csv = ""
        # Query information_schema.knowledge_bases for KB metadata
        # Based on README: SELECT kbs.project || '.' || kbs.name AS kb, q.sql AS kb_insert_query,
        # COALESCE(kbs.id_column, 'id') AS parent_query_id_column,
        # COALESCE(kbs.content_columns, 'content') AS parent_query_content_columns,
        # COALESCE(kbs.metadata_columns, '*') AS parent_query_metadata_columns
        metadata_query = f"""
            SELECT 
                kbs.project || '.' || kbs.name AS kb,
                q.sql AS kb_insert_query,
                COALESCE(kbs.id_column, 'id') AS parent_query_id_column,
                COALESCE(kbs.content_columns, 'content') AS parent_query_content_columns,
                COALESCE(kbs.metadata_columns, '*') AS parent_query_metadata_columns
            FROM information_schema.knowledge_bases kbs
            LEFT JOIN information_schema.queries q ON kbs.query_id = q.id
            WHERE kbs.name = '{kb_name}' AND kbs.project = '{project}'
        """
        try:
            result = self.sql_toolkit.execute(metadata_query)
            if isinstance(result, pd.DataFrame):
                metadata_csv = self._dataframe_to_csv(result)
            else:
                metadata_csv = f"Error retrieving metadata: Unexpected result type"
        except QueryError as e:
            logger.warning(f"Error getting metadata for KB {project}.{kb_name}: {e}")
            metadata_csv = f"Error retrieving metadata: {str(e)}"
        except Exception as e:
            logger.warning(f"Error getting metadata for KB {project}.{kb_name}: {e}")
            metadata_csv = f"Error retrieving metadata: {str(e)}"

        return {
            "sample_data": sample_data_csv,
            "metadata": metadata_csv,
            "sample_data_query": sample_data_query.strip(),
            "metadata_query": metadata_query.strip(),
            "kb_name": f"{project}.{kb_name}",
        }

    def build_data_catalog(
        self,
        tables: Optional[List[str]] = None,
        knowledge_bases: Optional[List[str]] = None,
        use_cache: bool = True,
    ) -> str:
        """
        Build complete data catalog for all tables and knowledge bases.

        Args:
            tables: List of table names (format: "schema.table" or just "table")
            knowledge_bases: List of KB names (format: "project.kbname" or just "kbname")
            use_cache: Whether to use cache for catalog

        Returns:
            CSV-formatted string containing the complete data catalog
        """
        cache_key = self._get_cache_key(tables=tables, knowledge_bases=knowledge_bases, suffix="data_catalog")
        
        # Disable cache if instance flag is set
        if self.disable_cache:
            use_cache = False
        
        # Check cache first
        if use_cache:
            cached_catalog = self.cache.get(cache_key)
            if cached_catalog:
                logger.debug(f"Using cached data catalog for tables={tables}, knowledge_bases={knowledge_bases}")
                return cached_catalog

        # Build catalog
        catalog_parts = []

        # Process tables
        if tables:
            catalog_parts.append("=== TABLES CATALOG ===")
            for table in tables:
                # Parse table name (format: "schema.table" or just "table")
                parts = table.split(".", 1)
                if len(parts) == 2:
                    schema, table_name = parts
                else:
                    schema = None
                    table_name = parts[0]

                entry = self.build_table_catalog_entry(table_name, schema)
                catalog_parts.append(f"\n--- Table: {entry['table_name']} ---")
                catalog_parts.append(f"Sample Data Query:\n{entry['sample_data_query']}")
                catalog_parts.append(f"Sample Data (csv):\n{entry['sample_data']}")
                if entry['metadata'] is not None:
                    catalog_parts.append(f"Metadata (csv):\n{entry['metadata']}")

        # Process knowledge bases
        if knowledge_bases:
            catalog_parts.append("\n=== KNOWLEDGE BASES CATALOG ===")
            for kb in knowledge_bases:
                # Parse KB name (format: "project.kbname" or just "kbname")
                parts = kb.split(".", 1)
                if len(parts) == 2:
                    project, kb_name = parts
                else:
                    project = None
                    kb_name = parts[0]

                entry = self.build_knowledge_base_catalog_entry(kb_name, project)
                catalog_parts.append(f"\n--- Knowledge Base: {entry['kb_name']} ---")
                catalog_parts.append(f"Sample Data Query:\n{entry['sample_data_query']}")
                catalog_parts.append(f"Sample Data (csv):\n{entry['sample_data']}")
                catalog_parts.append(f"Metadata (csv):\n{entry['metadata']}")

        catalog_str = "\n".join(catalog_parts)

        # Cache the catalog
        if use_cache:
            self.cache.set(cache_key, catalog_str)
            logger.debug(f"Cached data catalog for tables={tables}, knowledge_bases={knowledge_bases}")

        return catalog_str


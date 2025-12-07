"""Data catalog builder for agents - constructs and caches data catalogs for tables and knowledge bases"""

import csv
import hashlib
from io import StringIO
from typing import Dict, List, Optional, Any
import pandas as pd

from mindsdb.utilities import log
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.cache import get_cache
from mindsdb.interfaces.agents.utils.sql_toolkit import MindsDBToolKit, list_to_csv_str

logger = log.getLogger(__name__)

_MAX_CACHE_SIZE = 100


class DataCatalogBuilder:
    """Builds and caches data catalogs for agent data sources"""

    def __init__(self, sql_toolkit: MindsDBToolKit):
        """
        Initialize data catalog builder.

        Args:
            sql_toolkit: MindsDBToolKit instance for executing queries
        """
        self.sql_toolkit = sql_toolkit
        self.cache = get_cache("agent", max_size=_MAX_CACHE_SIZE)

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
        md5_hash = hashlib.md5(combined.encode('utf-8')).hexdigest()
        
        return f"{ctx.company_id}_{md5_hash}_{suffix}"

    def build_table_catalog_entry(self, table_name: str, schema: Optional[str] = None) -> Dict[str, str]:
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
        sample_data_query = f"SELECT * FROM `{schema}`.`{table_name}` LIMIT 5"
        try:
            result = self.sql_toolkit._call_engine(sample_data_query)
            if result and hasattr(result, 'data') and result.data:
                sample_rows = result.data.to_lists()
                if sample_rows:
                    # Get column names from result
                    columns = [col.name for col in result.data.columns]
                    sample_data_csv = list_to_csv_str([columns] + sample_rows)
        except Exception as e:
            logger.warning(f"Error getting sample data for table {schema}.{table_name}: {e}")
            sample_data_csv = f"Error retrieving sample data: {str(e)}"

        # Get metadata
        metadata_csv = ""
        # Query information_schema.columns for table metadata
        metadata_query = f"""
            SELECT 
                table_schema, table_name, column_name, column_type, original_type
            FROM information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = '{table_name}'
        """
        try:
            result = self.sql_toolkit._call_engine(metadata_query)
            if result and hasattr(result, 'data') and result.data:
                metadata_rows = result.data.to_lists()
                if metadata_rows:
                    columns = [col.name for col in result.data.columns]
                    metadata_csv = list_to_csv_str([columns] + metadata_rows)
        except Exception as e:
            logger.warning(f"Error getting metadata for table {schema}.{table_name}: {e}")
            metadata_csv = f"Error retrieving metadata: {str(e)}"

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
        sample_data_query = f"SELECT * FROM `{project}`.`{kb_name}` LIMIT 5"
        try:
            result = self.sql_toolkit._call_engine(sample_data_query)
            if result and hasattr(result, 'data') and result.data:
                sample_rows = result.data.to_lists()
                if sample_rows:
                    columns = [col.name for col in result.data.columns]
                    sample_data_csv = list_to_csv_str([columns] + sample_rows)
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
            result = self.sql_toolkit._call_engine(metadata_query)
            if result and hasattr(result, 'data') and result.data:
                metadata_rows = result.data.to_lists()
                if metadata_rows:
                    columns = [col.name for col in result.data.columns]
                    metadata_csv = list_to_csv_str([columns] + metadata_rows)
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
        
        # Check cache first
        if use_cache:
            cached_catalog = self.cache.get(cache_key)
            if cached_catalog:
                logger.info(f"Using cached data catalog for tables={tables}, knowledge_bases={knowledge_bases}")
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
                catalog_parts.append(f"Sample Data:\n{entry['sample_data']}")
                catalog_parts.append(f"Metadata:\n{entry['metadata']}")

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
                catalog_parts.append(f"Sample Data:\n{entry['sample_data']}")
                catalog_parts.append(f"Metadata Query:\n{entry['metadata_query']}")
                catalog_parts.append(f"Metadata:\n{entry['metadata']}")

        catalog_str = "\n".join(catalog_parts)

        # Cache the catalog
        if use_cache:
            self.cache.set(cache_key, catalog_str)
            logger.info(f"Cached data catalog for tables={tables}, knowledge_bases={knowledge_bases}")

        return catalog_str


"""Tools for semantic search and analysis."""

from typing import Dict, List, Optional

import mindsdb_sdk


class SemanticSearchTool:
    """Generic search tool for MindsDB knowledge bases with hybrid search support."""

    def __init__(self, kb_name: str = "confluence_kb"):
        """
        Initialize the search tool.

        Args:
            kb_name: Name of the knowledge base to search
        """
        self.server = mindsdb_sdk.connect()
        self.kb_name = kb_name

    def search(
        self,
        content: str = None,
        filters: Dict = None,
        top_k: int = 5,
        hybrid_search: bool = True,
        hybrid_search_alpha: float = 0.5,
    ) -> List[Dict]:
        """
        Search knowledge base with hybrid search (semantic + metadata filtering).

        Args:
            content: Search content query for semantic search
            filters: Metadata filters (e.g., {'status': 'current'})
            top_k: Maximum number of results to return
            hybrid_search: Enable hybrid search (semantic + keyword)
            hybrid_search_alpha: Balance between semantic (1.0) and keyword (0.0) relevance

        Returns:
            List of matching documents
        """
        query = f"SELECT * FROM {self.kb_name}"
        conditions = []

        if content:
            conditions.append(f"content='{content}'")

        if filters:
            for key, value in filters.items():
                if isinstance(value, dict) and "operator" in value:
                    # Handle advanced operators like LIKE, BETWEEN
                    operator = value["operator"]
                    if operator == "LIKE":
                        conditions.append(f"{key} LIKE '{value['value']}'")
                    elif operator == "BETWEEN":
                        conditions.append(
                            f"{key} BETWEEN '{value['start']}' AND '{value['end']}'"
                        )
                    elif operator == "IN":
                        values = "', '".join(value["values"])
                        conditions.append(f"{key} IN ('{values}')")
                    elif operator == "NOT_NULL":
                        conditions.append(f"{key} IS NOT NULL")
                else:
                    # Simple equality filter
                    conditions.append(f"{key}='{value}'")

        if hybrid_search:
            conditions.append("hybrid_search=true")
            conditions.append(f"hybrid_search_alpha={hybrid_search_alpha}")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        if top_k:
            query += f" LIMIT {top_k}"

        results = self.server.query(query).fetch()

        if results is not None and len(results) > 0:
            results_list = (
                results.to_dict(orient="records")
                if hasattr(results, "to_dict")
                else results
            )
            return results_list
        return []

    def search_by_meta(self, filters: Dict, top_k: int = 5) -> List[Dict]:
        """
        Search by metadata filters only.

        Args:
            filters: Dictionary of metadata filters
            top_k: Maximum number of results to return

        Returns:
            List of matching documents
        """
        return self.search(
            content=None, filters=filters, top_k=top_k, hybrid_search=False
        )

    def hybrid_search_with_date_range(
        self,
        content: str,
        date_field: str,
        start_date: str,
        end_date: str,
        additional_filters: Dict = None,
        top_k: int = 5,
    ) -> List[Dict]:
        """
        Perform hybrid search with date range filtering.

        Args:
            content: Semantic search query
            date_field: Name of the date field (e.g., 'created_at', 'updated_at')
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            additional_filters: Additional metadata filters
            top_k: Maximum number of results

        Returns:
            List of matching documents
        """
        filters = {
            date_field: {"operator": "BETWEEN", "start": start_date, "end": end_date}
        }

        if additional_filters:
            filters.update(additional_filters)

        return self.search(content=content, filters=filters, top_k=top_k)

    def hybrid_search_with_like(
        self,
        content: str,
        field: str,
        pattern: str,
        additional_filters: Dict = None,
        top_k: int = 5,
    ) -> List[Dict]:
        """
        Perform hybrid search with LIKE pattern matching.

        Args:
            content: Semantic search query
            field: Field to apply LIKE filter to
            pattern: SQL LIKE pattern (e.g., '%bug%', 'urgent%')
            additional_filters: Additional metadata filters
            top_k: Maximum number of results

        Returns:
            List of matching documents
        """
        filters = {field: {"operator": "LIKE", "value": pattern}}

        if additional_filters:
            filters.update(additional_filters)

        return self.search(content=content, filters=filters, top_k=top_k)

    def query_raw_data(
        self, datasource: str, table: str, filters: Optional[Dict] = None
    ) -> List[Dict]:
        """
        Query raw data from a datasource (not from KB).

        Args:
            datasource: Datasource name (e.g., 'confluence_datasource')
            table: Table name (e.g., 'pages')
            filters: Filters to apply

        Returns:
            List of matching records
        """
        # Access databases using dot notation
        db_obj = getattr(self.server.databases, datasource)
        table_obj = getattr(db_obj.tables, table)

        if filters:
            df = table_obj.filter(**filters).fetch()
        else:
            df = table_obj.fetch()

        return df.to_dict(orient="records") if hasattr(df, "to_dict") else df

    def refresh_kb(self, datasource: str, table: str) -> None:
        """
        Refresh datasource and update knowledge base.

        Args:
            datasource: Datasource name to refresh
            table: Table name to insert into KB
        """
        # Refresh the datasource
        query = f"REFRESH {datasource}"
        self.server.query(query)
        print(f"✓ Refreshed {datasource}")

        # Update KB
        try:
            kb = self.server.knowledge_bases.get(self.kb_name)
            kb.insert_query(self.server.databases[datasource].tables[table])
            print(f"✓ Updated {self.kb_name}")
        except Exception as e:
            print(f"Error updating KB: {e}")

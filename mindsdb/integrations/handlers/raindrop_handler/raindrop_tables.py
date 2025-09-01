import pandas as pd
from typing import List, Dict, Any

from mindsdb_sql_parser import ast
from mindsdb.integrations.libs.api_handler import APITable

from mindsdb.integrations.utilities.handlers.query_utilities.select_query_utilities import (
    SELECTQueryParser,
    SELECTQueryExecutor,
)
from mindsdb.integrations.utilities.handlers.query_utilities.delete_query_utilities import (
    DELETEQueryParser,
    DELETEQueryExecutor,
)
from mindsdb.integrations.utilities.handlers.query_utilities.update_query_utilities import (
    UPDATEQueryParser,
    UPDATEQueryExecutor,
)
from mindsdb.integrations.utilities.handlers.query_utilities import INSERTQueryParser

from mindsdb.utilities import log

logger = log.getLogger(__name__)


class RaindropsTable(APITable):
    """The Raindrop.io Raindrops (Bookmarks) Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls Raindrop.io raindrops data.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Raindrop.io raindrops matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        select_statement_parser = SELECTQueryParser(query, "raindrops", self.get_columns())
        (
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit,
        ) = select_statement_parser.parse_query()

        # Parse WHERE conditions for Raindrop.io specific filters
        collection_id = 0  # Default to All bookmarks
        search_query = None
        sort_order = None
        raindrop_ids = []
        api_supported_conditions = []  # Conditions that can be handled by Raindrop.io API
        local_filter_conditions = []  # Conditions that need local filtering

        # Parse conditions and categorize them
        parsed_conditions = self._parse_where_conditions(where_conditions)
        collection_id = parsed_conditions.get("collection_id", 0)
        search_query = parsed_conditions.get("search")
        sort_order = parsed_conditions.get("sort")
        raindrop_ids = parsed_conditions.get("raindrop_ids", [])
        api_supported_conditions = parsed_conditions.get("api_supported", [])
        local_filter_conditions = parsed_conditions.get("local_filters", [])
        complex_filters = parsed_conditions.get("complex_filters", {})

        # Handle sorting
        if order_by_conditions:
            for order_condition in order_by_conditions:
                if order_condition.column in ["created", "lastUpdate", "sort", "title"]:
                    if order_condition.ascending:
                        sort_order = order_condition.column
                    else:
                        sort_order = f"-{order_condition.column}"
                    break

        # If specific IDs are requested, try to fetch efficiently
        if raindrop_ids:
            raindrops_data = []
            # Process IDs individually with rate limiting
            # Raindrop.io doesn't have bulk get endpoints, so we need to be careful with rate limits
            for raindrop_id in raindrop_ids:
                try:
                    response = self.handler.connection.get_raindrop(raindrop_id)
                    if response.get("result") and response.get("item"):
                        raindrops_data.append(response["item"])
                except Exception as e:
                    logger.warning(f"Failed to fetch raindrop {raindrop_id}: {e}")
                    continue
        else:
            # Check if we can use advanced filtering endpoint
            if complex_filters and self._can_use_advanced_filters(complex_filters):
                # Use advanced filtering endpoint
                try:
                    response = self.handler.connection.get_raindrops_with_filters(
                        collection_id=collection_id, filters=complex_filters
                    )
                    raindrops_data = response.get("items", [])

                    # If advanced filtering worked, we might still need to apply local filters
                    # for conditions not supported by the advanced endpoint
                    if local_filter_conditions:
                        # Convert to DataFrame for local filtering
                        if raindrops_data:
                            temp_df = pd.json_normalize(raindrops_data)
                            temp_df = self._normalize_raindrop_data(temp_df)
                            temp_df = self._apply_local_filters(temp_df, local_filter_conditions)
                            raindrops_data = temp_df.to_dict("records")

                except Exception as e:
                    logger.warning(f"Advanced filtering failed, falling back to standard endpoint: {e}")
                    # Fall back to standard endpoint
                    raindrops_data = self._fetch_with_standard_endpoint(
                        collection_id, search_query, sort_order, result_limit, local_filter_conditions
                    )
            else:
                # Use standard endpoint
                raindrops_data = self._fetch_with_standard_endpoint(
                    collection_id, search_query, sort_order, result_limit, local_filter_conditions
                )

        # Convert to DataFrame
        if raindrops_data:
            raindrops_df = pd.json_normalize(raindrops_data)
            raindrops_df = self._normalize_raindrop_data(raindrops_df)
        else:
            # Create empty DataFrame with all expected columns
            raindrops_df = pd.DataFrame(columns=self.get_columns())

        # Ensure all expected columns exist (defensive check)
        expected_columns = self.get_columns()
        for col in expected_columns:
            if col not in raindrops_df.columns:
                logger.warning(f"Missing column after normalization: {col}, adding as None")
                raindrops_df[col] = None

        # Apply local filtering for advanced conditions
        if local_filter_conditions:
            raindrops_df = self._apply_local_filters(raindrops_df, local_filter_conditions)

        # Apply additional filtering and ordering using the executor (for any remaining conditions)
        remaining_conditions = [
            cond
            for cond in where_conditions
            if cond not in api_supported_conditions and cond not in local_filter_conditions
        ]
        if remaining_conditions:
            select_statement_executor = SELECTQueryExecutor(
                raindrops_df, selected_columns, remaining_conditions, order_by_conditions
            )
            raindrops_df = select_statement_executor.execute_query()
        else:
            # Apply ordering and column selection manually if no remaining conditions
            if order_by_conditions:
                raindrops_df = self._apply_ordering(raindrops_df, order_by_conditions)
            if selected_columns and selected_columns != self.get_columns():
                available_columns = [col for col in selected_columns if col in raindrops_df.columns]
                if available_columns:
                    raindrops_df = raindrops_df[available_columns]

        # Apply limit if needed
        if result_limit and len(raindrops_df) > result_limit:
            raindrops_df = raindrops_df.head(result_limit)

        return raindrops_df

    def insert(self, query: ast.Insert) -> None:
        """
        Inserts data into the Raindrop.io raindrops.

        Parameters
        ----------
        query : ast.Insert
           Given SQL INSERT query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        insert_statement_parser = INSERTQueryParser(query)
        values_to_insert = insert_statement_parser.parse_query()

        # Process multiple or single inserts
        if isinstance(values_to_insert, list):
            # Multiple inserts
            raindrops_data = []
            for row in values_to_insert:
                raindrop_data = self._prepare_raindrop_data(row)
                raindrops_data.append(raindrop_data)

            # Use batch insert if more than one item
            if len(raindrops_data) > 1:
                self.handler.connection.create_multiple_raindrops(raindrops_data)
            else:
                self.handler.connection.create_raindrop(raindrops_data[0])
        else:
            # Single insert
            raindrop_data = self._prepare_raindrop_data(values_to_insert)
            self.handler.connection.create_raindrop(raindrop_data)

    def update(self, query: ast.Update) -> None:
        """
        Updates data in the Raindrop.io raindrops.

        Parameters
        ----------
        query : ast.Update
           Given SQL UPDATE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        update_statement_parser = UPDATEQueryParser(query)
        values_to_update, where_conditions = update_statement_parser.parse_query()

        # Extract specific IDs and collection filters from WHERE conditions to avoid loading all data
        raindrop_ids = []
        collection_id = None
        search_query = None

        for condition in where_conditions:
            if condition.column in ["_id", "id"]:
                if isinstance(condition.value, list):
                    raindrop_ids.extend(condition.value)
                else:
                    raindrop_ids.append(condition.value)
            elif condition.column == "collection_id":
                collection_id = condition.value
            elif condition.column in ["search", "title"]:
                search_query = condition.value

        # If we have specific IDs, update them directly without loading all data
        if raindrop_ids:
            for raindrop_id in raindrop_ids:
                try:
                    update_data = self._prepare_raindrop_data(values_to_update)
                    self.handler.connection.update_raindrop(raindrop_id, update_data)
                except Exception as e:
                    logger.error(f"Failed to update raindrop {raindrop_id}: {e}")
            return

        # For complex filters, fetch only relevant data based on conditions
        fetch_params = {}
        if collection_id is not None:
            fetch_params["collection_id"] = collection_id
        if search_query:
            fetch_params["search"] = search_query

        # Fetch only the relevant subset of data
        raindrops_data = self.get_raindrops(**fetch_params)

        if not raindrops_data:
            logger.warning("No raindrops found matching the WHERE conditions")
            return

        raindrops_df = pd.json_normalize(raindrops_data)
        raindrops_df = self._normalize_raindrop_data(raindrops_df)

        # Apply remaining filters
        update_query_executor = UPDATEQueryExecutor(raindrops_df, where_conditions)
        raindrops_df = update_query_executor.execute_query()

        if raindrops_df.empty:
            logger.warning("No raindrops found matching the WHERE conditions")
            return

        raindrop_ids = raindrops_df["_id"].tolist()

        # Check if we should do bulk update or individual updates
        if len(raindrop_ids) > 1:
            # Try bulk update first
            collection_id = raindrops_df["collection.$id"].iloc[0] if "collection.$id" in raindrops_df.columns else 0

            try:
                update_data = self._prepare_raindrop_data(values_to_update)
                self.handler.connection.update_multiple_raindrops(
                    collection_id=collection_id, update_data=update_data, ids=raindrop_ids
                )
            except Exception as e:
                logger.warning(f"Bulk update failed, falling back to individual updates: {e}")
                # Fall back to individual updates
                for raindrop_id in raindrop_ids:
                    try:
                        update_data = self._prepare_raindrop_data(values_to_update)
                        self.handler.connection.update_raindrop(raindrop_id, update_data)
                    except Exception as e:
                        logger.error(f"Failed to update raindrop {raindrop_id}: {e}")
        else:
            # Single update
            raindrop_id = raindrop_ids[0]
            update_data = self._prepare_raindrop_data(values_to_update)
            self.handler.connection.update_raindrop(raindrop_id, update_data)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Raindrop.io raindrops.

        Parameters
        ----------
        query : ast.Delete
           Given SQL DELETE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        delete_statement_parser = DELETEQueryParser(query)
        where_conditions = delete_statement_parser.parse_query()

        # Extract specific IDs and collection filters from WHERE conditions to avoid loading all data
        raindrop_ids = []
        collection_id = None
        search_query = None

        for condition in where_conditions:
            if condition.column in ["_id", "id"]:
                if isinstance(condition.value, list):
                    raindrop_ids.extend(condition.value)
                else:
                    raindrop_ids.append(condition.value)
            elif condition.column == "collection_id":
                collection_id = condition.value
            elif condition.column in ["search", "title"]:
                search_query = condition.value

        # If we have specific IDs, delete them directly without loading all data
        if raindrop_ids:
            if len(raindrop_ids) > 1 and collection_id is not None:
                # Try bulk delete if we know the collection
                try:
                    self.handler.connection.delete_multiple_raindrops(collection_id=collection_id, ids=raindrop_ids)
                    return
                except Exception as e:
                    logger.warning(f"Bulk delete failed, falling back to individual deletes: {e}")

            # Individual deletes
            for raindrop_id in raindrop_ids:
                try:
                    self.handler.connection.delete_raindrop(raindrop_id)
                except Exception as e:
                    logger.error(f"Failed to delete raindrop {raindrop_id}: {e}")
            return

        # For complex filters, fetch only relevant data based on conditions
        fetch_params = {}
        if collection_id is not None:
            fetch_params["collection_id"] = collection_id
        if search_query:
            fetch_params["search"] = search_query

        # Fetch only the relevant subset of data
        raindrops_data = self.get_raindrops(**fetch_params)

        if not raindrops_data:
            logger.warning("No raindrops found matching the WHERE conditions")
            return

        raindrops_df = pd.json_normalize(raindrops_data)
        raindrops_df = self._normalize_raindrop_data(raindrops_df)

        # Apply remaining filters
        delete_query_executor = DELETEQueryExecutor(raindrops_df, where_conditions)
        raindrops_df = delete_query_executor.execute_query()

        if raindrops_df.empty:
            logger.warning("No raindrops found matching the WHERE conditions")
            return

        raindrop_ids = raindrops_df["_id"].tolist()

        # Check if we should do bulk delete or individual deletes
        if len(raindrop_ids) > 1:
            # Try bulk delete first
            collection_id = raindrops_df["collection.$id"].iloc[0] if "collection.$id" in raindrops_df.columns else 0

            try:
                self.handler.connection.delete_multiple_raindrops(collection_id=collection_id, ids=raindrop_ids)
            except Exception as e:
                logger.warning(f"Bulk delete failed, falling back to individual deletes: {e}")
                # Fall back to individual deletes
                for raindrop_id in raindrop_ids:
                    try:
                        self.handler.connection.delete_raindrop(raindrop_id)
                    except Exception as e:
                        logger.error(f"Failed to delete raindrop {raindrop_id}: {e}")
        else:
            # Single delete
            raindrop_id = raindrop_ids[0]
            self.handler.connection.delete_raindrop(raindrop_id)

    def get_columns(self) -> List[str]:
        """Get the column names for the raindrops table"""
        return [
            "_id",
            "link",
            "title",
            "excerpt",
            "note",
            "type",
            "cover",
            "tags",
            "important",
            "reminder",
            "removed",
            "created",
            "lastUpdate",
            "domain",
            "collection.id",
            "collection.title",
            "user.id",
            "broken",
            "cache",
            "file.name",
            "file.size",
            "file.type",
        ]

    def get_raindrops(self, **kwargs) -> List[Dict]:
        """Get raindrops data"""
        if not self.handler.connection:
            self.handler.connect()

        # Get from all collections by default
        response = self.handler.connection.get_raindrops(**kwargs)
        return response.get("items", [])

    def _normalize_raindrop_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize raindrop data for consistent column structure"""
        if df.empty:
            return df

        # Process nested data first to extract flattened columns
        try:
            # Handle nested collection data
            if "collection" in df.columns:
                df["collection.id"] = df["collection"].apply(lambda x: x.get("$id") if isinstance(x, dict) else None)
                df["collection.$id"] = df["collection"].apply(lambda x: x.get("$id") if isinstance(x, dict) else None)
                df["collection.title"] = df["collection"].apply(
                    lambda x: x.get("title") if isinstance(x, dict) else None
                )
        except Exception as e:
            logger.warning(f"Error processing collection data: {e}")

        try:
            # Handle nested user data
            if "user" in df.columns:
                df["user.id"] = df["user"].apply(lambda x: x.get("$id") if isinstance(x, dict) else None)
        except Exception as e:
            logger.warning(f"Error processing user data: {e}")

        try:
            # Handle nested file data
            if "file" in df.columns:
                df["file.name"] = df["file"].apply(lambda x: x.get("name") if isinstance(x, dict) else None)
                df["file.size"] = df["file"].apply(lambda x: x.get("size") if isinstance(x, dict) else None)
                df["file.type"] = df["file"].apply(lambda x: x.get("type") if isinstance(x, dict) else None)
        except Exception as e:
            logger.warning(f"Error processing file data: {e}")

        # Convert tags list to string
        try:
            if "tags" in df.columns:
                df["tags"] = df["tags"].apply(lambda x: ",".join(x) if isinstance(x, list) else x)
        except Exception as e:
            logger.warning(f"Error processing tags data: {e}")

        # Convert dates
        for date_col in ["created", "lastUpdate"]:
            try:
                if date_col in df.columns:
                    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
            except Exception as e:
                logger.warning(f"Error processing date column {date_col}: {e}")

        # Ensure ALL expected columns exist, even if empty
        # This must happen LAST to ensure any newly created columns are preserved
        expected_columns = self.get_columns()
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None

        return df

    def _apply_local_filters(self, df: pd.DataFrame, conditions: List) -> pd.DataFrame:
        """Apply local filtering for conditions not supported by Raindrop.io API"""
        if df.empty or not conditions:
            return df

        for condition in conditions:
            # Handle different condition formats
            if isinstance(condition, list) and len(condition) >= 3:
                op, column, value = condition[0], condition[1], condition[2]
            elif hasattr(condition, "op") and hasattr(condition, "column"):
                op = getattr(condition, "op", "=")
                column = condition.column
                value = getattr(condition, "value", None)
            else:
                # Skip malformed conditions
                logger.warning(f"Skipping malformed condition in local filter: {condition}")
                continue

            if column not in df.columns:
                logger.warning(f"Column '{column}' not found in DataFrame, skipping filter")
                continue

            try:
                if op == "=":
                    if isinstance(value, bool):
                        df = df[df[column] == value]
                    else:
                        df = df[df[column].astype(str).str.lower() == str(value).lower()]
                elif op == "!=":
                    df = df[df[column] != value]
                elif op == ">":
                    if column in ["created", "lastUpdate"]:
                        # Convert string dates to datetime for comparison
                        df[column] = pd.to_datetime(df[column], errors="coerce")
                        value = pd.to_datetime(value)
                    df = df[df[column] > value]
                elif op == "<":
                    if column in ["created", "lastUpdate"]:
                        df[column] = pd.to_datetime(df[column], errors="coerce")
                        value = pd.to_datetime(value)
                    df = df[df[column] < value]
                elif op == ">=":
                    if column in ["created", "lastUpdate"]:
                        df[column] = pd.to_datetime(df[column], errors="coerce")
                        value = pd.to_datetime(value)
                    df = df[df[column] >= value]
                elif op == "<=":
                    if column in ["created", "lastUpdate"]:
                        df[column] = pd.to_datetime(df[column], errors="coerce")
                        value = pd.to_datetime(value)
                    df = df[df[column] <= value]
                elif op == "between":
                    if column in ["created", "lastUpdate"]:
                        df[column] = pd.to_datetime(df[column], errors="coerce")
                        start_val, end_val = pd.to_datetime(value[0]), pd.to_datetime(value[1])
                    else:
                        start_val, end_val = value
                    df = df[(df[column] >= start_val) & (df[column] <= end_val)]
                elif op == "like":
                    # Simple LIKE implementation
                    pattern = str(value).replace("%", ".*").replace("_", ".")
                    df = df[df[column].astype(str).str.contains(pattern, case=False, regex=True, na=False)]
                elif op == "in":
                    if isinstance(value, list):
                        df = df[df[column].isin(value)]
                    else:
                        df = df[df[column] == value]
                else:
                    logger.warning(f"Unsupported operator '{op}' for column '{column}', skipping filter")

            except Exception as e:
                logger.warning(f"Error applying filter {op} on column '{column}': {e}")
                continue

        return df

    def _parse_where_conditions(self, conditions: List) -> Dict[str, Any]:
        """Parse WHERE conditions and categorize them for different handling strategies"""
        parsed = {
            "collection_id": 0,
            "search": None,
            "sort": None,
            "raindrop_ids": [],
            "api_supported": [],
            "local_filters": [],
            "complex_filters": {},
        }

        # Collect all search-related conditions for potential optimization
        search_conditions = []

        for condition in conditions:
            # Handle different condition formats
            if isinstance(condition, list) and len(condition) >= 3:
                op, column, value = condition[0], condition[1], condition[2]
            elif hasattr(condition, "op") and hasattr(condition, "column"):
                op = getattr(condition, "op", "=")
                column = condition.column
                value = getattr(condition, "value", None)
            else:
                # Skip malformed conditions
                logger.warning(f"Skipping malformed condition: {condition}")
                continue

            # Collect search-related conditions for optimization
            if self._is_search_condition(column, op):
                search_conditions.append((column, op, value, condition))

            # Categorize conditions based on API support and complexity
            # Defer search-related conditions until after optimization
            if column == "collection_id" and op == "=":
                parsed["collection_id"] = value
                parsed["api_supported"].append(condition)
            elif column == "search" and op == "=":
                # Only handle direct search conditions, defer field-specific searches
                parsed["search"] = value
                parsed["api_supported"].append(condition)
            elif (column in ["_id", "id"]) and op in ["=", "in"]:
                if isinstance(value, list):
                    parsed["raindrop_ids"].extend(value)
                else:
                    parsed["raindrop_ids"].append(value)
                parsed["api_supported"].append(condition)
            # Handle advanced conditions that need local filtering
            elif column in ["created", "lastUpdate", "sort"] and op in [">", "<", ">=", "<=", "between"]:
                parsed["local_filters"].append(condition)
            elif column == "important" and op == "=":
                parsed["local_filters"].append(condition)
            elif column in ["domain"] and op in ["=", "like", "in"]:
                # Only handle domain, defer other text fields until optimization
                parsed["local_filters"].append(condition)
            elif not self._is_search_condition(column, op):
                # For non-search conditions, add to local filtering immediately
                parsed["local_filters"].append(condition)
            # Search-related conditions (title, excerpt, note, tags with = or like) are deferred

        # Optimize search conditions before final categorization
        self._optimize_search_conditions(search_conditions, parsed)

        # Now categorize any remaining search conditions that weren't optimized
        for column, op, value, original_condition in search_conditions:
            if original_condition not in parsed["api_supported"] and original_condition not in parsed["local_filters"]:
                # This condition wasn't optimized, add it to local filters
                parsed["local_filters"].append(original_condition)

        # Build complex filters for advanced API endpoint if we have multiple criteria
        if parsed["search"] or parsed["local_filters"]:
            complex_filters = {}
            if parsed["search"]:
                complex_filters["search"] = parsed["search"]

            # Extract important flag if present in local filters
            for condition in parsed["local_filters"]:
                if isinstance(condition, list) and len(condition) >= 3:
                    op, column, value = condition[0], condition[1], condition[2]
                elif hasattr(condition, "op") and hasattr(condition, "column"):
                    op = getattr(condition, "op", "=")
                    column = condition.column
                    value = getattr(condition, "value", None)
                else:
                    continue

                if column == "important" and op == "=":
                    complex_filters["important"] = value
                elif column == "tags" and op in ["=", "in"]:
                    if isinstance(value, list):
                        complex_filters["tags"] = value
                    else:
                        complex_filters["tags"] = [value]

            if complex_filters:
                parsed["complex_filters"] = complex_filters

        return parsed

    def _is_search_condition(self, column: str, op: str) -> bool:
        """Check if a condition is search-related"""
        return ((column in ["search", "title", "excerpt", "note", "tags"]) and op in ["=", "like"]) or (
            column == "search" and op == "="
        )

    def _optimize_search_conditions(self, search_conditions: List, parsed: Dict[str, Any]) -> None:
        """Optimize multiple search conditions into a single API search query"""
        if not search_conditions:
            return

        # Check if we have a direct search condition (user explicitly specified search)
        has_direct_search = any(column == "search" and op == "=" for column, op, _, _ in search_conditions)

        # If user specified a direct search, still process other conditions but don't combine them
        # into the search query - just mark them as API supported if they can be optimized

        # Collect all text-based search terms
        search_terms = []
        like_conditions = []

        for column, op, value, original_condition in search_conditions:
            if op == "=" and column in ["title", "excerpt", "note"]:
                # If we have a direct search, don't combine field-specific searches
                # but still mark them as API supported if they can be optimized
                if not has_direct_search:
                    # Convert field-specific searches to general search terms
                    if column == "title":
                        search_terms.append(f"title:{value}")
                    elif column == "excerpt":
                        search_terms.append(f"excerpt:{value}")
                    elif column == "note":
                        search_terms.append(f"note:{value}")

                # Remove from local filters and mark as API supported
                if original_condition in parsed["local_filters"]:
                    parsed["local_filters"].remove(original_condition)
                if original_condition not in parsed["api_supported"]:
                    parsed["api_supported"].append(original_condition)

            elif op == "like" and column in ["title", "excerpt", "note", "tags"]:
                like_conditions.append((column, op, value, original_condition))

        # If we have multiple field-specific searches and no direct search, combine them
        if search_terms and not has_direct_search:
            combined_search = " ".join(search_terms)
            if len(search_terms) > 1:
                # For multiple terms, use AND logic
                combined_search = f"({' AND '.join(search_terms)})"

            parsed["search"] = combined_search

        # Optimize simple LIKE patterns that can use API search
        for column, op, value, original_condition in like_conditions:
            if self._can_use_api_search_for_like(column, value):
                # Convert simple LIKE patterns to API search
                api_search_term = self._convert_like_to_api_search(column, value)
                if api_search_term:
                    if not has_direct_search:
                        if parsed["search"]:
                            parsed["search"] += f" {api_search_term}"
                        else:
                            parsed["search"] = api_search_term

                    # Remove from local filters and mark as API supported
                    if original_condition in parsed["local_filters"]:
                        parsed["local_filters"].remove(original_condition)
                    if original_condition not in parsed["api_supported"]:
                        parsed["api_supported"].append(original_condition)

    def _can_use_api_search_for_like(self, column: str, value: str) -> bool:
        """Check if a LIKE pattern can be efficiently handled by API search"""
        if not isinstance(value, str):
            return False

        # Only optimize simple patterns that start and end with %
        if not (value.startswith("%") and value.endswith("%")):
            return False

        # Remove % and check if it's a simple word/pattern
        pattern = value.strip("%")

        # Don't optimize if pattern contains regex special chars, % in middle, or is too short
        if len(pattern) < 3 or any(char in pattern for char in ".*+?^$()[]{}|\\") or "%" in pattern:
            return False

        return column in ["title", "excerpt", "note", "tags"]

    def _convert_like_to_api_search(self, column: str, value: str) -> str:
        """Convert LIKE pattern to API search format"""
        if not isinstance(value, str):
            return None

        pattern = value.strip("%")

        # Create field-specific search term
        if column == "title":
            return f"title:{pattern}"
        elif column == "excerpt":
            return f"excerpt:{pattern}"
        elif column == "note":
            return f"note:{pattern}"
        elif column == "tags":
            return f"tag:{pattern}"

        return pattern

    def _can_use_advanced_filters(self, complex_filters: Dict[str, Any]) -> bool:
        """Check if we can use the advanced filtering endpoint"""
        # Use advanced filters if we have search, important, or tags criteria
        return any(key in complex_filters for key in ["search", "important", "tags"])

    def _fetch_with_standard_endpoint(
        self, collection_id: int, search_query: str, sort_order: str, result_limit: int, local_filter_conditions: List
    ) -> List[Dict]:
        """Fetch data using the standard Raindrop.io endpoint"""
        # If we have local filters, we may need to fetch more data than requested limit
        # to ensure we have enough data to filter locally
        fetch_limit = None
        if local_filter_conditions:
            # If we have local filters, fetch more data to account for filtering
            # We'll apply the original limit after local filtering
            fetch_limit = result_limit * 5 if result_limit else 1000  # Fetch more data for local filtering
        else:
            fetch_limit = result_limit

        response = self.handler.connection.get_raindrops(
            collection_id=collection_id,
            search=search_query,
            sort=sort_order,
            page=0,
            per_page=50,
            max_results=fetch_limit,
        )
        return response.get("items", [])

    def _apply_ordering(self, df: pd.DataFrame, order_by_conditions) -> pd.DataFrame:
        """Apply ordering to DataFrame"""
        if not order_by_conditions or df.empty:
            return df

        sort_cols = []
        ascending = []

        for order_condition in order_by_conditions:
            column = getattr(order_condition, "column", getattr(order_condition, "field", None))
            if column and column in df.columns:
                sort_cols.append(column)
                ascending.append(getattr(order_condition, "ascending", True))

        if sort_cols:
            df = df.sort_values(by=sort_cols, ascending=ascending)

        return df

    def _prepare_raindrop_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare raindrop data for API submission"""
        raindrop_data = {}

        # Map common fields
        field_mappings = {
            "link": "link",
            "title": "title",
            "excerpt": "excerpt",
            "note": "note",
            "type": "type",
            "cover": "cover",
            "important": "important",
            "collection_id": "collection",
            "collection.id": "collection",
        }

        for key, value in data.items():
            if key in field_mappings:
                api_key = field_mappings[key]
                if api_key == "collection" and value:
                    raindrop_data[api_key] = {"$id": int(value)}
                elif key == "important" and value is not None:
                    raindrop_data[api_key] = bool(value)
                elif value is not None:
                    raindrop_data[api_key] = value

        # Handle tags (convert string to list)
        if "tags" in data and data["tags"]:
            if isinstance(data["tags"], str):
                raindrop_data["tags"] = [tag.strip() for tag in data["tags"].split(",")]
            elif isinstance(data["tags"], list):
                raindrop_data["tags"] = data["tags"]

        return raindrop_data


class CollectionsTable(APITable):
    """The Raindrop.io Collections Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls Raindrop.io collections data.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Raindrop.io collections matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        select_statement_parser = SELECTQueryParser(query, "collections", self.get_columns())
        (
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit,
        ) = select_statement_parser.parse_query()

        # Get collections data
        collections_data = self.get_collections()

        # Convert to DataFrame
        if collections_data:
            collections_df = pd.json_normalize(collections_data)
            collections_df = self._normalize_collection_data(collections_df)
        else:
            # Create empty DataFrame with all expected columns
            collections_df = pd.DataFrame(columns=self.get_columns())

        # Ensure all expected columns exist (defensive check)
        expected_columns = self.get_columns()
        for col in expected_columns:
            if col not in collections_df.columns:
                logger.warning(f"Missing column after normalization: {col}, adding as None")
                collections_df[col] = None

        # Apply filtering and ordering
        select_statement_executor = SELECTQueryExecutor(
            collections_df, selected_columns, where_conditions, order_by_conditions
        )
        collections_df = select_statement_executor.execute_query()

        # Apply limit if needed
        if result_limit and len(collections_df) > result_limit:
            collections_df = collections_df.head(result_limit)

        return collections_df

    def insert(self, query: ast.Insert) -> None:
        """
        Inserts data into the Raindrop.io collections.

        Parameters
        ----------
        query : ast.Insert
           Given SQL INSERT query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        insert_statement_parser = INSERTQueryParser(query)
        values_to_insert = insert_statement_parser.parse_query()

        if isinstance(values_to_insert, list):
            # Multiple inserts
            for row in values_to_insert:
                collection_data = self._prepare_collection_data(row)
                self.handler.connection.create_collection(collection_data)
        else:
            # Single insert
            collection_data = self._prepare_collection_data(values_to_insert)
            self.handler.connection.create_collection(collection_data)

    def update(self, query: ast.Update) -> None:
        """
        Updates data in the Raindrop.io collections.

        Parameters
        ----------
        query : ast.Update
           Given SQL UPDATE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        update_statement_parser = UPDATEQueryParser(query)
        values_to_update, where_conditions = update_statement_parser.parse_query()

        # Extract specific IDs from WHERE conditions to avoid loading all data
        collection_ids = []

        for condition in where_conditions:
            if condition.column in ["_id", "id"]:
                if isinstance(condition.value, list):
                    collection_ids.extend(condition.value)
                else:
                    collection_ids.append(condition.value)

        # If we have specific IDs, update them directly without loading all data
        if collection_ids:
            for collection_id in collection_ids:
                try:
                    update_data = self._prepare_collection_data(values_to_update)
                    self.handler.connection.update_collection(collection_id, update_data)
                except Exception as e:
                    logger.error(f"Failed to update collection {collection_id}: {e}")
            return

        # For complex filters, we need to fetch and filter collections
        # Since collections are typically fewer in number than raindrops, this is more acceptable
        collections_data = self.get_collections()

        if not collections_data:
            logger.warning("No collections found")
            return

        collections_df = pd.json_normalize(collections_data)
        collections_df = self._normalize_collection_data(collections_df)

        # Apply filters
        update_query_executor = UPDATEQueryExecutor(collections_df, where_conditions)
        collections_df = update_query_executor.execute_query()

        if collections_df.empty:
            logger.warning("No collections found matching the WHERE conditions")
            return

        collection_ids = collections_df["_id"].tolist()

        # Update each collection individually
        for collection_id in collection_ids:
            try:
                update_data = self._prepare_collection_data(values_to_update)
                self.handler.connection.update_collection(collection_id, update_data)
            except Exception as e:
                logger.error(f"Failed to update collection {collection_id}: {e}")

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Raindrop.io collections.

        Parameters
        ----------
        query : ast.Delete
           Given SQL DELETE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        delete_statement_parser = DELETEQueryParser(query)
        where_conditions = delete_statement_parser.parse_query()

        # Extract specific IDs from WHERE conditions to avoid loading all data
        collection_ids = []

        for condition in where_conditions:
            if condition.column in ["_id", "id"]:
                if isinstance(condition.value, list):
                    collection_ids.extend(condition.value)
                else:
                    collection_ids.append(condition.value)

        # If we have specific IDs, delete them directly without loading all data
        if collection_ids:
            if len(collection_ids) > 1:
                try:
                    self.handler.connection.delete_multiple_collections(collection_ids)
                    return
                except Exception as e:
                    logger.warning(f"Bulk delete failed, falling back to individual deletes: {e}")

            # Individual deletes
            for collection_id in collection_ids:
                try:
                    self.handler.connection.delete_collection(collection_id)
                except Exception as e:
                    logger.error(f"Failed to delete collection {collection_id}: {e}")
            return

        # For complex filters, we need to fetch and filter collections
        # Since collections are typically fewer in number than raindrops, this is more acceptable
        collections_data = self.get_collections()

        if not collections_data:
            logger.warning("No collections found")
            return

        collections_df = pd.json_normalize(collections_data)
        collections_df = self._normalize_collection_data(collections_df)

        # Apply filters
        delete_query_executor = DELETEQueryExecutor(collections_df, where_conditions)
        collections_df = delete_query_executor.execute_query()

        if collections_df.empty:
            logger.warning("No collections found matching the WHERE conditions")
            return

        collection_ids = collections_df["_id"].tolist()

        # Check if we should do bulk delete or individual deletes
        if len(collection_ids) > 1:
            try:
                self.handler.connection.delete_multiple_collections(collection_ids)
            except Exception as e:
                logger.warning(f"Bulk delete failed, falling back to individual deletes: {e}")
                # Fall back to individual deletes
                for collection_id in collection_ids:
                    try:
                        self.handler.connection.delete_collection(collection_id)
                    except Exception as e:
                        logger.error(f"Failed to delete collection {collection_id}: {e}")
        else:
            # Single delete
            collection_id = collection_ids[0]
            self.handler.connection.delete_collection(collection_id)

    def get_columns(self) -> List[str]:
        """Get the column names for the collections table"""
        return [
            "_id",
            "title",
            "description",
            "color",
            "view",
            "public",
            "sort",
            "count",
            "created",
            "lastUpdate",
            "expanded",
            "parent.id",
            "user.id",
            "cover",
            "access.level",
            "access.draggable",
        ]

    def get_collections(self, **kwargs) -> List[Dict]:
        """Get collections data"""
        if not self.handler.connection:
            self.handler.connect()

        # Get all collections (root and nested) from the main collections endpoint
        response = self.handler.connection.get_collections()
        return response.get("items", [])

    def _normalize_collection_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize collection data for consistent column structure"""
        if df.empty:
            return df

        # Process nested data first to extract flattened columns
        try:
            # Handle nested parent data
            if "parent" in df.columns:
                df["parent.id"] = df["parent"].apply(lambda x: x.get("$id") if isinstance(x, dict) else None)
        except Exception as e:
            logger.warning(f"Error processing parent data: {e}")

        try:
            # Handle nested user data
            if "user" in df.columns:
                df["user.id"] = df["user"].apply(lambda x: x.get("$id") if isinstance(x, dict) else None)
        except Exception as e:
            logger.warning(f"Error processing user data: {e}")

        try:
            # Handle nested access data
            if "access" in df.columns:
                df["access.level"] = df["access"].apply(lambda x: x.get("level") if isinstance(x, dict) else None)
                df["access.draggable"] = df["access"].apply(
                    lambda x: x.get("draggable") if isinstance(x, dict) else None
                )
        except Exception as e:
            logger.warning(f"Error processing access data: {e}")

        # Convert cover list to string
        try:
            if "cover" in df.columns:
                df["cover"] = df["cover"].apply(lambda x: x[0] if isinstance(x, list) and x else x)
        except Exception as e:
            logger.warning(f"Error processing cover data: {e}")

        # Convert dates
        for date_col in ["created", "lastUpdate"]:
            try:
                if date_col in df.columns:
                    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
            except Exception as e:
                logger.warning(f"Error processing date column {date_col}: {e}")

        # Ensure ALL expected columns exist, even if empty
        # This must happen LAST to ensure any newly created columns are preserved
        expected_columns = self.get_columns()
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None

        return df

    def _prepare_collection_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare collection data for API submission"""
        collection_data = {}

        # Map common fields
        field_mappings = {
            "title": "title",
            "description": "description",
            "color": "color",
            "view": "view",
            "public": "public",
            "sort": "sort",
            "parent_id": "parent",
            "parent.id": "parent",
        }

        for key, value in data.items():
            if key in field_mappings:
                api_key = field_mappings[key]
                if api_key == "parent" and value:
                    collection_data[api_key] = {"$id": int(value)}
                elif key in ["public"] and value is not None:
                    collection_data[api_key] = bool(value)
                elif value is not None:
                    collection_data[api_key] = value

        return collection_data


class TagsTable(APITable):
    """The Raindrop.io Tags Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls Raindrop.io tags data.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Raindrop.io tags with usage statistics

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        select_statement_parser = SELECTQueryParser(query, "tags", self.get_columns())
        (
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit,
        ) = select_statement_parser.parse_query()

        # Get tags data from API
        tags_data = self.get_tags()

        # Convert to DataFrame
        if tags_data:
            tags_df = pd.json_normalize(tags_data)
            tags_df = self._normalize_tags_data(tags_df)
        else:
            # Create empty DataFrame with all expected columns
            tags_df = pd.DataFrame(columns=self.get_columns())

        # Ensure all expected columns exist (defensive check)
        expected_columns = self.get_columns()
        for col in expected_columns:
            if col not in tags_df.columns:
                logger.warning(f"Missing column after normalization: {col}, adding as None")
                tags_df[col] = None

        # Apply filtering and ordering using the executor
        select_statement_executor = SELECTQueryExecutor(
            tags_df, selected_columns, where_conditions, order_by_conditions
        )
        tags_df = select_statement_executor.execute_query()

        # Apply limit if needed
        if result_limit and len(tags_df) > result_limit:
            tags_df = tags_df.head(result_limit)

        return tags_df

    def insert(self, query: ast.Insert) -> None:
        """
        Tags are typically created automatically when bookmarks are tagged.
        Direct tag creation is not supported by the Raindrop.io API.

        Parameters
        ----------
        query : ast.Insert
           Given SQL INSERT query

        Returns
        -------
        None

        Raises
        ------
        NotImplementedError
            Direct tag creation is not supported
        """
        raise NotImplementedError(
            "Direct tag creation is not supported by Raindrop.io API. "
            "Tags are created automatically when bookmarks are tagged."
        )

    def update(self, query: ast.Update) -> None:
        """
        Tag updates are typically handled through bookmark updates.
        Direct tag updates are not supported by the Raindrop.io API.

        Parameters
        ----------
        query : ast.Update
           Given SQL UPDATE query

        Returns
        -------
        None

        Raises
        ------
        NotImplementedError
            Direct tag updates are not supported
        """
        raise NotImplementedError(
            "Direct tag updates are not supported by Raindrop.io API. Tag updates are handled through bookmark updates."
        )

    def delete(self, query: ast.Delete) -> None:
        """
        Tag deletion removes the tag from all bookmarks.
        This operation is not supported by the Raindrop.io API.

        Parameters
        ----------
        query : ast.Delete
           Given SQL DELETE query

        Returns
        -------
        None

        Raises
        ------
        NotImplementedError
            Tag deletion is not supported
        """
        raise NotImplementedError(
            "Tag deletion is not supported by Raindrop.io API. "
            "Tags are removed automatically when no bookmarks use them."
        )

    def get_columns(self) -> List[str]:
        """Get the column names for the tags table"""
        return [
            "_id",
            "label",
            "count",
            "created",
            "lastUpdate",
        ]

    def get_tags(self) -> List[Dict]:
        """Get tags data"""
        if not self.handler.connection:
            self.handler.connect()

        response = self.handler.connection.get_tags()
        return response.get("items", [])

    def _normalize_tags_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize tags data for consistent column structure"""
        if df.empty:
            return df

        # Convert dates
        for date_col in ["created", "lastUpdate"]:
            try:
                if date_col in df.columns:
                    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
            except Exception as e:
                logger.warning(f"Error processing date column {date_col}: {e}")

        # Ensure ALL expected columns exist, even if empty
        expected_columns = self.get_columns()
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None

        return df


class ParseTable(APITable):
    """The Raindrop.io Parse Table implementation for URL metadata extraction"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Parse URLs to extract metadata.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            URL metadata from parsed URLs

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        select_statement_parser = SELECTQueryParser(query, "parse", self.get_columns())
        (
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit,
        ) = select_statement_parser.parse_query()

        # Extract URLs to parse from WHERE conditions
        urls_to_parse = []

        for condition in where_conditions:
            # Handle different condition formats
            if isinstance(condition, list) and len(condition) >= 3:
                op, column, value = condition[0], condition[1], condition[2]
            elif hasattr(condition, "op") and hasattr(condition, "column"):
                op = getattr(condition, "op", "=")
                column = condition.column
                value = getattr(condition, "value", None)
            else:
                # Skip malformed conditions
                logger.warning(f"Skipping malformed condition: {condition}")
                continue

            if column == "url" and op == "=" and isinstance(value, str):
                urls_to_parse.append(value)
            elif column == "url" and op == "in" and isinstance(value, list):
                urls_to_parse.extend(value)

        if not urls_to_parse:
            raise ValueError(
                "Please specify URL(s) to parse using WHERE url = 'https://...' or WHERE url IN ('url1', 'url2')"
            )

        # Parse URLs and collect results
        parsed_results = []

        for url in urls_to_parse:
            try:
                response = self.handler.connection.parse_url(url)
                if response.get("result") and response.get("item"):
                    parsed_item = response["item"]
                    parsed_item["parsed_url"] = url  # Add original URL for reference
                    parsed_results.append(parsed_item)
                else:
                    logger.warning(f"Failed to parse URL: {url}")
                    # Add empty result for failed parsing
                    parsed_results.append(
                        {
                            "parsed_url": url,
                            "title": None,
                            "excerpt": None,
                            "domain": None,
                            "type": None,
                            "cover": None,
                            "error": "Failed to parse URL",
                        }
                    )
            except Exception as e:
                logger.error(f"Error parsing URL {url}: {e}")
                # Add error result
                parsed_results.append(
                    {
                        "parsed_url": url,
                        "title": None,
                        "excerpt": None,
                        "domain": None,
                        "type": None,
                        "cover": None,
                        "error": str(e),
                    }
                )

        # Convert to DataFrame
        if parsed_results:
            parse_df = pd.json_normalize(parsed_results)
            parse_df = self._normalize_parse_data(parse_df)
        else:
            # Create empty DataFrame with all expected columns
            parse_df = pd.DataFrame(columns=self.get_columns())

        # Ensure all expected columns exist (defensive check)
        expected_columns = self.get_columns()
        for col in expected_columns:
            if col not in parse_df.columns:
                logger.warning(f"Missing column after normalization: {col}, adding as None")
                parse_df[col] = None

        # Apply filtering and ordering using the executor
        select_statement_executor = SELECTQueryExecutor(
            parse_df,
            selected_columns,
            [],
            order_by_conditions,  # No additional filtering needed
        )
        parse_df = select_statement_executor.execute_query()

        # Apply limit if needed
        if result_limit and len(parse_df) > result_limit:
            parse_df = parse_df.head(result_limit)

        return parse_df

    def insert(self, query: ast.Insert) -> None:
        """
        URL parsing is a read-only operation.
        Use INSERT on the raindrops table to create bookmarks from parsed URLs.

        Parameters
        ----------
        query : ast.Insert
           Given SQL INSERT query

        Returns
        -------
        None

        Raises
        ------
        NotImplementedError
            URL parsing is read-only
        """
        raise NotImplementedError(
            "URL parsing is a read-only operation. "
            "Use INSERT on the raindrops table to create bookmarks from parsed URLs."
        )

    def update(self, query: ast.Update) -> None:
        """
        URL parsing is a read-only operation.

        Parameters
        ----------
        query : ast.Update
           Given SQL UPDATE query

        Returns
        -------
        None

        Raises
        ------
        NotImplementedError
            URL parsing is read-only
        """
        raise NotImplementedError("URL parsing is a read-only operation. Cannot update parsed URL metadata.")

    def delete(self, query: ast.Delete) -> None:
        """
        URL parsing is a read-only operation.

        Parameters
        ----------
        query : ast.Delete
           Given SQL DELETE query

        Returns
        -------
        None

        Raises
        ------
        NotImplementedError
            URL parsing is read-only
        """
        raise NotImplementedError("URL parsing is a read-only operation. Cannot delete parsed URL metadata.")

    def get_columns(self) -> List[str]:
        """Get the column names for the parse table"""
        return [
            "parsed_url",
            "title",
            "excerpt",
            "domain",
            "type",
            "cover",
            "media",
            "lastUpdate",
            "error",
        ]

    def _normalize_parse_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize parsed URL data for consistent column structure"""
        if df.empty:
            return df

        # Convert dates
        for date_col in ["lastUpdate"]:
            try:
                if date_col in df.columns:
                    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
            except Exception as e:
                logger.warning(f"Error processing date column {date_col}: {e}")

        # Ensure ALL expected columns exist, even if empty
        expected_columns = self.get_columns()
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None

        return df


class BulkOperationsTable(APITable):
    """The Raindrop.io Bulk Operations Table implementation for bulk move, update, and delete operations"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Bulk operations are not queryable. Use this table for bulk operations only.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Empty DataFrame with operation status information

        Raises
        ------
        NotImplementedError
            Bulk operations are not queryable
        """
        raise NotImplementedError(
            "Bulk operations table is not queryable. Use INSERT, UPDATE, or DELETE operations on this table for bulk operations."
        )

    def insert(self, query: ast.Insert) -> None:
        """
        Bulk operations are initiated through UPDATE or DELETE operations, not INSERT.

        Parameters
        ----------
        query : ast.Insert
           Given SQL INSERT query

        Returns
        -------
        None

        Raises
        ------
        NotImplementedError
            Bulk operations use UPDATE/DELETE
        """
        raise NotImplementedError(
            "Use UPDATE operations on the raindrops table for bulk updates, or DELETE operations for bulk deletions."
        )

    def update(self, query: ast.Update) -> None:
        """
        Perform bulk move operations between collections.

        Parameters
        ----------
        query : ast.Update
           Given SQL UPDATE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains invalid conditions
        """
        update_statement_parser = UPDATEQueryParser(query)
        values_to_update, where_conditions = update_statement_parser.parse_query()

        # Check if this is a move operation (has collection_id in update values)
        if "collection_id" not in values_to_update:
            raise ValueError("Bulk operations table only supports collection moves. Use 'collection_id' in SET clause.")

        target_collection_id = values_to_update["collection_id"]

        # Extract conditions for the move operation
        source_collection_id = None
        raindrop_ids = []
        search_query = None

        for condition in where_conditions:
            if condition.column == "source_collection_id":
                source_collection_id = condition.value
            elif condition.column in ["_id", "id"]:
                if isinstance(condition.value, list):
                    raindrop_ids.extend(condition.value)
                else:
                    raindrop_ids.append(condition.value)
            elif condition.column in ["search", "title"]:
                search_query = condition.value

        # Validate that we have at least one condition
        if not source_collection_id and not raindrop_ids and not search_query:
            raise ValueError(
                "Please specify source conditions using one of: source_collection_id = X, _id = Y, search = 'text'"
            )

        # Perform the bulk move operation
        try:
            result = self.handler.connection.move_raindrops_to_collection(
                target_collection_id=target_collection_id,
                source_collection_id=source_collection_id,
                search=search_query,
                ids=raindrop_ids if raindrop_ids else None,
            )

            if result.get("result"):
                logger.info(f"Successfully moved raindrops to collection {target_collection_id}")
            else:
                logger.warning(f"Bulk move operation may have failed: {result}")

        except Exception as e:
            logger.error(f"Failed to perform bulk move operation: {e}")
            raise

    def delete(self, query: ast.Delete) -> None:
        """
        Bulk delete operations are handled by the raindrops table.
        Use DELETE on the raindrops table for bulk deletions.

        Parameters
        ----------
        query : ast.Delete
           Given SQL DELETE query

        Returns
        -------
        None

        Raises
        ------
        NotImplementedError
            Bulk operations use raindrops table
        """
        raise NotImplementedError("Use DELETE operations on the raindrops table for bulk deletions.")

    def get_columns(self) -> List[str]:
        """Get the column names for the bulk operations table"""
        return [
            "operation",
            "status",
            "affected_count",
            "target_collection_id",
            "source_collection_id",
            "error",
        ]

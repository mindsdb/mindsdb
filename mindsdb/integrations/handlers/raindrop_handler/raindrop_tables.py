import pandas as pd
from typing import List, Dict, Any
from datetime import datetime

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

        for condition in where_conditions:
            if condition.column == "collection_id":
                collection_id = condition.value
            elif condition.column == "search" or condition.column == "title":
                search_query = condition.value
            elif condition.column == "_id" or condition.column == "id":
                if isinstance(condition.value, list):
                    raindrop_ids.extend(condition.value)
                else:
                    raindrop_ids.append(condition.value)

        # Handle sorting
        if order_by_conditions:
            for order_condition in order_by_conditions:
                if order_condition.column in ["created", "lastUpdate", "sort", "title"]:
                    direction = "asc" if order_condition.ascending else "desc"
                    sort_order = f"{order_condition.column},-{direction}"
                    break

        # If specific IDs are requested, try to fetch efficiently
        if raindrop_ids:
            raindrops_data = []
            # Batch process IDs to reduce API calls
            batch_size = 10  # Process in smaller batches to avoid overwhelming the API
            
            for i in range(0, len(raindrop_ids), batch_size):
                batch_ids = raindrop_ids[i:i + batch_size]
                
                # Try to fetch from the same collection if possible to use bulk operations
                # Since Raindrop.io doesn't have a bulk get endpoint, we'll minimize calls
                # by fetching efficiently in smaller batches
                for raindrop_id in batch_ids:
                    try:
                        response = self.handler.connection.get_raindrop(raindrop_id)
                        if response.get("result") and response.get("item"):
                            raindrops_data.append(response["item"])
                    except Exception as e:
                        logger.warning(f"Failed to fetch raindrop {raindrop_id}: {e}")
                        continue
        else:
            # Get raindrops from collection with pagination
            raindrops_data = []
            page = 0
            per_page = min(result_limit or 50, 50)  # API limit is 50
            total_fetched = 0

            while True:
                response = self.handler.connection.get_raindrops(
                    collection_id=collection_id,
                    search=search_query,
                    sort=sort_order,
                    page=page,
                    per_page=per_page
                )

                if not response.get("result") or not response.get("items"):
                    break

                items = response["items"]
                raindrops_data.extend(items)
                total_fetched += len(items)

                # Check if we've fetched enough or if there are no more items
                if result_limit and total_fetched >= result_limit:
                    break
                if len(items) < per_page:  # No more items available
                    break

                page += 1

        # Convert to DataFrame
        if raindrops_data:
            raindrops_df = pd.json_normalize(raindrops_data)
            raindrops_df = self._normalize_raindrop_data(raindrops_df)
        else:
            raindrops_df = pd.DataFrame()

        # Apply additional filtering and ordering using the executor
        select_statement_executor = SELECTQueryExecutor(
            raindrops_df, selected_columns, where_conditions, order_by_conditions
        )
        raindrops_df = select_statement_executor.execute_query()

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
            fetch_params['collection_id'] = collection_id
        if search_query:
            fetch_params['search'] = search_query
        
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
                    collection_id=collection_id,
                    update_data=update_data,
                    ids=raindrop_ids
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
                    self.handler.connection.delete_multiple_raindrops(
                        collection_id=collection_id,
                        ids=raindrop_ids
                    )
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
            fetch_params['collection_id'] = collection_id
        if search_query:
            fetch_params['search'] = search_query
        
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
                self.handler.connection.delete_multiple_raindrops(
                    collection_id=collection_id,
                    ids=raindrop_ids
                )
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
            "_id", "link", "title", "excerpt", "note", "type", "cover", "tags",
            "important", "reminder", "removed", "created", "lastUpdate",
            "domain", "collection.id", "collection.title", "user.id", "broken",
            "cache", "file.name", "file.size", "file.type"
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

        # Handle nested collection data
        if "collection" in df.columns and not df["collection"].isnull().all():
            df["collection.id"] = df["collection"].apply(lambda x: x.get("$id") if isinstance(x, dict) else None)
            df["collection.$id"] = df["collection"].apply(lambda x: x.get("$id") if isinstance(x, dict) else None)
            df["collection.title"] = df["collection"].apply(lambda x: x.get("title") if isinstance(x, dict) else None)

        # Handle nested user data
        if "user" in df.columns and not df["user"].isnull().all():
            df["user.id"] = df["user"].apply(lambda x: x.get("$id") if isinstance(x, dict) else None)

        # Handle nested file data
        if "file" in df.columns and not df["file"].isnull().all():
            df["file.name"] = df["file"].apply(lambda x: x.get("name") if isinstance(x, dict) else None)
            df["file.size"] = df["file"].apply(lambda x: x.get("size") if isinstance(x, dict) else None)
            df["file.type"] = df["file"].apply(lambda x: x.get("type") if isinstance(x, dict) else None)

        # Convert tags list to string
        if "tags" in df.columns:
            df["tags"] = df["tags"].apply(lambda x: ",".join(x) if isinstance(x, list) else x)

        # Convert dates
        for date_col in ["created", "lastUpdate"]:
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

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
            "collection.id": "collection"
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
            collections_df = pd.DataFrame()

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
            "_id", "title", "description", "color", "view", "public", "sort",
            "count", "created", "lastUpdate", "expanded", "parent.id", "user.id",
            "cover", "access.level", "access.draggable"
        ]

    def get_collections(self, **kwargs) -> List[Dict]:
        """Get collections data"""
        if not self.handler.connection:
            self.handler.connect()
        
        # Get both root and child collections
        root_response = self.handler.connection.get_collections()
        child_response = self.handler.connection.get_child_collections()
        
        all_collections = []
        all_collections.extend(root_response.get("items", []))
        all_collections.extend(child_response.get("items", []))
        
        return all_collections

    def _normalize_collection_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize collection data for consistent column structure"""
        if df.empty:
            return df

        # Handle nested parent data
        if "parent" in df.columns and not df["parent"].isnull().all():
            df["parent.id"] = df["parent"].apply(lambda x: x.get("$id") if isinstance(x, dict) else None)

        # Handle nested user data
        if "user" in df.columns and not df["user"].isnull().all():
            df["user.id"] = df["user"].apply(lambda x: x.get("$id") if isinstance(x, dict) else None)

        # Handle nested access data
        if "access" in df.columns and not df["access"].isnull().all():
            df["access.level"] = df["access"].apply(lambda x: x.get("level") if isinstance(x, dict) else None)
            df["access.draggable"] = df["access"].apply(lambda x: x.get("draggable") if isinstance(x, dict) else None)

        # Convert cover list to string
        if "cover" in df.columns:
            df["cover"] = df["cover"].apply(lambda x: x[0] if isinstance(x, list) and x else x)

        # Convert dates
        for date_col in ["created", "lastUpdate"]:
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

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
            "parent.id": "parent"
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

from io import BytesIO
from typing import List, Text, Dict, Optional
import json
import time

import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import (
    FilterCondition,
    SortColumn
)

from mindsdb.integrations.utilities.files.file_reader import FileReader
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class ListFilesTable(APIResource):
    """
    The table abstraction for querying the files (tables) in Microsoft OneDrive.
    """

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[Text] = None,
        **kwargs
    ):
        """
        Lists the files in Microsoft OneDrive.

        Args:
            conditions (List[FilterCondition]): The conditions to filter the files.
            limit (int): The maximum number of files to return.
            sort (List[SortColumn]): The columns to sort the files by.
            targets (List[Text]): The columns to return in the result.

        Returns:
            pd.DataFrame: The list of files in Microsoft OneDrive based on the specified clauses.
        """
        client = self.handler.connect()
        files = client.get_all_items()

        data = []
        for file in files:
            item = {
                "name": file["name"],
                "path": file["path"],
                "extension": file["name"].split(".")[-1]
            }

            # If the 'content' column is explicitly requested, fetch the content of the file.
            if targets and "content" in targets:
                # Use item_id and drive_id if available (for file picker items)
                item_id = file.get("id")
                drive_id = file.get("drive_id")
                item["content"] = client.get_item_content(
                    file["path"],
                    item_id=item_id,
                    drive_id=drive_id
                )

            # If a SELECT * query is executed, i.e., targets is empty, set the content to None.
            elif not targets:
                item["content"] = None

            data.append(item)

        df = pd.DataFrame(data)
        return df

    def get_columns(self):
        return ["name", "path", "extension", "content"]


class FileTable(APIResource):
    """
    The table abstraction for querying the content of a file (table) in Microsoft OneDrive.
    """

    def list(self, targets: List[str] = None, table_name=None, *args, **kwargs) -> pd.DataFrame:
        """
        Retrieves the content of the specified file (table) in Microsoft OneDrive.

        Args:
            targets (List[str]): The columns to return in the result.
            table_name (str): The name of the file (table) to retrieve.

        Returns:
            pd.DataFrame: The content of the specified file (table) in Microsoft OneDrive.
        """
        client = self.handler.connect()

        # Try to find the file in all items to get its item_id and drive_id
        # This is necessary for SharePoint organization sites
        item_id = None
        drive_id = None

        try:
            all_items = client.get_all_items()
            for item in all_items:
                # Match by name (table_name) or path
                if item.get("name") == table_name or item.get("path") == table_name:
                    item_id = item.get("id")
                    drive_id = item.get("drive_id")
                    logger.info(f"Found file {table_name} with item_id={item_id}, drive_id={drive_id}")
                    break
        except Exception as e:
            logger.warning(f"Could not retrieve item metadata for {table_name}: {e}. Falling back to path-based retrieval.")

        # Get file content using item_id and drive_id if available
        file_content = client.get_item_content(table_name, item_id=item_id, drive_id=drive_id)

        reader = FileReader(file=BytesIO(file_content), name=table_name)

        return reader.get_page_content()


class ConnectionMetadataTable(APIResource):
    """
    Table for storing and querying per-connection identity and metadata.

    Columns:
    - connection_id: Unique identifier for this connection
    - tenant_id: Azure AD tenant ID
    - account_id: User account ID
    - display_name: User display name
    - email: User email/UPN
    - drive_id: OneDrive drive ID
    - scopes: Granted scopes (comma-separated)
    - created_at: Connection creation timestamp
    - updated_at: Last update timestamp
    """

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[Text] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        List connection metadata records.

        Args:
            conditions: Filter conditions
            limit: Maximum number of records
            sort: Sort columns
            targets: Columns to return

        Returns:
            pd.DataFrame: Connection metadata records
        """
        try:
            # Load connection metadata from storage
            connection_id = self.handler.connection_id
            identity_key = f"connection_identity_{connection_id}.json"

            try:
                identity_content = self.handler.handler_storage.file_get(identity_key)
                if identity_content:
                    identity_data = json.loads(identity_content.decode('utf-8'))
                    df = pd.DataFrame([identity_data])
                else:
                    df = pd.DataFrame(columns=self.get_columns())
            except FileNotFoundError:
                df = pd.DataFrame(columns=self.get_columns())

            return df

        except Exception as e:
            logger.error(f"Error listing connection metadata: {e}")
            return pd.DataFrame(columns=self.get_columns())

    def get_columns(self) -> List[str]:
        return [
            "connection_id",
            "tenant_id",
            "account_id",
            "display_name",
            "email",
            "drive_id",
            "created_at",
            "updated_at"
        ]


class DeltaStateTable(APIResource):
    """
    Table for storing and managing delta sync state per connection/scope.

    Columns:
    - connection_id: Connection identifier
    - scope: Scope of the delta (e.g., 'root' or folder item ID)
    - delta_link: Current delta link for incremental sync
    - cursor_updated_at: When the delta link was last updated
    - last_success_at: Last successful sync timestamp
    - items_synced: Number of items synced in last run
    """

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[Text] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        List delta state records for the current connection.

        Args:
            conditions: Filter conditions (e.g., scope filter)
            limit: Maximum number of records
            sort: Sort columns
            targets: Columns to return

        Returns:
            pd.DataFrame: Delta state records
        """
        try:
            connection_id = self.handler.connection_id
            delta_state_key = f"delta_state_{connection_id}.json"

            try:
                state_content = self.handler.handler_storage.file_get(delta_state_key)
                if state_content:
                    state_data = json.loads(state_content.decode('utf-8'))
                    # state_data is a dict keyed by scope, convert to list of records
                    records = []
                    for scope, data in state_data.items():
                        record = {'scope': scope, **data}
                        records.append(record)
                    df = pd.DataFrame(records)
                else:
                    df = pd.DataFrame(columns=self.get_columns())
            except FileNotFoundError:
                df = pd.DataFrame(columns=self.get_columns())

            # Apply filters if provided
            if conditions:
                for condition in conditions:
                    if condition.column == 'scope':
                        df = df[df['scope'] == condition.value]

            return df

        except Exception as e:
            logger.error(f"Error listing delta state: {e}")
            return pd.DataFrame(columns=self.get_columns())

    def get_columns(self) -> List[str]:
        return [
            "connection_id",
            "scope",
            "delta_link",
            "cursor_updated_at",
            "last_success_at",
            "items_synced"
        ]

    def get_delta_link(self, scope: str = 'root') -> Optional[str]:
        """
        Retrieve the current delta link for a given scope.

        Args:
            scope: The scope identifier (default: 'root')

        Returns:
            Optional[str]: Delta link if exists, None otherwise
        """
        try:
            connection_id = self.handler.connection_id
            delta_state_key = f"delta_state_{connection_id}.json"

            state_content = self.handler.handler_storage.file_get(delta_state_key)
            if state_content:
                state_data = json.loads(state_content.decode('utf-8'))
                return state_data.get(scope, {}).get('delta_link')
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.error(f"Error getting delta link: {e}")

        return None

    def update_delta_link(
        self,
        scope: str,
        delta_link: str,
        items_synced: int = 0
    ) -> None:
        """
        Update the delta link for a given scope.

        Args:
            scope: The scope identifier
            delta_link: New delta link
            items_synced: Number of items synced in this run
        """
        try:
            connection_id = self.handler.connection_id
            delta_state_key = f"delta_state_{connection_id}.json"

            # Load existing state
            try:
                state_content = self.handler.handler_storage.file_get(delta_state_key)
                state_data = json.loads(state_content.decode('utf-8')) if state_content else {}
            except FileNotFoundError:
                state_data = {}

            # Update state for this scope
            current_time = time.time()
            state_data[scope] = {
                'connection_id': connection_id,
                'delta_link': delta_link,
                'cursor_updated_at': current_time,
                'last_success_at': current_time,
                'items_synced': items_synced
            }

            # Save back to storage
            self.handler.handler_storage.file_set(
                delta_state_key,
                json.dumps(state_data).encode('utf-8')
            )

            logger.info(f"Updated delta link for scope '{scope}': {items_synced} items synced")

        except Exception as e:
            logger.error(f"Error updating delta link: {e}")
            raise


class SyncStatsTable(APIResource):
    """
    Table for operational diagnostics and sync statistics.

    Columns:
    - connection_id: Connection identifier
    - sync_id: Unique sync run identifier
    - started_at: Sync start timestamp
    - completed_at: Sync completion timestamp
    - status: Status (success, failed, partial)
    - items_processed: Number of items processed
    - items_added: Number of new items
    - items_modified: Number of modified items
    - items_deleted: Number of deleted items
    - errors_count: Number of errors encountered
    - throttled_count: Number of throttling events
    - duration_seconds: Total sync duration
    """

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[Text] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        List sync statistics for the current connection.

        Args:
            conditions: Filter conditions
            limit: Maximum number of records
            sort: Sort columns
            targets: Columns to return

        Returns:
            pd.DataFrame: Sync statistics records
        """
        try:
            connection_id = self.handler.connection_id
            stats_key = f"sync_stats_{connection_id}.json"

            try:
                stats_content = self.handler.handler_storage.file_get(stats_key)
                if stats_content:
                    stats_data = json.loads(stats_content.decode('utf-8'))
                    df = pd.DataFrame(stats_data)
                else:
                    df = pd.DataFrame(columns=self.get_columns())
            except FileNotFoundError:
                df = pd.DataFrame(columns=self.get_columns())

            # Apply limit
            if limit and len(df) > limit:
                df = df.head(limit)

            return df

        except Exception as e:
            logger.error(f"Error listing sync stats: {e}")
            return pd.DataFrame(columns=self.get_columns())

    def get_columns(self) -> List[str]:
        return [
            "connection_id",
            "sync_id",
            "started_at",
            "completed_at",
            "status",
            "items_processed",
            "items_added",
            "items_modified",
            "items_deleted",
            "errors_count",
            "throttled_count",
            "duration_seconds"
        ]

    def record_sync(self, sync_stats: Dict) -> None:
        """
        Record a sync run's statistics.

        Args:
            sync_stats: Dictionary containing sync statistics
        """
        try:
            connection_id = self.handler.connection_id
            stats_key = f"sync_stats_{connection_id}.json"

            # Load existing stats
            try:
                stats_content = self.handler.handler_storage.file_get(stats_key)
                stats_list = json.loads(stats_content.decode('utf-8')) if stats_content else []
            except FileNotFoundError:
                stats_list = []

            # Add connection_id to stats
            sync_stats['connection_id'] = connection_id

            # Append new stats
            stats_list.append(sync_stats)

            # Keep only last 100 sync runs to avoid unbounded growth
            if len(stats_list) > 100:
                stats_list = stats_list[-100:]

            # Save back to storage
            self.handler.handler_storage.file_set(
                stats_key,
                json.dumps(stats_list).encode('utf-8')
            )

            logger.info(f"Recorded sync stats: {sync_stats.get('status')} - {sync_stats.get('items_processed')} items")

        except Exception as e:
            logger.error(f"Error recording sync stats: {e}")

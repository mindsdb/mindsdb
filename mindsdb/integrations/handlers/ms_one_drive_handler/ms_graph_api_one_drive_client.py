from typing import Text, List, Dict, Optional, Callable
import time

from requests.exceptions import RequestException

from mindsdb.integrations.utilities.handlers.api_utilities.microsoft.ms_graph_api_utilities import MSGraphAPIBaseClient
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class MSGraphAPIOneDriveClient(MSGraphAPIBaseClient):
    """
    The Microsoft Graph API client for the Microsoft OneDrive handler.
    This client is used for accessing the Microsoft OneDrive specific endpoints of the Microsoft Graph API.

    Features:
    - Automatic token refresh on 401 errors
    - Delta query support for incremental sync
    - Robust throttling with Retry-After support
    - Large file handling with chunking
    """

    # Large file threshold (10 MB)
    LARGE_FILE_THRESHOLD = 10 * 1024 * 1024

    def __init__(
        self,
        access_token: Text,
        refresh_callback: Optional[Callable[[], Optional[str]]] = None,
        authority: str = "common",
        page_size: int = 200,
        scope_type: str = "all",
        selected_items: Optional[List[Dict]] = None
    ) -> None:
        """
        Initialize the OneDrive client.

        Args:
            access_token: Current access token
            refresh_callback: Optional callback to invoke when token needs refresh
            authority: Azure AD authority (common, organizations, or tenant ID)
            page_size: Number of items per page for pagination
            scope_type: Scope type for file filtering ('all' or 'specific')
            selected_items: List of selected items from file picker (for scope_type='specific')
        """
        super().__init__(access_token)
        self.refresh_callback = refresh_callback
        self.authority = authority
        self.PAGINATION_COUNT = page_size
        self._retry_count = 0
        self._max_retries = 1  # One automatic retry after token refresh
        self.scope_type = scope_type
        self.selected_items = selected_items or []

    def update_access_token(self, new_token: str) -> None:
        """
        Update the access token after refresh.

        Args:
            new_token: New access token
        """
        self.access_token = new_token
        logger.info("Access token updated successfully")

    def check_connection(self) -> bool:
        """
        Checks the connection to the Microsoft Graph API by fetching the user's drive.

        Returns:
            bool: True if the connection is successful, False otherwise.
        """
        try:
            response = self._fetch_data("me/drive")
            response.json()  # Validate it's valid JSON
            return True
        except RequestException as request_error:
            logger.error(f"Error checking connection: {request_error}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error checking connection: {e}")
            return False

    def get_user_info(self) -> Dict:
        """
        Fetch current user information.

        Returns:
            Dict: User information including id, displayName, email, etc.
        """
        response = self._fetch_data("me")
        return response.json()

    def get_drive_info(self) -> Dict:
        """
        Fetch current user's drive information.

        Returns:
            Dict: Drive information including id, driveType, owner, quota, etc.
        """
        response = self._fetch_data("me/drive")
        return response.json()

    def get_all_items(self) -> List[Dict]:
        """
        Retrieves all items of the user's OneDrive.
        Respects scope_type setting:
        - 'all': Returns all items from OneDrive (default behavior)
        - 'specific': Returns only items in selected_items list

        Returns:
            List[Dict]: All items of the user's OneDrive based on scope.
        """
        # Check if we should use file picker scope
        if self.scope_type == "specific" and self.selected_items:
            logger.info(f"Using specific scope with {len(self.selected_items)} selected items")
            return self.get_items_from_selection(self.selected_items)

        # Default behavior: get all items
        all_items = []
        for root_item in self.get_root_items():
            if "folder" in root_item:
                all_items.extend(self.get_child_items(root_item["id"], root_item["name"]))

            else:
                # Add the path to the item.
                root_item["path"] = root_item["name"]
                all_items.append(root_item)

        return all_items

    def get_root_items(self) -> List[Dict]:
        """
        Retrieves the root items of the user's OneDrive.

        Returns:
            List[Dict]: The root items of the user's OneDrive.
        """
        root_items = []
        for items in self.fetch_paginated_data("me/drive/root/children"):
            root_items.extend(items)

        return root_items

    def get_child_items(self, item_id: Text, path: Text) -> List[Dict]:
        """
        Recursively retrieves the child items of the specified item.

        Args:
            item_id (Text): The ID of the item whose child items are to be retrieved.

        Returns:
            List[Dict]: The child items of the specified item.
        """
        child_items = []
        for items in self.fetch_paginated_data(f"me/drive/items/{item_id}/children"):
            for item in items:
                child_path = f"{path}/{item['name']}"
                # If the item is a folder, get its child items.
                if "folder" in item:
                    # Recursively get the child items of the folder.
                    child_items.extend(self.get_child_items(item["id"], child_path))

                else:
                    # Add the path to the item.
                    item["path"] = child_path
                    child_items.append(item)

        return child_items

    def get_item_content(self, path: Text, item_id: Optional[str] = None, drive_id: Optional[str] = None) -> bytes:
        """
        Retrieves the content of the specified item.

        Args:
            path (Text): The path of the item whose content is to be retrieved.
            item_id (Optional[str]): The ID of the item (alternative to path).
            drive_id (Optional[str]): The drive ID for cross-drive access.

        Returns:
            bytes: The content of the specified item.
        """
        # If item_id is provided, use ID-based endpoint
        if item_id:
            if drive_id:
                endpoint = f"drives/{drive_id}/items/{item_id}/content"
            else:
                endpoint = f"me/drive/items/{item_id}/content"
        else:
            # Use path-based endpoint
            endpoint = f"me/drive/root:/{path}:/content"

        return self.fetch_data_content(endpoint)

    def get_delta_items(
        self,
        delta_link: Optional[str] = None,
        folder_id: Optional[str] = None
    ) -> Dict[str, any]:
        """
        Retrieve delta changes for items in OneDrive using delta query.

        Args:
            delta_link: Optional delta link from previous sync to continue incremental sync
            folder_id: Optional folder ID to get delta for specific folder (defaults to root)

        Returns:
            Dict containing:
                - items: List of changed items
                - delta_link: New delta link to use for next sync
                - has_more: Whether there are more pages to fetch
        """
        try:
            # If delta link provided, use it directly
            if delta_link:
                # Extract just the path after the base URL
                if 'delta' in delta_link:
                    # Use the full delta link URL
                    response = self._make_request(delta_link)
                    response_json = response.json()
                else:
                    logger.warning(f"Invalid delta link format: {delta_link}")
                    delta_link = None

            # If no valid delta link, start new delta query
            if not delta_link:
                if folder_id:
                    endpoint = f"me/drive/items/{folder_id}/delta"
                else:
                    endpoint = "me/drive/root/delta"

                response = self._fetch_data(endpoint)
                response_json = response.json()

            items = response_json.get('value', [])
            next_link = response_json.get('@odata.nextLink')
            new_delta_link = response_json.get('@odata.deltaLink')

            return {
                'items': items,
                'delta_link': new_delta_link or next_link,
                'has_more': bool(next_link and not new_delta_link)
            }

        except Exception as e:
            logger.error(f"Error fetching delta items: {e}")
            raise

    def get_item_metadata(self, item_id: str) -> Dict:
        """
        Get detailed metadata for a specific item.

        Args:
            item_id: The ID of the item

        Returns:
            Dict: Item metadata including file hashes, size, modified date, etc.
        """
        response = self._fetch_data(f"me/drive/items/{item_id}")
        return response.json()

    def get_item_by_path(self, path: str) -> Dict:
        """
        Get item metadata by path.

        Args:
            path: The path to the item

        Returns:
            Dict: Item metadata
        """
        response = self._fetch_data(f"me/drive/root:/{path}")
        return response.json()

    def download_large_file(self, download_url: str, chunk_size: int = 1024 * 1024) -> bytes:
        """
        Download a large file in chunks using the @microsoft.graph.downloadUrl.

        Args:
            download_url: The download URL from item metadata
            chunk_size: Size of chunks to download (default 1 MB)

        Returns:
            bytes: Complete file content
        """
        import requests

        try:
            response = requests.get(download_url, stream=True)
            response.raise_for_status()

            chunks = []
            total_size = 0

            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    chunks.append(chunk)
                    total_size += len(chunk)

            logger.info(f"Downloaded {total_size} bytes in {len(chunks)} chunks")
            return b''.join(chunks)

        except Exception as e:
            logger.error(f"Error downloading large file: {e}")
            raise

    def list_folder_contents(
        self,
        folder_id: Optional[str] = None,
        folder_path: Optional[str] = None,
        select_fields: Optional[List[str]] = None
    ) -> List[Dict]:
        """
        List contents of a specific folder with optional field selection.

        Args:
            folder_id: Optional folder ID (mutually exclusive with folder_path)
            folder_path: Optional folder path (mutually exclusive with folder_id)
            select_fields: Optional list of fields to return (e.g., ['id', 'name', 'size'])

        Returns:
            List[Dict]: List of items in the folder
        """
        if folder_id:
            endpoint = f"me/drive/items/{folder_id}/children"
        elif folder_path:
            endpoint = f"me/drive/root:/{folder_path}:/children"
        else:
            endpoint = "me/drive/root/children"

        params = {}
        if select_fields:
            params['$select'] = ','.join(select_fields)

        items = []
        for page in self.fetch_paginated_data(endpoint, params=params):
            items.extend(page)

        return items

    def search_items(self, query: str, limit: Optional[int] = None) -> List[Dict]:
        """
        Search for items in OneDrive.

        Args:
            query: Search query string
            limit: Optional limit on number of results

        Returns:
            List[Dict]: List of matching items
        """
        endpoint = f"me/drive/root/search(q='{query}')"
        items = []

        for page in self.fetch_paginated_data(endpoint):
            items.extend(page)
            if limit and len(items) >= limit:
                items = items[:limit]
                break

        return items

    def get_item_by_id(
        self,
        item_id: str,
        drive_id: Optional[str] = None,
        sharepoint_endpoint: Optional[str] = None
    ) -> Dict:
        """
        Get a specific item by ID, optionally using a SharePoint endpoint.

        Args:
            item_id: The ID of the item
            drive_id: Optional drive ID for cross-drive access
            sharepoint_endpoint: Optional SharePoint endpoint URL

        Returns:
            Dict: Item metadata
        """
        if sharepoint_endpoint:
            # Use SharePoint endpoint directly
            # SharePoint endpoints are typically in format: https://tenant.sharepoint.com/sites/sitename/_api/v2.0
            # We need to extract the path after the base URL and append to our Graph API call
            logger.info(f"Using SharePoint endpoint for item {item_id}")
            # For SharePoint items, use drives/{driveId}/items/{itemId}
            if drive_id:
                endpoint = f"drives/{drive_id}/items/{item_id}"
            else:
                endpoint = f"me/drive/items/{item_id}"
        elif drive_id:
            # Use drive-specific endpoint
            endpoint = f"drives/{drive_id}/items/{item_id}"
        else:
            # Use default user drive endpoint
            endpoint = f"me/drive/items/{item_id}"

        response = self._fetch_data(endpoint)
        return response.json()

    def get_item_children_by_id(
        self,
        item_id: str,
        drive_id: Optional[str] = None,
        sharepoint_endpoint: Optional[str] = None,
        path_prefix: str = ""
    ) -> List[Dict]:
        """
        Recursively get all children of a folder item by ID.

        Args:
            item_id: The ID of the folder item
            drive_id: Optional drive ID for cross-drive access
            sharepoint_endpoint: Optional SharePoint endpoint URL
            path_prefix: Current path prefix for building full paths

        Returns:
            List[Dict]: List of all child items (files only, not folders)
        """
        if sharepoint_endpoint and drive_id:
            endpoint = f"drives/{drive_id}/items/{item_id}/children"
        elif drive_id:
            endpoint = f"drives/{drive_id}/items/{item_id}/children"
        else:
            endpoint = f"me/drive/items/{item_id}/children"

        child_items = []

        for items in self.fetch_paginated_data(endpoint):
            for item in items:
                # Build full path
                item_name = item.get('name', '')
                child_path = f"{path_prefix}/{item_name}" if path_prefix else item_name

                if "folder" in item:
                    # Recursively get children of this folder
                    child_items.extend(
                        self.get_item_children_by_id(
                            item["id"],
                            drive_id=drive_id,
                            sharepoint_endpoint=sharepoint_endpoint,
                            path_prefix=child_path
                        )
                    )
                else:
                    # Add path to file item
                    item["path"] = child_path
                    # Store drive_id for content retrieval
                    if drive_id:
                        item['drive_id'] = drive_id
                    child_items.append(item)

        return child_items

    def get_items_from_selection(self, selected_items: List[Dict]) -> List[Dict]:
        """
        Get all items based on a file picker selection.

        Args:
            selected_items: List of selected item objects with structure:
                {
                    "id": "item_id",
                    "name": "item_name",  # optional
                    "parentReference": {"driveId": "drive_id"},
                    "@sharePoint.endpoint": "https://..."  # optional
                }

        Returns:
            List[Dict]: List of all items (expanding folders recursively)
        """
        all_items = []

        for selected_item in selected_items:
            item_id = selected_item.get('id')
            if not item_id:
                logger.warning(f"Skipping item without ID: {selected_item}")
                continue

            # Extract drive ID from parentReference
            parent_ref = selected_item.get('parentReference', {})
            drive_id = parent_ref.get('driveId')

            # Extract SharePoint endpoint
            sharepoint_endpoint = selected_item.get('@sharePoint.endpoint')

            try:
                # Get the item metadata to check if it's a file or folder
                item_metadata = self.get_item_by_id(
                    item_id,
                    drive_id=drive_id,
                    sharepoint_endpoint=sharepoint_endpoint
                )

                # Use name from metadata if not provided in selection
                item_name = selected_item.get('name', item_metadata.get('name', ''))

                if "folder" in item_metadata:
                    # It's a folder, recursively get all children
                    logger.info(f"Fetching children of folder: {item_name}")
                    children = self.get_item_children_by_id(
                        item_id,
                        drive_id=drive_id,
                        sharepoint_endpoint=sharepoint_endpoint,
                        path_prefix=item_name
                    )
                    # Store drive_id in children metadata for content retrieval
                    for child in children:
                        if drive_id and 'drive_id' not in child:
                            child['drive_id'] = drive_id
                    all_items.extend(children)
                else:
                    # It's a file, add it directly
                    item_metadata["path"] = item_name
                    # Store drive_id for content retrieval
                    if drive_id:
                        item_metadata['drive_id'] = drive_id
                    all_items.append(item_metadata)

            except Exception as e:
                logger.error(f"Error fetching item {item_id}: {e}")
                continue

        return all_items

from typing import Text, List, Dict

from mindsdb.integrations.utilities.handlers.api_utilities.microsoft.ms_graph_api_utilities import MSGraphAPIBaseClient
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class MSGraphAPIOneDriveClient(MSGraphAPIBaseClient):
    """
    The Microsoft Graph API client for the Microsoft OneDrive handler.
    This client is used for accessing the Microsoft OneDrive specific endpoints of the Microsoft Graph API.
    Several common methods for submitting requests, fetching data, etc. are inherited from the base class.
    """
    def __init__(self, access_token: Text, user_principal_name: Text) -> None:
        super().__init__(access_token)
        self.user_principal_name = user_principal_name

    def check_connection(self) -> None:
        """
        Checks the connection to the Microsoft Graph API by fetching the user's profile.
        """
        self._fetch_data(f"users/{self.user_principal_name}")

    def get_all_items(self) -> List[Dict]:
        """
        Retrieves all items of the user's OneDrive.
        
        Returns:
            List[Dict]: All items of the user's OneDrive.
        """
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
        for items in self._fetch_data(f"users/{self.user_principal_name}/drive/root/children"):
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
        for items in self._fetch_data(f"users/{self.user_principal_name}/drive/items/{item_id}/children"):
            for item in items:
                path = f"{path}/{item['name']}"
                # If the item is a folder, get its child items.
                if "folder" in item:
                    # Recursively get the child items of the folder.
                    child_items.extend(self.get_child_items(item["id"], path))

                else:
                    # Add the path to the item.
                    item["path"] = path
                    child_items.append(item)

        return child_items
    
    def get_item_content(self, path: Text) -> bytes:
        """
        Retrieves the content of the specified item.
        
        Args:
            path (Text): The path of the item whose content is to be retrieved.
        
        Returns:
            bytes: The content of the specified item.
        """
        content = self._make_request(
            self._get_api_url(f"users/{self.user_principal_name}/drive/root:/{path}:/content"),
        )
        return content
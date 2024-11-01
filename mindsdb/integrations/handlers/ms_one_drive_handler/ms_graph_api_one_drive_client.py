from typing import Text, List, Dict
from mindsdb.integrations.utilities.handlers.api_utilities.microsoft.ms_graph_api_utilities import MSGraphAPIBaseClient


class MSGraphAPIOneDriveClient(MSGraphAPIBaseClient):
    """
    The Microsoft Graph API client for the Microsoft OneDrive handler.
    This client is used for accessing the Microsoft OneDrive specific endpoints of the Microsoft Graph API.
    Several common methods for submitting requests, fetching data, etc. are inherited from the base class.
    """
    def __init__(self, access_token: Text, user_principal_name: Text) -> None:
        super().__init__(access_token)
        self.user_principal_name = user_principal_name
    
    def get_root_drive_items(self) -> List[Dict]:
        """
        Retrieves the root items of the signed-in user's OneDrive.
        
        Returns:
            List[Dict]: The root items of the user's OneDrive.
        """
        root_drive_items = []
        for items in self._fetch_data(f"users/{self.user_principal_name}/drive/root/children"):
            root_drive_items.extend(items)

        return root_drive_items
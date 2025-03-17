from typing import Text, List, Dict, Optional

from requests.exceptions import RequestException

from mindsdb.integrations.utilities.handlers.api_utilities.microsoft.ms_graph_api_utilities import MSGraphAPIBaseClient


class MSGraphAPITeamsClient(MSGraphAPIBaseClient): 
    """
    The Microsoft Graph API client for the Microsoft Teams handler.
    This client is used for accessing the Microsoft Teams specific endpoints of the Microsoft Graph API.
    Several common methods for submitting requests, fetching data, etc. are inherited from the base class.
    """

    def check_connection(self) -> bool:
        """
        Check if the connection to Microsoft Teams is established.

        Returns:
            bool: True if the connection is established, False otherwise.
        """
        try:
            self.fetch_data_json("me/joinedTeams")
            return True
        except RequestException as request_error:
            return False
        
    def _get_group_ids(self) -> List[Text]:
        """
        Get all group IDs related to Microsoft Teams.

        Returns:
            List[Text]: The group IDs.
        """
        if not self._group_ids:
            groups = self.fetch_data_json("me/joinedTeams")
            self._group_ids = [group["id"] for group in groups]

        return self._group_ids
    
    def get_channel_in_group_by_id(self, group_id: Text, channel_id: Text) -> Dict:
        """
        Get a channel by its ID and the ID of the group that it belongs to.

        Args:
            group_id (Text): The ID of the group that the channel belongs to.
            channel_id (Text): The ID of the channel.

        Returns:
            Dict: The channel data.
        """
        api_url = self._get_api_url(f"teams/{group_id}/channels/{channel_id}")
        channel = self.fetch_data_json(api_url)
        # Add the group ID to the channel data.
        channel.update({"teamId": group_id})

        return channel
    
    def get_channels_in_group_by_ids(self, group_id: Text, channel_ids: List[Text]) -> List[Dict]:
        """
        Get channels by their IDs and the ID of the group that they belong to.

        Args:
            group_id (Text): The ID of the group that the channels belong to.
            channel_ids (List[Text]): The IDs of the channels.

        Returns:
            List[Dict]: The channels data.
        """
        channels = []
        for channel_id in channel_ids:
            channels.append(self.get_channel_in_group_by_id(group_id, channel_id))

        return channels
    
    def get_all_channels_in_group(self, group_id: Text) -> List[Dict]:
        """
        Get all channels of a group by its ID.

        Args:
            group_id (Text): The ID of the group.

        Returns:
            List[Dict]: The channels data.
        """
        channels = self.fetch_data_json(f"teams/{group_id}/channels")
        for channel in channels:
            channel["teamId"] = group_id

        return channels
    
    def get_all_channels_across_all_groups(self) -> List[Dict]:
        """
        Get all channels.

        Returns:
            List[Dict]: The channels data.
        """
        channels = []
        for group_id in self._get_group_ids():
            channels += self.get_all_channels_in_group(group_id)

        return channels
    
    def get_channels_across_all_groups_by_ids(self, channel_ids: List[Text]) -> List[Dict]:
        """
        Get channels by their IDs.

        Args:
            channel_ids (List[Text]): The IDs of the channels.

        Returns:
            List[Dict]: The channels data.
        """
        channels = self.get_all_channels_across_all_groups()

        return [channel for channel in channels if channel["id"] in channel_ids]

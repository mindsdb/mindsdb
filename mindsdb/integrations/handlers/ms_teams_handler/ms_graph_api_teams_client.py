from typing import Text, List, Dict, Optional

from requests.exceptions import RequestException

from mindsdb.integrations.utilities.handlers.api_utilities.microsoft.ms_graph_api_utilities import MSGraphAPIBaseClient
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class MSGraphAPITeamsDelegatedPermissionsClient(MSGraphAPIBaseClient): 
    """
    The Microsoft Graph API client for the Microsoft Teams handler with delegated permissions.
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
            logger.error(f"Failed to check connection to Microsoft Teams: {request_error}")
            return False
        
    def get_all_groups(self) -> List[Dict]:
        """
        Get all groups that the signed in user is a member of.

        Returns:
            List[Dict]: The groups data.
        """
        return self.fetch_data_json("me/joinedTeams")
        
    def _get_all_group_ids(self) -> List[Text]:
        """
        Get all group IDs related to Microsoft Teams.

        Returns:
            List[Text]: The group IDs.
        """
        if not self._group_ids:
            groups = self.get_all_groups()
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
        channel = self.fetch_data_json(f"teams/{group_id}/channels/{channel_id}")
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
        Get all channels across all groups that the signed in user is a member of.

        Returns:
            List[Dict]: The channels data.
        """
        channels = []
        for group_id in self._get_all_group_ids():
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
    
    def get_message_in_channel_by_id(self, group_id: Text, channel_id: Text, message_id: Text) -> Dict:
        """
        Get a message by its ID, the ID of the group that it belongs to, and the ID of the channel that it belongs to.

        Args:
            group_id (Text): The ID of the group that the channel belongs to.
            channel_id (Text): The ID of the channel that the message belongs to.
            message_id (Text): The ID of the message.

        Returns:
            Dict: The message data.
        """
        return self.fetch_data_json(f"teams/{group_id}/channels/{channel_id}/messages/{message_id}")
    
    def get_messages_in_channel_by_ids(self, group_id: Text, channel_id: Text, message_ids: List[Text]) -> List[Dict]:
        """
        Get messages by their IDs, the ID of the group that they belong to, and the ID of the channel that they belong to.

        Args:
            group_id (Text): The ID of the group that the channel belongs to.
            channel_id (Text): The ID of the channel that the messages belong to.
            message_ids (List[Text]): The IDs of the messages.

        Returns:
            List[Dict]: The messages data.
        """
        messages = []
        for message_id in message_ids:
            messages.append(self.get_message_in_channel_by_id(group_id, channel_id, message_id))

        return messages

    def get_all_messages_in_channel(self, group_id: Text, channel_id: Text, limit: int = None) -> List[Dict]:
        """
        Get messages of a channel by its ID and the ID of the group that it belongs to.

        Args:
            group_id (Text): The ID of the group that the channel belongs to.
            channel_id (Text): The ID of the channel.

        Returns:
            List[Dict]: The messages data.
        """
        messages = []
        for messages_batch in self.fetch_paginated_data(f"teams/{group_id}/channels/{channel_id}/messages"):
            messages += messages_batch

            if limit and len(messages) >= limit:
                break

        return messages[:limit]
    
    def get_chat_by_id(self, chat_id: Text) -> Dict:
        """
        Get a chat by its ID.

        Args:
            chat_id (Text): The ID of the chat.

        Returns:
            Dict: The chat data.
        """
        return self.fetch_data_json(f"/me/chats/{chat_id}")
    
    def get_chats_by_ids(self, chat_ids: List[Text]) -> List[Dict]:
        """
        Get chats by their IDs.

        Args:
            chat_ids (List[Text]): The IDs of the chats.

        Returns:
            List[Dict]: The chats data.
        """
        chats = []
        for chat_id in chat_ids:
            chats.append(self.get_chat_by_id(chat_id))

        return chats

    def get_all_chats(self, limit: int = None) -> List[Dict]:
        """
        Get all chats of the signed in user.

        Args:
            limit (int): The maximum number of chats to return.

        Returns:
            List[Dict]: The chats data.
        """
        chats = []
        for chat_batch in self.fetch_paginated_data("me/chats"):
            chats += chat_batch

            if limit and len(chats) >= limit:
                break

        return chats[:limit]
    
    def get_message_in_chat_by_id(self, chat_id: Text, message_id: Text) -> Dict:
        """
        Get a message by its ID and the ID of the chat that it belongs to.

        Args:
            chat_id (Text): The ID of the chat that the message belongs to.
            message_id (Text): The ID of the message.

        Returns:
            Dict: The message data.
        """
        return self.fetch_data_json(f"me/chats/{chat_id}/messages/{message_id}")
    
    def get_messages_in_chat_by_ids(self, chat_id: Text, message_ids: List[Text]) -> List[Dict]:
        """
        Get messages by their IDs and the ID of the chat that they belong to.

        Args:
            chat_id (Text): The ID of the chat that the messages belong to.
            message_ids (List[Text]): The IDs of the messages.

        Returns:
            List[Dict]: The messages data.
        """
        messages = []
        for message_id in message_ids:
            messages.append(self.get_message_in_chat_by_id(chat_id, message_id))

        return messages
    
    def get_all_messages_in_chat(self, chat_id: Text, limit: int = None) -> List[Dict]:
        """
        Get messages of a chat by its ID.

        Args:
            chat_id (Text): The ID of the chat.

        Returns:
            List[Dict]: The messages data.
        """
        messages = []
        for messages_batch in self.fetch_paginated_data(f"me/chats/{chat_id}/messages"):
            messages += messages_batch

            if limit and len(messages) >= limit:
                break

        return messages[:limit]


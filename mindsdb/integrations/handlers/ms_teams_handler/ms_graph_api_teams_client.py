from typing import Text, List, Dict, Optional
from mindsdb.integrations.utilities.handlers.api_utilities.microsoft.ms_graph_api_utilities import MSGraphAPIBaseClient


class MSGraphAPITeamsClient(MSGraphAPIBaseClient): 
    """
    The Microsoft Graph API client for the Microsoft Teams handler.
    This client is used for accessing the Microsoft Teams specific endpoints of the Microsoft Graph API.
    Several common methods for submitting requests, fetching data, etc. are inherited from the base class.
    """

    def _get_group_ids(self) -> List[Text]:
        """
        Get all group IDs related to Microsoft Teams.

        Returns
        -------
        List[Text]
            The group IDs.
        """

        if not self._group_ids:
            api_url = self._get_api_url("groups")
            # only get the id and resourceProvisioningOptions fields
            params = {"$select": "id,resourceProvisioningOptions"}
            groups = self._get_response_value_unsafe(self._make_request(api_url, params=params))
            # filter out only the groups that are related to Microsoft Teams
            self._group_ids = [item["id"] for item in groups if "Team" in item["resourceProvisioningOptions"]]

        return self._group_ids
    
    def _get_channel_ids(self, group_id: Text) -> List[Text]:
        """
        Get all channel IDs related to a group.

        Parameters
        ----------
        group_id : Text
            The ID of the group.

        Returns
        -------
        List[Text]
            The channel IDs for that group.
        """

        api_url = self._get_api_url(f"teams/{group_id}/channels")
        channels_ids = self._get_response_value_unsafe(self._make_request(api_url))

        return channels_ids
    
    def get_channel(self, group_id: Text, channel_id: Text) -> Dict:
        """
        Get a channel by its ID and the ID of the group that it belongs to.

        Parameters
        ----------
        group_id : str
            The ID of the group that the channel belongs to.

        channel_id : str
            The ID of the channel.

        Returns
        -------
        Dict
            The channel data.
        """

        api_url = self._get_api_url(f"teams/{group_id}/channels/{channel_id}")
        channel = self._make_request(api_url)
        # add the group ID to the channel data
        channel.update({"teamId": group_id})

        return channel
    
    def get_channels(self) -> List[Dict]:
        """
        Get all channels.

        Returns
        -------
        List[Dict]
            The channels data.
        """

        channels = []
        for group_id in self._get_group_ids():
            for group_channels in self._fetch_data(f"teams/{group_id}/channels", pagination=False):
                [group_channel.update({"teamId": group_id}) for group_channel in group_channels]
                channels.extend(group_channels)

        return channels
    
    def get_channel_message(self, group_id: Text, channel_id: Text, message_id: Text) -> Dict:
        """
        Get a channel message by its ID and the IDs of the group and channel that it belongs to.

        Parameters
        ----------
        group_id : str
            The ID of the group that the channel belongs to.

        channel_id : str
            The ID of the channel that the message belongs to.

        message_id : str
            The ID of the message.

        Returns
        -------
        Dict
            The channel message data.
        """

        api_url = self._get_api_url(f"teams/{group_id}/channels/{channel_id}/messages/{message_id}")
        message = self._make_request(api_url)

        return message

    def get_channel_messages(self) -> List[Dict]:
        """
        Get all channel messages.

        Returns
        -------
        List[Dict]
            The channel messages data.
        """

        channel_messages = []
        for group_id in self._get_group_ids():
            for channel_id in self._get_channel_ids(group_id):
                for messages in self._fetch_data(f"teams/{group_id}/channels/{channel_id['id']}/messages"):
                    channel_messages.extend(messages)

        return channel_messages
    
    def send_channel_message(self, group_id: Text, channel_id: Text, message: Text, subject: Optional[Text] = None) -> None:
        """
        Send a message to a channel.

        Parameters
        ----------
        group_id : Text
            The ID of the group that the channel belongs to.

        channel_id : Text
            The ID of the channel.

        message : Text
            The message to send.

        subject : Text, Optional
            The subject of the message.
        """

        api_url = self._get_api_url(f"teams/{group_id}/channels/{channel_id}/messages")
        data = {
            "subject": subject,
            "body": {
                "content": message
            }
        }

        self._make_request(api_url, data=data, method="POST")

    def get_chat(self, chat_id: Text) -> Dict:
        """
        Get a chat by its ID.

        Parameters
        ----------
        chat_id : str
            The ID of the chat.
        """

        api_url = self._get_api_url(f"chats/{chat_id}")
        # expand the response with the last message preview
        chat = self._make_request(api_url, params={"$expand": "lastMessagePreview"})

        return chat
    
    def get_chats(self) -> List[Dict]:
        """
        Get all chats.

        Returns
        -------
        List[Dict]
            The chats data.
        """

        chats = []
        for chat in self._fetch_data("chats", params={"$expand": "lastMessagePreview"}):
            chats.extend(chat)

        return chats
    
    def get_chat_message(self, chat_id: Text, message_id: Text) -> Dict:
        """
        Get a chat message by its ID and the ID of the chat that it belongs to.

        Parameters
        ----------
        chat_id : str
            The ID of the chat that the message belongs to.

        message_id : str
            The ID of the message.

        Returns
        -------
        Dict
            The chat message data.
        """

        api_url = self._get_api_url(f"chats/{chat_id}/messages/{message_id}")
        message = self._make_request(api_url)

        return message
    
    def get_chat_messages(self, chat_id: Text) -> List[Dict]:
        """
        Get all chat messages for a chat.

        Parameters
        ----------
        chat_id : str
            The ID of the chat.

        Returns
        -------
        List[Dict]
            The chat messages data.
        """

        chat_messages = []
        for messages in self._fetch_data(f"chats/{chat_id}/messages"):
            chat_messages.extend(messages)

        return chat_messages

    def get_all_chat_messages(self) -> List[Dict]:
        """
        Get all chat messages for all chats.

        Returns
        -------
        List[Dict]
            The chat messages data.
        """

        chat_messages = []
        for chat_id in [chat["id"] for chat in self.get_chats()]:
            for messages in self._fetch_data(f"chats/{chat_id}/messages"):
                chat_messages.extend(messages)

        return chat_messages
    
    def send_chat_message(self, chat_id: Text, message: Text, subject: Optional[Text] = None) -> None:
        """
        Send a message to a chat.

        Parameters
        ----------
        chat_id : Text
            The ID of the chat.

        message : Text
            The message to send.

        subject : Text, Optional
            The subject of the message.
        """

        api_url = self._get_api_url(f"chats/{chat_id}/messages")
        data = {
            "subject": subject,
            "body": {
                "content": message
            }
        }
        
        self._make_request(api_url, data=data, method="POST")

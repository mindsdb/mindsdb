from typing import Optional
from mindsdb.integrations.handlers.utilities.api_utilities.microsoft.ms_graph_api_utilities import MSGraphAPIBaseClient


class MSGraphAPITeamsClient(MSGraphAPIBaseClient):   
    def get_channel(self, group_id: str, channel_id: str):
        api_url = self._get_api_url(f"teams/{group_id}/channels/{channel_id}")
        channel = self._make_request(api_url)
        channel.update({"teamId": group_id})
        return channel
    
    def _get_group_ids(self):
        if not self._group_ids:
            api_url = self._get_api_url("groups")
            params = {"$select": "id,resourceProvisioningOptions"}
            groups = self._get_response_value_unsafe(self._make_request(api_url, params=params))
            self._group_ids = [item["id"] for item in groups if "Team" in item["resourceProvisioningOptions"]]
        return self._group_ids

    def get_channels(self):
        channels = []
        for group_id in self._get_group_ids():
            for group_channels in self._fetch_data(f"teams/{group_id}/channels", pagination=False):
                [group_channel.update({"teamId": group_id}) for group_channel in group_channels]
                channels.extend(group_channels)

        return channels
    
    def get_channel_message(self, group_id: str, channel_id: str, message_id: str):
        api_url = self._get_api_url(f"teams/{group_id}/channels/{channel_id}/messages/{message_id}")
        message = self._make_request(api_url)
        return message
    
    def _get_channel_ids(self, group_id: str):
        api_url = self._get_api_url(f"teams/{group_id}/channels")
        channels_ids = self._get_response_value_unsafe(self._make_request(api_url))
        return channels_ids

    def get_channel_messages(self):
        channel_messages = []
        for group_id in self._get_group_ids():
            for channel_id in self._get_channel_ids(group_id):
                for messages in self._fetch_data(f"teams/{group_id}/channels/{channel_id['id']}/messages"):
                    channel_messages.extend(messages)

        return channel_messages
    
    def send_channel_message(self, group_id: str, channel_id: str, message: str, subject: Optional[str] = None):
        api_url = self._get_api_url(f"teams/{group_id}/channels/{channel_id}/messages")
        data = {
            "subject": subject,
            "body": {
                "content": message
            }
        }
        self._make_request(api_url, data=data, method="POST")

    def get_chat(self, chat_id: str):
        api_url = self._get_api_url(f"chats/{chat_id}")
        chat = self._make_request(api_url, params={"$expand": "lastMessagePreview"})
        return chat
    
    def get_chats(self):
        chats = []
        for chat in self._fetch_data("chats", params={"$expand": "lastMessagePreview"}):
            chats.extend(chat)

        return chats
    
    def get_chat_message(self, chat_id: str, message_id: str):
        api_url = self._get_api_url(f"chats/{chat_id}/messages/{message_id}")
        message = self._make_request(api_url)
        return message
    
    def get_chat_messages(self, chat_id: str):
        chat_messages = []
        for messages in self._fetch_data(f"chats/{chat_id}/messages"):
            chat_messages.extend(messages)

        return chat_messages

    def get_all_chat_messages(self):
        chat_messages = []
        for chat_id in [chat["id"] for chat in self.get_chats()]:
            for messages in self._fetch_data(f"chats/{chat_id}/messages"):
                chat_messages.extend(messages)

        return chat_messages
    
    def send_chat_message(self, chat_id: str, message: str, subject: Optional[str] = None):
        api_url = self._get_api_url(f"chats/{chat_id}/messages")
        data = {
            "subject": subject,
            "body": {
                "content": message
            }
        }
        self._make_request(api_url, data=data, method="POST")

import requests
from typing import Dict, List


class RocketChatClient(object):
    """Client to interact with the Rocket Chat API.
    
    Attributes:
        domain (str): Path to Rocket Chat domain to use (e.g. https://mindsdb.rocket.chat).
        auth_token (str): Rocket Chat authorization token to use for all API requests.
        auth_user_id (str): Rocket Chat user ID to associate with all API requests
    """

    def __init__(
            self,
            domain: str,
            token: str = None,
            user_id: str = None,
            username: str = None,
            password: str = None):
        self.domain = domain
        self.base_url = f'{self.domain}/api/v1'
        self.auth_token = token
        self.auth_user_id = user_id
        if self.auth_token is None or self.auth_user_id is None:
            login_data = {
                'user': username,
                'password': password
            }
            login_response = requests.post(f'{self.base_url}/login', data=login_data)
            try:
                self.auth_token = login_response.json()['data']['authToken']
                self.auth_user_id = login_response.json()['data']['userId']
            except KeyError:
                login_response.raise_for_status()

        self.headers = {
            'X-Auth-Token': self.auth_token,
            'X-User-Id': self.auth_user_id
        }

    def get_all_channel_messages(self, room_id: str, limit: int = None) -> List[Dict]:
        """Gets all messages associated with a Rocket Chat channel.
        
        Returns a list of message objects. 
        See https://developer.rocket.chat/reference/api/rest-api/endpoints/core-endpoints/channels-endpoints/messages

        Args:
            room_id (str): The channel's ID
            limit (int): At most, how many messages to return.
        """

        params = {
            'roomId': room_id
        }
        if limit is not None:
            params['count'] = limit
        messages_response = requests.get(
            f'{self.base_url}/channels.messages',
            params=params,
            headers=self.headers)
        if not messages_response.ok:
            messages_response.raise_for_status()
        return messages_response.json()['messages']

    def get_direct_messages(self, username: str) -> List[Dict]:
        """Gets all messages sent directly to a user

        Returns a list of message objects.
        See https://developer.rocket.chat/reference/api/rest-api/endpoints/core-endpoints/im-endpoints/messages

        Args:
            username (str): The username to get direct messages for.
        """

        params = {
            'username': username
        }
        messages_response = requests.get(
            f'{self.base_url}/im.messages',
            params=params,
            headers=self.headers)
        if not messages_response.ok:
            messages_response.raise_for_status()
        return messages_response.json()['messages']

    def post_message(
            self,
            room_id: str,
            text: str,
            alias: str = None,
            emoji: str = None,
            avatar: str = None):
        """Sends a new message to a Rocket Chat channel.

        See https://developer.rocket.chat/reference/api/rest-api/endpoints/core-endpoints/chat-endpoints/postmessage
        
        Args:
            room_id (str): Room ID of where the message is to be sent.
            text (str): The text of the message to send.
            alias (str): What the message's name will appear as (username will still display).
            emoji (str): Sets the avatar on the message to be this emoji.
            avatar (str): Image URL to use for the message avatar.
        """

        message_data = {
            'roomId': room_id,
        }
        if text is not None:
            message_data['text'] = text
        if alias is not None:
            message_data['alias'] = alias
        if emoji is not None:
            message_data['emoji'] = emoji
        if avatar is not None:
            message_data['avatar'] = avatar

        post_response = requests.post(
            f'{self.base_url}/chat.postMessage',
            data=message_data,
            headers=self.headers)
        if not post_response.ok:
            post_response.raise_for_status()
        return post_response.json()['message']

    def ping(self) -> bool:
        """Sends a basic request to the Rocket Chat API to see if it succeeds.
        
        Returns whether or not the connection to the Rocket Chat API is valid.
        See https://developer.rocket.chat/reference/api/rest-api/endpoints/core-endpoints/channels-endpoints/list
        """

        # No ping or similar endpoint exists, so we'll try listing all channels on the domain.
        channels_response = requests.get(
            f'{self.base_url}/channels.list?count=1',
            headers=self.headers)
        return channels_response.ok

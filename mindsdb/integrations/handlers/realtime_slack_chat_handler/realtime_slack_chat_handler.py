from slack_sdk.web import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.socket_mode import SocketModeClient
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.socket_mode.response import SocketModeResponse

from threading import Event
from typing import Callable, Dict

from mindsdb.integrations.libs.realtime_chat_handler import RealtimeChatHandler
from mindsdb.interfaces.chatbot.chatbot_message import ChatBotMessage
from mindsdb.interfaces.chatbot.chatbot_response import ChatBotResponse

class RealtimeSlackChatHandler(RealtimeChatHandler):
    """Implements RealtimeChatHandler interface for sending/receiving Slack messages."""

    def __init__(self, on_message: Callable[[ChatBotMessage], None], params: Dict[str, str]):
        super().__init__('SlackChatHandler', on_message)

        if 'app_token' not in params or 'web_token' not in params:
            raise ValueError('Need app_token and web_token parameters to use Slack chat handler')
        self._stop_event = Event()
        self._socket_mode_client = SocketModeClient(
            # This app-level token will be used only for establishing a connection
            app_token=params['app_token'],  # xapp-A111-222-xyz
            # You will be using this WebClient for performing Web API calls in listeners
            web_client=WebClient(token=params['web_token'])  # xoxb-111-222-xyz
        )
        self._socket_mode_client.socket_mode_request_listeners.append(self._process_websocket_message)

    def _process_websocket_message(self, client: SocketModeClient, request: SocketModeRequest):
        # Acknowledge the request
        response = SocketModeResponse(envelope_id=request.envelope_id)
        client.send_socket_mode_response(response)

        if request.type != 'events_api':
            return
        
        payload_event = request.payload['event']
        if payload_event['type'] != 'message':
            return
        if 'subtype' in payload_event:
            # Don't respond to message_changed, message_deleted, etc.
            return
        if payload_event['channel_type'] != 'im':
            # Only support IMs currently.
            return
        if 'bot_id' in payload_event:
            # A bot sent this message.
            return

        chatbot_message = ChatBotMessage(
            ChatBotMessage.Type.DIRECT,
            payload_event['text'],
            # In Slack direct messages are treated as channels themselves.
            payload_event['channel'],
            payload_event['channel']
        )
        self.on_message(chatbot_message)

    def connect(self):
        """Begins listening for Slack messages using underlying websocket client."""
        self._socket_mode_client.connect()
        self._stop_event.wait()
    
    def disconnect(self):
        """Stops listening for Slack messages."""
        self._stop_event.set()
    
    def send_message(self, message: ChatBotMessage) -> ChatBotResponse:
        """
        Sends a Slack message.
        
        Parameters: message (ChatBotMessage): The message to send

        Returns: response (ChatBotResponse): Response indicating whether the message was sent successfully
        """
        if message.type != ChatBotMessage.Type.DIRECT:
            raise NotImplementedError('Only sending direct messages is supported by RealtimeSlackChatHandler')
        response = self._socket_mode_client.web_client.chat_postMessage(
            channel=message.destination,
            text=message.text
        )
        try:
            response.validate()
            return ChatBotResponse(message.text)
        except SlackApiError as e:
            return ChatBotResponse(message.text, error=str(e))

        
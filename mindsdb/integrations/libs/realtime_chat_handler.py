from typing import Callable

from mindsdb.interfaces.chatbot.chatbot_message import ChatBotMessage
from mindsdb.interfaces.chatbot.chatbot_response import ChatBotResponse


class RealtimeChatHandler:
    """Interface to send and receive messages over a chat application (Slack, RocketChat, etc)"""

    def __init__(self, name: str, on_message: Callable[[ChatBotMessage], None]):
        self.name = name
        # Should be called every time a message is received.
        self.on_message = on_message

    def connect(self):
        """Connects to chat application and starts listening for messages."""
        raise NotImplementedError()

    def disconnect(self):
        """Disconnects from the chat application and stops listening for messages."""
        raise NotImplementedError()

    def send_message(self, message: ChatBotMessage) -> ChatBotResponse:
        """
        Sends a message through the chat application

        Parameters:
            message (ChatBotMessage): Message to send

        Returns:
            response (ChatBotResponse): Response indicating whether the message was sent successfully
        """
        raise NotImplementedError()

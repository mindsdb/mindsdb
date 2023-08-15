from enum import Enum
import datetime as dt


class BotException(Exception):
    pass


class ChatBotMessage:
    """
    Represents a message sent and received by chatbots.

    Attributes:
        type (ChatBotMessage.Type): Type of message
        text (str): Actual message content
        user (str): The user that sent the message
        destination (str): The user or channel that received the message

    """

    class Type(Enum):
        DIRECT = 1
        CHANNEL = 2

    def __init__(self, type: Type, text: str, user: str, destination: str = None, sent_at: dt.datetime = None):
        self.type = type
        self.text = text
        self.user = user
        self.destination = destination
        self.sent_at = sent_at or dt.datetime.now()


class Function:

    def __init__(self, name, description, callback):
        self.name = name
        self.description = description
        self.callback = callback

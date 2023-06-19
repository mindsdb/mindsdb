from enum import Enum

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

    def __init__(self, type: Type, text: str, user: str, destination: str):
        self.type = type
        self.text = text
        self.user = user
        self.destination = destination

class ChatBotResponse:
    """
    Represents a response from a chatbot sending a message over a chat application.
    
    Attributes:
        text (str): Text of the message sent by the chatbot
        error (str): If sending the message failed, the cause of failure
    """
    def __init__(self, text: str, error: str=None):
        self.text = text
        self.error = error
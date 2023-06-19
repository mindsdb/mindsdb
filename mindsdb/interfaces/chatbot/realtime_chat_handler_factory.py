from typing import Callable, Dict

from mindsdb.integrations.handlers.realtime_slack_chat_handler.realtime_slack_chat_handler import RealtimeSlackChatHandler
from mindsdb.integrations.libs.realtime_chat_handler import RealtimeChatHandler
from mindsdb.interfaces.chatbot.chatbot_message import ChatBotMessage

class RealtimeChatHandlerFactory:
    """Creates a RealtimeChatHandler based on configuration."""
    
    def create_realtime_chat_handler(self, chat_engine: str, on_message: Callable[[ChatBotMessage], None], params: Dict[str, str]) -> RealtimeChatHandler:
        """
        Creates a RealtimeChatHandler from the given chat engine and parameters.

        Parameters:
            chat_engine (str): The chat application to use
            on_message (Callable[[ChatBotMessage], None]): Callback for every message received
            params (Dict[str, str]): Parameters to pass to the handler

        Returns:
            handler (RealtimeChatHandler): Created chat handler
        """
        if chat_engine == 'slack':
            return RealtimeSlackChatHandler(
                on_message,
                params
            )
        raise NotImplementedError(f'Chat handler for engine {chat_engine} does not exist')
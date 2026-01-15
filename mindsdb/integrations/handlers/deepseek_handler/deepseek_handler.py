import os
from typing import Optional, Dict

from mindsdb.integrations.handlers.openai_handler.openai_handler import OpenAIHandler
from mindsdb.utilities import log
from mindsdb.integrations.handlers.openai_handler.constants import OPENAI_API_BASE

logger = log.getLogger(__name__)

class DeepSeekHandler(OpenAIHandler):
    """
    The DeepSeek handler.
    """
    name = 'deepseek'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_model = 'deepseek-chat'
        self.supported_ft_models = [] # DeepSeek fine-tuning not yet standard via this API in same way
        self.api_base = 'https://api.deepseek.com'

    def create_engine(self, connection_args: Dict) -> None:
        """
        Validate the DeepSeek API credentials on engine creation.
        """
        connection_args = connection_args.copy()
        # Set default API base if not provided
        if 'api_base' not in connection_args:
             connection_args['api_base'] = self.api_base
        
        # Determine API key
        if 'deepseek_api_key' in connection_args:
             connection_args['openai_api_key'] = connection_args['deepseek_api_key']
        elif 'api_key' in connection_args:
             connection_args['openai_api_key'] = connection_args['api_key']

        super().create_engine(connection_args)

    def create_validation(self, target, args=None, **kwargs):
        if args is None:
            args = {}
        
        # Enforce API base
        if 'api_base' not in args:
             args['api_base'] = self.api_base
        
        super().create_validation(target, args, **kwargs)

    # TODO: Override _completion or inner logic to capture 'reasoning_content'
    # For now, we reuse OpenAI logic which works for standard chat.
    # Reasoning content support requires deeper changes to OpenAIHandler 
    # or copying significant code because _tidy is an inner function.

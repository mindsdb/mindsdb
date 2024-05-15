from typing import Text, List
from pydantic_settings import BaseSettings


class AnyscaleHandlerConfig(BaseSettings):
    """
    Configuration for the Anyscale handler.

    Attributes
    ----------

    ANYSCALE_API_BASE : Text
        Base URL for the Anyscale API.

    MIN_FT_VAL_LEN : int
        Minimum number of validation chats required for fine-tuning.

    MIN_FT_DATASET_LEN : int
        Minimum number of training and validation chats required for fine-tuning.

    DEFAULT_MODEL : Text
        Default model to use for models.

    DEFAULT_MODE : Text
        Default mode to use for models. Can be 'default', 'conversational', or 'conversational-full'.

    SUPPORTED_MODES : List
        List of supported modes. Can be 'default', 'conversational', or 'conversational-full'.

    RATE_LIMIT : int
        Number of requests per minute.

    MAX_BATCH_SIZE : int
        Maximum batch size for requests.

    DEFAULT_MAX_TOKENS : int
        Default maximum tokens for requests.
    """

    ANYSCALE_API_BASE: Text = 'https://api.endpoints.anyscale.com/v1'
    MIN_FT_VAL_LEN: int = 20
    MIN_FT_DATASET_LEN: int = MIN_FT_VAL_LEN * 2
    DEFAULT_MODEL: Text = 'meta-llama/Llama-2-7b-chat-hf'
    DEFAULT_MODE: Text = 'default'
    SUPPORTED_MODES: List = ['default', 'conversational', 'conversational-full']
    RATE_LIMIT: int = 25
    MAX_BATCH_SIZE: int = 20
    DEFAULT_MAX_TOKENS: int = 100


anyscale_handler_config = AnyscaleHandlerConfig()

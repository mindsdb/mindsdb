from pydantic import BaseSettings

from mindsdb.integrations.handlers.openai_handler.constants import OPENAI_API_BASE


class AnyscaleHandlerConfig(BaseSettings):
    """
    Configuration for the Anyscale handler.

    Attributes
    ----------

    ANYSCALE_API_BASE : str
        Base URL for the Anyscale API.

    MIN_FT_VAL_LEN : int
    
    MIN_FT_DATASET_LEN : int

    """

    ANYSCALE_API_BASE = 'https://api.endpoints.anyscale.com/v1'
    OPENAI_API_BASE = OPENAI_API_BASE
    # TODO: Are these comments correct? Update the attribute definitions in the above docstring as well.
    MIN_FT_VAL_LEN = 20  # anyscale checks for at least 20 validation chats
    MIN_FT_DATASET_LEN = MIN_FT_VAL_LEN * 2  # we ask for 20 training chats as well

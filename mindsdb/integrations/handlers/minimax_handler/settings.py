from pydantic_settings import BaseSettings


class MiniMaxHandlerConfig(BaseSettings):
    """
    Configuration for MiniMax handler.

    Attributes
    ----------

    BASE_URL : str
        Base URL for the MiniMax API (OpenAI-compatible endpoint).
    DEFAULT_MODEL : str
        Default model to use for MiniMax API.
    DEFAULT_MODE : str
        Default mode to use for MiniMax API.
    SUPPORTED_MODES : list[str]
        List of supported modes for MiniMax API.
    """

    BASE_URL: str = "https://api.minimax.io/v1"
    DEFAULT_MODEL: str = "MiniMax-M2.7"
    DEFAULT_MODE: str = "default"
    SUPPORTED_MODES: list[str] = [
        'default',
        'conversational',
        'conversational-full',
    ]


minimax_handler_config = MiniMaxHandlerConfig()

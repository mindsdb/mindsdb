from pydantic_settings import BaseSettings


class GroqHandlerConfig(BaseSettings):
    """
    Configuration for Groq handler.

    Attributes
    ----------

    BASE_URL : str
        Base URL for the Groq API.
    DEFAULT_MODEL : str
        Default model to use for Groq API.
    DEFAULT_MODE : str
        Default mode to use for Groq API.
    SUPPORTED_MODES : list[str]
        List of supported modes for Groq API.
    """

    BASE_URL: str = "https://api.groq.com/openai/v1"
    DEFAULT_MODEL: str = "llama3-8b-8192"
    DEFAULT_MODE: str = "default"
    SUPPORTED_MODES: list[str] = [
        'default',
        'conversational',
        'conversational-full',
    ]


groq_handler_config = GroqHandlerConfig()

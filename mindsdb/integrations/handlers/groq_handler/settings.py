from pydantic_settings import BaseSettings


class GroqHandlerConfig(BaseSettings):
    """
    Configuration for Groq handler.

    Attributes
    ----------

    BASE_URL : str
        Base URL for the Groq API.
    """

    BASE_URL: str = "https://api.groq.com/openai/v1"


groq_handler_config = GroqHandlerConfig()

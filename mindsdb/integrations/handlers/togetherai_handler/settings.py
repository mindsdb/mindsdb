from pydantic_settings import BaseSettings


class TogetherAIConfig(BaseSettings):
    """
    Configuration for TogetherAI handler.

    Attributes
    ----------

    BASE_URL : str
        Base URL for the TogetherAI API.
    DEFAULT_MODEL : str
        Default model to use for TogetherAI API.
    DEFAULT_MODE : str
        Defat mode to use for TogetherAI API.
    SUPPORTED_MODES : list[str]
        List of supported modes for TogetherAI API.
    """

    BASE_URL: str = "https://api.together.xyz/v1"
    DEFAULT_MODEL: str = "meta-llama/Llama-3.2-3B-Instruct-Turbo"
    DEFAULT_EMBEDDING_MODEL: str = "togethercomputer/m2-bert-80M-32k-retrieval"
    DEFAULT_MODE: str = "default"
    SUPPORTED_MODES: list[str] = [
        "default",
        "conversational",
        "conversational-full",
        "embedding",
    ]


togetherai_handler_config = TogetherAIConfig()

from pydantic_settings import BaseSettings


class MindsEndpointHandlerConfig(BaseSettings):
    """
    Configuration for Minds Endpoint handler.

    Attributes
    ----------

    BASE_URL : str
        Base URL for the Minds Endpoint API.
    """

    BASE_URL: str = "https://llm.mdb.ai/"


minds_endpoint_handler_config = MindsEndpointHandlerConfig()

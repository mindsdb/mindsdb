from pydantic import BaseSettings


class MindsDBCloudHandlerConfig(BaseSettings):
    """
    Configuration for MindsDB Cloud handler.

    Attributes
    ----------

    BASE_URL : str
        Base URL for the MindsDB Cloud Endpoints API.
    """

    BASE_URL = "https://llm.mdb.ai/"


mindsdb_cloud_handler_config = MindsDBCloudHandlerConfig()

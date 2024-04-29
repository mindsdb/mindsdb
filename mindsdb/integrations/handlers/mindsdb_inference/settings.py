from pydantic import BaseSettings


class MindsDBInferenceHandlerConfig(BaseSettings):
    """
    Configuration for MindsDB Inference handler.

    Attributes
    ----------

    BASE_URL : str
        Base URL for the MindsDB Inference Endpoints API.
    """

    BASE_URL = "https://llm.mdb.ai/"


mindsdb_inference_handler_config = MindsDBInferenceHandlerConfig()

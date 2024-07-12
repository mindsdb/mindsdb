import pandas as pd
from typing import Optional, Dict

from mindsdb.utilities import log

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.libs.api_handler_exceptions import MissingConnectionParams
from mindsdb.integrations.handlers.bedrock_handler.handler_settings import AmazonBedrockEngineConfig, AmazonBedrockModelConfig


logger = log.getLogger(__name__)

class AmazonBedrockHandler(BaseMLEngine):
    """
    This handler handles connection and inference with the Amazon Bedrock API.
    """

    name = 'bedrock'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generative = True

    def create_engine(self, connection_args: Dict) -> None:
        """
        Validates the AWS credentials provided on engine creation.

        Args:
            connection_args (Dict): Parameters for the engine.

        Raises:
            Exception: If the handler is not configured with valid API credentials.
        """
        connection_args = {k.lower(): v for k, v in connection_args.items()}
        AmazonBedrockEngineConfig(**connection_args)

    @staticmethod
    def create_validation(target: str, args: Dict = None, **kwargs: Dict) -> None:
        """
        Validates the arguments provided on model creation.

        Args:
            target (str): Target column.
            args (Dict): Parameters for the model.

        Raises:
            MissingConnectionParams: If a USING clause is not provided.

            ValueError: If the parameters provided in the USING clause are invalid.
        """
        if 'using' not in args:
            raise MissingConnectionParams("Twelve Labs engine requires a USING clause! Refer to its documentation for more details.")
        else:
            args = args['using']
            AmazonBedrockModelConfig(**args)

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        pass

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        pass

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        pass
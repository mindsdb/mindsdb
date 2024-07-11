import pandas as pd
from typing import Optional, Dict

from mindsdb.utilities import log
from mindsdb.integrations.libs.base import BaseMLEngine

from mindsdb.integrations.handlers.bedrock_handler.settings import AmazonBedrockEngineConfig


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
        Validate the OpenAI API credentials on engine creation.

        Args:
            connection_args (Dict): Parameters for the engine.

        Raises:
            Exception: If the handler is not configured with valid API credentials.

        Returns:
            None
        """
        connection_args = {k.lower(): v for k, v in connection_args.items()}
        AmazonBedrockEngineConfig(**connection_args)

    @staticmethod
    def create_validation(target: str, args: Dict = None, **kwargs: Dict) -> None:
        pass

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        pass

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        pass

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        pass
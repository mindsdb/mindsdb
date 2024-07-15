import pandas as pd
from typing import Optional, Dict, Any

from mindsdb.utilities import log

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.libs.api_handler_exceptions import MissingConnectionParams
from mindsdb.integrations.handlers.bedrock_handler.handler_settings import AmazonBedrockHandlerEngineConfig, AmazonBedrockHandlerModelConfig


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
        AmazonBedrockHandlerEngineConfig(**connection_args)

    def create(self, target, args: Dict = None, **kwargs: Any) -> None:
        """
        Create a model by connecting to the Amazon Bedrock API.

        Args:
            target (Text): Target column name.
            args (Dict): Parameters for the model.
            kwargs (Any): Other keyword arguments.

        Raises:
            Exception: If the model is not configured with valid parameters.

        Returns:
            None
        """
        if 'using' not in args:
            raise MissingConnectionParams("Amazon Bedrock engine requires a USING clause! Refer to its documentation for more details.")
        else:
            args = args['using']
            args['target'] = target
            handler_model_config = AmazonBedrockHandlerModelConfig(**args, engine=self.engine_storage)

            # Save the model configuration to the storage.
            llm_parameters = handler_model_config.llm_config.model_dump()['parameters']
            logger.info(f"Saving model configuration: {llm_parameters}")
            args['llm_parameters'] = llm_parameters
            self.model_storage.json_set('args', args)

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        pass

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        pass
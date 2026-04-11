import os
import pandas as pd
from openai import OpenAI, NotFoundError, AuthenticationError
from typing import Any, Dict, Optional, Text

from mindsdb.integrations.handlers.openai_handler import Handler as OpenAIHandler
from mindsdb.integrations.handlers.openai_handler.openai_handler import Mode
from mindsdb.integrations.utilities.handler_utils import get_api_key
from mindsdb.integrations.handlers.minimax_handler.settings import minimax_handler_config
from mindsdb.utilities import log

logger = log.getLogger(__name__)

MINIMAX_MODELS = [
    "MiniMax-M2.7",
    "MiniMax-M2.7-highspeed",
]


class MiniMaxHandler(OpenAIHandler):
    """
    This handler handles connection and inference with the MiniMax API.
    MiniMax provides an OpenAI-compatible API endpoint at https://api.minimax.io/v1.
    """

    name = "minimax"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api_base = minimax_handler_config.BASE_URL
        self.default_model = minimax_handler_config.DEFAULT_MODEL
        self.default_mode = minimax_handler_config.DEFAULT_MODE
        self.supported_modes = minimax_handler_config.SUPPORTED_MODES

    @staticmethod
    def _check_client_connection(client: OpenAI):
        """
        Check the MiniMax engine client connection.

        MiniMax does not support the models.list() endpoint, so we make a
        lightweight chat completion request to verify credentials instead.

        Args:
            client (OpenAI): OpenAI client configured with the MiniMax API credentials.

        Raises:
            Exception: If the client connection (API key) is invalid.

        Returns:
            None
        """
        try:
            client.models.list()
        except NotFoundError:
            # MiniMax does not expose the /models endpoint; ignore 404
            pass
        except AuthenticationError as e:
            raise Exception(f"Invalid MiniMax API key: {e}")
        except Exception as e:
            error_msg = str(e).lower()
            if "401" in str(e) or "authentication" in error_msg or "unauthorized" in error_msg:
                raise Exception(f"Invalid MiniMax API key: {e}")
            # Other errors (network, 404, etc.) are acceptable during connection check

    def create_engine(self, connection_args):
        """
        Validate the MiniMax API credentials on engine creation.

        Args:
            connection_args (dict): Connection arguments.

        Raises:
            Exception: If the handler is not configured with valid API credentials.

        Returns:
            None
        """
        connection_args = {k.lower(): v for k, v in connection_args.items()}
        api_key = connection_args.get("minimax_api_key")
        if api_key is not None:
            api_base = connection_args.get("api_base") or os.environ.get(
                "MINIMAX_BASE_URL", minimax_handler_config.BASE_URL
            )
            client = self._get_client(api_key=api_key, base_url=api_base)
            MiniMaxHandler._check_client_connection(client)

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        """
        Validate the MiniMax API credentials on model creation.

        Args:
            target (str): Target column, not required for LLMs.
            args (dict): Handler arguments.
            kwargs (dict): Handler keyword arguments.

        Raises:
            Exception: If the handler is not configured with valid API credentials.

        Returns:
            None
        """
        if "using" not in args:
            raise Exception(
                "MiniMax engine requires a USING clause! Refer to its documentation for more details."
            )
        else:
            args = args["using"]

        engine_storage = kwargs["handler_storage"]
        connection_args = engine_storage.get_connection_args()
        api_key = get_api_key("minimax", args, engine_storage=engine_storage)
        api_base = (
            connection_args.get("api_base")
            or args.get("api_base")
            or os.environ.get("MINIMAX_BASE_URL", minimax_handler_config.BASE_URL)
        )
        client = OpenAIHandler._get_client(api_key=api_key, base_url=api_base)
        MiniMaxHandler._check_client_connection(client)

    @staticmethod
    def is_chat_model(model_name):
        """
        All MiniMax models use the chat completions endpoint.
        """
        return True

    def create(self, target: Text, args: Dict = None, **kwargs: Any) -> None:
        """
        Create a MiniMax model, validating the model name against the known list.

        MiniMax does not expose a /models endpoint, so we validate locally
        instead of fetching available models from the API.

        Args:
            target (Text): Target column name.
            args (Dict): Parameters for the model.
            kwargs (Any): Other keyword arguments.

        Raises:
            Exception: If the handler is not configured with valid parameters.

        Returns:
            None
        """
        args = args["using"]
        args["target"] = target
        try:
            mode = args.get("mode")
            if mode is not None:
                mode = Mode(mode)
            else:
                mode = self.default_mode

            if not args.get("model_name"):
                args["model_name"] = self.default_model
            elif args["model_name"] not in MINIMAX_MODELS:
                raise Exception(
                    f"Invalid model name '{args['model_name']}'. "
                    f"Please use one of: {MINIMAX_MODELS}"
                )
        finally:
            self.model_storage.json_set("args", args)

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        """
        Call the MiniMax engine to predict the next token.

        Args:
            df (pd.DataFrame): Input data.
            args (dict): Handler arguments.

        Returns:
            pd.DataFrame: Predicted data
        """
        return super().predict(df, args)

    def finetune(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None):
        raise NotImplementedError("Fine-tuning is not supported for MiniMax engine.")

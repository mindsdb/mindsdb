import os
from typing import Dict, Optional

from mindsdb.integrations.handlers.openai_handler.openai_handler import OpenAIHandler
from mindsdb.integrations.handlers.openai_handler.constants import CHAT_MODELS_PREFIXES as OPENAI_CHAT_PREFIXES
from mindsdb.integrations.utilities.handler_utils import get_api_key
from mindsdb.utilities import log

from .constants import (
    NOVITA_API_BASE,
    DEFAULT_CHAT_MODEL,
    DEFAULT_EMBEDDING_MODEL,
    CHAT_MODELS_PREFIXES,
    ALL_SUPPORTED_MODELS,
)

logger = log.getLogger(__name__)


class NovitaHandler(OpenAIHandler):
    """
    This handler handles connection and inference with the Novita AI API.

    Novita AI provides an OpenAI-compatible API, so this handler extends
    OpenAIHandler with custom base URL and model configurations.
    """

    name = "novita"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generative = True
        self.default_model = DEFAULT_CHAT_MODEL
        self.default_embedding_model = DEFAULT_EMBEDDING_MODEL
        self.api_key_name = "novita"
        self.api_base = NOVITA_API_BASE
        self.supported_ft_models = []  # Novita doesn't support OpenAI-style fine-tuning

    def create_engine(self, connection_args: Dict) -> None:
        """
        Validate the Novita API credentials on engine creation.

        Args:
            connection_args (Dict): Parameters for the engine.

        Raises:
            Exception: If the handler is not configured with valid API credentials.

        Returns:
            None
        """
        connection_args = {k.lower(): v for k, v in connection_args.items()}
        api_key = connection_args.get("novita_api_key")
        if api_key is not None:
            api_base = connection_args.get("api_base") or os.environ.get("NOVITA_API_BASE", NOVITA_API_BASE)
            client = self._get_client(api_key=api_key, base_url=api_base, org=None, args=connection_args)
            OpenAIHandler._check_client_connection(client)

    @staticmethod
    def create_validation(target: str, args: Dict = None, **kwargs) -> None:
        """
        Validate the Novita API credentials on model creation.

        Args:
            target (str): Target column name.
            args (Dict): Parameters for the model.
            kwargs: Other keyword arguments.

        Raises:
            Exception: If the handler is not configured with valid API credentials.

        Returns:
            None
        """
        if "using" not in args:
            raise Exception("Novita engine requires a USING clause! Refer to its documentation for more details.")
        else:
            args = args["using"]

        if len(set(args.keys()) & {"question_column", "prompt_template", "prompt"}) == 0:
            raise Exception("One of `question_column`, `prompt_template` or `prompt` is required for this engine.")

        engine_storage = kwargs["handler_storage"]
        connection_args = engine_storage.get_connection_args()
        api_key = get_api_key("novita", args, engine_storage=engine_storage)
        api_base = (
            args.get("api_base")
            or connection_args.get("api_base")
            or os.environ.get("NOVITA_API_BASE", NOVITA_API_BASE)
        )
        client = NovitaHandler._get_client(api_key=api_key, base_url=api_base, org=None, args=args)
        NovitaHandler._check_client_connection(client)

    def create(self, target, args: Dict = None, **kwargs) -> None:
        """
        Create a model by connecting to the Novita API.

        Args:
            target: Target column name.
            args (Dict): Parameters for the model.
            kwargs: Other keyword arguments.

        Raises:
            Exception: If the model is not configured with valid parameters.

        Returns:
            None
        """
        args = args["using"]
        args["target"] = target

        # Set default model if not specified
        if not args.get("model_name"):
            # Check mode to determine default model
            mode = args.get("mode", "default")
            if mode == "embedding":
                args["model_name"] = self.default_embedding_model
            else:
                args["model_name"] = self.default_model

        self.model_storage.json_set("args", args)

    @staticmethod
    def is_chat_model(model_name):
        """
        Check if the model is a chat model based on Novita's model naming conventions.

        Args:
            model_name (str): The model name to check.

        Returns:
            bool: True if the model is a chat model, False otherwise.
        """
        # Check Novita-specific prefixes
        for prefix in CHAT_MODELS_PREFIXES:
            if model_name.startswith(prefix):
                return True
        # Fall back to OpenAI prefixes for backward compatibility
        for prefix in OPENAI_CHAT_PREFIXES:
            if model_name.startswith(prefix):
                return True
        return False

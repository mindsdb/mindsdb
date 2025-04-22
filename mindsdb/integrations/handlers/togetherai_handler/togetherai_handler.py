import os
import textwrap
from typing import Optional, Dict, Any
import requests
import pandas as pd
from openai import OpenAI, AuthenticationError
from mindsdb.integrations.handlers.openai_handler import Handler as OpenAIHandler
from mindsdb.integrations.utilities.handler_utils import get_api_key
from mindsdb.integrations.handlers.togetherai_handler.settings import (
    togetherai_handler_config,
)

from mindsdb.utilities import log

logger = log.getLogger(__name__)


class TogetherAIHandler(OpenAIHandler):
    """
    This handler handles connection to the TogetherAI.
    """

    name = "togetherai"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generative = True
        self.api_base = togetherai_handler_config.BASE_URL
        self.default_model = togetherai_handler_config.DEFAULT_MODEL
        self.default_embedding_model = togetherai_handler_config.DEFAULT_EMBEDDING_MODEL
        self.default_mode = togetherai_handler_config.DEFAULT_MODE
        self.supported_modes = togetherai_handler_config.SUPPORTED_MODES

    @staticmethod
    def _check_client_connection(client: OpenAI):
        """
        Check the TogetherAI engine client connection by listing models.

        Args:
            client (OpenAI): OpenAI client configured with the TogetherAI API credentials.

        Raises:
            Exception: If the client connection (API key) is invalid.

        Returns:
            None
        """

        try:
            TogetherAIHandler._get_supported_models(client.api_key, client.base_url)

        except Exception as e:
            raise Exception(f"Something went wrong: {e}")

    def create_engine(self, connection_args):
        """
        Validate the TogetherAI API credentials on engine creation.

        Args:
            connection_args (dict): Connection arguments.

        Raises:
            Exception: If the handler is not configured with valid API credentials.

        Returns:
            None
        """

        connection_args = {k.lower(): v for k, v in connection_args.items()}
        api_key = connection_args.get("togetherai_api_key")
        if api_key is not None:
            api_base = connection_args.get("api_base") or os.environ.get(
                "TOGETHERAI_API_BASE", togetherai_handler_config.BASE_URL
            )
            client = self._get_client(api_key=api_key, base_url=api_base)
            TogetherAIHandler._check_client_connection(client)

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        """
        Validate the TogetherAI API credentials on model creation.

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
                "TogetherAI engine require a USING clause! Refer to its documentation for more details"
            )
        else:
            args = args["using"]

        if (
            len(set(args.keys()) & {"question_column", "prompt_template", "prompt"})
            == 0
        ):
            raise Exception(
                "One of `question_column`, `prompt_template` or `prompt` is required for this engine."
            )

        keys_collection = [
            ["prompt_template"],
            ["question_column", "context_column"],
            ["prompt", "user_column", "assistant_column"],
        ]
        for keys in keys_collection:
            if keys[0] in args and any(
                x[0] in args for x in keys_collection if x != keys
            ):
                raise Exception(
                    textwrap.dedent(
                        """\
                    Please provide one of
                        1) a `prompt_template`
                        2) a `question_column` and an optional `context_column`
                        3) a `prompt`, `user_column` and `assistant_column`
                """
                    )
                )

        engine_storage = kwargs["handler_storage"]
        connection_args = engine_storage.get_connection_args()
        api_key = get_api_key("togetherai", args, engine_storage=engine_storage)
        api_base = connection_args.get("api_base") or os.environ.get(
            "TOGETHERAI_API_BASE", togetherai_handler_config.BASE_URL
        )
        client = TogetherAIHandler._get_client(api_key=api_key, base_url=api_base)
        TogetherAIHandler._check_client_connection(client)

    def create(self, target, args: Dict = None, **kwargs: Any) -> None:
        """
        Create a model for TogetherAI engine.

        Args:
            target (str): Target column, not required for LLMs.
            args (dict): Handler arguments.
            kwargs (dict): Handler keyword arguments.

        Raises:
            Exception: If the handler is not configured with valid API credentials.

        Returns:
            None
        """
        args = args["using"]
        args["target"] = target
        try:
            api_key = get_api_key(self.api_key_name, args, self.engine_storage)
            connection_args = self.engine_storage.get_connection_args()
            api_base = (
                args.get("api_base")
                or connection_args.get("api_base")
                or os.environ.get("TOGETHERAI_API_BASE")
                or self.api_base
            )
            available_models = self._get_supported_models(api_key, api_base)

            if args.get("mode") is None:
                args["mode"] = self.default_mode
            elif args["mode"] not in self.supported_modes:
                raise Exception(
                    f"Invalid operation mode. Please use one of {self.supported_modes}"
                )

            if not args.get("model_name"):
                if args["mode"] == "embedding":
                    args["model_name"] = self.default_embedding_model
                else:
                    args["model_name"] = self.default_model
            elif args["model_name"] not in available_models:
                raise Exception(
                    f"Invalid model name. Please use one of {available_models}"
                )
        finally:
            self.model_storage.json_set("args", args)

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        """
        Call the TogetherAI engine to predict the next token.

        Args:
            df (pd.DataFrame): Input data.
            args (dict): Handler arguments.

        Returns:
            pd.DataFrame: Predicted data.
        """

        api_key = get_api_key("togetherai", args, engine_storage=self.engine_storage)
        supported_models = self._get_supported_models(api_key, self.api_base)
        self.chat_completion_models = supported_models
        return super().predict(df, args)

    @staticmethod
    def _get_supported_models(api_key, base_url):
        """
        Get the list of supported models from the TogetherAI engine.

        Args:
            api_key (str): TogetherAI API key.
            base_url (str): TogetherAI API base URL.

        Returns:
            list: List of supported models.
        """

        list_model_endpoint = f"{base_url}/models"
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {api_key}",
        }
        response = requests.get(url=list_model_endpoint, headers=headers)

        if response.status_code == 200:
            model_list = response.json()
            chat_completion_models = list(map(lambda model: model["id"], model_list))
            return chat_completion_models
        elif response.status_code == 401:
            raise AuthenticationError(message="Invalid API key")
        else:
            raise Exception(f"Failed to get supported models: {response.text}")

    def finetune(
        self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None
    ) -> None:
        raise NotImplementedError("Fine-tuning is not supported for TogetherAI engine")

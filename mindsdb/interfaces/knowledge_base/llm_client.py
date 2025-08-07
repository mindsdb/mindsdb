import copy
import os
from typing import List

from openai import OpenAI, AzureOpenAI

from mindsdb.integrations.utilities.handler_utils import get_api_key
from mindsdb.utilities.config import config


class LLMClient:
    """
    Class for accession to LLM.
    It chooses openai client or litellm handler depending on the config
    """

    def __init__(self, llm_params: dict = None):
        params = copy.deepcopy(config.get("default_llm", {}))

        if llm_params:
            params.update(llm_params)

        self.params = params

        self.provider = params.get("provider", "openai")

        if "api_key" not in params:
            params["api_key"] = get_api_key(self.provider, params, strict=False)

        if self.provider == "azure_openai":
            azure_api_key = params.get("api_key") or os.getenv("AZURE_OPENAI_API_KEY")
            azure_api_endpoint = params.get("base_url") or os.environ.get("AZURE_OPENAI_ENDPOINT")
            azure_api_version = params.get("api_version") or os.environ.get("AZURE_OPENAI_API_VERSION")
            self._llm_client = AzureOpenAI(
                api_key=azure_api_key, azure_endpoint=azure_api_endpoint, api_version=azure_api_version, max_retries=2
            )
        elif self.provider == "openai":
            openai_api_key = params.get("api_key") or os.getenv("OPENAI_API_KEY")
            kwargs = {"api_key": openai_api_key, "max_retries": 2}
            base_url = params.get("base_url")
            if base_url:
                kwargs["base_url"] = base_url
            self.client = OpenAI(**kwargs)

        else:
            # try to use litellm
            from mindsdb.api.executor.controllers.session_controller import SessionController

            session = SessionController()
            module = session.integration_controller.get_handler_module("litellm")

            if module is None or module.Handler is None:
                raise ValueError(f'Unable to use "{self.provider}" provider. Litellm handler is not installed')

            self.client = module.Handler

    def completion(self, messages: List[dict], json_output: bool = False) -> str:
        """
        Call LLM completion and get response
        """
        params = self.params
        params["json_output"] = json_output
        if self.provider in ("azure_openai", "openai"):
            response = self.client.chat.completions.create(
                model=params["model_name"],
                messages=messages,
            )
            return response.choices[0].message.content
        else:
            kwargs = params.copy()
            model = kwargs.pop("model_name")
            kwargs.pop("provider", None)
            response = self.client.completion(self.provider, model=model, messages=messages, args=kwargs)
            return response.choices[0].message.content

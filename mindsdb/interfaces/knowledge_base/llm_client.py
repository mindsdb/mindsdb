import os
from typing import List

from openai import OpenAI, AzureOpenAI

from mindsdb.integrations.utilities.handler_utils import get_api_key

try:
    from mindsdb.integrations.handlers.openai_handler.helpers import retry_with_exponential_backoff
except ImportError:

    def retry_with_exponential_backoff(func):
        """
        An empty decorator
        """

        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper


def run_in_batches(batch_size):
    """
    decorator to run function into batches if input is greater than batch_size
    """

    def decorator(func):
        def wrapper(self, messages, *args, **kwargs):
            if len(messages) <= batch_size:
                return func(self, messages, *args, **kwargs)

            chunk_num = 0
            results = []
            while chunk_num * batch_size < len(messages):
                chunk = messages[chunk_num * batch_size : (chunk_num + 1) * batch_size]
                results.extend(func(self, chunk, *args, **kwargs))
                chunk_num += 1

            return results

        return wrapper

    return decorator


class LLMClient:
    """
    Class for accession to LLM.
    It chooses openai client or litellm handler depending on the config
    """

    def __init__(self, params: dict = None, session=None):
        self._session = session
        self.params = params

        self.provider = params.get("provider", "openai")

        if "api_key" not in params:
            params["api_key"] = get_api_key(self.provider, params, strict=False)

        self.engine = "openai"

        if self.provider == "azure_openai":
            azure_api_key = params.get("api_key") or os.getenv("AZURE_OPENAI_API_KEY")
            azure_api_endpoint = params.get("base_url") or os.environ.get("AZURE_OPENAI_ENDPOINT")
            azure_api_version = params.get("api_version") or os.environ.get("AZURE_OPENAI_API_VERSION")
            self.client = AzureOpenAI(
                api_key=azure_api_key, azure_endpoint=azure_api_endpoint, api_version=azure_api_version, max_retries=2
            )
        elif self.provider == "openai":
            openai_api_key = params.get("api_key") or os.getenv("OPENAI_API_KEY")
            kwargs = {"api_key": openai_api_key, "max_retries": 2}
            base_url = params.get("base_url")
            if base_url:
                kwargs["base_url"] = base_url
            self.client = OpenAI(**kwargs)
        elif self.provider == "ollama":
            kwargs = params.copy()
            kwargs.pop("model_name")
            kwargs.pop("provider", None)
            if kwargs["api_key"] is None:
                kwargs["api_key"] = "n/a"
            self.client = OpenAI(**kwargs)
        else:
            # try to use litellm
            if self._session is None:
                from mindsdb.api.executor.controllers.session_controller import SessionController

                self._session = SessionController()
            module = self._session.integration_controller.get_handler_module("litellm")

            if module is None or module.Handler is None:
                raise ValueError(f'Unable to use "{self.provider}" provider. Litellm handler is not installed')

            self.client = module.Handler
            self.engine = "litellm"

    @run_in_batches(1000)
    @retry_with_exponential_backoff()
    def embeddings(self, messages: List[str]):
        params = self.params
        if self.engine == "openai":
            response = self.client.embeddings.create(
                model=params["model_name"],
                input=messages,
            )
            return [item.embedding for item in response.data]
        else:
            kwargs = params.copy()
            model = kwargs.pop("model_name")
            kwargs.pop("provider", None)

            return self.client.embeddings(self.provider, model=model, messages=messages, args=kwargs)

    @run_in_batches(100)
    def completion(self, messages: List[dict], json_output: bool = False) -> List[str]:
        """
        Call LLM completion and get response
        """
        params = self.params
        params["json_output"] = json_output
        if self.engine == "openai":
            response = self.client.chat.completions.create(
                model=params["model_name"],
                messages=messages,
            )
            return [item.message.content for item in response.choices]
        else:
            kwargs = params.copy()
            model = kwargs.pop("model_name")
            kwargs.pop("provider", None)
            response = self.client.completion(self.provider, model=model, messages=messages, args=kwargs)
            return [item.message.content for item in response.choices]

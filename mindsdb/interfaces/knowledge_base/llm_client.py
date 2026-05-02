import os
import time
import asyncio
from typing import List

from openai import OpenAI, AzureOpenAI

from mindsdb.integrations.utilities.handler_utils import get_api_key

from mindsdb.interfaces.knowledge_base.providers.bedrock import BedrockClient
from mindsdb.interfaces.knowledge_base.providers.gemini import GeminiClient
from mindsdb.interfaces.knowledge_base.providers.snowflake import SnowflakeClient


def retry_with_exponential_backoff(func):
    def decorator(*args, **kwargs):
        max_retries = 3
        num_retries = 0
        delay = 1
        exponential_base = 2

        while True:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                message = str(e).lower()
                if "connection error" not in message and "timeout" not in message.lower():
                    raise e

                num_retries += 1
                if num_retries > max_retries:
                    raise Exception(f"Maximum number of retries ({max_retries}) exceeded.") from e
                # Increment the delay and wait
                delay *= exponential_base
                time.sleep(delay)

    return decorator


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
    It chooses provider client depending on the config
    """

    def __init__(self, params: dict = None, session=None):
        self._session = session
        params = params.copy()

        # Normalise 'model' -> 'model_name' in case the config uses 'model' as the key.
        model_name = params.pop("model_name", None) or params.pop("model", None)
        if model_name is None:
            raise ValueError("model_name is required. Please set it in config.")
        self.model_name = model_name

        self.provider = params.pop("provider", "openai")
        if self.provider == "google":
            self.provider = "gemini"

        if "api_key" not in params:
            api_key = get_api_key(self.provider, params, strict=False)
            if api_key is not None:
                params["api_key"] = api_key

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
            if params.get("api_key") is None:
                params["api_key"] = "n/a"
            self.client = OpenAI(**params)
        elif self.provider == "bedrock":
            if "aws_region" in params:
                params["aws_region_name"] = params.pop("aws_region")
            self.client = BedrockClient(**params)
        elif self.provider == "gemini":
            self.client = GeminiClient(**params)
        elif self.provider == "snowflake":
            self.client = SnowflakeClient(**params)
        else:
            raise NotImplementedError(f'Provider "{self.provider}" is not supported')

    @run_in_batches(1000)
    @retry_with_exponential_backoff
    def embeddings(self, messages: List[str]):
        if self.provider in ("openai", "azure_openai", "ollama"):
            response = self.client.embeddings.create(
                model=self.model_name,
                input=messages,
            )
            return [item.embedding for item in response.data]
        else:
            return self.client.embeddings(self.model_name, messages)

    @run_in_batches(100)
    def completion(self, messages: List[dict], json_output: bool = False) -> List[str]:
        """
        Call LLM completion and get response
        """

        if self.provider in ("openai", "azure_openai", "ollama"):
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=messages,
            )
            return [item.message.content for item in response.choices]
        else:
            return [self.client.completion(self.model_name, messages)]

    async def abatch(self, messages_list: List[List[dict]], json_output: bool = False) -> List[List[str]]:
        """
        Process multiple message lists asynchronously in parallel

        Args:
            messages_list: List of message lists, where each message list is a List[dict]
            json_output: Whether to request JSON output

        Returns:
            List of results, where each result is a List[str] (same format as completion)
        """
        if not messages_list:
            return []

        # Get the running event loop
        loop = asyncio.get_running_loop()

        async def process_single_messages(messages: List[dict]) -> List[str]:
            """Process a single messages list asynchronously"""
            # Run completion in executor for async compatibility
            result = await loop.run_in_executor(None, self.completion, messages, json_output)
            return result

        # Process all message lists in parallel
        results = await asyncio.gather(*[process_single_messages(messages) for messages in messages_list])

        return results

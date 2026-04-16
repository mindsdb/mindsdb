from typing import Any, List, Optional


class GeminiClient:
    """Wrapper around google-genai SDK"""

    def __init__(self, api_key: str):
        self._api_key = api_key
        self._client = None
        self._types = None

    @property
    def client(self):
        if self._client is None:
            try:
                from google import genai
            except ImportError as exc:
                raise ImportError("google.genai is required. Install it with `pip install google-genai`.") from exc
            self._client = genai.Client(api_key=self._api_key)
        return self._client

    @property
    def types(self):
        if self._types is None:
            try:
                from google.genai import types
            except ImportError as exc:
                raise ImportError("google.genai is required. Install it with `pip install google-genai`.") from exc
            self._types = types
        return self._types

    def embeddings(self, model_name: str, messages: List[str]) -> List[List[float]]:
        """Generate embedding vectors for each text in `messages`."""
        result = self.client.models.embed_content(model=model_name, contents=messages)

        return [item.values for item in result.embeddings]

    def _prepare_messages(self, messages: List[dict]) -> List[Any]:
        """Convert chat messages into google-genai content payloads."""
        contents = []
        for message in messages:
            role = message["role"]
            # system role is not supported
            if role != "user":
                role = "model"

            contents.append(self.types.Content(role=role, parts=[self.types.Part(text=message["content"])]))
        return contents

    def completion(
        self,
        model_name: str,
        messages: List[dict],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        top_p: Optional[float] = None,
    ) -> str:
        """Produce a chat response"""
        config = {}
        if temperature:
            config["temperature"] = temperature
        if max_tokens:
            config["max_output_tokens"] = max_tokens
        if top_p:
            config["top_p"] = top_p

        contents = self._prepare_messages(messages)

        result = self.client.models.generate_content(model=model_name, contents=contents, config=config)

        return result.text

    async def acompletion(
        self,
        model_name: str,
        messages: List[dict],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        top_p: Optional[float] = None,
    ) -> str:
        """Async variant of `completion` using the SDK aio client."""
        config = {}
        if temperature:
            config["temperature"] = temperature
        if max_tokens:
            config["max_output_tokens"] = max_tokens
        if top_p:
            config["top_p"] = top_p

        contents = self._prepare_messages(messages)

        result = await self.client.aio.models.generate_content(model=model_name, contents=contents, config=config)

        return result.text

from typing import List, Optional

import requests
import httpx


def _raise_for_status(response):
    # show response text in error
    if 400 <= response.status_code < 600:
        if hasattr(response, "reason"):
            reason = response.reason
        elif hasattr(response, "reason_phrase"):
            reason = response.reason_phrase
        else:
            reason = "Error"
        raise requests.HTTPError(f"{reason}: {response.text}", response=response)


class SnowflakeClient:
    def __init__(self, account_id: str = None, api_key: str = None):
        if account_id is None:
            raise ValueError("account_id must be provided")
        if api_key is None:
            raise ValueError("api_key must be provided")

        self.account_id = account_id.lower()
        self.api_key = api_key

        self.auth_type = "KEYPAIR_JWT"
        if self.api_key.startswith("pat/"):
            self.api_key = self.api_key[4:]
            self.auth_type = "PROGRAMMATIC_ACCESS_TOKEN"

    def _get_base_url(self):
        return f"https://{self.account_id}.snowflakecomputing.com/api/v2"

    def _get_headers(self):
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": "Bearer " + self.api_key,
            "X-Snowflake-Authorization-Token-Type": self.auth_type,
        }

    def embeddings(self, model_name: str, messages: List[str]):
        url = f"{self._get_base_url()}/cortex/inference:embed"

        payload = {"text": messages, "model": model_name}

        response = requests.post(url, json=payload, headers=self._get_headers())
        _raise_for_status(response)

        embeddings = []
        for item in response.json()["data"]:
            embeddings.append(item["embedding"][0])
        return embeddings

    def completion(
        self,
        model_name: str,
        messages: List[dict],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        top_p: Optional[float] = None,
    ):
        url = f"{self._get_base_url()}/cortex/inference:complete"

        payload = {
            "model": model_name,
            "stream": False,
            "messages": messages,
        }

        if temperature:
            payload["temperature"] = temperature
        if max_tokens:
            payload["max_tokens"] = max_tokens
        if top_p:
            payload["top_p"] = top_p

        response = requests.post(url, json=payload, headers=self._get_headers())
        _raise_for_status(response)
        data = response.json()
        return data["choices"][0]["message"]["content"]

    async def acompletion(
        self,
        model_name: str,
        messages: List[dict],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        top_p: Optional[float] = None,
    ):
        url = f"{self._get_base_url()}/cortex/inference:complete"

        payload = {
            "model": model_name,
            "stream": False,
            "messages": messages,
        }

        if temperature:
            payload["temperature"] = temperature
        if max_tokens:
            payload["max_tokens"] = max_tokens
        if top_p:
            payload["top_p"] = top_p

        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=payload, headers=self._get_headers())
            _raise_for_status(response)
            data = response.json()
            return data["choices"][0]["message"]["content"]

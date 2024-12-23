import requests
from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __description__ as description
from .__about__ import __version__ as version
from .connection_args import connection_args, connection_args_example

class UpstashVectorHandler:
    """
    Handler for Upstash Vector integration with MindsDB.
    """

    def __init__(self, host: str, api_key: str):
        self.host = host.rstrip("/")  # Ensure no trailing slash
        self.api_key = api_key
        self.headers = {"Authorization": f"Bearer {self.api_key}"}

    def _request(self, endpoint: str, method: str = "GET", payload: dict = None):
        url = f"{self.host}/{endpoint}"
        try:
            if method == "GET":
                response = requests.get(url, headers=self.headers, params=payload)
            elif method == "POST":
                response = requests.post(url, headers=self.headers, json=payload)
            else:
                raise ValueError("Unsupported HTTP method")

            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise Exception(f"UpstashVector API request failed: {str(e)}")

    def create_collection(self, collection_name: str, metadata: dict = None):
        payload = {"name": collection_name}
        if metadata:
            payload["metadata"] = metadata
        return self._request("collections", "POST", payload)

    def insert_vector(self, collection_name: str, vector: list, metadata: dict):
        payload = {
            "collection": collection_name,
            "vector": vector,
            "metadata": metadata
        }
        return self._request("vectors", "POST", payload)

    def query_similar_vectors(self, collection_name: str, query_vector: list, top_k: int = 10, filters: dict = None):
        payload = {
            "collection": collection_name,
            "query_vector": query_vector,
            "top_k": top_k,
        }
        if filters:
            payload["filters"] = filters
        return self._request("query", "POST", payload)

    def delete_collection(self, collection_name: str):
        return self._request(f"collections/{collection_name}", "DELETE")

# Metadata for handler registration
title = "Upstash Vector"
name = "upstash_vector"
type = HANDLER_TYPE.DATA
icon_path = "icon.png"

__all__ = [
    "UpstashVectorHandler",
    "version",
    "name",
    "type",
    "title",
    "description",
    "connection_args",
    "connection_args_example",
    "icon_path",
]

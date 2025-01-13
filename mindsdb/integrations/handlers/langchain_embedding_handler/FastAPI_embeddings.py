from typing import Any, List, Union, Dict
from langchain_core.embeddings import Embeddings
import requests


class FastAPIEmbeddings(Embeddings):
    """An embedding extension that interfaces with FAST API. Useful for custom serving solutions."""

    def __init__(
        self,
        api_base: str,
        model: str,
        batch_size: int = 32,
        **kwargs: Any,
    ):
        """Initialize the embeddings class.

        Args:
            api_base: Base URL for the VLLM server
            model: Model name/path to use for embeddings
            batch_size: Batch size for generating embeddings
        """
        super().__init__()
        self.api_base = api_base
        self.model = model
        self.batch_size = batch_size

        # initialize requests here with the api_base

    def _get_embeddings(self, texts: List[str]) -> Union[List[List[float]], List[Dict[int, float]]]:
        """Get embeddings for a batch of text chunks."""

        headers = {"accept": "application/json", "Content-Type": "application/json"}

        data = {
            "input": texts,
            "model": "string"
        }

        response = requests.post(self.api_base, headers=headers, json=data)

        response.raise_for_status()

        embeddings = []
        for response_dict in response.json()["data"]:
            embeddings.append(response_dict["embedding"])

        return embeddings

    def embed_documents(self, texts: List[str]) -> Union[List[List[float]], List[Dict[int, float]]]:
        """Embed a list of documents using vLLM.

        Args:
            texts: List of documents to embed

        Returns:
            List of embeddings, one for each document.
            For sparse embeddings, returns a list of dictionaries mapping indices to values.
            For dense embeddings, returns a list of float lists.
        """

        return self._get_embeddings(texts)

    def embed_query(self, text: str) -> Union[List[float], Dict[int, float]]:
        """Embed a single query text using vLLM.

        Args:
            text: Query text to embed

        Returns:
            Query embedding.
            For sparse embeddings, returns a dictionary mapping indices to values.
            For dense embeddings, returns a list of floats.
        """

        return self._get_embeddings([text])[0]

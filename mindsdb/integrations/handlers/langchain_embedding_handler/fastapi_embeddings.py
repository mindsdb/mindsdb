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
            "model": self.model
        }

        response = requests.post(self.api_base, headers=headers, json=data)

        response.raise_for_status()

        embeddings = []
        for response_dict in response.json()["data"]:
            embedding = response_dict["embedding"]
            if isinstance(embedding, str):
                # Check if it's a sparse vector string in format "{key:value,...}/size"
                if embedding.startswith('{') and '/' in embedding:
                    # Parse sparse vector format
                    vector_part = embedding.split('/')[0][1:-1]  # Remove braces and size
                    embedding_dict = {}
                    for pair in vector_part.split(','):
                        key, value = pair.split(':')
                        embedding_dict[int(key)] = float(value)
                    embedding = embedding_dict
                else:
                    # Try to parse as a list of floats
                    try:
                        import json
                        embedding = json.loads(embedding)
                    except json.JSONDecodeError:
                        raise ValueError(f"Unable to parse embedding string: {embedding}")
            embeddings.append(embedding)

        # Validate the output format
        if embeddings and isinstance(embeddings[0], dict):
            # Ensure all embeddings are Dict[int, float]
            for emb in embeddings:
                if not all(isinstance(k, int) and isinstance(v, float) for k, v in emb.items()):
                    raise ValueError("Sparse embeddings must be Dict[int, float]")
        elif embeddings and isinstance(embeddings[0], list):
            # Ensure all embeddings are List[float]
            for emb in embeddings:
                if not all(isinstance(x, float) for x in emb):
                    raise ValueError("Dense embeddings must be List[float]")
        else:
            raise ValueError("Invalid embedding format")

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

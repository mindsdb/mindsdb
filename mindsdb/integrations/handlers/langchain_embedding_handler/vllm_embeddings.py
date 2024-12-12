from typing import Any, List
from langchain_core.embeddings import Embeddings
from openai import OpenAI


class VLLMEmbeddings(Embeddings):
    """VLLMEmbeddings uses a VLLM server to generate embeddings."""

    def __init__(
        self,
        openai_api_base: str,
        model: str,
        batch_size: int = 32,
        **kwargs: Any,
    ):
        """Initialize the embeddings class.

        Args:
            openai_api_base: Base URL for the VLLM server
            model: Model name/path to use for embeddings
            batch_size: Batch size for generating embeddings
        """
        super().__init__()
        self.model = model
        self.batch_size = batch_size

        # Initialize OpenAI client
        openai_kwargs = kwargs.copy()
        if 'input_columns' in openai_kwargs:
            del openai_kwargs['input_columns']

        self.client = OpenAI(
            api_key="EMPTY",  # vLLM doesn't need an API key
            base_url=openai_api_base,
            **openai_kwargs
        )

    def _chunk_list(self, texts: List[str], chunk_size: int) -> List[List[str]]:
        """Split list into chunks of specified size."""
        return [texts[i:i + chunk_size] for i in range(0, len(texts), chunk_size)]

    def _get_embeddings(self, texts: List[str]) -> List[List[float]]:
        """Get embeddings for a batch of texts."""
        embeddings = []
        for i in range(0, len(texts), self.batch_size):
            batch = texts[i:i + self.batch_size]
            response = self.client.embeddings.create(
                model=self.model,
                input=batch
            )
            embeddings.extend([data.embedding for data in response.data])
        return embeddings

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """Embed a list of documents using vLLM.

        Args:
            texts: List of documents to embed

        Returns:
            List of embeddings, one for each document
        """
        return self._get_embeddings(texts)

    def embed_query(self, text: str) -> List[float]:
        """Embed a single query text using vLLM.

        Args:
            text: Query text to embed

        Returns:
            Query embedding
        """
        return self._get_embeddings([text])[0]

from typing import Any, List
from langchain_core.embeddings import Embeddings
from openai import OpenAI


class VLLMEmbeddings(Embeddings):
    """Custom LangChain embeddings class for vLLM embedding models.

    Example usage:
        ```python
        # Initialize the embeddings model
        embeddings = VLLMEmbeddings(
            model_name="your_model_path",  # e.g. "/models/bert-base-uncased"
            host="localhost",              # vLLM server host
            port="8003",                   # vLLM server port
            batch_size=8                   # Number of texts to process at once
        )
    """

    def __init__(
        self,
        model_name: str,
        host: str = "localhost",
        port: str = "8003",
        batch_size: int = 8,
        **kwargs: Any,
    ) -> None:
        """Initialize the VLLMEmbeddings.

        Args:
            model_name: Name/path of the model to use
            host: Hostname where vLLM server is running
            port: Port number where vLLM server is running
            batch_size: Number of texts to embed at once
            **kwargs: Additional arguments to pass to OpenAI client
        """
        super().__init__()
        self.model_name = model_name
        self.batch_size = batch_size

        # Initialize OpenAI client
        self.client = OpenAI(
            api_key="EMPTY",  # vLLM doesn't need an API key
            base_url=f"http://{host}:{port}/v1",
            **kwargs
        )

    def _chunk_list(self, texts: List[str], chunk_size: int) -> List[List[str]]:
        """Split list into chunks of specified size."""
        return [texts[i:i + chunk_size] for i in range(0, len(texts), chunk_size)]

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """Embed a list of documents using vLLM.

        Args:
            texts: List of documents to embed

        Returns:
            List of embeddings, one for each document
        """
        embeddings = []

        # Process in batches
        for batch in self._chunk_list(texts, self.batch_size):
            response = self.client.embeddings.create(
                model=self.model_name,
                input=batch
            )
            embeddings.extend([data.embedding for data in response.data])

        return embeddings

    def embed_query(self, text: str) -> List[float]:
        """Embed a single query text using vLLM.

        Args:
            text: Query text to embed

        Returns:
            Query embedding
        """
        response = self.client.embeddings.create(
            model=self.model_name,
            input=[text]
        )
        return response.data[0].embedding

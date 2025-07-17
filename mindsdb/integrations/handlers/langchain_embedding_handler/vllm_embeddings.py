from typing import Any, List
from langchain_core.embeddings import Embeddings
from openai import AsyncOpenAI
import asyncio


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
        self.is_nomic = "nomic-embed-text" in model.lower()

        # Initialize OpenAI client
        openai_kwargs = kwargs.copy()
        if "input_columns" in openai_kwargs:
            del openai_kwargs["input_columns"]

        self.client = AsyncOpenAI(
            api_key="EMPTY",  # vLLM doesn't need an API key
            base_url=openai_api_base,
            **openai_kwargs,
        )

    def _format_text(self, text: str, is_query: bool = False) -> str:
        """
        Format text according to nomic-embed requirements if using nomic model.
        e.g. see here for more details: https://huggingface.co/nomic-ai/nomic-embed-text-v1.5#task-instruction-prefixes
        """

        if not self.is_nomic:
            return text
        prefix = "search_query: " if is_query else "search_document: "
        return prefix + text

    def _get_embeddings(self, texts: List[str]) -> List[List[float]]:
        """Get embeddings for a batch of texts."""

        async def await_openai_call(batch):
            return await self.client.embeddings.create(model=self.model, input=batch)

        embeddings = []
        embedding_coroutines = []
        chunk_start_indices = range(0, len(texts), self.batch_size)
        for i in chunk_start_indices:

            batch = texts[i: i + self.batch_size]
            embedding_coroutines.append(await_openai_call(batch))

            # if at max-concurrency, then run with gather
            if len(embedding_coroutines) == 512 or len(embedding_coroutines) == len(
                chunk_start_indices
            ):

                openai_responses = []

                async def gather_coroutines(openai_responses):
                    # define a function to gather and save responses.
                    intermediate = await asyncio.gather(*embedding_coroutines)
                    openai_responses.extend(intermediate)

                # run asynchronously
                asyncio.run(gather_coroutines(openai_responses))

                # extract embeddings from responses
                for response in openai_responses:
                    embeddings.extend([data.embedding for data in response.data])

                # reset the embedding_coroutines list
                embedding_coroutines = []

        return embeddings

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """Embed a list of documents using vLLM.

        Args:
            texts: List of documents to embed

        Returns:
            List of embeddings, one for each document
        """
        formatted_texts = [self._format_text(text) for text in texts]
        return self._get_embeddings(formatted_texts)

    def embed_query(self, text: str) -> List[float]:
        """Embed a single query text using vLLM.

        Args:
            text: Query text to embed

        Returns:
            Query embedding
        """
        formatted_text = self._format_text(text, is_query=True)
        return self._get_embeddings([formatted_text])[0]

"""Custom embedding model utilities to replace langchain construct_model_from_args"""

import copy
from typing import Dict, Any, List

from mindsdb.interfaces.knowledge_base.llm_client import LLMClient
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class CustomEmbeddingModel:
    """
    Custom embedding model wrapper that uses LLMClient for embeddings.
    This replaces langchain embedding models for use in knowledge_base.
    """

    def __init__(self, args: Dict[str, Any], session=None):
        """
        Initialize the embedding model

        Args:
            args: Dictionary with model parameters (model_name, provider, etc.)
            session: Optional session for LLMClient
        """
        # Prepare params for LLMClient
        # Handle model_name -> model mapping if needed
        params = {
            "model_name": args.get("model", args.get("model_name")),
            "provider": args.get("provider", "openai"),
            **{k: v for k, v in args.items() if k not in ["model", "model_name", "provider", "class", "target"]},
        }

        self.llm_client = LLMClient(params=params, session=session)
        self.model_name = params["model_name"]

    def embed_query(self, text: str) -> List[float]:
        """
        Embed a single query string

        Args:
            text: Text to embed

        Returns:
            List of floats representing the embedding vector
        """
        embeddings = self.llm_client.embeddings([text])
        return embeddings[0] if embeddings else []

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """
        Embed a list of documents

        Args:
            texts: List of text strings to embed

        Returns:
            List of embedding vectors (each is a list of floats)
        """
        return self.llm_client.embeddings(texts)


def construct_embedding_model_from_args(args: Dict[str, Any], session=None):
    """
    Construct an embedding model from arguments (replacement for langchain's construct_model_from_args)

    Args:
        args: Dictionary with embedding model parameters
            - class: Embedding class name (for compatibility, but not used)
            - model or model_name: Model name to use
            - provider: Provider name (openai, etc.)
            - Other provider-specific parameters
        session: Optional session for LLMClient

    Returns:
        CustomEmbeddingModel instance
    """
    # Work on a copy to avoid mutating the original
    args_copy = copy.deepcopy(args)

    # Extract class name for logging (but we don't use it)
    class_name = args_copy.pop("class", "OpenAIEmbeddings")
    target = args_copy.pop("target", None)

    logger.debug(f"Constructing embedding model with class: {class_name}, args: {args_copy}")

    # Create the custom embedding model
    model = CustomEmbeddingModel(args_copy, session=session)

    # Restore args for compatibility (in case caller expects them)
    if target is not None:
        args["target"] = target
    args["class"] = class_name

    return model

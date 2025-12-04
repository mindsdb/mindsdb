"""Base VectorStore interface to replace langchain VectorStore"""

from typing import Any, List, Optional, Tuple
from abc import ABC, abstractmethod


class VectorStore(ABC):
    """Base class for vector stores to replace langchain VectorStore"""
    
    @property
    @abstractmethod
    def embeddings(self) -> Optional[Any]:
        """Return embeddings model if available"""
        pass
    
    @abstractmethod
    def similarity_search(
        self,
        query: str,
        k: int = 4,
        **kwargs: Any,
    ) -> List[Any]:
        """Return most similar documents to query"""
        pass
    
    def similarity_search_with_score(
        self,
        query: str,
        k: int = 4,
        **kwargs: Any,
    ) -> List[Tuple[Any, float]]:
        """Return most similar documents with scores"""
        # Default implementation using similarity_search
        docs = self.similarity_search(query, k=k, **kwargs)
        # Return with dummy scores if not overridden
        return [(doc, 0.0) for doc in docs]
    
    def as_retriever(self, **kwargs: Any) -> Any:
        """Return a retriever interface"""
        # Create a simple retriever wrapper
        class SimpleRetriever:
            def __init__(self, vector_store):
                self.vector_store = vector_store
            
            def get_relevant_documents(self, query: str) -> List[Any]:
                return self.vector_store.similarity_search(query, **kwargs)
            
            def invoke(self, query: str) -> List[Any]:
                return self.get_relevant_documents(query)
        
        return SimpleRetriever(self)
    
    def add_texts(self, *args: Any, **kwargs: Any) -> List[str]:
        """Add texts to the vector store"""
        raise NotImplementedError("add_texts not implemented")
    
    @classmethod
    def from_texts(cls, *args: Any, **kwargs: Any):
        """Create vector store from texts"""
        raise NotImplementedError("from_texts not implemented")


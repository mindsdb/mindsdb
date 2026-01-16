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
    
    def add_documents(self, documents: List[Any], **kwargs: Any) -> List[str]:
        """
        Add documents to the vector store.
        Extracts page_content and metadata from documents and calls add_texts.
        
        Args:
            documents: List of document-like objects with page_content and metadata attributes
            **kwargs: Additional arguments to pass to add_texts
            
        Returns:
            List of document IDs (if supported by implementation)
        """
        texts = []
        metadatas = []
        for doc in documents:
            # Use duck typing to access page_content and metadata
            page_content = getattr(doc, 'page_content', str(doc))
            metadata = getattr(doc, 'metadata', {})
            texts.append(page_content)
            metadatas.append(metadata)
        
        # Call add_texts with texts and metadatas
        return self.add_texts(texts, metadatas=metadatas, **kwargs)
    
    @classmethod
    def from_texts(cls, *args: Any, **kwargs: Any):
        """Create vector store from texts"""
        raise NotImplementedError("from_texts not implemented")
    
    @classmethod
    def from_documents(cls, documents: List[Any], embedding: Any, **kwargs: Any):
        """
        Create vector store from documents.
        Extracts texts and metadata from documents and calls from_texts.
        
        Args:
            documents: List of document-like objects with page_content and metadata attributes
            embedding: Embedding model/function
            **kwargs: Additional arguments to pass to from_texts
            
        Returns:
            VectorStore instance
        """
        texts = []
        metadatas = []
        for doc in documents:
            # Use duck typing to access page_content and metadata
            page_content = getattr(doc, 'page_content', str(doc))
            metadata = getattr(doc, 'metadata', {})
            texts.append(page_content)
            metadatas.append(metadata)
        
        # Call from_texts with texts, metadatas, and embedding
        return cls.from_texts(texts, embedding=embedding, metadatas=metadatas, **kwargs)


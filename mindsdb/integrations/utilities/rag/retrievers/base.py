from abc import ABC, abstractmethod
from typing import Protocol, List, Any


class RunnableRetriever(Protocol):
    """Protocol for retriever runnable objects that can be invoked to retrieve documents"""
    
    def invoke(self, query: str) -> List[Any]:
        """Sync invocation - retrieve documents for a query"""
        ...
    
    async def ainvoke(self, query: str) -> List[Any]:
        """Async invocation - retrieve documents for a query"""
        ...
    
    def get_relevant_documents(self, query: str) -> List[Any]:
        """Get relevant documents (sync) - alternative interface"""
        ...


class BaseRetriever(ABC):
    """Represents a base retriever for a RAG pipeline"""

    @abstractmethod
    def as_runnable(self) -> RunnableRetriever:
        """
        Return a runnable retriever object that can be invoked.
        
        Returns:
            RunnableRetriever: An object that implements invoke(), ainvoke(), or get_relevant_documents()
        """
        pass

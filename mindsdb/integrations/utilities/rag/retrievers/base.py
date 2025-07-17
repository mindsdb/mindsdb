from abc import ABC, abstractmethod
from langchain_core.runnables import RunnableSerializable


class BaseRetriever(ABC):
    """Represents a base retriever for a RAG pipeline"""

    @abstractmethod
    def as_runnable(self) -> RunnableSerializable:
        pass

import pathlib
from typing import Iterator, List, Any

_FILE_LOADER_IMPORT_ERROR = None


class _BaseLoaderFallback:
    """Minimal stand-in so FileLoader can be defined without LangChain installed."""

    def __init__(self, *args, **kwargs):
        pass


try:  # pragma: no cover - optional dependency
    from langchain_core.document_loaders import BaseLoader as _LangchainBaseLoader
except ModuleNotFoundError as exc:  # LangChain not installed
    BaseLoader = _BaseLoaderFallback
    _FILE_LOADER_IMPORT_ERROR = exc
else:
    BaseLoader = _LangchainBaseLoader

try:  # pragma: no cover - optional dependency
    from langchain_core.documents.base import Document
except ModuleNotFoundError as exc:
    Document = Any
    _FILE_LOADER_IMPORT_ERROR = _FILE_LOADER_IMPORT_ERROR or exc

try:  # pragma: no cover - optional dependency
    from langchain_community.document_loaders.csv_loader import CSVLoader
    from langchain_community.document_loaders import (
        PyMuPDFLoader,
        TextLoader,
        UnstructuredHTMLLoader,
        UnstructuredMarkdownLoader,
    )
except ModuleNotFoundError as exc:
    CSVLoader = PyMuPDFLoader = TextLoader = UnstructuredHTMLLoader = UnstructuredMarkdownLoader = None
    _FILE_LOADER_IMPORT_ERROR = _FILE_LOADER_IMPORT_ERROR or exc


def _require_file_loader_dependency():
    if _FILE_LOADER_IMPORT_ERROR is not None:
        raise ImportError(
            "File loading requires the optional knowledge base dependencies. Install them via `pip install mindsdb[kb]`."
        ) from _FILE_LOADER_IMPORT_ERROR


class FileLoader(BaseLoader):
    """Loads files of various types into vector database document representation"""

    def __init__(self, path: str):
        _require_file_loader_dependency()
        self.path = path
        super().__init__()

    def _get_loader_from_extension(self, extension: str, path: str) -> BaseLoader:
        if extension == ".pdf":
            return PyMuPDFLoader(path)
        if extension == ".csv":
            return CSVLoader(path)
        if extension == ".html":
            return UnstructuredHTMLLoader(path)
        if extension == ".md":
            return UnstructuredMarkdownLoader(path)
        return TextLoader(path, encoding="utf-8")

    def _lazy_load_documents_from_file(self, path: str) -> Iterator[Document]:
        file_extension = pathlib.Path(path).suffix
        loader = self._get_loader_from_extension(file_extension, path)

        for doc in loader.lazy_load():
            doc.metadata["extension"] = file_extension
            yield doc

    def load(self) -> List[Document]:
        """Loads a file and converts the contents into a vector database Document representation"""
        return list(self.lazy_load())

    def lazy_load(self) -> Iterator[Document]:
        """Loads a file and converts the contents into a vector database Document representation"""
        for doc in self._lazy_load_documents_from_file(self.path):
            yield doc

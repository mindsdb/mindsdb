import pathlib
from typing import Iterator, List
from langchain_community.document_loaders.csv_loader import CSVLoader
from langchain_community.document_loaders import PyMuPDFLoader
from langchain_community.document_loaders import TextLoader
from langchain_community.document_loaders import UnstructuredHTMLLoader
from langchain_community.document_loaders import UnstructuredMarkdownLoader
from langchain_core.documents.base import Document
from langchain_core.document_loaders import BaseLoader


class FileLoader(BaseLoader):
    '''Loads files of various types into vector database document representation'''
    def __init__(self, path: str):
        self.path = path
        super().__init__()

    def _get_loader_from_extension(self, extension: str, path: str) -> BaseLoader:
        if extension == '.pdf':
            return PyMuPDFLoader(path)
        if extension == '.csv':
            return CSVLoader(path)
        if extension == '.html':
            return UnstructuredHTMLLoader(path)
        if extension == '.md':
            return UnstructuredMarkdownLoader(path)
        return TextLoader(path, encoding='utf-8')

    def _lazy_load_documents_from_file(self, path: str) -> Iterator[Document]:
        file_extension = pathlib.Path(path).suffix
        loader = self._get_loader_from_extension(file_extension, path)

        for doc in loader.lazy_load():
            doc.metadata['extension'] = file_extension
            yield doc

    def load(self) -> List[Document]:
        '''Loads a file and converts the contents into a vector database Document representation'''
        return list(self.lazy_load())

    def lazy_load(self) -> Iterator[Document]:
        '''Loads a file and converts the contents into a vector database Document representation'''
        for doc in self._lazy_load_documents_from_file(self.path):
            yield doc

import pathlib
from typing import Iterator, List

from mindsdb.interfaces.knowledge_base.preprocessing.document_types import SimpleDocument
from mindsdb.integrations.utilities.rag.loaders.document_loaders import (
    BaseDocumentLoader,
    CSVDocumentLoader,
    PDFDocumentLoader,
    TextDocumentLoader,
    HTMLDocumentLoader,
    MarkdownDocumentLoader,
)


class FileLoader:
    '''Loads files of various types into vector database document representation'''
    def __init__(self, path: str):
        self.path = path

    def _get_loader_from_extension(self, extension: str, path: str) -> BaseDocumentLoader:
        """Get appropriate loader based on file extension"""
        if extension == '.pdf':
            return PDFDocumentLoader(path)
        if extension == '.csv':
            return CSVDocumentLoader(path)
        if extension == '.html':
            return HTMLDocumentLoader(path)
        if extension == '.md':
            return MarkdownDocumentLoader(path)
        return TextDocumentLoader(path)

    def _lazy_load_documents_from_file(self, path: str) -> Iterator[SimpleDocument]:
        """Load documents from file based on extension"""
        file_extension = pathlib.Path(path).suffix
        loader = self._get_loader_from_extension(file_extension, path)

        for doc in loader.lazy_load():
            # Ensure extension is in metadata
            if 'extension' not in doc.metadata:
                doc.metadata['extension'] = file_extension
            yield doc

    def load(self) -> List[SimpleDocument]:
        '''Loads a file and converts the contents into a vector database Document representation'''
        return list(self.lazy_load())

    def lazy_load(self) -> Iterator[SimpleDocument]:
        '''Loads a file and converts the contents into a vector database Document representation'''
        for doc in self._lazy_load_documents_from_file(self.path):
            yield doc

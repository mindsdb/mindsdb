"""Custom document loaders to replace langchain document loaders"""

from typing import Iterator

import pandas as pd

from mindsdb.interfaces.knowledge_base.preprocessing.document_types import SimpleDocument
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class BaseDocumentLoader:
    """Base class for document loaders"""
    
    def __init__(self, path: str):
        self.path = path
    
    def lazy_load(self) -> Iterator[SimpleDocument]:
        """Load documents lazily"""
        raise NotImplementedError("Subclasses must implement lazy_load")


class CSVDocumentLoader(BaseDocumentLoader):
    """Load CSV files and convert rows to documents"""
    
    def lazy_load(self) -> Iterator[SimpleDocument]:
        """Load CSV file and yield each row as a document"""
        try:
            df = pd.read_csv(self.path)
            
            for idx, row in df.iterrows():
                # Convert row to text representation
                row_text = ", ".join([f"{col}: {val}" for col, val in row.items() if pd.notna(val)])
                
                metadata = {
                    "source": str(self.path),
                    "row_index": int(idx),
                    "total_rows": len(df),
                }
                
                yield SimpleDocument(page_content=row_text, metadata=metadata)
        except Exception:
            logger.exception(f"Error loading CSV file {self.path}:")
            raise


class PDFDocumentLoader(BaseDocumentLoader):
    """Load PDF files using pymupdf (fitz)"""
    
    def lazy_load(self) -> Iterator[SimpleDocument]:
        """Load PDF file and extract text from all pages"""
        try:
            import fitz  # pymupdf
            
            with fitz.open(self.path) as pdf:
                all_text = []
                for page_num, page in enumerate(pdf):
                    page_text = page.get_text()
                    all_text.append(page_text)
                    
                    # Yield each page as a separate document
                    metadata = {
                        "source": str(self.path),
                        "page": page_num + 1,
                        "total_pages": len(pdf),
                    }
                    yield SimpleDocument(page_content=page_text, metadata=metadata)
        except ImportError:
            raise ImportError("pymupdf (fitz) is required for PDF loading. Install it with: pip install pymupdf")
        except Exception:
            logger.exception(f"Error loading PDF file {self.path}:")
            raise


class TextDocumentLoader(BaseDocumentLoader):
    """Load plain text files with encoding detection"""
    
    def lazy_load(self) -> Iterator[SimpleDocument]:
        """Load text file with proper encoding detection"""
        try:
            from charset_normalizer import from_bytes
            
            # Read file as bytes first
            with open(self.path, 'rb') as f:
                byte_str = f.read()
            
            # Detect encoding
            encoding_meta = from_bytes(
                byte_str[:32 * 1024],  # Sample first 32KB
                steps=32,
                chunk_size=1024,
                explain=False,
            )
            
            best_meta = encoding_meta.best()
            if best_meta is not None:
                encoding = best_meta.encoding
            else:
                encoding = 'utf-8'
            
            # Decode with detected encoding
            try:
                text = byte_str.decode(encoding)
            except UnicodeDecodeError:
                # Fallback to utf-8 with error handling
                text = byte_str.decode('utf-8', errors='replace')
            
            metadata = {
                "source": str(self.path),
                "encoding": encoding,
            }
            
            yield SimpleDocument(page_content=text, metadata=metadata)
        except ImportError:
            # Fallback to utf-8 if charset_normalizer not available
            try:
                with open(self.path, 'r', encoding='utf-8') as f:
                    text = f.read()
                metadata = {
                    "source": str(self.path),
                    "encoding": "utf-8",
                }
                yield SimpleDocument(page_content=text, metadata=metadata)
            except UnicodeDecodeError:
                # Try with error replacement
                with open(self.path, 'r', encoding='utf-8', errors='replace') as f:
                    text = f.read()
                metadata = {
                    "source": str(self.path),
                    "encoding": "utf-8 (with replacement)",
                }
                yield SimpleDocument(page_content=text, metadata=metadata)
        except Exception:
            logger.exception(f"Error loading text file {self.path}:")
            raise


class HTMLDocumentLoader(BaseDocumentLoader):
    """Load HTML files and extract text content"""
    
    def lazy_load(self) -> Iterator[SimpleDocument]:
        """Load HTML file and extract text content"""
        try:
            from bs4 import BeautifulSoup
            
            with open(self.path, 'r', encoding='utf-8', errors='replace') as f:
                html_content = f.read()
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            # Get text
            text = soup.get_text(separator='\n', strip=True)
            
            metadata = {
                "source": str(self.path),
                "title": soup.title.string if soup.title else None,
            }
            
            yield SimpleDocument(page_content=text, metadata=metadata)
        except ImportError:
            # Fallback to html.parser from standard library
            try:
                from html.parser import HTMLParser
                
                class TextExtractor(HTMLParser):
                    def __init__(self):
                        super().__init__()
                        self.text = []
                        self.skip = False
                    
                    def handle_starttag(self, tag, attrs):
                        if tag in ['script', 'style']:
                            self.skip = True
                    
                    def handle_endtag(self, tag):
                        if tag in ['script', 'style']:
                            self.skip = False
                    
                    def handle_data(self, data):
                        if not self.skip:
                            self.text.append(data)
                
                with open(self.path, 'r', encoding='utf-8', errors='replace') as f:
                    html_content = f.read()
                
                parser = TextExtractor()
                parser.feed(html_content)
                text = '\n'.join(parser.text)
                
                metadata = {
                    "source": str(self.path),
                }
                
                yield SimpleDocument(page_content=text, metadata=metadata)
            except Exception:
                logger.exception(f"Error loading HTML file {self.path}:")
                raise
        except Exception:
            logger.exception(f"Error loading HTML file {self.path}:")
            raise


class MarkdownDocumentLoader(BaseDocumentLoader):
    """Load Markdown files as text"""
    
    def lazy_load(self) -> Iterator[SimpleDocument]:
        """Load markdown file as plain text"""
        try:
            from charset_normalizer import from_bytes
            
            # Read file as bytes first
            with open(self.path, 'rb') as f:
                byte_str = f.read()
            
            # Detect encoding
            encoding_meta = from_bytes(
                byte_str[:32 * 1024],  # Sample first 32KB
                steps=32,
                chunk_size=1024,
                explain=False,
            )
            
            best_meta = encoding_meta.best()
            if best_meta is not None:
                encoding = best_meta.encoding
            else:
                encoding = 'utf-8'
            
            # Decode with detected encoding
            try:
                text = byte_str.decode(encoding)
            except UnicodeDecodeError:
                # Fallback to utf-8 with error handling
                text = byte_str.decode('utf-8', errors='replace')
            
            metadata = {
                "source": str(self.path),
                "encoding": encoding,
            }
            
            yield SimpleDocument(page_content=text, metadata=metadata)
        except ImportError:
            # Fallback to utf-8 if charset_normalizer not available
            try:
                with open(self.path, 'r', encoding='utf-8') as f:
                    text = f.read()
                metadata = {
                    "source": str(self.path),
                    "encoding": "utf-8",
                }
                yield SimpleDocument(page_content=text, metadata=metadata)
            except UnicodeDecodeError:
                # Try with error replacement
                with open(self.path, 'r', encoding='utf-8', errors='replace') as f:
                    text = f.read()
                metadata = {
                    "source": str(self.path),
                    "encoding": "utf-8 (with replacement)",
                }
                yield SimpleDocument(page_content=text, metadata=metadata)
        except Exception:
            logger.exception(f"Error loading markdown file {self.path}:")
            raise


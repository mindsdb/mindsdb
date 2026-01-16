"""Custom text splitter implementations to replace langchain splitters"""

import re
from typing import List, Callable, Optional, Tuple, Any
from html.parser import HTMLParser

from mindsdb.interfaces.knowledge_base.preprocessing.document_types import SimpleDocument
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class RecursiveCharacterTextSplitter:
    """
    Custom implementation of RecursiveCharacterTextSplitter to replace langchain's version.
    Splits text recursively by trying different separators in order.
    """

    def __init__(
        self,
        chunk_size: int = 1000,
        chunk_overlap: int = 200,
        length_function: Callable[[str], int] = len,
        separators: Optional[List[str]] = None,
    ):
        """
        Initialize RecursiveCharacterTextSplitter

        Args:
            chunk_size: Maximum size of chunks (measured by length_function)
            chunk_overlap: Overlap between chunks
            length_function: Function to measure text length (default: len)
            separators: List of separators to try, in order of priority
        """
        if separators is None:
            separators = ["\n\n", "\n", ". ", " ", ""]
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.length_function = length_function
        self.separators = separators

    def split_text(self, text: str) -> List[str]:
        """
        Split text into chunks

        Args:
            text: Text to split

        Returns:
            List of text chunks
        """
        if self.length_function(text) <= self.chunk_size:
            return [text]

        chunks = []
        start_idx = 0

        while start_idx < len(text):
            # Get the next chunk
            end_idx = min(start_idx + self.chunk_size, len(text))
            chunk = text[start_idx:end_idx]

            # If we're at the end, add the remaining text
            if end_idx >= len(text):
                chunks.append(chunk)
                break

            # Try to find a good split point using separators
            split_idx = None
            for separator in self.separators:
                if separator == "":
                    # Last resort: split at chunk_size
                    split_idx = end_idx
                    break

                # Look for separator near the end of the chunk
                search_start = max(0, end_idx - self.chunk_size // 2)
                pos = text.rfind(separator, search_start, end_idx)

                if pos != -1:
                    split_idx = pos + len(separator)
                    break

            if split_idx is None:
                # Fallback: split at chunk_size
                split_idx = end_idx

            # Extract chunk
            chunk = text[start_idx:split_idx]
            chunks.append(chunk)

            # Move start_idx forward, accounting for overlap
            start_idx = max(start_idx + 1, split_idx - self.chunk_overlap)

        return chunks

    def split_documents(self, documents: List[SimpleDocument]) -> List[SimpleDocument]:
        """
        Split documents into chunks

        Args:
            documents: List of SimpleDocument objects to split

        Returns:
            List of SimpleDocument chunks with preserved metadata
        """
        split_docs = []
        for doc in documents:
            chunks = self.split_text(doc.page_content)
            for chunk in chunks:
                # Preserve metadata from original document
                split_docs.append(
                    SimpleDocument(
                        page_content=chunk,
                        metadata=doc.metadata.copy() if doc.metadata else {}
                    )
                )
        return split_docs
    
    def create_documents(self, texts: List[str], metadatas: Optional[List[dict]] = None) -> List[SimpleDocument]:
        """
        Create documents from a list of texts (compatible with langchain interface)

        Args:
            texts: List of text strings
            metadatas: Optional list of metadata dicts (one per text)

        Returns:
            List of SimpleDocument objects
        """
        if metadatas is None:
            metadatas = [{}] * len(texts)
        
        docs = []
        for text, metadata in zip(texts, metadatas):
            docs.append(SimpleDocument(page_content=text, metadata=metadata))
        
        return docs
    
    @classmethod
    def from_language(
        cls,
        language: Any,
        chunk_size: int = 1000,
        chunk_overlap: int = 200,
        **kwargs
    ) -> 'RecursiveCharacterTextSplitter':
        """
        Create a RecursiveCharacterTextSplitter with language-specific separators
        
        Args:
            language: Language enum or string (e.g., Language.PYTHON or "python")
            chunk_size: Maximum size of chunks
            chunk_overlap: Overlap between chunks
            **kwargs: Additional arguments
            
        Returns:
            RecursiveCharacterTextSplitter instance with language-specific separators
        """
        # Get language name as string (handle both enum and string)
        if hasattr(language, 'value'):
            lang_name = language.value.lower()
        elif hasattr(language, 'name'):
            lang_name = language.name.lower()
        else:
            lang_name = str(language).lower()
        
        # Language-specific separators (based on langchain's implementation)
        language_separators = {
            'python': ["\n\n", "\n", "def ", "class ", "    ", " ", ""],
            'javascript': ["\n\n", "\n", "function ", "class ", "    ", " ", ""],
            'typescript': ["\n\n", "\n", "function ", "class ", "    ", " ", ""],
            'java': ["\n\n", "\n", "public ", "private ", "class ", "    ", " ", ""],
            'cpp': ["\n\n", "\n", "namespace ", "class ", "    ", " ", ""],
            'c': ["\n\n", "\n", "    ", " ", ""],
            'go': ["\n\n", "\n", "func ", "    ", " ", ""],
            'rust': ["\n\n", "\n", "fn ", "    ", " ", ""],
            'ruby': ["\n\n", "\n", "def ", "class ", "    ", " ", ""],
            'php': ["\n\n", "\n", "function ", "class ", "    ", " ", ""],
            'swift': ["\n\n", "\n", "func ", "class ", "    ", " ", ""],
            'kotlin': ["\n\n", "\n", "fun ", "class ", "    ", " ", ""],
            'scala': ["\n\n", "\n", "def ", "class ", "    ", " ", ""],
        }
        
        # Get separators for language, or use default
        separators = language_separators.get(lang_name, ["\n\n", "\n", ". ", " ", ""])
        
        return cls(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            separators=separators,
            **kwargs
        )


class MarkdownHeaderTextSplitter:
    """
    Custom implementation of MarkdownHeaderTextSplitter to replace langchain's version.
    Splits markdown text by headers.
    """

    def __init__(self, headers_to_split_on: List[Tuple[str, str]]):
        """
        Initialize MarkdownHeaderTextSplitter

        Args:
            headers_to_split_on: List of tuples (header_marker, header_name)
                e.g., [("#", "Header 1"), ("##", "Header 2")]
        """
        self.headers_to_split_on = headers_to_split_on
        # Sort by header level (more # = higher level) in reverse order
        # to match from most specific to least specific
        self.headers_to_split_on = sorted(
            headers_to_split_on,
            key=lambda x: len(x[0]),
            reverse=True
        )

    def split_text(self, text: str) -> List[SimpleDocument]:
        """
        Split markdown text by headers

        Args:
            text: Markdown text to split

        Returns:
            List of SimpleDocument objects, each containing a section
        """
        lines = text.split('\n')
        documents = []
        current_chunk_lines = []
        current_metadata = {}
        current_header_stack = []  # Track header hierarchy

        i = 0
        while i < len(lines):
            line = lines[i]
            matched_header = None

            # Check if this line matches any header pattern
            for header_marker, header_name in self.headers_to_split_on:
                # Match header pattern: optional whitespace, header marker, space, header text
                pattern = rf'^\s*{re.escape(header_marker)}\s+(.+)$'
                match = re.match(pattern, line)
                if match:
                    matched_header = (header_marker, header_name, match.group(1))
                    break

            if matched_header:
                # Save previous chunk if it has content
                if current_chunk_lines:
                    chunk_text = '\n'.join(current_chunk_lines).strip()
                    if chunk_text:
                        documents.append(
                            SimpleDocument(
                                page_content=chunk_text,
                                metadata=current_metadata.copy()
                            )
                        )

                # Start new chunk
                header_marker, header_name, header_text = matched_header
                current_chunk_lines = [line]  # Include header in chunk
                
                # Update header stack - remove headers at same or lower level
                header_level = len(header_marker)
                current_header_stack = [
                    h for h in current_header_stack
                    if len(h[0]) < header_level
                ]
                current_header_stack.append((header_marker, header_name, header_text))

                # Build metadata with header hierarchy
                current_metadata = {}
                for idx, (marker, name, text) in enumerate(current_header_stack):
                    current_metadata[f"{name.lower().replace(' ', '_')}_{idx}"] = text
                    # Also add the most recent header
                    if idx == len(current_header_stack) - 1:
                        current_metadata["header"] = text
                        current_metadata["header_level"] = len(marker)
            else:
                # Add line to current chunk
                current_chunk_lines.append(line)

            i += 1

        # Add final chunk
        if current_chunk_lines:
            chunk_text = '\n'.join(current_chunk_lines).strip()
            if chunk_text:
                documents.append(
                    SimpleDocument(
                        page_content=chunk_text,
                        metadata=current_metadata.copy()
                    )
                )

        # If no headers found, return entire text as one document
        if not documents:
            documents.append(
                SimpleDocument(
                    page_content=text,
                    metadata={}
                )
            )

        return documents


class HTMLHeaderParser(HTMLParser):
    """HTML parser to extract header tags and their positions"""

    def __init__(self, headers_to_split_on: List[Tuple[str, str]]):
        super().__init__()
        self.headers_to_split_on = {tag.lower() for tag, _ in headers_to_split_on}
        self.header_positions = []  # List of (position, tag, text)
        self.current_position = 0
        self.current_tag = None
        self.current_text = ""

    def handle_starttag(self, tag, attrs):
        if tag.lower() in self.headers_to_split_on:
            self.current_tag = tag.lower()
            self.current_text = ""

    def handle_endtag(self, tag):
        if tag.lower() == self.current_tag:
            text = self.current_text.strip()
            if text:
                self.header_positions.append((self.current_position, tag.lower(), text))
            self.current_tag = None
            self.current_text = ""

    def handle_data(self, data):
        if self.current_tag:
            self.current_text += data
        self.current_position += len(data)


class HTMLHeaderTextSplitter:
    """
    Custom implementation of HTMLHeaderTextSplitter to replace langchain's version.
    Splits HTML text by header tags.
    """

    def __init__(self, headers_to_split_on: List[Tuple[str, str]]):
        """
        Initialize HTMLHeaderTextSplitter

        Args:
            headers_to_split_on: List of tuples (tag_name, header_name)
                e.g., [("h1", "Header 1"), ("h2", "Header 2")]
        """
        self.headers_to_split_on = headers_to_split_on
        # Create mapping from tag to header name
        self.tag_to_name = {tag.lower(): name for tag, name in headers_to_split_on}

    def split_text(self, text: str) -> List[SimpleDocument]:
        """
        Split HTML text by header tags

        Args:
            text: HTML text to split

        Returns:
            List of SimpleDocument objects, each containing a section
        """
        # Parse HTML to find header positions
        parser = HTMLHeaderParser(self.headers_to_split_on)
        parser.feed(text)
        header_positions = parser.header_positions

        if not header_positions:
            # No headers found, return entire text as one document
            return [SimpleDocument(page_content=text, metadata={})]

        documents = []
        current_pos = 0
        current_metadata = {}
        current_header_stack = []  # Track header hierarchy

        for header_pos, tag, header_text in header_positions:
            # Extract content before this header
            if header_pos > current_pos:
                chunk_text = text[current_pos:header_pos].strip()
                if chunk_text:
                    documents.append(
                        SimpleDocument(
                            page_content=chunk_text,
                            metadata=current_metadata.copy()
                        )
                    )

            # Update header stack - remove headers at same or lower level
            header_level = int(tag[1]) if tag[1].isdigit() else 6  # h1=1, h2=2, etc.
            current_header_stack = [
                h for h in current_header_stack
                if int(h[0][1]) < header_level
            ]
            current_header_stack.append((tag, self.tag_to_name.get(tag, tag), header_text))

            # Build metadata with header hierarchy
            current_metadata = {}
            for idx, (tag_name, header_name, text) in enumerate(current_header_stack):
                current_metadata[f"{header_name.lower().replace(' ', '_')}_{idx}"] = text
                # Also add the most recent header
                if idx == len(current_header_stack) - 1:
                    current_metadata["header"] = text
                    current_metadata["header_level"] = header_level

            current_pos = header_pos

        # Add final chunk
        if current_pos < len(text):
            chunk_text = text[current_pos:].strip()
            if chunk_text:
                documents.append(
                    SimpleDocument(
                        page_content=chunk_text,
                        metadata=current_metadata.copy()
                    )
                )

        # If no documents created, return entire text
        if not documents:
            documents.append(SimpleDocument(page_content=text, metadata={}))

        return documents


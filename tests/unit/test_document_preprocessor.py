import pytest
from mindsdb.interfaces.knowledge_base.preprocessing.document_preprocessor import (
    DocumentPreprocessor,
    TextChunkingPreprocessor,

)
from mindsdb.interfaces.knowledge_base.preprocessing.models import (
    Document,
    TextChunkingConfig,
)


class TestDocumentPreprocessor:
    def test_deterministic_id_generation(self):
        """Test that ID generation is deterministic for same content"""
        preprocessor = DocumentPreprocessor()

        # Same content should generate same ID
        content = "test content"
        id1 = preprocessor._generate_deterministic_id(content)
        id2 = preprocessor._generate_deterministic_id(content)
        assert id1 == id2

        # Different content should generate different IDs
        different_content = "different content"
        id3 = preprocessor._generate_deterministic_id(different_content)
        assert id1 != id3

    def test_chunk_id_generation(self):
        """Test chunk ID generation with and without indices"""
        preprocessor = DocumentPreprocessor()
        content = "test content"
        # Test basic ID generation
        base_id = preprocessor._generate_chunk_id(content)
        assert base_id == preprocessor._generate_deterministic_id(content)
        # Test chunk index handling
        chunk_id = preprocessor._generate_chunk_id(content, chunk_index=0)
        assert chunk_id.endswith("_chunk_0")
        assert chunk_id.startswith(base_id)


class TestTextChunkingPreprocessor:
    @pytest.fixture
    def preprocessor(self):
        config = TextChunkingConfig(chunk_size=100, chunk_overlap=20)
        return TextChunkingPreprocessor(config)

    def test_single_document_processing(self, preprocessor):
        """Test processing of a single document without chunking"""
        doc = Document(content="Short content that won't be chunked")
        chunks = preprocessor.process_documents([doc])
        assert len(chunks) == 1
        assert chunks[0].content == doc.content
        assert chunks[0].id == preprocessor._generate_deterministic_id(doc.content)

    def test_document_chunking(self, preprocessor):
        """Test processing of a document that requires chunking"""
        long_content = " ".join(["word"] * 50)  # Create content long enough to be chunked
        doc = Document(content=long_content)
        chunks = preprocessor.process_documents([doc])
        assert len(chunks) > 1
        # Verify chunk IDs are deterministic and unique
        chunk_ids = [chunk.id for chunk in chunks]
        assert len(chunk_ids) == len(set(chunk_ids))  # All IDs should be unique
        # Process same content again and verify same IDs
        new_chunks = preprocessor.process_documents([doc])
        new_chunk_ids = [chunk.id for chunk in new_chunks]
        assert chunk_ids == new_chunk_ids


def test_duplicate_document_handling():
    """Test handling of duplicate documents"""
    preprocessor = TextChunkingPreprocessor()
    # Create two documents with same content
    content = "Test content"
    doc1 = Document(content=content)
    doc2 = Document(content=content)
    chunks1 = preprocessor.process_documents([doc1])
    chunks2 = preprocessor.process_documents([doc2])
    # Verify same content generates same IDs
    assert chunks1[0].id == chunks2[0].id


def test_content_column_id_generation():
    """Test ID generation with content column consideration"""
    preprocessor = DocumentPreprocessor()
    content = "test content"
    # Generate IDs with different content columns
    id1 = preprocessor._generate_deterministic_id(content, "column1")
    id2 = preprocessor._generate_deterministic_id(content, "column2")
    # Same content but different columns should have different IDs
    assert id1 != id2


def test_metadata_preservation():
    """Test that metadata is preserved and merged with defaults during processing"""
    preprocessor = TextChunkingPreprocessor()
    metadata = {"key": "value"}
    doc = Document(content="Test content", metadata=metadata)
    chunks = preprocessor.process_documents([doc])
    # Verify metadata is preserved and includes default values
    expected_metadata = {
        "source": "default",  # Default metadata
        "key": "value"       # User-provided metadata
    }
    assert chunks[0].metadata == expected_metadata


def test_empty_content_handling():
    """Test handling of empty content - should not produce any chunks"""
    preprocessor = TextChunkingPreprocessor()
    doc = Document(content="")
    chunks = preprocessor.process_documents([doc])
    assert len(chunks) == 0  # Empty content should produce no chunks


def test_whitespace_content_handling():
    """Test handling of whitespace-only content - should not produce any chunks"""
    preprocessor = TextChunkingPreprocessor()
    doc = Document(content="   \n   \t   ")
    chunks = preprocessor.process_documents([doc])
    assert len(chunks) == 0  # Whitespace-only content should produce no chunks


def test_multiple_documents_with_empty():
    """Test processing multiple documents including empty ones"""
    preprocessor = TextChunkingPreprocessor()
    docs = [
        Document(content=""),  # empty
        Document(content="valid content"),  # valid
        Document(content="   \n  "),  # whitespace
        Document(content="more valid content")  # valid
    ]
    chunks = preprocessor.process_documents(docs)
    assert len(chunks) == 2  # Only valid content should produce chunks
    assert all(chunk.content.strip() for chunk in chunks)  # All chunks should have non-empty content


@pytest.mark.parametrize("content,metadata,expected_metadata", [
    ("test", None, {"source": "default"}),  # Updated to expect default metadata
    ("test", {}, {"source": "default"}),    # Updated to expect default metadata
    ("test", {"key": "value"}, {"key": "value", "source": "default"}),  # Merged with default
])
def test_metadata_handling(content, metadata, expected_metadata):
    """Test various metadata scenarios"""
    preprocessor = TextChunkingPreprocessor()
    doc = Document(content=content, metadata=metadata)
    chunks = preprocessor.process_documents([doc])
    assert chunks[0].metadata == expected_metadata

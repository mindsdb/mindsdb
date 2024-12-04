import pytest
from mindsdb.interfaces.knowledge_base.preprocessing.document_preprocessor import (
    DocumentPreprocessor,
    TextChunkingPreprocessor,
    ContextualPreprocessor
)
from mindsdb.interfaces.knowledge_base.preprocessing.models import (
    Document,
    TextChunkingConfig,
    ContextualConfig,
    ProcessedChunk
)
from typing import List
from unittest.mock import Mock, patch


class TestDocumentPreprocessor:
    @pytest.fixture
    def preprocessor(self):
        class TestPreprocessor(DocumentPreprocessor):
            def process_documents(self, documents: List[Document]) -> List[ProcessedChunk]:
                return []  # Dummy implementation for testing base class methods
        return TestPreprocessor()

    def test_deterministic_id_generation(self, preprocessor):
        """Test that ID generation is deterministic for same content"""
        # Same content should generate same ID
        content = "test content"
        id1 = preprocessor._generate_deterministic_id(content)
        id2 = preprocessor._generate_deterministic_id(content)
        assert id1 == id2

        # Different content should generate different IDs
        different_content = "different content"
        id3 = preprocessor._generate_deterministic_id(different_content)
        assert id1 != id3

        # Test with provided_id
        provided_id = "test_id"
        content_column = "test_column"
        id4 = preprocessor._generate_deterministic_id(content, content_column, provided_id)
        assert id4 == f"{provided_id}_{content_column}"

    def test_chunk_id_generation(self, preprocessor):
        """Test chunk ID generation with and without indices"""
        content = "test content"
        content_column = "test_column"
        provided_id = "test_id"

        # Test basic ID generation
        base_id = preprocessor._generate_chunk_id(content)
        assert base_id == preprocessor._generate_deterministic_id(content)

        # Test with content_column
        column_id = preprocessor._generate_chunk_id(content, content_column=content_column)
        assert column_id == preprocessor._generate_deterministic_id(content, content_column)

        # Test with provided_id
        provided_chunk_id = preprocessor._generate_chunk_id(content, content_column=content_column, provided_id=provided_id)
        assert provided_chunk_id == preprocessor._generate_deterministic_id(content, content_column, provided_id)

        # Test chunk index handling
        chunk_id = preprocessor._generate_chunk_id(content, chunk_index=0)
        assert chunk_id.endswith("_chunk_0")
        assert chunk_id.startswith(preprocessor._generate_deterministic_id(content))

    def test_split_document_without_splitter(self, preprocessor):
        """Test that splitting without a configured splitter raises error"""
        doc = Document(content="test content")
        with pytest.raises(ValueError, match="Splitter not configured"):
            preprocessor._split_document(doc)


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
        assert chunks[0].metadata['source'] == 'TextChunkingPreprocessor'

    def test_document_chunking(self, preprocessor):
        """Test processing of a document that requires chunking"""
        long_content = " ".join(["word"] * 50)
        doc = Document(content=long_content)
        chunks = preprocessor.process_documents([doc])
        assert len(chunks) > 1
        # Verify chunk IDs are deterministic and unique
        chunk_ids = [chunk.id for chunk in chunks]
        assert len(chunk_ids) == len(set(chunk_ids))
        # Verify source metadata
        assert all(chunk.metadata['source'] == 'TextChunkingPreprocessor' for chunk in chunks)
        # Verify chunk indices
        assert all('chunk_index' in chunk.metadata for chunk in chunks)
        # Process same content again and verify same IDs
        new_chunks = preprocessor.process_documents([doc])
        new_chunk_ids = [chunk.id for chunk in new_chunks]
        assert chunk_ids == new_chunk_ids


def test_metadata_preservation():
    """Test that metadata is preserved during processing"""
    preprocessor = TextChunkingPreprocessor()
    metadata = {"key": "value", "content_column": "test_column"}
    doc = Document(content="Test content", metadata=metadata)
    chunks = preprocessor.process_documents([doc])
    # Verify metadata is preserved and includes source
    assert chunks[0].metadata['source'] == 'TextChunkingPreprocessor'
    assert chunks[0].metadata['key'] == 'value'
    assert chunks[0].metadata['content_column'] == 'test_column'


def test_content_column_handling():
    """Test handling of content column in metadata"""
    preprocessor = TextChunkingPreprocessor()
    metadata = {"content_column": "test_column"}
    doc = Document(content="Test content", metadata=metadata)
    chunks = preprocessor.process_documents([doc])
    # Verify content column is used in ID generation
    assert "test_column" in chunks[0].id


def test_provided_id_handling():
    """Test handling of provided document IDs"""
    preprocessor = TextChunkingPreprocessor()
    doc = Document(content="Test content", id="test_id")
    chunks = preprocessor.process_documents([doc])
    # Verify provided ID is incorporated into chunk ID
    assert chunks[0].metadata['original_doc_id'] == 'test_id'


def test_empty_content_handling():
    """Test handling of empty content"""
    preprocessor = TextChunkingPreprocessor()
    doc = Document(content="")
    chunks = preprocessor.process_documents([doc])
    assert len(chunks) == 0


def test_whitespace_content_handling():
    """Test handling of whitespace-only content"""
    preprocessor = TextChunkingPreprocessor()
    doc = Document(content="   \n   \t   ")
    chunks = preprocessor.process_documents([doc])
    assert len(chunks) == 0


@pytest.mark.parametrize("content,metadata,expected_source", [
    ("test", None, "TextChunkingPreprocessor"),
    ("test", {}, "TextChunkingPreprocessor"),
    ("test", {"key": "value"}, "TextChunkingPreprocessor"),
])
def test_source_metadata(content, metadata, expected_source):
    """Test source metadata is correctly set"""
    preprocessor = TextChunkingPreprocessor()
    doc = Document(content=content, metadata=metadata)
    chunks = preprocessor.process_documents([doc])
    assert chunks[0].metadata['source'] == expected_source


class TestContextualPreprocessor:
    @pytest.fixture
    def preprocessor(self):
        with patch('mindsdb.interfaces.knowledge_base.preprocessing.document_preprocessor.create_chat_model') as mock_create_chat_model:
            # Create a mock LLM that returns a simple response
            mock_llm = Mock()
            mock_llm.return_value.content = "Test context"
            mock_create_chat_model.return_value = mock_llm

            config = ContextualConfig(
                chunk_size=100,
                chunk_overlap=20,
                llm_config={"model_name": "test_model", "provider": "test"}
            )
            return ContextualPreprocessor(config)

    def test_source_metadata(self, preprocessor):
        """Test that source metadata is correctly set"""
        doc = Document(content="Test content")
        chunks = preprocessor.process_documents([doc])
        assert chunks[0].metadata['source'] == 'ContextualPreprocessor'

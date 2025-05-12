import pytest
import json

import pandas as pd

from mindsdb.interfaces.knowledge_base.preprocessing.models import (
    Document,
)
from mindsdb.interfaces.knowledge_base.preprocessing.json_chunker import (
    JSONChunkingConfig,
    JSONChunkingPreprocessor
)


class TestJSONChunker:
    """Test suite for the JSON chunker"""

    def test_basic_chunking(self):
        """Test basic chunking of JSON objects"""
        # Create a simple JSON object
        json_data = [
            {
                "id": 1,
                "name": "John Doe",
                "skills": ["Python", "SQL", "Machine Learning"],
                "contact": {
                    "email": "john@example.com",
                    "phone": "123-456-7890"
                }
            },
            {
                "id": 2,
                "name": "Jane Smith",
                "skills": ["Java", "C++", "Data Science"],
                "contact": {
                    "email": "jane@example.com",
                    "phone": "987-654-3210"
                }
            }
        ]

        # Create documents
        documents = []
        for i, item in enumerate(json_data):
            doc = Document(
                id=f"doc_{i}",
                content=json.dumps(item),
                metadata={"source": "test"}
            )
            documents.append(doc)

        # Create preprocessor with default config
        preprocessor = JSONChunkingPreprocessor()

        # Process documents
        chunks = preprocessor.process_documents(documents)

        # Check results
        assert len(chunks) == 2  # One chunk per JSON object
        assert "John Doe" in chunks[0].content
        assert "Jane Smith" in chunks[1].content

        # Check metadata
        assert chunks[0].metadata["original_doc_id"] == "doc_0"
        assert chunks[1].metadata["original_doc_id"] == "doc_1"

        # Check chunk IDs
        assert chunks[0].id.startswith("doc_0:")
        assert chunks[1].id.startswith("doc_1:")

    def test_flatten_nested(self):
        """Test flattening of nested JSON structures"""
        # Create a nested JSON object
        json_data = {
            "id": 1,
            "name": "John Doe",
            "contact": {
                "email": "john@example.com",
                "address": {
                    "street": "123 Main St",
                    "city": "Anytown",
                    "zip": "12345"
                }
            },
            "experience": [
                {
                    "company": "ABC Corp",
                    "position": "Developer",
                    "years": 3
                },
                {
                    "company": "XYZ Inc",
                    "position": "Senior Developer",
                    "years": 2
                }
            ]
        }

        # Create document
        doc = Document(
            id="doc_1",
            content=json.dumps(json_data),
            metadata={"source": "test"}
        )

        # Create preprocessor with flatten_nested=True
        config = JSONChunkingConfig(flatten_nested=True)
        preprocessor = JSONChunkingPreprocessor(config)

        # Process document
        chunks = preprocessor.process_documents([doc])

        # Check results
        assert len(chunks) == 1
        assert "contact.email: john@example.com" in chunks[0].content
        assert "contact.address.street: 123 Main St" in chunks[0].content

        # Create preprocessor with flatten_nested=False
        config = JSONChunkingConfig(flatten_nested=False)
        preprocessor = JSONChunkingPreprocessor(config)

        # Process document
        chunks = preprocessor.process_documents([doc])

        # Check results
        assert len(chunks) == 1
        assert "\"contact\": {" in chunks[0].content
        assert "\"address\": {" in chunks[0].content

    def test_chunk_by_field(self):
        """Test chunking by field instead of object"""
        # Create a JSON object
        json_data = {
            "id": 1,
            "name": "John Doe",
            "skills": ["Python", "SQL", "Machine Learning"],
            "contact": {
                "email": "john@example.com",
                "phone": "123-456-7890"
            },
            "summary": "Experienced software developer with 5+ years of experience"
        }

        # Create document
        doc = Document(
            id="doc_1",
            content=json.dumps(json_data),
            metadata={"source": "test"}
        )

        # Create preprocessor with chunk_by_object=False
        config = JSONChunkingConfig(chunk_by_object=False)
        preprocessor = JSONChunkingPreprocessor(config)

        # Process document
        chunks = preprocessor.process_documents([doc])

        # Check results
        assert len(chunks) == 5  # One chunk per field

        # Check field names in metadata
        field_names = [chunk.metadata.get("field_name") for chunk in chunks]
        assert "id" in field_names
        assert "name" in field_names
        assert "skills" in field_names
        assert "contact" in field_names
        assert "summary" in field_names

    def test_include_exclude_fields(self):
        """Test including and excluding specific fields"""
        # Create a JSON object
        json_data = {
            "id": 1,
            "name": "John Doe",
            "skills": ["Python", "SQL", "Machine Learning"],
            "contact": {
                "email": "john@example.com",
                "phone": "123-456-7890"
            },
            "summary": "Experienced software developer with 5+ years of experience"
        }

        # Create document
        doc = Document(
            id="doc_1",
            content=json.dumps(json_data),
            metadata={"source": "test"}
        )

        # Test exclude_fields
        config = JSONChunkingConfig(exclude_fields=["id", "contact"])
        preprocessor = JSONChunkingPreprocessor(config)
        chunks = preprocessor.process_documents([doc])

        # Check results
        assert len(chunks) == 1
        assert "id: 1" not in chunks[0].content
        assert "contact.email: john@example.com" not in chunks[0].content
        assert "name: John Doe" in chunks[0].content

        # Test include_fields
        config = JSONChunkingConfig(include_fields=["name", "summary"])
        preprocessor = JSONChunkingPreprocessor(config)
        chunks = preprocessor.process_documents([doc])

        # Check results
        assert len(chunks) == 1
        assert "name: John Doe" in chunks[0].content
        assert "summary: Experienced" in chunks[0].content
        assert "skills" not in chunks[0].content
        assert "contact" not in chunks[0].content

    def test_error_handling(self):
        """Test handling of invalid JSON data"""
        # Create an invalid JSON document
        doc = Document(
            id="doc_error",
            content="This is not valid JSON",
            metadata={"source": "test"}
        )

        # Create preprocessor
        preprocessor = JSONChunkingPreprocessor()

        # Process document
        chunks = preprocessor.process_documents([doc])

        # Check results
        assert len(chunks) == 1
        assert "Error processing document" in chunks[0].content
        assert chunks[0].id == "doc_error_error"

    def test_metadata_extraction_specific_fields(self):
        """Test extracting specific fields to metadata"""
        # Create a nested JSON object
        json_data = {
            "id": 1,
            "name": "John Doe",
            "contact": {
                "email": "john@example.com",
                "phone": "123-456-7890"
            },
            "skills": ["Python", "SQL", "Machine Learning"]
        }

        # Create document
        doc = Document(
            id="doc_1",
            content=json.dumps(json_data),
            metadata={"source": "test"}
        )

        # Create preprocessor with metadata_fields
        config = JSONChunkingConfig(
            metadata_fields=["name", "contact.email", "skills"]
        )
        preprocessor = JSONChunkingPreprocessor(config)

        # Process document
        chunks = preprocessor.process_documents([doc])

        # Check results
        assert len(chunks) == 1
        assert "field_name" in chunks[0].metadata
        assert chunks[0].metadata["field_name"] == "John Doe"
        assert "field_contact.email" in chunks[0].metadata
        assert chunks[0].metadata["field_contact.email"] == "john@example.com"
        assert "field_skills" in chunks[0].metadata
        assert chunks[0].metadata["field_skills"] == ["Python", "SQL", "Machine Learning"]

    def test_metadata_extraction_all_primitives(self):
        """Test extracting all primitive values to metadata"""
        # Create a nested JSON object
        json_data = {
            "id": 1,
            "name": "John Doe",
            "age": 30,
            "is_active": True,
            "contact": {
                "email": "john@example.com",
                "phone": "123-456-7890"
            }
        }

        # Create document
        doc = Document(
            id="doc_1",
            content=json.dumps(json_data),
            metadata={"source": "test"}
        )

        # Create preprocessor with extract_all_primitives=True
        config = JSONChunkingConfig(extract_all_primitives=True)
        preprocessor = JSONChunkingPreprocessor(config)

        # Process document
        chunks = preprocessor.process_documents([doc])

        # Check results
        assert len(chunks) == 1
        assert "field_id" in chunks[0].metadata
        assert chunks[0].metadata["field_id"] == 1
        assert "field_name" in chunks[0].metadata
        assert chunks[0].metadata["field_name"] == "John Doe"
        assert "field_age" in chunks[0].metadata
        assert chunks[0].metadata["field_age"] == 30
        assert "field_is_active" in chunks[0].metadata
        assert chunks[0].metadata["field_is_active"] is True
        assert "field_contact.email" in chunks[0].metadata
        assert chunks[0].metadata["field_contact.email"] == "john@example.com"
        assert "field_contact.phone" in chunks[0].metadata
        assert chunks[0].metadata["field_contact.phone"] == "123-456-7890"

    def test_to_dataframe(self):
        """Test conversion to DataFrame"""
        # Create a simple JSON object
        json_data = {
            "id": 1,
            "name": "John Doe"
        }

        # Create document
        doc = Document(
            id="doc_1",
            content=json.dumps(json_data),
            metadata={"source": "test"}
        )

        # Create preprocessor
        preprocessor = JSONChunkingPreprocessor()

        # Process document
        chunks = preprocessor.process_documents([doc])

        # Convert to DataFrame
        df = preprocessor.to_dataframe(chunks)

        # Check results
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert "id" in df.columns
        assert "content" in df.columns
        assert "metadata" in df.columns


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])

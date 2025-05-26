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

        # Check for chunk content to verify field-based chunking
        chunk_contents = [chunk.content for chunk in chunks]

        # Verify each field is in a separate chunk
        assert any("id: 1" in content for content in chunk_contents)
        assert any("name: John Doe" in content for content in chunk_contents)
        assert any("skills:" in content for content in chunk_contents)
        assert any("contact.email: john@example.com" in content for content in chunk_contents)
        assert any("summary: Experienced software developer" in content for content in chunk_contents)

        # Verify metadata extraction is working
        for chunk in chunks:
            # Each chunk should have the same metadata extracted from the original JSON
            assert chunk.metadata.get("field_name") == "John Doe"
            assert chunk.metadata.get("field_id") == 1
            assert "field_summary" in chunk.metadata

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

    def test_utility_bill_schema(self):
        """Test chunking of a utility bill JSON schema"""
        # Create a utility bill JSON object based on the provided schema
        utility_bill = {
            "billingPeriod": "03/17/2025 - 04/14/2025",
            "amountDue": 86.77,
            "accountNumber": "110 167 082 509",
            "dueDate": "05/09/2025",
            "previousBalance": 99.51,
            "payments": 99.51,
            "kwhUsed": 592,
            "balancesByCompany": [
                {
                    "previousBalance": 59.39,
                    "payments": 59.39,
                    "currentCharges": 52.49,
                    "amountDue": 52.49
                },
                {
                    "previousBalance": 40.12,
                    "payments": 40.12,
                    "currentCharges": 34.28,
                    "amountDue": 34.28
                }
            ]
        }

        # Create document
        doc = Document(
            id="utility_bill_1",
            content=json.dumps(utility_bill),
            metadata={"source": "test", "document_type": "utility_bill"}
        )

        # Test 1: Default chunking (chunk by object, flatten nested)
        preprocessor = JSONChunkingPreprocessor()
        chunks = preprocessor.process_documents([doc])

        # Check results
        assert len(chunks) == 1  # One chunk for the whole bill
        assert "billingPeriod: 03/17/2025 - 04/14/2025" in chunks[0].content
        assert "accountNumber: 110 167 082 509" in chunks[0].content
        assert "balancesByCompany[0].previousBalance: 59.39" in chunks[0].content
        assert "balancesByCompany[1].amountDue: 34.28" in chunks[0].content

        # Test 2: Chunk by field instead of object
        config = JSONChunkingConfig(chunk_by_object=False)
        preprocessor = JSONChunkingPreprocessor(config)
        chunks = preprocessor.process_documents([doc])

        # Check results - should have one chunk per top-level field
        assert len(chunks) == 8  # One chunk per top-level field

        # Verify field names in metadata
        field_names = [chunk.metadata.get("field_name") for chunk in chunks]
        assert "billingPeriod" in field_names
        assert "amountDue" in field_names
        assert "accountNumber" in field_names
        assert "balancesByCompany" in field_names

        # Test 3: Non-flattened nested structure
        config = JSONChunkingConfig(flatten_nested=False)
        preprocessor = JSONChunkingPreprocessor(config)
        chunks = preprocessor.process_documents([doc])

        # Check results
        assert len(chunks) == 1
        assert '"balancesByCompany": [' in chunks[0].content
        assert '"previousBalance": 59.39' in chunks[0].content
        assert '"currentCharges": 34.28' in chunks[0].content

        # Test 4: Include only specific fields
        config = JSONChunkingConfig(
            include_fields=["accountNumber", "dueDate", "amountDue"]
        )
        preprocessor = JSONChunkingPreprocessor(config)
        chunks = preprocessor.process_documents([doc])

        # Check results
        assert len(chunks) == 1
        assert "accountNumber: 110 167 082 509" in chunks[0].content
        assert "dueDate: 05/09/2025" in chunks[0].content
        assert "amountDue: 86.77" in chunks[0].content
        assert "billingPeriod" not in chunks[0].content
        assert "kwhUsed" not in chunks[0].content
        assert "balancesByCompany" not in chunks[0].content

        # Test 5: Include nested fields
        config = JSONChunkingConfig(
            include_fields=["accountNumber", "balancesByCompany"],
            flatten_nested=False
        )
        preprocessor = JSONChunkingPreprocessor(config)
        chunks = preprocessor.process_documents([doc])

        # Check results
        assert len(chunks) == 1
        assert "accountNumber" in chunks[0].content
        assert "balancesByCompany" in chunks[0].content
        assert "previousBalance" in chunks[0].content  # From the nested structure
        assert "currentCharges" in chunks[0].content  # From the nested structure
        assert "billingPeriod" not in chunks[0].content

        # Test 6: Extract metadata fields
        config = JSONChunkingConfig(
            metadata_fields=["accountNumber", "dueDate", "amountDue"]
        )
        preprocessor = JSONChunkingPreprocessor(config)
        chunks = preprocessor.process_documents([doc])

        # Check metadata extraction
        assert chunks[0].metadata.get("field_accountNumber") == "110 167 082 509"
        assert chunks[0].metadata.get("field_dueDate") == "05/09/2025"
        assert chunks[0].metadata.get("field_amountDue") == 86.77

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

    def test_metadata_field_extraction(self):
        """Test that metadata fields are correctly extracted from JSON content"""
        # Sample CV data similar to what you're working with
        cv_data = {
            "_id": {"$oid": "680f82222b413ec943328950"},
            "skills": {
                "soft_skills": None,
                "human_languages": ["English", "French"],
                "technical_skills": ["Operations Management", "Strategic Planning"]
            },
            "summary": "Product Management leader with 13+ years of experience",
            "metadata": {
                "user_id": "2024_67eee2750c5f19.58992827",
                "timestamp": "2025-04-28T13:26:34.474653Z"
            },
            "contact_information": {
                "name": "John Doe",
                "email": "john@example.com"
            }
        }

        # Create a document with the CV data
        doc = Document(id="test_cv", content=json.dumps(cv_data))

        # Configure the JSON chunker to extract specific metadata fields
        config = JSONChunkingConfig(
            # Extract these specific fields into metadata
            metadata_fields=[
                "contact_information.name",  # Nested field using dot notation
                "contact_information.email",
                "metadata.user_id",
                "summary"
            ],
            # Other useful configuration options
            chunk_by_object=True,  # Process the whole object as one chunk
            flatten_nested=True,   # Flatten nested structures for better text representation
            extract_all_primitives=False  # Don't extract all primitive values, just the ones specified
        )

        # Create the JSON chunker with the config
        preprocessor = JSONChunkingPreprocessor(config)

        # Process the document
        chunks = preprocessor.process_documents([doc])

        # Verify that the metadata fields were extracted
        assert len(chunks) == 1  # Should have one chunk since chunk_by_object=True
        chunk = chunks[0]

        # Check that the specified fields were extracted into metadata with 'field_' prefix
        assert chunk.metadata.get("field_contact_information.name") == "John Doe"
        assert chunk.metadata.get("field_contact_information.email") == "john@example.com"
        assert chunk.metadata.get("field_metadata.user_id") == "2024_67eee2750c5f19.58992827"
        assert chunk.metadata.get("field_summary") == "Product Management leader with 13+ years of experience"

    def test_extract_all_primitives(self):
        """Test that all primitive values are extracted when extract_all_primitives=True"""
        # Simple JSON data
        data = {
            "name": "John Doe",
            "age": 30,
            "is_active": True,
            "nested": {
                "key1": "value1",
                "key2": 42
            }
        }

        # Create a document
        doc = Document(id="test_primitives", content=json.dumps(data))

        # Configure the JSON chunker to extract all primitive values
        config = JSONChunkingConfig(
            extract_all_primitives=True,  # Extract all primitive values
            chunk_by_object=True,         # Process the whole object as one chunk
            flatten_nested=True           # Flatten nested structures
        )

        # Create the JSON chunker with the config
        preprocessor = JSONChunkingPreprocessor(config)

        # Process the document
        chunks = preprocessor.process_documents([doc])

        # Verify that all primitive values were extracted
        assert len(chunks) == 1
        chunk = chunks[0]

        # Check that all primitive fields were extracted with 'field_' prefix
        assert chunk.metadata.get("field_name") == "John Doe"
        assert chunk.metadata.get("field_age") == 30
        assert chunk.metadata.get("field_is_active") is True
        assert chunk.metadata.get("field_nested.key1") == "value1"
        assert chunk.metadata.get("field_nested.key2") == 42

    def test_default_metadata_extraction(self):
        """Test that top-level primitive fields are extracted by default when metadata_fields is empty"""
        # Simple JSON data with top-level primitives
        data = {
            "name": "John Doe",
            "age": 30,
            "is_active": True,
            "nested": {
                "key1": "value1",
                "key2": 42
            }
        }

        # Create a document
        doc = Document(id="test_default", content=json.dumps(data))

        # Configure the JSON chunker with default metadata_fields (empty list)
        config = JSONChunkingConfig(
            # metadata_fields is empty by default
            chunk_by_object=True,  # Process the whole object as one chunk
            flatten_nested=False   # Don't flatten nested structures
        )

        # Create the JSON chunker with the config
        preprocessor = JSONChunkingPreprocessor(config)

        # Process the document
        chunks = preprocessor.process_documents([doc])

        # Verify that top-level primitive fields were extracted
        assert len(chunks) == 1
        chunk = chunks[0]

        # Check that top-level primitive fields were extracted with 'field_' prefix
        assert chunk.metadata.get("field_name") == "John Doe"
        assert chunk.metadata.get("field_age") == 30
        assert chunk.metadata.get("field_is_active") is True

        # Nested fields should not be extracted by default
        assert chunk.metadata.get("field_nested.key1") is None
        assert chunk.metadata.get("field_nested.key2") is None

    def test_default_metadata_extraction_complex_object(self):
        """Test that all primitive fields are extracted when metadata_fields is empty and there are no top-level primitives"""
        # Complex JSON data with no top-level primitives
        data = {
            "contact_information": {
                "name": "John Doe",
                "email": "john@example.com",
                "phone": "123-456-7890"
            },
            "skills": {
                "technical": ["Python", "SQL", "Machine Learning"],
                "languages": ["English", "Spanish"]
            },
            "metadata": {
                "user_id": "user123",
                "timestamp": "2025-05-12T15:50:12+03:00"
            }
        }

        # Create a document
        doc = Document(id="test_complex", content=json.dumps(data))

        # Configure the JSON chunker with default metadata_fields (empty list)
        config = JSONChunkingConfig(
            # metadata_fields is empty by default
            chunk_by_object=True,  # Process the whole object as one chunk
            flatten_nested=True    # Flatten nested structures
        )

        # Create the JSON chunker with the config
        preprocessor = JSONChunkingPreprocessor(config)

        # Process the document
        chunks = preprocessor.process_documents([doc])

        # Verify that primitive fields were extracted
        assert len(chunks) == 1
        chunk = chunks[0]

        # Check that primitive fields were extracted with 'field_' prefix
        assert chunk.metadata.get("field_contact_information.name") == "John Doe"
        assert chunk.metadata.get("field_contact_information.email") == "john@example.com"
        assert chunk.metadata.get("field_contact_information.phone") == "123-456-7890"
        assert chunk.metadata.get("field_metadata.user_id") == "user123"
        assert chunk.metadata.get("field_metadata.timestamp") == "2025-05-12T15:50:12+03:00"


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])

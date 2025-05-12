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


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])

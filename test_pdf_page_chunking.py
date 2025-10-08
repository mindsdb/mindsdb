#!/usr/bin/env python3
"""
Test script to demonstrate PDF page-based chunking functionality.
This script shows how to use the new page_chunking parameter.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "mindsdb"))

from mindsdb.integrations.utilities.rag.splitters.file_splitter import FileSplitter, FileSplitterConfig
from mindsdb.interfaces.knowledge_base.preprocessing.models import Document


def test_pdf_page_chunking():
    """Test PDF page-based chunking vs regular chunking"""

    print("=== PDF Page Chunking Test ===\n")

    # Create a mock PDF content (simulating what would come from a real PDF)
    mock_pdf_content = b"""%PDF-1.4
1 0 obj
<<
/Type /Catalog
/Pages 2 0 R
>>
endobj

2 0 obj
<<
/Type /Pages
/Kids [3 0 R 4 0 R]
/Count 2
>>
endobj

3 0 obj
<<
/Type /Page
/Parent 2 0 R
/Contents 5 0 R
>>
endobj

4 0 obj
<<
/Type /Page
/Parent 2 0 R
/Contents 6 0 R
>>
endobj

5 0 obj
<<
/Length 44
>>
stream
BT
/F1 12 Tf
100 700 Td
(Page 1: Introduction to Machine Learning) Tj
ET
endstream
endobj

6 0 obj
<<
/Length 45
>>
stream
BT
/F1 12 Tf
100 700 Td
(Page 2: Deep Learning Fundamentals) Tj
ET
endstream
endobj

xref
0 7
0000000000 65535 f 
0000000009 00000 n 
0000000058 00000 n 
0000000115 00000 n 
0000000172 00000 n 
0000000229 00000 n 
0000000323 00000 n 
trailer
<<
/Size 7
/Root 1 0 R
>>
startxref
418
%%EOF"""

    # Create a document with PDF content
    doc = Document(
        content="This is a placeholder - actual content will come from PDF parsing",
        metadata={"extension": ".pdf", "source": "test_document.pdf", "pdf_content": mock_pdf_content},
    )

    print("1. Testing REGULAR chunking (character-based):")
    print("-" * 50)

    # Test regular chunking
    regular_config = FileSplitterConfig(page_chunking=False, chunk_size=100, chunk_overlap=20)
    regular_splitter = FileSplitter(regular_config)

    try:
        regular_docs = regular_splitter.split_documents([doc])
        print(f"Regular chunking created {len(regular_docs)} chunks")
        for i, chunk in enumerate(regular_docs):
            print(f"  Chunk {i + 1}: {chunk.page_content[:50]}...")
            print(f"    Metadata: {chunk.metadata}")
    except Exception as e:
        print(f"Regular chunking failed (expected for mock PDF): {e}")

    print("\n2. Testing PAGE chunking:")
    print("-" * 50)

    # Test page-based chunking
    page_config = FileSplitterConfig(page_chunking=True, max_page_size=1000, chunk_overlap=50)
    page_splitter = FileSplitter(page_config)

    try:
        page_docs = page_splitter.split_documents([doc])
        print(f"Page chunking created {len(page_docs)} chunks")
        for i, chunk in enumerate(page_docs):
            print(f"  Chunk {i + 1}: {chunk.page_content[:50]}...")
            print(f"    Metadata: {chunk.metadata}")
    except Exception as e:
        print(f"Page chunking failed: {e}")

    print("\n3. Configuration parameters:")
    print("-" * 50)
    print("Available parameters for SQL API:")
    print("- page_chunking: bool (default: false)")
    print("- max_page_size: int (default: 4000)")
    print("- chunk_size: int (default: 1000)")
    print("- chunk_overlap: int (default: 200)")

    print("\n4. SQL API Usage Example:")
    print("-" * 50)
    print("""
-- Enable page-based chunking for PDFs
UPDATE knowledge_bases 
SET preprocessing = {
    "type": "text_chunking",
    "text_chunking_config": {
        "page_chunking": true,
        "max_page_size": 4000,
        "chunk_size": 1000,
        "chunk_overlap": 200
    }
}
WHERE name = 'my_knowledge_base';

-- Insert PDF files
INSERT INTO knowledge_bases (files) 
VALUES (['document1.pdf', 'document2.pdf']);
""")


if __name__ == "__main__":
    test_pdf_page_chunking()

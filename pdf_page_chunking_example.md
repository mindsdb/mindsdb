# PDF Page-Based Chunking

This implementation adds support for chunking PDF documents by pages instead of arbitrary character-based chunking. This preserves page structure and enables better semantic search results.

## Features

- **Page-based chunking**: Each PDF page becomes a separate chunk
- **Page metadata**: Preserves page numbers, total pages, and file information
- **Configurable page size**: Large pages can be split further if needed
- **SQL API support**: Configurable via SQL commands
- **Backward compatibility**: Falls back to regular chunking if page chunking fails

## Configuration Parameters

- `page_chunking`: Boolean flag to enable page-based chunking (default: false)
- `max_page_size`: Maximum characters per page before further splitting (default: 4000)
- `chunk_size`: Target chunk size for regular chunking (default: 1000)
- `chunk_overlap`: Overlap between chunks (default: 200)

## SQL API Usage

### Enable Page Chunking

```sql
-- Update existing knowledge base to use page chunking
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
```

### Create New Knowledge Base with Page Chunking

```sql
-- Create knowledge base with page chunking enabled
CREATE KNOWLEDGE BASE my_pdf_kb
WITH preprocessing = {
    "type": "text_chunking", 
    "text_chunking_config": {
        "page_chunking": true,
        "max_page_size": 5000,
        "chunk_size": 1000,
        "chunk_overlap": 100
    }
};
```

### Insert PDF Files

```sql
-- Insert PDF files into knowledge base
INSERT INTO my_pdf_kb (files) 
VALUES (['document1.pdf', 'document2.pdf', 'report.pdf']);
```

## Benefits

1. **Better Semantic Search**: Page boundaries often align with logical content sections
2. **Page References**: Users can reference specific pages in queries
3. **Preserved Context**: Page-level metadata provides additional context
4. **Improved Retrieval**: More meaningful chunk boundaries lead to better search results

## Metadata Structure

Each chunk includes the following metadata:

```json
{
    "source": "document.pdf",
    "page_number": 5,
    "total_pages": 20,
    "file_type": "pdf",
    "chunk_index": 0,  // Only if page was split further
    "total_chunks": 1  // Only if page was split further
}
```

## Example Query Results

When searching with page chunking enabled, results will include page information:

```sql
SELECT content, metadata 
FROM my_pdf_kb 
WHERE content LIKE '%machine learning%';
```

Results might look like:
- Content: "Introduction to Machine Learning concepts..."
- Metadata: `{"page_number": 1, "total_pages": 15, "source": "ml_guide.pdf"}`

## Technical Details

- Uses PyMuPDF (fitz) for PDF processing
- Automatically handles oversized pages by splitting them further
- Maintains backward compatibility with existing chunking methods
- Integrates seamlessly with existing vector database handlers

#!/usr/bin/env python3
"""
Example usage of DuckDB Faiss Handler
"""

# Example SQL commands to use with the handler:

example_sql = """
-- 1. Create database connection
CREATE DATABASE vector_db
WITH
    ENGINE = 'duckdb_faiss',
    PARAMETERS = {
        "metric": "cosine",
        "backend": "hnsw",
        "use_gpu": false,
        "hnsw_m": 32,
        "hnsw_ef_search": 64
    };

-- 2. Create table with vector column
CREATE TABLE vector_db.documents (
    id TEXT PRIMARY KEY,
    content TEXT,
    metadata JSON,
    title TEXT,
    category TEXT,
    embeddings VECTOR(384)
);

-- 3. Insert data
INSERT INTO vector_db.documents (id, content, metadata, title, category, embeddings)
VALUES 
    ('doc1', 'This is a news article about technology', '{"source": "news_site", "date": "2024-01-01"}', 'Tech News', 'news', '[0.1, 0.2, 0.3, ...]'),
    ('doc2', 'A scientific paper about AI research', '{"source": "journal", "date": "2024-01-02"}', 'AI Research', 'science', '[0.4, 0.5, 0.6, ...]'),
    ('doc3', 'Business update on market trends', '{"source": "business_site", "date": "2024-01-03"}', 'Market Update', 'business', '[0.7, 0.8, 0.9, ...]');

-- 4. Vector similarity search (legacy syntax)
SELECT id, content, title, 
       embeddings <-> '[0.1, 0.2, 0.3, ...]' as distance
FROM vector_db.documents 
WHERE embeddings <-> '[0.1, 0.2, 0.3, ...]' < 0.5
ORDER BY embeddings <-> '[0.1, 0.2, 0.3, ...]'
LIMIT 10;

-- 4b. Vector similarity search (enhanced syntax - cleaner!)
SELECT id, content, title, 
       embeddings <-> '[0.1, 0.2, 0.3, ...]' as distance
FROM vector_db.documents 
WHERE distance < 0.5
ORDER BY distance
LIMIT 10;

-- 5. Metadata filtering
SELECT id, content, title, category
FROM vector_db.documents 
WHERE category = 'news'
  AND metadata->>'source' = 'news_site';

-- 6. Hybrid search (metadata + vector) - legacy syntax
SELECT id, content, title, 
       embeddings <-> '[0.1, 0.2, 0.3, ...]' as distance
FROM vector_db.documents 
WHERE category = 'news'
  AND embeddings <-> '[0.1, 0.2, 0.3, ...]' < 0.5
ORDER BY embeddings <-> '[0.1, 0.2, 0.3, ...]'
LIMIT 10;

-- 6b. Hybrid search (metadata + vector) - enhanced syntax!
SELECT id, content, title, 
       embeddings <-> '[0.1, 0.2, 0.3, ...]' as distance
FROM vector_db.documents 
WHERE category = 'news'
  AND distance < 0.5
ORDER BY distance
LIMIT 10;

-- 7. Update document
UPDATE vector_db.documents 
SET content = 'Updated content', 
    embeddings = '[0.2, 0.3, 0.4, ...]'
WHERE id = 'doc1';

-- 8. Delete document
DELETE FROM vector_db.documents 
WHERE id = 'doc2';

-- 9. List tables
SHOW TABLES;

-- 10. Describe table structure
DESCRIBE vector_db.documents;
"""

print("DuckDB Faiss Handler - Example Usage")
print("=" * 50)
print(example_sql)

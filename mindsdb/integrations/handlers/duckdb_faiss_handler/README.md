# DuckDB + Faiss Handler

This handler combines DuckDB for metadata storage and SQL filtering with Faiss for high-performance vector similarity search.

## Features

- **DuckDB**: Store metadata, content, and IDs with full SQL filtering capabilities
- **Faiss**: High-speed vector indexing and similarity search (CPU/GPU support)
- **Hybrid Search**: Combine metadata filtering with vector similarity search
- **Persistence**: Automatic persistence via MindsDB's handler storage system

## Configuration

### Connection Parameters

- `metric`: Distance metric - "cosine" or "l2" (default: "cosine")
- `backend`: Faiss backend - "ivf", "flat", "hnsw" (default: "hnsw")
- `use_gpu`: Enable GPU acceleration (default: False)
- `nlist`: IVF parameter for clustering (default: 1024)
- `nprobe`: IVF search parameter (default: 32)
- `hnsw_m`: HNSW connectivity parameter (default: 32)
- `hnsw_ef_search`: HNSW search parameter (default: 64)
- `persist_directory`: Optional custom storage path

## Usage

### Create Database Connection

```sql
CREATE DATABASE faiss_db
WITH
    ENGINE = 'duckdb_faiss',
    PARAMETERS = {};
```

### Create knowledge base

```sql
create knowledge base kb_faiss
using storage = faiss_db.kb_faiss,
embedding_model={"provider": "openai", "model_name": "text-embedding-3-small"},
metadata_columns=["title", "category"];
```

### Insert Data

```sql
INSERT INTO kb_faiss (id, content, metadata, title, category, embeddings)
VALUES 
    ('doc1', 'This is a news article about technology', 'Tech News', 'news'),
    ('doc2', 'A scientific paper about AI research', 'AI Research', 'science'),
    ('doc3', 'Business update on market trends', 'Market Update', 'business');
```

### Vector Search

```sql
-- Vector similarity search
SELECT * FROM kb_faiss 
WHERE content = 'paper' and distance < 0.5
LIMIT 10;

-- With metadata search
SELECT * FROM kb_faiss 
WHERE content = 'paper' and category = 'news'
LIMIT 10;

-- Hybrid search (keyword + vector)
SELECT * FROM kb_faiss 
WHERE content = 'paper' and category = 'news' and hybrid_search=true
LIMIT 10;
```

### Delete document
```sql
DELETE FROM kb_faiss
WHERE id = 'doc2';
```

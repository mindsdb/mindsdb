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

### Vector Column Syntax

Use `VECTOR(dimension)` to define vector columns:

```sql
CREATE TABLE my_table (
    id TEXT PRIMARY KEY,
    content TEXT,
    metadata JSON,
    embeddings VECTOR(384)
);
```

## Usage

### Create Database Connection

```sql
CREATE DATABASE vector_db
WITH
    ENGINE = 'duckdb_faiss',
    PARAMETERS = {
        "metric": "cosine",
        "backend": "hnsw",
        "use_gpu": false
    };
```

### Create Table with Vector Column

```sql
CREATE TABLE vector_db.documents (
    id TEXT PRIMARY KEY,
    content TEXT,
    metadata JSON,
    embeddings VECTOR(384)
);
```

### Insert Data

```sql
INSERT INTO vector_db.documents (id, content, metadata, embeddings)
VALUES (
    'doc1',
    'Sample document content',
    '{"category": "news", "source": "web"}',
    '[0.1, 0.2, 0.3, ...]'
);
```

### Vector Search

```sql
-- Vector similarity search (legacy syntax)
SELECT * FROM vector_db.documents 
WHERE embeddings <-> '[0.1, 0.2, 0.3, ...]' < 0.5
ORDER BY embeddings <-> '[0.1, 0.2, 0.3, ...]'
LIMIT 10;

-- Vector similarity search (enhanced syntax - cleaner!)
SELECT id, content, title, 
       embeddings <-> '[0.1, 0.2, 0.3, ...]' as distance
FROM vector_db.documents 
WHERE distance < 0.5
ORDER BY distance
LIMIT 10;

-- Hybrid search (metadata filter + vector search) - enhanced syntax!
SELECT id, content, title, 
       embeddings <-> '[0.1, 0.2, 0.3, ...]' as distance
FROM vector_db.documents 
WHERE metadata->>'category' = 'news'
  AND distance < 0.5
ORDER BY distance
LIMIT 10;
```

## Enhanced Vector Search Syntax

The handler supports both legacy and enhanced vector search syntax:

### Legacy Syntax
```sql
-- Repeats the vector operation multiple times
SELECT * FROM documents 
WHERE embeddings <-> '[0.1, 0.2, ...]' < 0.5
ORDER BY embeddings <-> '[0.1, 0.2, ...]'
```

### Enhanced Syntax (Recommended)
```sql
-- Cleaner, more intuitive SQL
SELECT id, content, 
       embeddings <-> '[0.1, 0.2, ...]' as distance
FROM documents 
WHERE distance < 0.5
ORDER BY distance
```

**Benefits of Enhanced Syntax:**
- **Cleaner SQL**: No need to repeat vector operations
- **Better Performance**: Calculate distance once, use multiple times
- **More Intuitive**: Standard SQL patterns for computed columns
- **Flexible**: Support complex WHERE clauses on distance
- **Compatible**: Works with existing SQL tools and ORMs

### Supported Distance Operations
- `WHERE distance < 0.5` - Less than threshold
- `WHERE distance > 0.1` - Greater than threshold  
- `WHERE distance BETWEEN 0.1 AND 0.5` - Range filtering
- `WHERE distance = 0.0` - Exact match

## Performance Notes

- Faiss indices are kept in memory for fast access
- DuckDB provides efficient metadata filtering
- Hybrid search optimizes by filtering metadata first when possible
- GPU acceleration available with `use_gpu: true` and faiss-gpu installation

## Dependencies

- duckdb>=0.9.0
- faiss-cpu>=1.7.4 (or faiss-gpu for GPU support)
- numpy>=1.21.0
- pandas>=1.3.0

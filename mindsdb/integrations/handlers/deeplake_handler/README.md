# Deep Lake Handler

Deep Lake handler for MindsDB provides integration with Deep Lake vector database, enabling storage and retrieval of vector embeddings along with associated metadata.

## Deep Lake

Deep Lake is a Database for AI that stores vectors, images, texts, videos, and more. It provides efficient vector search capabilities and integrates well with machine learning workflows.

## Implementation

This handler is implemented by extending the `VectorStoreHandler` class and provides:

- Vector similarity search with multiple distance metrics
- Metadata filtering and querying
- Support for local and cloud-based datasets
- Integration with Deep Lake's tensor database capabilities

## Usage

To use this handler, you need to create a connection to Deep Lake:

### Local Dataset

```sql
CREATE DATABASE my_deeplake
WITH ENGINE = 'deeplake',
PARAMETERS = {
    "dataset_path": "./my_local_dataset",
    "create_embedding_dim": 384,
    "search_distance_metric": "cosine"
};
```

### Cloud Dataset

```sql
CREATE DATABASE my_deeplake
WITH ENGINE = 'deeplake',
PARAMETERS = {
    "dataset_path": "hub://my_org/my_dataset",
    "token": "your_deeplake_token",
    "org_id": "your_org_id",
    "runtime": {"tensor_db": true}
};
```

## Connection Parameters

- `dataset_path` (required): Path to Deep Lake dataset (local, S3, GCS, Azure, or hub://)
- `token` (optional): Deep Lake token for authentication
- `org_id` (optional): Organization ID for hub datasets
- `runtime` (optional): Runtime configuration (e.g., {"tensor_db": true})
- `read_only` (optional): Open dataset in read-only mode
- `search_default_limit` (optional): Default limit for searches (default: 10)
- `search_distance_metric` (optional): Distance metric: 'l2', 'cosine', 'max_inner_product'
- `search_exec_option` (optional): Execution option: 'python', 'compute_engine', 'tensor_db'
- `create_overwrite` (optional): Overwrite existing dataset when creating
- `create_embedding_dim` (optional): Embedding dimension for new datasets
- `create_max_chunk_size` (optional): Maximum chunk size for data storage
- `create_compression` (optional): Compression algorithm

## Example Queries

### Create Table
```sql
CREATE TABLE my_vectors (
    id TEXT,
    content TEXT,
    embeddings ARRAY,
    metadata JSON
);
```

### Insert Vectors
```sql
INSERT INTO my_vectors (id, content, embeddings, metadata)
VALUES 
    ('1', 'Sample text', '[0.1, 0.2, 0.3, ...]', '{"category": "document"}'),
    ('2', 'Another text', '[0.4, 0.5, 0.6, ...]', '{"category": "article"}');
```

### Vector Similarity Search
```sql
SELECT id, content, distance
FROM my_vectors
WHERE search_vector = '[0.1, 0.2, 0.3, ...]'
AND metadata.category = 'document'
ORDER BY distance
LIMIT 5;
```

### Regular Query
```sql
SELECT id, content, embeddings
FROM my_vectors
WHERE metadata.category = 'document'
LIMIT 10;
```

## Features

- **Vector Search**: Efficient similarity search with configurable distance metrics
- **Metadata Filtering**: Filter results based on metadata fields
- **Flexible Storage**: Support for local files, cloud storage, and managed services
- **Scalable**: Built on Deep Lake's optimized storage format
- **Multi-modal**: Can store various data types beyond just vectors

## Requirements

- `deeplake>=3.8.0`

## Supported Operations

- `CREATE TABLE`: Create new Deep Lake datasets
- `INSERT`: Add vectors and metadata
- `SELECT`: Query with vector search and metadata filtering  
- `DELETE`: Remove records by ID
- `DROP TABLE`: Delete datasets (with limitations)
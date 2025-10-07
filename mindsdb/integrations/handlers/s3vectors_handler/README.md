# AWS S3 Vectors Handler

This is the implementation of the AWS S3 Vectors handler for MindsDB.

## AWS S3 Vectors

AWS S3 Vectors (part of S3 Tables) is a fully-managed vector storage and search service that provides sub-second similarity search capabilities. It integrates seamlessly with AWS services and leverages existing S3 security and IAM controls.

## Implementation

This handler uses the `boto3` Python library to connect to the AWS S3 Vectors service.

### Connection Parameters

The required parameter to establish a connection is:

* `vector_bucket`: The name of the S3 vector bucket

Optional parameters include:

* `aws_access_key_id`: AWS access key ID (not needed if using IAM role)
* `aws_secret_access_key`: AWS secret access key (not needed if using IAM role)
* `aws_session_token`: AWS session token (for temporary credentials)
* `region_name`: AWS region (default: us-east-1)

These optional parameters are used with `CREATE TABLE` statements:

* `dimension`: Dimensions of the vectors to be stored in the index (default: 1536)
* `metric`: Distance metric to be used for similarity search. Options: `cosine`, `euclidean`, `dotproduct` (default: cosine)

## Authentication

The handler supports multiple authentication methods, in the following priority order:

1. **Explicit Credentials**: Provide `aws_access_key_id` and `aws_secret_access_key`
2. **IAM Role**: Uses IAM role from the environment (e.g., ECS task role, EC2 instance profile)
3. **AWS Profile**: Uses AWS CLI profiles for local development

## Usage

### Connecting with IAM Role (Recommended for Production)

When running MindsDB in AWS (ECS, EKS, EC2), use IAM roles:

```sql
CREATE DATABASE s3vec_db
WITH ENGINE = "s3vectors",
PARAMETERS = {
   "region_name": "us-east-1",
   "vector_bucket": "my-vector-bucket"
};
```

### Connecting with Explicit Credentials

For development or external access:

```sql
CREATE DATABASE s3vec_db
WITH ENGINE = "s3vectors",
PARAMETERS = {
   "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
   "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
   "region_name": "us-east-1",
   "vector_bucket": "my-vector-bucket"
};
```

## Knowledge Base Integration

S3 Vectors works seamlessly with MindsDB's Knowledge Base system, including automatic document chunking and embedding generation.

### Creating a Knowledge Base

**Important**: You must specify both the database name and table name (index name) using the format `database.table`:

```sql
CREATE KNOWLEDGE BASE my_kb
USING
  vector_database = s3vec_db.my_index,
  embedding_model = {
    "provider": "openai",
    "model_name": "text-embedding-3-small"
  },
  preprocessing = {
    "chunk_size": 500,
    "chunk_overlap": 50
  };
```

This will:
- Store vectors in the S3 Vectors database `s3vec_db`
- Use (or create) the index named `my_index`
- Automatically chunk documents with 500 character chunks and 50 character overlap
- Generate embeddings using OpenAI's text-embedding-3-small model

### Inserting Documents

Documents are automatically chunked and embedded by MindsDB:

```sql
-- Insert from file
INSERT INTO my_kb (content)
VALUES ('path/to/document.pdf');

-- Insert from query
INSERT INTO my_kb (content)
SELECT text_column FROM my_database.documents;

-- Insert with metadata
INSERT INTO my_kb (content, metadata)
VALUES
  ('Document content here', '{"source": "manual", "category": "technical"}');
```

### Querying the Knowledge Base

Perform similarity search:

```sql
-- Basic similarity search
SELECT * FROM my_kb
WHERE content = "What is machine learning?"
LIMIT 5;

-- With metadata filtering
SELECT * FROM my_kb
WHERE content = "AWS services"
  AND metadata.category = "technical"
LIMIT 10;

-- With relevance threshold
SELECT * FROM my_kb
WHERE content = "data analysis"
  AND relevance >= 0.8
LIMIT 5;
```

## Direct Vector Operations

You can also work directly with vector indexes (without Knowledge Base):

### Creating a Vector Index

```sql
CREATE TABLE s3vec_db.my_vectors (
  SELECT * FROM other_db.embeddings_table LIMIT 100
);
```

### Inserting Vectors

```sql
INSERT INTO s3vec_db.my_vectors (id, embeddings, metadata)
VALUES (
  'doc_1',
  '[0.1, 0.2, 0.3, ...]',  -- 1536-dimensional vector
  '{"title": "Example", "author": "John Doe"}'
);
```

### Querying by Vector

```sql
SELECT * FROM s3vec_db.my_vectors
WHERE search_vector = '[0.1, 0.2, 0.3, ...]'
LIMIT 10;
```

### Querying by ID

```sql
SELECT * FROM s3vec_db.my_vectors
WHERE id = "doc_1";
```

### Deleting Vectors

```sql
DELETE FROM s3vec_db.my_vectors
WHERE id = "doc_1";
```

### Dropping an Index

```sql
DROP TABLE s3vec_db.my_vectors;
```

## Features

### Document Chunking Support

S3 Vectors fully supports MindsDB's document preprocessing features:

- **Text Splitting**: Automatically splits long documents into manageable chunks
- **Contextual Chunking**: Preserves context across chunk boundaries
- **JSON Chunking**: Handles structured JSON data
- **Multiple Content Columns**: Process multiple text fields per document
- **Metadata Preservation**: Maintains metadata across all chunks

### Distance Metrics

The handler supports three distance metrics:

- `cosine`: Cosine similarity (default, recommended for most use cases)
- `euclidean` (or `l2`): Euclidean distance
- `dotproduct` (or `ip`): Inner product

### Metadata Filtering

Filter results by metadata fields:

```sql
SELECT * FROM my_kb
WHERE content = "search query"
  AND metadata.category = "technical"
  AND metadata.year > 2020;
```

## Limitations

- Deletion by metadata filter is not yet implemented (delete by ID works)
- The `content` field is not stored separately in S3 Vectors (only in metadata)
- Requires AWS S3 Tables service with vector support enabled
- Region availability may vary

## Performance Tips

1. **Use IAM Roles**: Avoid managing credentials by using IAM roles in AWS environments
2. **Batch Inserts**: Insert data in batches for better performance
3. **Appropriate Chunk Size**: Use chunk sizes between 200-1000 tokens for best results
4. **Metadata Indexing**: Structure metadata for efficient filtering
5. **Region Selection**: Deploy in the same region as your data sources

## Error Handling

Common errors and solutions:

- **"Invalid vectordatabase table name: Need the form 'database_name.table_name'"**:
  - When creating a Knowledge Base, you must specify both database and table (index) name
  - Correct format: `vector_database = s3vec_db.my_index`
  - Incorrect: `vector_database = s3vec_db`

- **"vector_bucket must be provided"**: Ensure you specify the bucket name in connection parameters

- **"NoSuchVectorBucket"**: Verify the bucket name and that it exists in your AWS account

- **"Invalid dimension"**: Ensure vector dimensions match between embeddings and index

- **"Access Denied"**: Check IAM permissions for S3 Tables operations

## Required AWS Permissions

Your IAM role/user needs the following permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3tables:ListVectorBuckets",
        "s3tables:ListIndexes",
        "s3tables:CreateIndex",
        "s3tables:DeleteIndex",
        "s3tables:PutVectors",
        "s3tables:QueryVectors",
        "s3tables:GetVectors",
        "s3tables:DeleteVectors"
      ],
      "Resource": "*"
    }
  ]
}
```

## Example: Complete RAG Pipeline

Here's a complete example of setting up a RAG (Retrieval-Augmented Generation) system:

```sql
-- 1. Create S3 Vectors database connection
CREATE DATABASE s3vec_db
WITH ENGINE = "s3vectors",
PARAMETERS = {
  "region_name": "us-east-1",
  "vector_bucket": "my-docs-vectors"
};

-- 2. Create Knowledge Base with OpenAI embeddings
-- Note: Use database.table format (s3vec_db.company_docs_index)
CREATE KNOWLEDGE BASE company_docs
USING
  vector_database = s3vec_db.company_docs_index,
  embedding_model = {
    "provider": "openai",
    "model_name": "text-embedding-3-small"
  },
  preprocessing = {
    "chunk_size": 500,
    "chunk_overlap": 50
  };

-- 3. Load documents from S3
INSERT INTO company_docs (content)
SELECT content FROM files
WHERE path LIKE 's3://my-docs-bucket/*.pdf';

-- 4. Create an AI agent with RAG
CREATE AGENT company_assistant
USING
  model = "gpt-4",
  skills = ["company_docs"];

-- 5. Query the agent
SELECT * FROM company_assistant
WHERE question = "What is our vacation policy?";
```

## Support

For issues or questions:
- GitHub: https://github.com/mindsdb/mindsdb
- Documentation: https://docs.mindsdb.com
- Community: https://community.mindsdb.com

#Add Upstash Vector Handler Integration to MindsDB

**Description**

This issue proposes the addition of a new handler to integrate MindsDB with Upstash Vector, a serverless vector database. Upstash Vector enables efficient storage, retrieval, and similarity querying of vector embeddings with high performance and low latency. It uses the DiskANN algorithm and supports similarity functions like Euclidean distance and Cosine similarity. Additionally, it allows metadata attachment and filtering.

**Motivation**

MindsDB currently supports integrations with vector databases such as Chroma and Pinecone. Adding support for Upstash Vector would:

Expand the vector database ecosystem for MindsDB users.
Provide a serverless solution with high scalability.
Leverage Upstash's unique DiskANN-based approach for efficient similarity querying.
This integration would enable users to:

Create and manage collections in Upstash Vector.
Insert vector embeddings and attach metadata.
Perform similarity searches and apply metadata-based filters.

**Proposed Implementation**

The implementation will follow the structure and design of existing handlers (e.g., ChromaDB Handler):

**Handler Implementation:
Develop a Python handler class UpstashVectorHandler.
Include methods for connecting to Upstash, managing collections, inserting vectors, and querying for similar vectors.

**Configuration Options:**
host: Upstash Vector host endpoint.
api_key: API key for authentication.


*Usage:*
Example syntax for connecting to Upstash Vector:

```
CREATE DATABASE upstash_db
WITH ENGINE = "upstash_vector",
PARAMETERS = {
    "host": "https://example.upstash.io",
    "api_key": "your_api_key"
};
```

**References:**

Upstash Vector Documentation
Existing MindsDB Handlers

**Acceptance Criteria**

A new UpstashVectorHandler is added to the MindsDB integrations folder.
The handler is tested for the following functionalities:
Connection establishment.
Collection management (create, delete).
Vector insertion with metadata.
Similarity querying with filters.
Documentation for the integration is provided, including usage examples.
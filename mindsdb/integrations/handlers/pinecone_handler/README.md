# Pinecone Handler

This is the implementation of the Pinecone for MindsDB.

## ChromaDB

Pinecone is a vector database which is fully-managed, developer-friendly, and easily scalable.

## Implementation

This handler uses `chromadb` python library connect to a chromadb instance, it uses langchain to make use of their pre-existing semantic search functionality

The required arguments to establish a connection are:

* `api_key`: the API key that can be found in your pinecone account
* `environment`: the environment name corresponding to the `api_key`

## Tasks

- [ ] `CREATE TABLE` support
    - Creating a table in Pinecone requires 2 additional parameters: dimension (int) and metric (string enum)
- [ ] Support for [namespaces](https://docs.pinecone.io/docs/namespaces)

## Usage

In order to make use of this handler and connect to an environment, use the following syntax:

```sql
CREATE DATABASE pinecone_dev
WITH ENGINE = "pinecone",
PARAMETERS = {
   "api_key": "...",
   "environment": "..."
};
```

-----------------------------------------



You can insert data into a new collection like so

```sql
create table chroma_dev.fda_10 (
select * from mysql_demo_db.demo_fda_context limit 10);
```

You can query a collection within your ChromaDB as follows:

```sql
SELECT *
FROM chroma_dev.fda_10
Limit 5
```

You can also filter a collection on metadata

```sql
SELECT *
FROM chroma_dev.fda_context_10
Where meta_data_filter = "column:type_of_product"
```

Or alternatively it is possible to do a semantic search

```sql
SELECT *
FROM chroma_dev.fda_context_10
Where search_query='products for cold' limit 20

```

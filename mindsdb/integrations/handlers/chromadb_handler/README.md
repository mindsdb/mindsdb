# ChromaDB Handler

This is the implementation of the ChromaDB for MindsDB.

## ChromaDB

Chroma is the open-source embedding database. Chroma makes it easy to build LLM apps by making knowledge, facts, and skills pluggable for LLMs.

## Implementation

This handler uses `chromadb` python library connect to a chromadb instance, it uses langchain to make use of their pre-existing semantic search functionality

The required arguments to establish a connection are:

* `chroma_api_impl`: the api implementation, likely should be `rest` or `local`
* `chroma_server_host`: the host name or IP address of the ChromaDB instance
* `chroma_server_http_port`: the port to use when connecting
* `persist_directory`: the directory to use for persisting data, this should only be used when `chroma_api_impl` is set to `local`


## Usage

In order to make use of this handler and connect to a ChromaDB server in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE chroma_dev
WITH ENGINE = "chromadb",
PARAMETERS = {
   "chroma_api_impl": "rest",
   "chroma_server_host": "localhost",
   "chroma_server_http_port": 8000,
    }
```

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

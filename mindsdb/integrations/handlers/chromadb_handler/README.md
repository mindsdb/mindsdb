# ChromaDB Handler

This is the implementation of the ChromaDB for MindsDB.

## ChromaDB

Chroma is the open-source embedding database. Chroma makes it easy to build LLM apps by making knowledge, facts, and skills pluggable for LLMs.

## Implementation

This handler uses `chromadb` python library connect to a chromadb instance, it uses langchain to make use of their pre-existing semantic search functionality

The required arguments to establish a connection are:

* `host`: the host name or IP address of the ChromaDB instance
* `port`: the port to use when connecting
* `persist_directory`: the directory to use for persisting data


## Usage

In order to make use of this handler and connect to a hosted ChromaDB instance in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE chroma_dev
WITH ENGINE = "chromadb",
PARAMETERS = {
   "host": "localhost",
   "port": "8000"
    }
```

Another option is to use in memory ChromaDB instance, you can do so by using the following syntax:

```sql
CREATE DATABASE chroma_dev
WITH ENGINE = "chromadb",
PARAMETERS = {
   "persist_directory": "<persist_directory>"
    }
```

You can insert data into a new collection like so

```sql
create table chroma_dev.test_embeddings (
SELECT embeddings,'{"source": "fda"}' as metadata FROM mysql_demo_db.test_embeddings
);
```

You can query a collection within your ChromaDB as follows:

```sql
SELECT * FROM chroma_dev.test_embeddings;
```

filter by metadata

```sql
SELECT * FROM chroma_dev.test_embeddings
where `metadata.source` = "fda";
```

search for similar embeddings

```sql
SELECT * FROM chroma_dev.test_embeddings
WHERE search_vector = (select embeddings from mysql_demo_db.test_embeddings limit 1);
```

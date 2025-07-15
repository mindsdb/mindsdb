---
title: ChromaDB
sidebarTitle: ChromaDB
---

In this section, we present how to connect ChromaDB to MindsDB.

[ChromaDB](https://www.trychroma.com/) is the open-source embedding database. Chroma makes it easy to build LLM apps by making knowledge, facts, and skills pluggable for LLMs.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).
2. To connect ChromaDB to MindsDB, install the required dependencies following [this instruction](/setup/self-hosted/docker#install-dependencies).
3. Install or ensure access to ChromaDB.

## Connection

This handler is implemented using the `chromadb` Python library.

To connect to a remote ChromaDB instance, use the following statement:

```sql
CREATE DATABASE chromadb_datasource
WITH ENGINE = 'chromadb'
PARAMETERS = {
    "host": "YOUR_HOST",
    "port": YOUR_PORT,
    "distance": "l2/cosine/ip" -- optional, default is cosine
}
```

The required parameters are:

* `host`: The host name or IP address of the ChromaDB instance.
* `port`: The TCP/IP port of the ChromaDB instance.
* `distance`: It defines how the distance between vectors is calculated. Available method include l2, cosine, and ip, as [explained here](https://docs.trychroma.com/docs/collections/configure).

To connect to an in-memory ChromaDB instance, use the following statement:

```sql
CREATE DATABASE chromadb_datasource
WITH ENGINE = "chromadb",
PARAMETERS = {
    "persist_directory": "YOUR_PERSIST_DIRECTORY",
    "distance": "l2/cosine/ip" -- optional
}
```

The required parameters are:

* `persist_directory`: The directory to use for persisting data.
* `distance`: It defines how the distance between vectors is calculated. Available method include l2, cosine, and ip, as [explained here](https://docs.trychroma.com/docs/collections/configure).

## Usage

Now, you can use the established connection to create a collection (or table in the context of MindsDB) in ChromaDB and insert data into it:

```sql
CREATE TABLE chromadb_datasource.test_embeddings (
    SELECT embeddings,'{"source": "fda"}' as metadata
    FROM mysql_datasource.test_embeddings
);
```

<Note>
`mysql_datasource` is another MindsDB data source that has been created by connecting to a MySQL database. The `test_embeddings` table in the `mysql_datasource` data source contains the embeddings that we want to store in ChromaDB.
</Note>

You can query your collection (table) as shown below:

```sql
SELECT * 
FROM chromadb_datasource.test_embeddings;
```

To filter the data in your collection (table) by metadata, you can use the following query:

```sql
SELECT * 
FROM chromadb_datasource.test_embeddings
WHERE `metadata.source` = "fda";

```

To conduct a similarity search, the following query can be used:

```sql
SELECT *
FROM chromadb_datasource.test_embeddings
WHERE search_vector = (
    SELECT embeddings
    FROM mysql_datasource.test_embeddings
    LIMIT 1
);

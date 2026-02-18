# LanceDB Handler

This is the implementation of the LanceDB for MindsDB.

## LanceDB

LanceDB is an open-source database for vector-search built with persistent storage, which greatly simplifies retrieval, filtering and management of embeddings. LanceDB is the first and only vector database that supports full reproducibility natively. Taking advantage of Lance columnar format. Refer this notebook https://github.com/lancedb/lancedb/blob/main/docs/src/notebooks/reproducibility.ipynb

## Implementation

This handler uses `lancedb` python library connect to a LanceDB instance.

The required arguments to establish a connection are:

* `persist_directory`: The uri of the LanceDB database. Usually a local path.
* `api_key`: If presented, connect to LanceDB cloud. Otherwise, connect to a database on file system or cloud storage.
* `region`: The region to use for LanceDB Cloud.
* `host_override`: The override url for LanceDB Cloud.

Refer https://lancedb.github.io/lancedb/python/python/

## Usage

In order to make use of this handler and connect to a local LanceDB instance in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE lancedb
WITH ENGINE = "lancedb",
PARAMETERS = {
    "persist_directory" : "~/lancedb"
};
```

You can insert data into a new collection like so

```sql
CREATE TABLE lancedb.test_data15
(SELECT * FROM myexample_db.lance_test_data);
```

You can query a collection within your LanceDB as follows:

```sql
select * from lancedb.test_data15;
```

filter

```sql
select * from lancedb.test_data15
where id = '1';
```

search for similar embeddings

```sql
select * from lancedb.test_data15
where search_vector = '[1.5, 1.5]'
;
```

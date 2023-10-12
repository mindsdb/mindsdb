# Xata Handler

This is the implementation of the Xata for MindsDB.

## Xata

Xata is a serverless database platform powered by PostgreSQL. It aims to make the data part easy with the functionality your application needs to evolve and scale.

## Implementation

This handler uses `xata` python library connect to a xata instance

The required arguments to establish a connection are:

* `db_url`: Xata database url with region, database and, optionally the branch information
* `api_key`: personal Xata API key

Optional arguments to create a table are:

* `dimension`: default dimension of embeddings vector used to create table when using create (default=8)











## Usage

In order to make use of this handler and connect to a hosted Xata instance in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE xata_dev
WITH ENGINE = "xata",
PARAMETERS = {
   "chroma_server_host": "localhost",
   "chroma_server_http_port": 8000
}
```

Another option is to use in memory Xata instance, you can do so by using the following syntax:

```sql
CREATE DATABASE xata_dev
WITH ENGINE = "xata",
PARAMETERS = {
   "persist_directory": <persist_directory>
    }
```

You can insert data into a new collection like so

```sql
create table xata_dev.test_embeddings (
SELECT embeddings,'{"source": "fda"}' as metadata FROM mysql_demo_db.test_embeddings
);
```

You can query a collection within your Xata as follows:

```sql
SELECT * FROM xata_dev.test_embeddings;
```

filter by metadata

```sql
SELECT * FROM xata_dev.test_embeddings
where `metadata.source` = "fda";
```

search for similar embeddings

```sql
SELECT * FROM xata_dev.test_embeddings
WHERE search_vector = (select embeddings from mysql_demo_db.test_embeddings limit 1);
```

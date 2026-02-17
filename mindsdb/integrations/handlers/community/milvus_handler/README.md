# Milvus Handler

This is the implementation of the Milvus for MindsDB.

## Milvus

Milvus is an open-source and blazing fast vector database built for scalable similarity search.

## Implementation

This handler uses `pymilvus` python library connect to a Milvus instance.

The required arguments to establish a connection are:

* `uri`: uri for milvus database, can be set to local ".db" file or docker or cloud service
* `token`: token to support docker or cloud service according to uri option

The optional arguments to establish a connection are:

These are used for `SELECT` queries:
* `search_default_limit`: default limit to be passed in select statements (default=100)
* `search_metric_type`: metric type used for searches (default="L2")
* `search_ignore_growing`: whether to ignore growing segments during similarity searches (default=False)
* `search_params`: specific to the `search_metric_type` (default={"nprobe": 10})

These are used for `CREATE` queries:
* `create_auto_id`: whether to auto generate id when inserting records with no ID (default=False)
* `create_id_max_len`: maximum length of the id field when creating a table (default=64)
* `create_embedding_dim`: embedding dimension for creating table (default=8)
* `create_dynamic_field`: whether or not the created tables have dynamic fields or not (default=True)
* `create_content_max_len`: max length of the content column (default=200)
* `create_content_default_value`: default value of content column (default='')
* `create_schema_description`: description of the created schemas (default='')
* `create_alias`: alias of the created schemas (default='default')
* `create_index_params`: parameters of the index created on embeddings column (default={})
* `create_index_metric_type`: metric used to create the index (default='L2')
* `create_index_type`: the type of index (default='AUTOINDEX')

For more information about how these perameters map to Milvus API, look at Milvus' documentation

## Usage

Before continuing, make sure that `pymilvus` version is same as your Milvus instance version. You can check and change the `requirements.txt` file in this directory to accomodate that. This integration is tested on version `2.3`

### Setting up milvus using docker locally

To set up docker locally, refer to this [link](https://milvus.io/docs/install_standalone-docker.md). You can deploy milvus as a cluster or as a standalone service.

### Creating connection

In order to make use of this handler and connect to a Milvus server in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE milvus_datasource
WITH
  ENGINE = 'milvus',
  PARAMETERS = {
    "uri": "./milvus_local.db",
    "token": "",
    "create_embedding_dim": 3,
    "create_auto_id": true
};
```

### Dropping connection

To drop the connection, use this command

```sql
DROP DATABASE milvus_datasource;
```

### Creating tables

To insert data from a pre-existing table, use `CREATE`

```sql
CREATE TABLE milvus_datasource.test
(SELECT * FROM sqlitedb.test);
```

### Dropping collections

Dropping a collection is not supported

### Querying and selecting

To query database using a search vector, you can use `search_vector` in `WHERE` clause

Caveats:
- If you omit `LIMIT`, the `search_default_limit` is used since Milvus requires it
- Metadata column is not supported, but if the collection has dynamic schema enabled, you can query like normal, see the example below
- Dynamic fields cannot be displayed but can be queried

```sql
SELECT * from milvus_datasource.test
WHERE search_vector = '[3.0, 1.0, 2.0, 4.5]'
LIMIT 10;
```

If you omit the `search_vector`, this becomes a basic search and `LIMIT` or `search_default_limit` amount of entries in collection are returned

```sql
SELECT * from milvus_datasource.test
```

You can use `WHERE` clause on dynamic fields like normal SQL

```sql
SELECT * FROM milvus_datasource.createtest
WHERE category = "science";
```

### Deleting records

You can delete entries using `DELETE` just like in SQL.

Caveats:
- Milvus only supports deleting entities with clearly specified primary keys
- You can only use `IN` operator

```sql
DELETE FROM milvus_datasource.test
WHERE id IN (1, 2, 3);
```

### Inserting records

You can also insert individual rows like so:

```sql
INSERT INTO milvus_test.testable (id,content,metadata,embeddings)
VALUES ("id3", 'this is a test', '{"test": "test"}', '[1.0, 8.0, 9.0]');
```

### Updating

Updating records is not supported by Milvus API. You can try using combination of `DELETE` and `INSERT`

# Milvus Handler

This is the implementation of the Milvus for MindsDB.

## Milvus

Milvus is an open-source and blazing fast vector database built for scalable similarity search.

## Implementation

This handler uses `pymilvus` python library connect to a Milvus instance.

The required arguments to establish a connection are:

* `alias`: alias of the Milvus connection to construct
* `host`: IP address of the Milvus server
* `port`: port of the Milvus server
* `user`: username of the Milvus server
* `password`: password of the username of the Milvus server

The optional arguments to establish a connection are:

These are used for `SELECT` queries:
* `search_metric_type`: metric type used for searches
* `search_ignore_growing`: whether to ignore growing segments during similarity searches
* `search_params`: specific to the `search_metric_type`

These are used for `CREATE` queries:
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

Before continuing, make sure that `pymilvus` version is same as your Milvus instance version.

### CREATE DATABASE

In order to make use of this handler and connect to a Milvus server in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE milvus_datasource
WITH
  ENGINE = 'milvus',
  PARAMETERS = {
    "alias": "default",
    "host": "127.0.0.1",
    "port": 19530,
    "user": "username",
    "password": "password",
    "search_metric_type": "L2",
    "search_ignore_growing": True,
    "search_params": {"nprobe": 10},
    "create_embedding_dim": 8,
    "create_dynamic_field": True,
    "create_content_max_len": 200,
    "create_content_default_value": "",
    "create_schema_description": "MindsDB generated table",
    "create_alias": "default",
    "create_index_params": {},
    "create_index_metric_type": "L2",
    "create_index_type": "AUTOINDEX"
};
```

### DROP DATABASE

To drop a collection use this command

```sql
DROP DATABASE milvus_datasource;
```

### SELECT

To query database using a search vector, you can use `search_vector` in `WHERE` clause

```sql
SELECT * from milvus_datasource.test
WHERE search_vector = '[3.0, 1.0, 2.0, 4.5]'
LIMIT 10;
```

One thing to note is that `LIMIT` is required to use `search_vector`

If you omit the `search_vector`, 100 entires in collection are returned

```sql
SELECT * from milvus_datasource.test
```

### UPDATE

Updating records is not supported by Milvus API. You can try using combination of `DELETE` and `INSERT`















You can insert data into a new collection like so

```sql
create table chroma_dev.fda_10 (
select * from mysql_demo_db.demo_fda_context limit 10);
```

You can query a collection within your Milvus as follows:

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

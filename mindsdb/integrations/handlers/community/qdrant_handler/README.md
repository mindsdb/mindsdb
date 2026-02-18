<div align="center">
  <a href="https://qdrant.tech/">
    <img height="100" width="450" style="display: inline-block;" src="https://github.com/qdrant/qdrant/raw/master/docs/logo.svg" alt="Qdrant">
  </a>
  <h1><a href="https://qdrant.tech/">qdrant.tech</a> handler for MindsDB<h1>
</div>

## About Qdrant 🚀

A High-performance, massive-scale vector database for the next generation of AI. Also available in the cloud.

## Implementation

The handler uses the [qdrant-client](https://github.com/qdrant/qdrant-client) Python library to establish a connection to a Qdrant instance.


## Usage
To use this handler and get started with Qdrant, the following syntax can be used.
```sql
CREATE DATABASE qdrant_test
WITH ENGINE = "qdrant",
PARAMETERS = {
    "location": ":memory:",
    "collection_config": {
        "size": 386,
        "distance": "Cosine"
    }
}
```
The available arguments for instantiating Qdrant can be found [here](https://github.com/mindsdb/mindsdb/blob/23a509cb26bacae9cc22475497b8644e3f3e23c3/mindsdb/integrations/handlers/qdrant_handler/qdrant_handler.py#L408-L468).

## Creating a new table

- Qdrant options for creating a collection can be specified as `collection_config` in the `CREATE DATABASE` parameters.
- By default, UUIDs are set as collection IDs. You can provide your own IDs under the `id` column.
```sql
CREATE TABLE qdrant_test.test_table (
   SELECT embeddings,'{"source": "bbc"}' as metadata FROM mysql_demo_db.test_embeddings
);
```

## Querying the database

#### Perform a full retrieval using the following syntax.

```sql
SELECT * FROM qdrant_test.test_table
```
By default, the `LIMIT` is set to 10 and the `OFFSET` is set to 0.

#### Perform a similarity search using your embeddings, like so
```sql
SELECT * FROM qdrant_test.test_table
WHERE search_vector = (select embeddings from mysql_demo_db.test_embeddings limit 1)
```

#### Perform a search using filters
```sql
SELECT * FROM qdrant_test.test_table
WHERE `metadata.source` = 'bbc';
```

#### Delete entries using IDs
```sql
DELETE FROM qtest.test_table_6
WHERE id = 2
```

#### Delete entries using filters
```sql
DELETE * FROM qdrant_test.test_table
WHERE `metadata.source` = 'bbc';
```

#### Drop a table
```sql
 DROP TABLE qdrant_test.test_table;
```

## NOTICE
Qdrant supports payload indexing that vastly improves retrieval efficiency with filters and is highly recommended. Please note that this feature currently cannot be configured via MindsDB and must be set up separately if needed.

For detailed information on payload indexing, you can refer to the documentation available [here](https://qdrant.tech/documentation/concepts/indexing/#payload-index).

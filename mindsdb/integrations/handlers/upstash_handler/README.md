# Upstash Handler

This is the implementation of the Upstash Vector for MindsDB.

## Upstash Vector

Upstash Vector is a serverless vector database for managing vector embeddings, enabling efficient storage, retrieval, and similarity querying with high performance and low latency using the DiskANN algorithm.

## Implementation

This handler uses `upstash-vector` SDK which simplifies interaction with Upstash Vector through the [Upstash Vector API](https://upstash.com/docs/vector/api/get-started).

Before using `upstash-vector`, you’ll need to set up a vector database on [Upstash Console](https://console.upstash.com/vector/12eb0ca4-37a8-414d-a03d-18cbbbf47084). Once created, grab your URL and TOKEN from the Upstash console.

* `url`: The UPSTASH_VECTOR_REST_URL that can be found in the Upstash Console.
* `token`: The UPSTASH_VECTOR_REST_TOKEN that can be found in the Upstash Console.

These optional arguments are used with `CREATE` statements:

* `retries`: The number of times to retry a request if it fails. Default is 3.
* `retry_interval`: The interval between retries in seconds. Default is 1.

## Limitations

- [ ] `DROP TABLE` support: You can only delete the index from the Upstash console. In this handler, only the namespace is deleted, but the index is not.
- [ ] Deleting vectors based on metadata or content is not supported since Upstash Vector does not support these features.
- [ ] `OR` operator is not supported while filtering based on metadata.

## Usage

In order to make use of this handler and connect to an environment, use the following syntax:

```sql
CREATE DATABASE upstash_dev
WITH ENGINE = "upstash",
PARAMETERS = {
   "url": "UPSTASH_VECTOR_REST_URL",
   "token": "UPSTASH_VECTOR_REST_TOKEN",
   "retries": 3,
   "retry_interval": 1.0
};
```

Table names are used as the namespace in the Upstash Vector index. All of the following examples use the table name `temp`, i.e. the namespace `temp` in the Upstash Vector index.

You can upsert vectors into the Upstash Vector index, make sure the dimensions of the embeddings match the dimensions of the index:

```sql
INSERT INTO upstash_dev.temp (id, content, metadata, embeddings)
VALUES (
    'id1', 
    'this is a test', 
    '{"test": "test"}', 
    '[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]'
);
```

`INSERT` statements can also be used to update vectors in the Upstash Vector index:

```sql
INSERT INTO upstash_dev.temp (id, content, metadata, embeddings)
VALUES (
    'id1', 
    'this is a test again', 
    '{"test": "test again"}', 
    '[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]'
);
```

You can remove vectors from the Upstash Vector index based on their identifiers:

```sql
DELETE FROM upstash_dev.temp
WHERE id = 'id1';
```

You can fetch multiple vectors from the Upstash Vector index based on their identifiers:

```sql
SELECT * from upstash_dev.temp
WHERE id = '["id1", "id2"]';
```

This outputs the vectors with the identifiers `id1` and `id2` and their corresponding metadata and content.

You can query the Upstash Vector index based on the `search_vector`:

```sql
SELECT * from upstash_dev.temp
WHERE search_vector = '[1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8]'
LIMIT 2;
```

This outputs the similarity scores of each vector on top of other information.

If you do not provide a limit in the query, the default limit is 10.

If you provide both the `id` and `search_vector` in the query, the `id` will be ignored, and the query will be based on the `search_vector`.

You can filter your queries based on metadata as well:

```sql
SELECT * from upstash_dev.temp
WHERE search_vector = '[1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8]' AND temp.metadata.test = 'test';
```

To learn more about Upstash Vector, visit the [Upstash Vector documentation](https://upstash.com/docs/vector/overall/getstarted).
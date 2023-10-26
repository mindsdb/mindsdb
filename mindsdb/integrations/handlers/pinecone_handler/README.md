# Pinecone Handler

This is the implementation of the Pinecone for MindsDB.

## Pinecone

Pinecone is a vector database which is fully-managed, developer-friendly, and easily scalable.

## Implementation

This handler uses `pinecone-client` python library connect to a pinecone environment.

The required arguments to establish a connection are:

* `api_key`: the API key that can be found in your pinecone account
* `environment`: the environment name corresponding to the `api_key`

These optional arguments are used with `CREATE` statements:

* `dimension`: dimensions of the vectors to be stored in the index (default=8)
* `metric`: distance metric to be used for similarity search (default='cosine')
* `pods`: number of pods for the index to use, including replicas (default=1)
* `replicas`: the number of replicas. replicas duplicate your index. they provide higher availability and throughput (default=1)
* `pod_type`: the type of pod to use, refer to pinecone documentation (default='p1')

## Limitations

- [ ] `DROP TABLE` support
- [ ] Support for [namespaces](https://docs.pinecone.io/docs/namespaces)
- [ ] Display score/distance
- [ ] Support for creating/reading sparse values
- [ ] `content` column is not supported since it does not exist in Pinecone

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

You can query pinecone indexes (`temp` in the following examples) based on `id` or `search_vector`, but not both:

```sql
SELECT * from pinecone_dev.temp
WHERE id = "abc"
LIMIT 1
```

```sql
SELECT * from pinecone_dev.temp
WHERE search_vector = "[1,2,3,4,5,6,7,8]"
```

If you are using subqueries, make sure that the result is only a single row since the use of multiple search vectors is not allowed

```sql
SELECT * from pinecone_database.temp
WHERE search_vector = (
    SELECT embeddings FROM sqlitetesterdb.test WHERE id = 10
)
```

Optionally, you can filter based on metadata too:

```sql
SELECT * from pinecone_dev.temp
WHERE id = "abc" AND temp.metadata.hello < 100
```

You can delete records using `id` or `metadata` like so:

```sql
DELETE FROM pinecone_dev.temp
WHERE id = "abc"
```

Note that deletion through metadata is not supported in starter tier

```sql
DELETE FROM pinecone_dev.temp
WHERE temp.metadata.tbd = true
```

You can insert data into a new collection like so:

```sql
CREATE TABLE pinecone_dev.temp (
SELECT * FROM mysql_demo_db.temp LIMIT 10);
```

To update records, you can use insert statement. When there is a conflicting ID in pinecone index, the record is updated with new values. It might take a bit to see it reflected.

```sql
INSERT INTO pinecone_test.testtable (id,content,metadata,embeddings)
VALUES (
    'id1', 'this is a test', '{"test": "test"}', '[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]'
);
```

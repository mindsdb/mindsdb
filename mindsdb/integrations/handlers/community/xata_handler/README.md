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

Optional arguments for vector similarity searches are:

* `similarity_function`: similarity function to use for vector searches (default=cosineSimilarity)

## Limitations

- Performing queries on columns other than vector database specified columns is not supported
    - You can use metadata column for general query filtering
- Metadata filtering does not work on vector similarity search queries

## Usage

### Create Database

In order to make use of this handler and connect to a hosted Xata instance in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE xata_test
WITH
    ENGINE = 'xata',
    PARAMETERS = {{
     "api_key": "...",
     "db_url": "..."
};

```

### Create Table

You can insert data into a new table like so:

```sql
CREATE TABLE xata_test.testingtable (SELECT * FROM pg.df)
```

The table will have default parameters as specified in `CREATE DATABASE` command

### Select

You can query a collection within your Xata as follows:

```sql
SELECT * FROM xata_test.testingtable
```

```sql
SELECT * FROM xata_test.testingtable
WHERE testingtable.metadata.price > 10 AND testingtable.metadata.price <= 100
```

```sql
SELECT * FROM xata_test.testingtable
WHERE content LIKE 'test%'
```

### Similarity search

Search for similar embeddings by specifying search vector. Note that you cannot use metadata column with search vector.

```sql
SELECT * FROM xata_test.testingtable
WHERE search_vector = '[1.0, 2.0, 3.0]'
```

```sql
SELECT * FROM xata_test.testingtable
WHERE search_vector = '[1.0, 2.0, 3.0]'
AND content LIKE 'test%'
```

### Insert

You can insert into table in various ways:

```sql
INSERT INTO xata_test.testingtable (content,metadata,embeddings)
VALUES ('this is a test', '{"test": "test"}', '[1.0, 2.0, 3.0]')
```

```sql
INSERT INTO xata_test.testingtable (content,metadata,embeddings)
SELECT content,metadata,embeddings FROM pg.df2
```

## Delete

You can delete only using ID and = operator. Deleting non existing records does not have any effect.

```sql
DELETE FROM xata_test.testingtable
WHERE id = 'id2'
```

# Couchbase Vectore Store Handler

This is the implementation of the Couchbase Vector Store handler for MindsDB.

## Implementation

This handler was implemented using the `couchbase` library, the Python driver for Couchbase.

The required arguments to establish a connection are:

* `connection_string`: the connection string for the endpoint of the Couchbase server
* `bucket`: the bucket name to use when connecting with the Couchbase server
* `user`: the user to authenticate with the Couchbase server
* `password`: the password to authenticate the user with the Couchbase server
* `scope`:  scopes are a level of data organization within a bucket. If omitted, will default to `_default`

Note: The connection string expects either the couchbases:// or couchbase:// protocol.

If you are using Couchbase Capella, you can find the `connection_string` under the Connect tab.
To use vector search, You will need to create a vector index using Search UI on Couchbase Capella or Couchbase Self Managed Server.

## Usage

In order to make use of this handler and connect to a Couchbase server in MindsDB, the following syntax can be used. Note, the example uses the default `travel-sample` bucket which can be enabled from the couchbase UI with pre-defined scope and documents. 


### Creating connection

```sql
CREATE DATABASE couchbase_vectorsource
WITH
engine='couchbase',
parameters={
    "connection_string": "couchbase://localhost",
    "bucket":"travel-sample",
    "user": "admin",
    "password": "password",
    "scope": "inventory"
};
```

### Dropping connection

To drop the connection, use this command

```sql
DROP DATABASE couchbase_vectorsource;
```

### Creating tables

Now, you can use the established connection to create a collection (or table in the context of MindsDB) in Couchbase and insert data into it:

```sql
CREATE TABLE couchbase_vectorsource.test_embeddings (
    SELECT embeddings
    FROM mysql_datasource.test_embeddings
);
```

<Note>
`mysql_datasource` is another MindsDB data source that has been created by connecting to a MySQL database. The `test_embeddings` table in the `mysql_datasource` data source contains the embeddings that we want to store in Couchbase.
</Note>

### Querying and searching

You can query your collection (table) as shown below:

```sql
SELECT * 
FROM couchbase_vectorsource.test_embeddings;
```

To filter the data in your collection (table) by some filter, you can use the following query:

```sql
SELECT * 
FROM couchbase_vectorsource.test_embeddings
WHERE id = "some_id";

```

To perform a vector search, the following query can be used:

```sql
SELECT *
FROM couchbase_vectorsource.test_embeddings
WHERE embeddings = (
    SELECT embeddings
    FROM mysql_datasource.test_embeddings
    LIMIT 1
);
```

### Deleting records

You can delete documents using `DELETE` just like in SQL.


```sql
DELETE FROM couchbase_vectorsource.test_embeddings
WHERE `metadata.test` = 'test1';
```
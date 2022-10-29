# Materialize Handler

This is the implementation of the Materialize handler for MindsDB.

## Materialize
Materialize is a new storage engine for PostgreSQL, bringing a modern approach to database capacity, capabilities and performance to the world's most-loved database platform.

Materialize consists of an extension, building on the innovative table access method framework and other standard Postgres extension interfaces. By extending and enhancing the current table access methods, Materialize opens the door to a future of more powerful storage models that are optimized for cloud and modern hardware architectures.
## Implementation

This handler was implemented by extending postres connector.

The required arguments to establish a connection are:

* `host`: the host name of the Materialize connection 
* `port`: the port to use when connecting 
* `user`: the user to authenticate 
* `password`: the password to authenticate the user
* `database`: database name

## Usage

In order to make use of this handler and connect to a Materialize server in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE materialize_datasource
WITH ENGINE = "materialize",
PARAMETERS = { 
  "user": "USERNAME",
  "password": "|I<34|",
  "host": "hostname",
  "port": 6875,
  "database": "materialize"
}
```

Now, you can use this established connection to query your database as follows:

```sql
SELECT * FROM materialize_datasource.loveU LIMIT 10;
```
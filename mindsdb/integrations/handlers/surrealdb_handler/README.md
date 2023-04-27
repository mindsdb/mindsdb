# Surrealdb Handler

This is the implementation of the Surrealdb handler for MindsDB.

## Surrealdb

SurrealDB is an innovative NewSQL cloud database, suitable for serverless applications,
jamstack applications, single-page applications, and traditional applications.
It is unmatched in its versatility and financial value, with the ability for deployment on cloud,
on-premise, embedded, and edge computing environments.

## Implementation

This handler was implemented by using the python library `pysurrealdb`.

The required arguments to establish a connection are:

* `host`: the host name of the Surrealdb connection
* `port`: the port to use when connecting
* `user`: the user to authenticate
* `password`: the password to authenticate the user
* `database`: database name to be connected
* `namespace`: namespace name to be connected

## Usage

In order to make use of this handler and connect to a Surrealdb server in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE example
WITH ENGINE = 'surrealdb',
PARAMETERS = {
  "host": "localhost",
  "port": "8000",
  "user": "admin",
  "password": "password",
  "database": "test",
  "namespace": "test"
};
```

Now, you can use this established connection to query your database tables as follows:

```sql
SELECT * FROM example.table_name
```
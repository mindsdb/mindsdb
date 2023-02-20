# DuckDB Handler
This is the implementation of the DuckDB handler for MindsDB.

## DuckDB
DuckDB is an open-source analytical database system. DuckDB is designed for fast execution of analytical queries.
There are no external dependencies and the DBMS runs completly embedded within a host process, similar to SQLite.
DuckDB provides a rich SQL dialect with support for complex queries with transactional guarantees (ACID).

## Implementation
This handler was implemented using the `duckdb` python client library.

The required arguments to establish a connection are:

* `database`: the name of the DuckDB database file. May also be set to `:memory:`, which will create an in-memory database.

The optional arguments are:

* `read_only`: a flag that specifies if the connection should be made in read-only mode.
This is required if multiple processes want to access the same database file at the same time.

## Usage
In order to make use of this handler and connect to a DuckDB database in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE duckdb_datasource
WITH
engine='duckdb',
parameters={
    "database":"db.duckdb"
};
```

Now, you can use this established connection to query your database as follows:
```sql
SELECT * FROM duckdb_datasource.my_table;
```
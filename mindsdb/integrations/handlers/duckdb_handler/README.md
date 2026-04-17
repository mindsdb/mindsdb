# DuckDB Handler  
This is the implementation of the DuckDB handler for MindsDB.

## DuckDB
DuckDB is an open-source analytical database system. DuckDB is designed for fast execution of analytical queries.
There are no external dependencies, and the DBMS runs completely embedded within a host process, similar to SQLite.
DuckDB provides a rich SQL dialect with support for complex queries with transactional guarantees (ACID).

## Implementation  
This handler was implemented using the `duckdb` Python client library.

### DuckDB version
The DuckDB handler is currently using the `1.1.3` release version of the Python client library. In case of issues, make sure your DuckDB or MotherDuck database is compatible with this version. See the DuckDB handler [requirements.txt](requirements.txt) for details.

The required arguments to establish a connection are:

* `database`: the name of the DuckDB or MotherDuck database file.
  - Set to `:memory:` to create an in-memory database.
  - For MotherDuck, specify the database and motherduck_token.

Additional optional arguments include:

* `motherduck_token`: a token to authenticate with MotherDuck.
* `read_only`: a flag that specifies if the connection should be made in read-only mode.
  - This is required if multiple processes want to access the same database file simultaneously.

## Usage
To connect to a DuckDB or MotherDuck database in MindsDB, the following syntax can be used:

### DuckDB Example
```sql
CREATE DATABASE duckdb_datasource
WITH
engine='duckdb',
parameters={
    "database": "db.duckdb"
};
```

### MotherDuck Example
```sql
CREATE DATABASE md_datasource
WITH  
engine='duckdb',
parameters={  
    "database": "sample_data",
    "motherduck_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
};
```

Once the connection is established, you can query the database:

```sql
SELECT * FROM duckdb_datasource.my_table;
```

For MotherDuck:
```sql
SELECT * FROM md_datasource.movies;
```

By leveraging these features, MindsDB provides powerful integrations with DuckDB and MotherDuck for scalable analytics.
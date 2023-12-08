# LibSQL Handler

This is the implementation of the LibSQL handler for MindsDB.

## LibSQL

libSQL is a fork of SQLite that is both Open Source, and Open Contributions. It comes with it's own server for replication, embedded replicas, multi-tenancy, and edge nodes for deploying on the edge.

[LibSQL](https://turso.tech/libsql/)

## Implementation

This handler was implemented using the standard `libsql-experimental` library which has bindings for Python, and is compatible with the sqlite3 module.

The only required argument to establish a connection is `database`. This points to the local database file that the connection is to be made to.
Optionally, you can parse `sync_url` along with `auth_token` to sync the local database with the remote database on the edge.

## Usage

If you have local file that need to connect into MindsDB, you have to [deploy MindsDB locally](https://docs.mindsdb.com/setup/self-hosted/pip/source), ways like via Docker or via pip. Then copy the file into the desired folder in source folder. This way MindsDB can successfully access your file.

In order to make use of this handler and connect to a LibSQL/SQLite database in MindsDB, the following syntax can be used,

```sql
CREATE DATABASE libsql_dev
WITH
    engine='libsql',
    parameters={
        "database":"example.db"
    };
```

OR

With `sync_url` and `auth_token`

```sql
CREATE DATABASE libsql_dev
WITH
    engine='libsql',
    parameters={
        "database": "example.db",
        "sync_url": "libsql://exampledb-org.turso.io",
        "auth_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
    };
```

Now, you can use this established connection to query your database as follows,
```sql
SELECT * FROM libsql_dev.example_tbl
```

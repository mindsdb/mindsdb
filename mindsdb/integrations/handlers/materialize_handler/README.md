# OrioleDB Handler

This is the implementation of the OrioleDB handler for MindsDB.

## OrioleDB
OrioleDB is a new storage engine for PostgreSQL, bringing a modern approach to database capacity, capabilities and performance to the world's most-loved database platform.

OrioleDB consists of an extension, building on the innovative table access method framework and other standard Postgres extension interfaces. By extending and enhancing the current table access methods, OrioleDB opens the door to a future of more powerful storage models that are optimized for cloud and modern hardware architectures.
## Implementation

This handler was implemented by extending postres connector.

The required arguments to establish a connection are:

* `host`: the host name of the OrioleDB connection 
* `port`: the port to use when connecting 
* `user`: the user to authenticate 
* `password`: the password to authenticate the user
* `database`: database name

## Usage

In order to make use of this handler and connect to a OrioleDB server in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE orioledb_data
WITH ENGINE = "orioledb",
PARAMETERS = { 
  "user": "root",
  "password": "root",
  "host": "hostname",
  "port": "5432",
  "database": "postgres"
}
```

Now, you can use this established connection to query your database as follows:

```sql
SELECT * FROM orioledb_data.loveU LIMIT 10;
```
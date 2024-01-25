# ClickHouse Handler

This is the implementation of the ClickHouse handler for MindsDB.

## ClickHouse

ClickHouse is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP). https://clickhouse.com/docs/en/intro/



## Implementation
This handler was implemented using the standard `clickhouse-sqlalchemy` library https://clickhouse-sqlalchemy.readthedocs.io/en/latest/.
Please install it before using this handler:

```
pip install clickhouse-sqlalchemy
```

The required arguments to establish a connection are as follows:

* `host` is the hostname or IP address of the ClickHouse server.
* `port` is the TCP/IP port of the ClickHouse server.
* `user` is the username used to authenticate with the ClickHouse server.
* `password` is the password to authenticate the user with the ClickHouse server.
* `database` defaults to `default`. It is the database name to use when connecting with the ClickHouse server.
* `protocol` defaults to `native`. It is an optional parameter. Its supported values are `native`, `http` and `https`.

## Usage

To connect to ClickHouse use add `engine=clickhouse` to the CREATE DATABASE statement as:

```sql
CREATE DATABASE clic
WITH ENGINE = "clickhouse",
PARAMETERS = {
   "host": "127.0.0.1",
    "port": "8443",
    "user": "root",
    "password": "mypass",
    "database": "test_data",
    "protocol" : "https" 
    }
```

Now, you can use this established connection to query your database as follows,

```sql
SELECT * FROM clic.test_data.table
```
## Implementation

This handler is implemented by extending the MySQLHandler.

The required arguments to establish a connection are as follows:

* `user` is the database user.
* `password` is the database password.
* `host` is the host name, IP address, or URL.
* `port` is the port used to make TCP/IP connection.
* `database` is the database name.

There are several optional arguments that can be used as well.

* `ssl` is the `ssl` parameter value that indicates whether SSL is enabled (`True`) or disabled (`False`).
* `ssl_ca` is the SSL Certificate Authority.
* `ssl_cert` stores SSL certificates.
* `ssl_key` stores SSL keys.

## Usage

In order to make use of this handler and connect to the SingleStore database in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE singlestore_datasource
WITH
  ENGINE = 'singlestore',
  PARAMETERS = {
    "host": "127.0.0.1",
    "port": 3306,
    "database": "singlestore",
    "user": "root",
    "password": "password"
  };
```

You can use this established connection to query your table as follows:

```sql
SELECT *
FROM singlestore_datasource.example_table;
```

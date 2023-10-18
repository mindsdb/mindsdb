# Apache Doris Handler

This is the implementation of the Apache Doris for MindsDB.

## Apache Doris

Apache Doris is a new-generation open-source real-time data warehouse based on MPP architecture, with easier use and higher performance for big data analytics.

## Implementation

Since Doris uses MySQL dilect of SQL, this handler is implemented using the `mysql-connector-python` library.

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

### Database connection

In order to make use of this handler and connect to the Doris database in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE doris_datasource
WITH
  ENGINE = 'apache_doris',
  PARAMETERS = {
    "host": "127.0.0.1",
    "port": 9030,
    "database": "testdb",
    "user": "root",
    "password": "password"
};
```

### Queries

You can use this established connection to query your table just like you would a normal MySQL server.

```sql
SELECT * FROM mysql_datasource.example_table;
```

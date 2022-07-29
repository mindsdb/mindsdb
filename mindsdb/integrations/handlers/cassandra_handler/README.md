# Cassandra Handler

This is the implementation of the Apache Cassandra handler for MindsDB.

## Cassandra

Cassandra is a free and open-source, distributed, wide-column store, NoSQL database management system designed to handle large amounts of data across many commodity servers, providing high availability with no single point of failure. https://cassandra.apache.org/_/index.html

## Implementation

ScyllaDB is API-compatible with Apache Cassandra so this handler just extends the ScyllaHandler and is using the python `scylla-driver` library.

The required arguments to establish a connection are:

* `host`: the host name or IP address of the Cassandra 
* `port`: the port to use when connecting 
* `user`: the user to authenticate 
* `password`: the password to authenticate the user
* `keyspace`: the keyspace to connect to(top level container for tables)
* `protocol_version`: not required, default to 4

## Usage

In order to make use of this handler and connect to a Cassandra server in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE sc
WITH ENGINE = "cassandra",
PARAMETERS = {
    "host": "127.0.0.1",
    "port": "9043",
    "user": "user",
    "password": "pass",
    "keyspace": "test_data",
    "protocol_version": 4
    }
```

Now, you can use this established connection to query your database as follows:

```sql
SELECT * FROM sc.example_table LIMIT 10;
```
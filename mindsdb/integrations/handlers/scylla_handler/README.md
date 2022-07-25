# ScyllaDB Handler

This is the implementation of the ScyllaDB handler for MindsDB.

## ScyllaDB

Scylla is an open-source distributed NoSQL wide-column data store. It was designed to be compatible with Apache Cassandra while achieving significantly higher throughputs and lower latencies. For more info check https://www.scylladb.com/.

## Implementation

This handler was implemented using the python `scylla-driver` library.

The required arguments to establish a connection are:

* `host`: the host name or IP address of the ScyllaDB 
* `port`: the port to use when connecting 
* `user`: the user to authenticate 
* `password`: the password to authenticate the user
* `keyspace`: the keyspace to connect to(top level container for tables)
* `protocol_version`: not required, default to 4

## Usage

In order to make use of this handler and connect to a Scylla server in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE sc
WITH ENGINE = "scylladb",
PARAMETERS = {
    "host": "127.0.0.1",
    "port": "9042",
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
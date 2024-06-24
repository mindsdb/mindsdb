# MindsDB ScyllaDB Handler

This README provides details on the ScyllaDB handler integration for MindsDB.

## Introduction to ScyllaDB

ScyllaDB is an open-source distributed NoSQL wide-column data store. It was purposefully designed to offer compatibility with Apache Cassandra while outperforming it with higher throughputs and reduced latencies. For a comprehensive understanding of ScyllaDB, visit ScyllaDB's official website.

### Integration Implementation

The ScyllaDB handler for MindsDB was developed using the scylla-driver library for Python.
Connection Parameters:

- host: Host name or IP address of ScyllaDB.
- port: Connection port
- user: Authentication username. Optional; required only if authentication is enabled.
- password: Authentication password. Optional; required only if authentication is enabled.
- keyspace: The specific keyspace (top-level container for tables) to connect to.
- protocol_version: Optional. Defaults to 4.
- secure_connect_bundle: Optional. Needed only for connections to DataStax Astra.

## Usage Guide

To set up a connection between MindsDB and a Scylla server, utilize the following SQL syntax:

```sql

CREATE DATABASE scylladb_datasource
WITH ENGINE='scylladb',
PARAMETERS={
  "user":"user@mindsdb.com",
  "password": "pass",
  "host": "127.0.0.1",
  "port": 9042,
  "keyspace": "test_data"
};
```

> ℹ️ Tip: The protocol version is set to 4 by default. Should you wish to modify it, simply include "protocol_version": 5 within the PARAMETERS dictionary in the query above.

## Querying the Keyspace:

With the connection established, you can execute queries on your keyspace as demonstrated below:

```sql
SELECT * FROM scylladb_datasource.keystore.example_table LIMIT 10;
```

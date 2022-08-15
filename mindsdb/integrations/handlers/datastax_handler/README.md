# Datastax Astra DB Handler

This is the implementation of the Datastax Astra DB handler for MindsDB.

## Datastax 

DataStax, Inc. is a real-time data company based in Santa Clara, California.[3] Its product Astra DB is a cloud database-as-a-service based on Apache Cassandra. DataStax also offers DataStax Enterprise (DSE), an on-premises database built on Apache Cassandra, and Astra Streaming, a messaging and event streaming cloud service based on Apache Pulsar.

## Implementation

Datastax Astra DB is API-compatible with Apache Cassandra and Scylla DB so this handler just extends the ScyllaHandler and is using the python `scylla-driver` library.

The required arguments to establish a connection are:

* `user`: the user to authenticate 
* `password`: the password to authenticate the user
* `secure_connection_bundle`: Path to the secure_connection_bundle zip file

## Usage

In order to make use of this handler and connect to the Astra DB in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE astra_connection
WITH ENGINE = "astra",
PARAMETERS = {
    "user": "user",
    "password": "pass",
    "secure_connection_bundle": "/home/Downloads/file.zip"
    }
```

Now, you can use this established connection to query your database as follows:

```sql
SELECT * FROM astra_connection.keystore.example_table LIMIT 10;
```
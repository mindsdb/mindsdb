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
    "secure_connect_bundle": "/home/Downloads/file.zip"
    }
```

or, reference the bundle from Datastax s3 as:

```sql
CREATE DATABASE astra_connection
WITH ENGINE = "astra",
PARAMETERS = {
    "user": "user",
    "password": "pass",
    "secure_connect_bundle": "https://datastax-cluster-config-prod.s3.us-east-2.amazonaws.com/32312-b9eb-4e09-a641-213eaesa12-1/secure-connect-demo.zip?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AK..."
}  
```

Now, you can use this established connection to query your database as follows:

```sql
SELECT * FROM astra_connection.keystore.example_table LIMIT 10;
```
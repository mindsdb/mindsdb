# YugabyteDB Handler

This is the implementation of the YugabyteDB handler for MindsDB.

## YugabyteDB

YugabyteDB is a high-performance, cloud-native distributed SQL database that aims to support all PostgreSQL features. It is best to fit for cloud-native OLTP (i.e. real-time, business-critical) applications that need absolute data correctness and require at least one of the following: scalability, high tolerance to failures, or globally-distributed deployments.

## Implementation
This handler was implemented using the `psycopg`, a Python library that allows you to use Python code to run SQL commands on YugabyteDB.

The required arguments to establish a connection are,
* `user`: username asscociated with database
* `password`: password to authenticate your access
* `host`: host to server IP Address or hostname
* `port`: port through which TCP/IP connection is to be made
* `database`: Database name to be connected
* `schema`(OPTIONAL): comma seperated schemas to be considered for querying (e.g., "**class,company**")
* `sslmode`(OPTIONAL): Specifies the SSL mode for the connection, determining whether to use SSL encryption and the level of verification required (e.g., "**disable**", "**allow**", "**prefer**", "**require**", "**verify-ca**", "**verify-full**").


## Usage

In order to make use of this handler and connect to yugabyte in MindsDB, the following syntax can be used,

```sql
CREATE DATABASE yugabyte_datasource
WITH
engine='yugabyte',
parameters={
    "user":"admin",
    "password":"1234",
    "host":"127.0.0.1",
    "port":5433,
    "database":"yugabyte",
    "schema":"your_schema_name"
};
```

Now, you can use this established connection to query your database as follows,

```sql
SELECT * FROM yugabyte_datasource.demo;
```

NOTE : If you are using YugabyteDB Cloud with MindsDB Cloud website you need to add below 3 static IPs of MindsDB Cloud to `allow IP list` for accessing it publicly.
```
18.220.205.95
3.19.152.46
52.14.91.162
```
![public](https://github-production-user-asset-6210df.s3.amazonaws.com/75653580/238903548-1b054591-f5db-4a6d-a3d0-d048671e4cfa.png)


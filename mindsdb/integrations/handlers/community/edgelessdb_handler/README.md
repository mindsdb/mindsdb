# EdgelessDB Handler

This is the implementation of the EdgelessDB Handler for MindsDB.

## EdgelessDB
EdgelessDB is an open-source MySQL-compatible database for confidential computing. EdgelessDB runs entirely inside runtime-encrypted Intel SGX enclaves. In contrast to other databases, EdgelessDB ensures that all data is always encrypted—in memory as well as on disk. EdgelessDB has no storage constraints and delivers close to native performance.

Central to EdgelessDB is the concept of a manifest. The manifest is defined in JSON and is similar to a smart contract. It defines the initial state of the database, including access control, in an attestable way.

## Implementation

This handler was implemented by extending mysql connector.

The required arguments to establish a connection are:

* `host`: the host name of the EdgelessDB connection 
* `port`: the port to use when connecting 
* `user`: the user to authenticate 
* `password`: the password to authenticate the user
* `database`: database name

## Usage

In order to make use of this handler and connect to a EdgelessDB server in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE edgelessdb_datasource
WITH ENGINE = "EdgelessDB",
PARAMETERS = { 
  "user": "root",
  "password": "password",
  "host": "localhost",
  "port": 8080,
  "database": "test"
}
```

Now, you can use this established connection to query your database as follows:

```sql
SELECT * FROM edgelessdb_datasource.test LIMIT 10;
```
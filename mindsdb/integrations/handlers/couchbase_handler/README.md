# Couchbase Handler

This is the implementation of the Couchbase handler for MindsDB.

## Implementation

This handler was implemented using the `couchbase` library, the Python driver for Couchbase.

The required arguments to establish a connection are:

* `host`: the host name or IP address of the Couchbase server
* `bucket`: the bjcket name to use when connecting with the Couchbase server
* `user`: the user to authenticate with the Couchbase server
* `password`: the password to authenticate the user with the Couchbase server
* `scope`:  scopes are a level of data organization within a bucket. If omitted, will default to `_default`

If you are using Couchbase Capella, the `host` should be the connection string for the cluster. When you navigate to your cluster, the connection string can be found under the Connect tab.
It will also be required to whitelist the machine(s) that will be running MindsDB and database credentials will need to be created for the user. These steps can also be taken under the Connect tab.

## Usage

In order to make use of this handler and connect to a Couchbase server in MindsDB, the following syntax can be used. Note, the example uses the default `travel-sample` bucket which can be enabled from the couchbase UI with pre-defined scope and documents. 

```sql
CREATE DATABASE couchbase_datasource
WITH
engine='couchbase',
parameters={
    "host": "127.0.0.1",
    "bucket":"travel-sample",
    "user": "admin",
    "password": "password",
    "scope": "inventory"
};
```

Now, you can use this established connection to query your database as follows:

```sql
SELECT * FROM couchbase_datasource.airport
```
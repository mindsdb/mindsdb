# Vitess Handler

This is the implementation of the Vitess Handler for MindsDB.

## Vitess
Vitess is a database solution for deploying, scaling and managing large clusters of open-source database instances. It currently supports MySQL and Percona Server for MySQL. It's architected to run as effectively in a public or private cloud architecture as it does on dedicated hardware. It combines and extends many important SQL features with the scalability of a NoSQL database. Vitess can help you with the following problems:

  *  Scaling a SQL database by allowing you to shard it, while keeping application changes to a minimum.
  * Migrating from baremetal to a private or public cloud.
  * Deploying and managing a large number of SQL database instances.

## Implementation

This handler was implemented by extending mysql connector.

The required arguments to establish a connection are:

* `host`: the host name of the Vitess connection 
* `port`: the port to use when connecting 
* `user`: the user to authenticate 
* `password`: the password to authenticate the user
* `database`: database name

## Usage

In order to make use of this handler and connect to a Vitess server in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE vitess_datasource
WITH ENGINE = "vitess",
PARAMETERS = { 
  "user": "root",
  "password": "",
  "host": "localhost",
  "port": 33577,
  "database": "commerce"
}
```

Now, you can use this established connection to query your database as follows:

```sql
SELECT * FROM vitess_datasource.product LIMIT 10;
```
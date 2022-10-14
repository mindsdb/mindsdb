# Solr Handler

This is the implementation of the  Solr handler for MindsDB.

## Solr
The Apache Solr ™  is a highly reliable, scalable and fault tolerant, providing distributed indexing, replication and load-balanced querying, automated failover and recovery, centralized configuration and more.

## Implementation
This handler was implemented using the `sqlalchemy-solr` library, which provides a Python / SQLAlchemy interface.

The required arguments to establish a connection are,
* `username`: the username used to authenticate with the Solr server. This parameter is optional.
* `password`: the password to authenticate the user with the Solr server. This parameter is optional.
* `host`: the host name or IP address of the Solr server(.
* `port`: the port number of the Solr server.
* `server_path`: Defaults to solr in case not provided.
* `collection`: Solr Collection name.
* `use_ssl`: Defaults to false in case not provide. true|false
Refer [https://pypi.org/project/sqlalchemy-solr/](https://pypi.org/project/sqlalchemy-solr/)

## Usage
In order to make use of this handler and connect to Hive in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE solr_datasource
WITH
engine='solr',
parameters={
    "username": "demo_user",
    "password": "demo_password",
    "host": "127.0.0.1",
    "port": "8981",
    "server_path": "solr",
    "collection": "gettingstarted",
    "use_ssl": "false"
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM solr_datasource.gettingstarted limit 10000;
~~~~

## Requirements
A Solr instance with a Parallel SQL supported up and running.

There are certain limitations that need to be taken into account when issuing queries to Solr.
Refer [https://solr.apache.org/guide/solr/latest/query-guide/sql-query.html#parallel-sql-queries](https://solr.apache.org/guide/solr/latest/query-guide/sql-query.html#parallel-sql-queries).

Tip: Don't forget to put limit in the end of the SQL statement

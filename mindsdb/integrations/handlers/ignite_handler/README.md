# Apache Ignite Handler

This is the implementation of the Apache Ignite handler for MindsDB.

## Apache Ignite
Apache Ignite is a distributed database for high-performance computing with in-memory speed.
https://ignite.apache.org/docs/latest/

## Implementation
This handler was implemented using the `pyignite` library, the Apache Ignite thin (binary protocol) client for Python.

The required arguments to establish a connection are,
* `host`: the host name or IP address of the Apache Ignite cluster's node.
* `port`: the TCP/IP port of the Apache Ignite cluster's node. Must be an integer.

There are several optional arguments that can be used as well,
* `username`: the username used to authenticate with the Apache Ignite cluster. This parameter is optional. Default: None.
* `password`: the password to authenticate the user with the Apache Ignite cluster. This parameter is optional. Default: None.
* `schema`: the schema to use for the connection to the Apache Ignite cluster. This parameter is optional. Default: PUBLIC.

## Usage
In order to make use of this handler and connect to a Apache Ignite cluster in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE ignite_datasource
WITH ENGINE = 'ignite',
PARAMETERS = {
    "db_engine": "ignite",
    "host": "127.0.0.1",
    "port": 10800,
    "username": "admin",
    "password": "password",
    "schema": "example_schema"
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM ignite_datasource.example_tbl
~~~~

At the moment, a connection can be established to only a single node in the cluster. Steps will be taken in the future, to configure the client to automatically fail over to another node if the connection to the current node fails or times out by providing the hosts and ports for many nodes as explained here,
https://ignite.apache.org/docs/latest/thin-clients/python-thin-client

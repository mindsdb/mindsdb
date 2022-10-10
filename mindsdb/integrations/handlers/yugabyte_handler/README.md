# YugabyteDB Handler

This is the implementation of the  YugabyteDB handler for MindsDB.

## YugabyteDB
YugabyteDB is a high-performance, cloud-native distributed SQL database that aims to support all PostgreSQL features. It is best to fit for cloud-native OLTP (i.e. real-time, business-critical) applications that need absolute data correctness and require at least one of the following: scalability, high tolerance to failures, or globally-distributed deployments.

## Implementation
This handler was implemented using the `psycopg2`, a Python library that allows you to use Python code to run SQL commands on YugabyteDB.

The required arguments to establish a connection are,
* `user`: username asscociated with database
* `password`: password to authenticate your access
* `host`: host to server IP Address or hostname
* `port`: port through which TCPIP connection is to be made
* `database`: Database name to be connected


## Usage
In order to make use of this handler and connect to yugabyte in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE yugabyte_datasource
WITH
engine='yugabyte',
parameters={
    "user":"admin",
    "password":"1234",
    "host":"127.0.0.1",
    "port":5433,
    "database":"yugabyte"
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM yugabyte_datasource.demo;
~~~~

NOTE : If you are using YugabyteDB Cloud with mindsdb cloud websit you need to add **`0.0.0.0\0`** to `allow IP list` for accessing it publicly.
![public](https://user-images.githubusercontent.com/75653580/185357710-932da3a0-dd6b-4f7c-afe3-8022cff220eb.png)

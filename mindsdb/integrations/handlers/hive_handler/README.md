# Hive Handler

This is the implementation of the  Hive handler for MindsDB.

## Hive
The Apache Hive ™ data warehouse software facilitates reading, writing, and managing large datasets residing in distributed storage using SQL. Structure can be projected onto data already in storage.

## Implementation
This handler was implemented using the `pyHive`, a Python library that allows you to use Python code to run SQL commands on Hive.

The required arguments to establish a connection are,
* `user`: username asscociated with database
* `password`: password to authenticate your access
* `host`: host to server IP Address or hostname
* `port`: port through which TCPIP connection is to be made
* `database`: Database name to be connected
* `auth`: Defaults to CUSTOM in case not provided. Check for other options in [https://pypi.org/project/PyHive/](https://pypi.org/project/PyHive/)


## Usage
In order to make use of this handler and connect to Hive in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE hive_datasource
WITH
engine='hive',
parameters={
    "user": "demo_user",
    "password": "demo_password",
    "host": "127.0.0.1",
    "port": "10000",
    "database": "default"
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM hive_datasource.test_hdb;
~~~~

NOTE : To install `pyHive` the following linux packages are required `libsasl2-dev sasl2-bin libsasl2-2 libsasl2-dev libsasl2-modules libsasl2-modules-gssapi-mit`

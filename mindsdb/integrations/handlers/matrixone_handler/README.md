# Matrixone Handler

This is the implementation of the  Matrixone handler for MindsDB.

##  Matrixone
MatrixOne is a future-oriented hyper-converged cloud and edge native DBMS that supports transactional, analytical, and streaming workloads with a simplified and distributed database engine, across multiple data centers, clouds, edges and other heterogeneous infrastructures.

For more Info Click [HERE](https://github.com/matrixorigin/matrixone)

## Implementation
This handler was implemented using the `PyMySQL`, a Python library that allows you to use Python code to run SQL commands on Matrixone Database.

The required arguments to establish a connection are,
* `user`: username asscociated with database
* `password`: password to authenticate your access
* `host`: host to server IP Address or hostname
* `port`: port through which TCPIP connection is to be made
* `database`: Database name to be connected
* `ssl`: If you want to enable SSL Security **(Boolean)**
* `ssl_ca`: Path or URL of the Certificate Authority (CA) certificate file
* `ssl_cert`: Path name or URL of the server public key certificate file
* `ssl_key`: The path name or URL of the server private key file
  

## Usage
In order to make use of this handler and connect to Matrixone in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE matrixone_datasource
WITH
engine='matrixone',
parameters={
    "user":"dump",
    "password":"111",
    "host":"127.0.0.1",
    "port":6001,
    "database":"mo_catalog"
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM Matrixone_datasource.demo;
~~~~

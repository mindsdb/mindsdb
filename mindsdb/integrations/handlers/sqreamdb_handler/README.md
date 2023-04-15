# SQreamDB Handler

This is the implementation of the  SQreamDB handler for MindsDB.

##  SQreamDB
A SQL database that empowers organizations to perform complex analytics on a petabyte-scale of data and gain time-sensitive business insights faster and cheaper than from any other solution. 

## Implementation
This handler was implemented using the `pysqream`, a Python library that allows you to use Python code to run SQL commands on SQreamDB Database.

The required arguments to establish a connection are,
* `user`: username asscociated with database
* `password`: password to authenticate your access
* `host`: host to server IP Address or hostname
* `port`: port through which sevice is exposed
* `database`: Database name to be connected
* `service`: Optional: service queue (default: "sqream")
* `use_ssl`: use SSL connection (default: False)
* `clustered`: Optional: Connect through load balancer, or direct to worker (Default: false - direct to worker)


## Usage
In order to make use of this handler and connect to SQreamDB in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE sqreamdb_datasource
WITH
engine='sqreamdb',
parameters={
    "user":"master",
    "password":"sqream",
    "host":"127.0.0.1",
    "port":5000,
    "database":"sqream"
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM sqreamdb_datasource.sampledb;
~~~~

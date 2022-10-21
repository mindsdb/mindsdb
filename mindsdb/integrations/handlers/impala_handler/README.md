# Impala Handler

This is the implementation of the  Impala handler for MindsDB.

##  Impala
Apache Impala is a MPP (Massive Parallel Processing) SQL query engine for processing huge volumes of data that is stored in Hadoop cluster. It is an open source software which is written in C++ and Java. It provides high performance and low latency compared to other SQL engines for Hadoop.
In other words, Impala is the highest performing SQL engine (giving RDBMS-like experience) which provides the fastest way to access data that is stored in Hadoop Distributed File System.

## Implementation
This handler was implemented using the `impyla`, a Python library that allows you to use Python code to run SQL commands on Impala.

The required arguments to establish a connection are,
* `user`: username asscociated with database
* `password`: password to authenticate your access
* `host`: host to server IP Address or hostname
* `port`: port through which TCP/IP connection is to be made
* `database`: Database name to be connected

## Usage
In order to make use of this handler and connect to Impala in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE impala_datasource
WITH
engine='impala',
parameters={
    "user":"root",
    "password":"p@55w0rd",
    "host":"127.0.0.1",
    "port":21050,
    "database":"Db_NamE"
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM impala_datasource.TEST;
~~~~

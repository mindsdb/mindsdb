# MonetDB Handler

This is the implementation of the MonetDB handler for MindsDB.

## MonetDB
MonetDB is an open-source column-oriented relational database management system originally developed at the Centrum Wiskunde & Informatica in the Netherlands. It is designed to provide high performance on complex queries against large databases, such as combining tables with hundreds of columns and millions of rows.

## Implementation
This handler was implemented using the `pymonetdb`, a Python library that allows you to use Python code to run SQL commands on MonetDB Database.

The required arguments to establish a connection are,
* `user`: username asscociated with database
* `password`: password to authenticate your access
* `host`: host to server IP Address or hostname
* `port`: port through which TCPIP connection is to be made
* `database`: Database name to be connected
* `schema_name `: schema name to get tables. (_Optional_) **Default it will select current schema.**

## Usage
In order to make use of this handler and connect to DB2 in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE monetdb_datasource
WITH
engine='monetdb',
parameters={
    "user":"monetdb",
    "password":"monetdb",
    "host":"127.0.0.1",
    "port":50000,
    "schema_name":"sys",
    "database":"demo"
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM monetdb_datasource.demo;
~~~~

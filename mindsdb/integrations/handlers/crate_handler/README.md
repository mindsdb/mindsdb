# Crate DB Handler

This is the implementation of the Crate DB handler for MindsDB.

## Crate DB
CrateDB is a distributed SQL database management system that integrates a fully searchable document-oriented data store. It is open-source, written in Java, based on a shared-nothing architecture, and designed for high scalability. CrateDB includes components from Trino, Lucene, Elasticsearch and Netty. 


## Implementation
This handler was implemented using the `crate`, a Python library that allows you to use Python code to run SQL commands on Crate DB.

The required arguments to establish a connection are,
* `user`: username asscociated with database
* `password`: password to authenticate your access
* `host`: host to server IP Address or hostname
* `port`: port through which  connection is to be made.
* `schema`: schema name to get tables 

## Usage
In order to make use of this handler and connect to Crate DB in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE crate_datasource
WITH
engine='crate',
parameters={
    "user":"crate",
    "password":"",
    "host":"127.0.0.1",
    "port":4200,
    "schema_name":"doc"
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM crate_datasource.demo;
~~~~

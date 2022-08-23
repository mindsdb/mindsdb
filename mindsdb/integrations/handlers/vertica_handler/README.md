# Vertica Handler

This is the implementation of the  Vertica handler for MindsDB.

##  Vertica
The column-oriented Vertica Analytics Platform was designed to manage large, fast-growing volumes of data and with fast query performance for data warehouses and other query-intensive applications. The product claims to greatly improve query performance over traditional relational database systems, and to provide high availability and exabyte scalability on commodity enterprise servers. Vertica runs on multiple cloud computing systems as well as on Hadoop nodes. Vertica's Eon Mode separates compute from storage, using S3 object storage and dynamic allocation of compute notes

## Implementation
This handler was implemented using the `vertica-python`, a Python library that allows you to use Python code to run SQL commands on Vertica Database.

The required arguments to establish a connection are,
* `user`: username asscociated with database
* `password`: password to authenticate your access
* `host`: host to server IP Address or hostname
* `port`: port through which TCPIP connection is to be made
* `database`: Database name to be connected
* `schema`: schema name to get tables 

## Usage
In order to make use of this handler and connect to Vertica in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE vertica_datasource
WITH
engine='vertica',
parameters={
    "user":"dbadmin",
    "password":"password",
    "host":"127.0.0.1",
    "port":5433,
    "schema_name":"public",
    "database":"VMart"
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM vertica_datasource.TEST;
~~~~

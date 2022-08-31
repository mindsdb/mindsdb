# IBM Informix Handler

This is the implementation of the IBM DB2 handler for MindsDB.

## IBM Informix
IBM Informix is a product family within IBM's Information Management division that is centered on several relational database management system (RDBMS) offerings.The Informix server supports the object–relational model and supports (through extensions) data types that are not a part of the SQL standard. The most widely used of these are the JSON, BSON, time series and spatial extensions, which provide both data type support and language extensions that permit high performance domain specific queries and efficient storage for data sets based on semi-structured, time series, and spatial data. 

## Implementation
This handler was implemented using the `IfxPy/IfxPyDbi`, a Python library that allows you to use Python code to run SQL commands on DB2 Database.

The required arguments to establish a connection are,
* `user`: username asscociated with database
* `password`: password to authenticate your access
* `host`: host to server IP Address or hostname
* `port`: port through which TCPIP connection is to be made
* `database`: Database name to be connected
* `schema_name`: schema name to get tables 
* `server`: Name of server you want connect
* `loging_enabled`: Is loging is enabled or not. Default is True

## Usage
In order to make use of this handler and connect to DB2 in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE Informix_datasource
WITH
engine='informix',
parameters={
        "server": "server",
        "host": "127.0.0.1",
        "port": 9091,
        "user": "informix",
        "password": "in4mix",
        "database": "stores_demo",
        "schema_name": "love",
        "loging_enabled": False
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM informix_datasource.items;
~~~~


This integration uses IfxPy it is in develpment stage there it can be install using `pip install IfxPy`.But it doesn't work for higher version of python, therfore you have to build it from source.

For more Info checkout [here](https://github.com/OpenInformix/IfxPy) also it has some prerequisite.

There are many method for Build but wheel method easy and Recommended.

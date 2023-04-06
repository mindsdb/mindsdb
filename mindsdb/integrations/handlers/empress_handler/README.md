# Empress Embedded Handler

This is the implementation of the Empress Embedded handler for MindsDB.

## Empress Embedded
Empress Embedded is a relational database management system that is designed to run in embedded environments such as mobile devices, IoT devices, and other resource-constrained systems. It is a lightweight and fast database that provides a high-performance storage engine, efficient indexing, and support for transactions and recovery.

## Implementation
This handler was implemented using [pyodbc](https://pypi.org/project/pyodbc/), interacting with the [Empress Embedded ODBC Interface](http://www.empress.com/products/api-hliodbc.html).

The required arguments to establish a connection are:
* `host`: host to server IP Address or hostname
* `port`: port through which TCPIP connection is to be made
* `user`: username associated with database
* `password`: password to authenticate your access
* `server`: Server name to be connected
* `database`: Database name to be connected


## Usage

You should be able to access any ODBC Data Source providing that a corresponding driver exists to support that Data Source. The Data Source may reside on a remote Server platform connected by  a network or locally on the same computer. 

Documentation for installation and set up can be found [here](https://www.tmphero.org/test/empress8_manual/english/prodoc/d2/d2_2.htm).

In order to make use of this handler and connect to Empress Embedded in MindsDB, the following syntax can be used:
~~~~sql
CREATE
DATABASE empress_db
WITH engine='empress',
parameters={
    "host": "127.0.0.1",
    "port": "6322" ,
    "user": "admin",
    "password": "password",
    "server": "test_server",
    "database": "test_db"
};
~~~~

Now, you can use this established connection to query your database as follows:
~~~~sql
SELECT * FROM test_db.test;
~~~~


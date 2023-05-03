# DB2 Handler

This is the implementation of the Apache Derby handler for MindsDB.

## IBM DB2
Apache Derby, an Apache DB subproject, is an open source relational database implemented entirely in Java and available under the Apache License, Version 2.0.  

Some key features include:

* Derby has a small footprint -- about 3.5 megabytes for the base engine and embedded JDBC driver.
* Derby is based on the Java, JDBC, and SQL standards.
* Derby provides an embedded JDBC driver that lets you embed Derby in any Java-based solution.
* Derby also supports the more familiar client/server mode with the Derby Network Client JDBC driver and Derby Network Server.
* Derby is easy to install, deploy, and use.


## Implementation
This handler was implemented using the JDBC drivers provided by Apache Derby. To establish connection with the database, `JayDeBeApi` library is used. The `JayDeBeApi` module allows you to connect from Python code to databases using Java JDBC. It provides a Python DB-API v2.0 to that database.

The required arguments to establish a connection are,
* `host`: host to server IP Address or hostname
* `port`: port through which TCPIP connection is to be made
* `database`: Database name to be connected

## Usage
In order to make use of this handler and connect to Apache Derby in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE derby_datasource
WITH engine='derby',
parameters={
    "host": "localhost",
    "port": "1527",
    "database": "seconddb"
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM derby_datasource.TESTTABLE;
~~~~

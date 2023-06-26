# Altibase Handler

This is the implementation of the Altibase handler for MindsDB.

## Altibase
ALTIBASE is a hybrid database, relational open source database management system manufactured by The Altibase Corporation. The software comes with a hybrid architecture which allows it to access both memory-resident and disk-resident tables using single interface.

## Implementation
This handler was implemented using the JDBC drivers provided by Altibase. To establish connection with the database, `JayDeBeApi` library is used. The `JayDeBeApi` module allows you to connect from Python code to databases using Java JDBC.

### The required arguments to establish a connection are:
* `host`: host to server IP Address or hostname
* `port`: port through which TCPIP connection is to be made
* `database`: Database name to be connected
* `jdbc_class`: Java class name of the JDBC driver
### The optional arguments to establish a connection are:
* `user`: username asscociated with database
* `password`: password to authenticate your access
* `jar_location`: Jar filename for the JDBC driver 

## Usage
In order to make use of this handler and connect to Altibase in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE altibase_datasource
WITH
engine='Altibase',
parameters={
    "user":"sys",
    "password":"manager",
    "host":"127.0.0.1",
    "port":20300,
    "database":"mydb"
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM altibase_datasource.test;
~~~~

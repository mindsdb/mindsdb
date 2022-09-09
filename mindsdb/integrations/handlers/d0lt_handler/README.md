# D0lt Handler

This is the implementation of the  D0lt handler for MindsDB.

##  Dolt is Git for Data!
Dolt is a single-node and embedded DBMS that incorporates Git-style versioning as a first-class entity. Dolt behaves like Git where it is a content addressable local database where the main objects are tables instead of files. In Dolt, a user creates a database locally. The database contains tables that can be read and updated using SQL. Similar to Git, writes are staged until the user issues a commit. Upon commit, the writes are appended to permanent storage.

Branch/merge semantics are supported allowing for the tables to evolve at a different pace for multiple users. This allows for loose collaboration on data as well as multiple views on the same core data. Merge conflicts are detected for schema and data conflicts. Data conflicts are cell-based, not line-based. Remote repositories allow for cooperation among repository instances. Clone, push, and pull semantics are all available.

## Implementation
This handler was implemented using the `mysql-connector`, a Python library that allows you to use Python code to run SQL commands on D0lt Database.

The required arguments to establish a connection are,
* `user`: username asscociated with database
* `password`: password to authenticate your access
* `host`: host to server IP Address or hostname
* `port`: port through which TCPIP connection is to be made
* `database`: Database name to be connected


## Usage
In order to make use of this handler and connect to D0lt in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE d0lt_datasource
WITH
engine='d0lt',
parameters={
    "user":"root",
    "password":"",
    "host":"127.0.0.1",
    "port":3306,
    "database":"information_schema"
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM D0lt_datasource.TEST;
~~~~

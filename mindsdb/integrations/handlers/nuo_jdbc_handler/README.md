# NuoDB Handler

This is the implementation of the Apache Derby handler for MindsDB.

## NuoDB
NuoDB delivers consistent, resilient distributed SQL for your mission critical applications, so you can deploy on premises, in public or private clouds, in hybrid environments, or across clouds.
NuoDB is the distributed SQL database designed to meet the rapidly evolving demands of today’s enterprises, scale on demand, eliminate downtime and reduce total cost of ownership. All while maintaining SQL compatibility.

## Implementation
This handler was implemented using the JDBC drivers provided by Apache Derby. To establish connection with the database, `JayDeBeApi` library is used. The `JayDeBeApi` module allows you to connect from Python code to databases using Java JDBC. It provides a Python DB-API v2.0 to that database.

The required arguments to establish a connection are,
* `host`: host to server IP Address or hostname
* `port`: port through which TCPIP connection is to be made
* `database`: Database name to be connected
* `user`: The username to authenticate with the NuoDB server.
* `password`: The password to authenticate the user with the NuoDB server.
* `is_direct`: This argument indicates whether a direct connection to the TE is to be attempted. 

## Usage
In order to make use of this handler and connect to Apache Derby in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE nuo_datasource
WITH engine='nuo_jdbc',
parameters={
    "host": "localhost",
    "port": "48006",
    "database": "test",
    "schema": "hockey",
    "user": "dba",
    "password": "goalie",
    "is_direct": "true",
};
~~~~
Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM derby_datasource.PLAYERS;
~~~~

# NuoDB Handler

This is the implementation of the NuoDB handler for MindsDB.

## NuoDB
NuoDB delivers consistent, resilient distributed SQL for your mission critical applications, so you can deploy on premises, in public or private clouds, in hybrid environments, or across clouds.
NuoDB is the distributed SQL database designed to meet the rapidly evolving demands of today’s enterprises, scale on demand, eliminate downtime and reduce total cost of ownership. All while maintaining SQL compatibility.

## Implementation
This handler was implemented using the JDBC driver provided by NuoDB. To establish connection with the database, `JayDeBeApi` library is used. The `JayDeBeApi` module allows you to connect from Python code to databases using Java JDBC. It provides a Python DB-API v2.0 to that database.

The required arguments to establish a connection are,
* `host`: host to server IP Address or hostname
* `port`: port through which TCPIP connection is to be made
* `database`: Database name to be connected
* `user`: The username to authenticate with the NuoDB server.
* `password`: The password to authenticate the user with the NuoDB server.
* `is_direct`: This argument indicates whether a direct connection to the TE is to be attempted. 

Other optional arguments are, 
* `schema`: The schema name to use when connecting with the NuoDB.
* `jar_location`: The location of the jar files which contain the JDBC class. This need not be specified if the required classes are already added to the CLASSPATH variable.
* `driver_args`: The extra arguments which can be specified to the driver. Specify this in the format: "arg1=value1,arg2=value2. 
More information on the supported parameters can be found at: https://doc.nuodb.com/nuodb/latest/deployment-models/physical-or-vmware-environments-with-nuodb-admin/reference-information/connection-properties/

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
SELECT * FROM nuo_datasource.PLAYERS;
~~~~

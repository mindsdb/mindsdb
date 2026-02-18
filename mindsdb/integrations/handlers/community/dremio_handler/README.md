# Dremio Handler

This is the implementation of the Dremio handler for MindsDB.

## Dremio
Dremio is the only data lakehouse that empowers data engineers and analysts with easy-to-use self-service SQL analytics.
<br>
https://www.dremio.com/why-dremio/

## Implementation
This handler was implemented using the `requests` and `pandas` libraries.

The required arguments to establish a connection are,
* `host`: the host name or IP address of the Dremio server.
* `port`: the port that Dremio is running on.
* `username`: the username used to authenticate with the Dremio server.
* `password`: the password to authenticate the user with the Dremio server.

## Usage
In order to make use of this handler and connect to Dremio in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE dremio_datasource
WITH
engine='dremio',
parameters={
    "host": "localhost",
    "port": 9047,
    "username": "username",
    "password": "password"
};
~~~~

Now, you can use this established connection to query your data source as follows,
~~~~sql
SELECT * FROM dremio_datasource.example_tbl
~~~~
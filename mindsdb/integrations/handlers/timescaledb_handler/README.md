# TimeScaleDB Handler

This is the implementation of the TimeScaleDB handler for MindsDB.

## TimeScaleDB
TimescaleDB is an open-source database designed to make SQL scalable for time-series data. It is engineered up from PostgreSQL and packaged as a PostgreSQL extension, providing automatic partitioning across time and space (partitioning key), as well as full SQL support. Build powerful data-intensive applications.

## Implementation
This handler was implemented using the `psycopg`, this handler extends Postgres Handler and use it to perform all Queries.

The required arguments to establish a connection are,
* `user`: username asscociated with database
* `password`: password to authenticate your access
* `host`: host to server IP Address or hostname
* `port`: port through which connection is to be made
* `database`: Database name to be connected

## Usage
In order to make use of this handler and connect to TimeScaleDB in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE timescaledb_datasource
WITH
engine='timescaledb',
parameters={
        "host": "127.0.0.1",
        "port": 36806,
        "user": "tsdbadmin",
        "password": "p455WorD",
        "database": "tsdb",

};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM timescaledb_datasource.sensors;
~~~~

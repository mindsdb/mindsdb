# InfluxDB Handler

This is the implementation of the InfluxDB handler for MindsDB.

## InfluxDB
In short, InfluxDB is a time series database that can be used to collect data & monitor the system & devices, especially IoT devices.
In this handler, influxdb 1.x api is used and more information about this api can be found (here)[https://docs.influxdata.com/influxdb/v1.7/query_language/schema_exploration/]


## Implementation
This handler was implemented using `duckdb`, a library that allows SQL queries to be executed on `pandas` DataFrames.

In essence, when querying a particular table, the entire table will first be pulled into a `pandas` DataFrame using the InfluxDB API. Once this is done, SQL queries can be run on the DataFrame using `duckdb`.

Note: Since the entire table needs to be pulled into memory first (DataFrame), it is recommended to be somewhat careful when querying large tables so as not to overload your machine.

The required arguments to establish a connection are,
* `influxdb_url`  : Hosted url of InfluxDB Cloud
* `influxdb_token`: Authentication token for the hosted influxdb cloud instance
* `influxdb_query`: InfluxDB Query 
* `influxdb_db_name`: Database name of the influxdb cloud instance
* `influxdb_table_name`: Table name of the influxdb cloud instance

## Usage
In order to make use of this handler and connect to an Jira in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE influxdb_source
WITH
engine='influxdb',
parameters={
    "influxdb_url": "https://ap-southeast-2-1.aws.cloud2.influxdata.com",
    "influxdb_token": "2KdXsJPE0yGpwxm6ybELC9-VjHYLN32QDTcNpRZUOVlgFJqJC7aUSLKcl26YwFP9_9jHqoih7FUWIy_zaIxfuw==",
    "influxdb_query": "SELECT * FROM airSensors Limit 10",
    "influxdb_db_name": "mindsdb",
    "influxdb_table_name": "airSensors"
};
~~~~

Now, you can use this established connection to query your table as follows,
~~~~sql
SELECT * FROM influxdb_source.example_tbl
~~~~

At the moment, only `SELECT` queries are allowed to be executed through `duckdb`. This, however, has no restriction on running machine learning algorithms against your data in InfluxDB using `CREATE PREDICTOR` statements.

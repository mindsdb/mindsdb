# Databricks Handler

This is the implementation of the Databricks handler for MindsDB.

## Databricks
A data lakehouse unifies the best of data warehouses and data lakes in one simple platform to handle all your data, analytics and AI use cases. It’s built on an open and reliable data foundation that efficiently handles all data types and applies one common security and governance approach across all of your data and cloud platforms.<br>
https://databricks.com/

## Implementation
This handler was implemented using the `databricks-sql-connector`, a Python library that allows you to use Python code to run SQL commands on Databricks clusters and Databricks SQL warehouses.

The required arguments to establish a connection are,
* `server_hostname`: the server hostname for the cluster or SQL warehouse
* `http_path`: the HTTP path of the cluster or SQL warehouse
* `access_token`: a Databricks personal access token for the workspace for the cluster or SQL warehouse

There are several optional arguments that can be used as well,
* `session_configuration`: a dictionary of Spark session configuration parameters
* `http_headers`: additional (key, value) pairs to set in HTTP headers on every RPC request the client makes
* `catalog`: catalog to use for the connection. If left blank, the default catalog, typically `hive_metastore` will be used
* `schema`: schema (database) to use for the connection. If left blank, the default schema `default` will be used

## Usage
In order to make use of this handler and connect to Databricks in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE databricks_datasource
WITH
engine='databricks',
parameters={
    "server_hostname": "adb-1234567890123456.7.azuredatabricks.net",
    "http_path": "sql/protocolv1/o/1234567890123456/1234-567890-test123",
    "access_token": "dapi1234567890ab1cde2f3ab456c7d89efa",
    "schema": "example_db"
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM databricks_datasource.example_tbl
~~~~
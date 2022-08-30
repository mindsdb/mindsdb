# Apache Druid Handler

This is the implementation of the Apache Druid handler for MindsDB.

## Apache Druid
Apache Druid is a real-time analytics database designed for fast slice-and-dice analytics ("OLAP" queries) on large data sets. Most often, Druid powers use cases where real-time ingestion, fast query performance, and high uptime are important.
<br>
https://druid.apache.org/docs/latest/design

## Implementation
This handler was implemented using the `pydruid` library, the Python API for Apache Druid.

The required arguments to establish a connection are,
* `host`: the host name or IP address of Apache Druid.
* `port`: the port that Apache Druid is running on.
* `path`: the query path.
* `scheme`: the URI schema. This parameter is optional and the default will be http.
* `user`: the username used to authenticate with Apache Druid. This parameter is optional.
* `password`: the password used to authenticate with Apache Druid. This parameter is optional.

## Usage
In order to make use of this handler and connect to Apache Druid in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE druid_datasource
WITH
engine='druid',
parameters={
    "host": "localhost",
    "port": 8888,
    "path": "/druid/v2/sql/",
    "scheme": "http"
};
~~~~

Now, you can use this established connection to query your data source as follows,
~~~~sql
SELECT * FROM druid_datasource.example_tbl
~~~~
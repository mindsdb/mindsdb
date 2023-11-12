# InfluxDB Handler
This is the implementation of the InfluxDB handler for MindsDB.

## InfluxDB
In short, InfluxDB is a time series database that can be used to collect data & monitor the system & devices, especially Edge devices.
In this handler, influxdb 1.x api is used and more information about this api can be found (here)[https://docs.influxdata.com/influxdb/v1.7/query_language/schema_exploration/]

Please follow this (link)[https://docs.influxdata.com/influxdb/cloud/security/tokens/create-token/#create-a-token-in-the-influxdb-ui] to generate token for accessing InfluxDB API


## Implementation
This handler was implemented as per the (Application Handler framework)[https://docs.mindsdb.com/contribute/app-handlers]

The required arguments to establish a connection are,
* `influxdb_url`  : Hosted url of InfluxDB Cloud
* `influxdb_token`: Authentication token for the hosted influxdb cloud instance
* `influxdb_db_name`: Database name of the influxdb cloud instance
* `influxdb_table_name`: Table name of the influxdb cloud instance
* `org` : Organisation of the influxdb cloud instance


## Install Dependencies

```
pip install influxdb3-python

```
## Usage
In order to make use of this handler and connect to an Jira in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE influxdb_source
WITH
engine='influxdb',
parameters={
    "influxdb_url": "<influxdb-hosted-url>",
    "influxdb_token": "<api-key-token",
    "influxdb_db_name": "<database-name>",
    "influxdb_table_name": "<table-name>",
    "org": "Organisation"
};
~~~~

For querying different tables, you need to create another database with `influxdb` handler as engine & mention the appropriate database & table name in the following  parameters `influxdb_db_name` & `influxdb_table_name`

Now, you can use this established connection to query your table as follows,
~~~~sql
SELECT * FROM influxdb_source.tables
~~~~

Advanced queries for the InfluxDB Handler
~~~~sql
SELECT name,time,sensor_id,temperature
FROM influxdb_source5.tables
ORDER BY temperature DESC
LIMIT 65;
~~~~

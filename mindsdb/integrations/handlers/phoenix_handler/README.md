# Apache Phoenix Handler

This is the implementation of the Apache Phoenix handler for MindsDB.

## Apache Phoenix
Apache Phoenix takes your SQL query, compiles it into a series of HBase scans, and orchestrates the running of those scans to produce regular JDBC result sets.
<br>
https://phoenix.apache.org/

It is a SQL skin over HBase delivered as a client-embedded JDBC driver targeting low latency queries over HBase data.

## Implementation
This handler was implemented using the `pyphoenix` library, the Python library for accessing the Phoenix SQL database using the remote query server introduced in Phoenix 4.4..

The required arguments to establish a connection are,
* `url`: the URL to the Phoenix Query Server

## Usage
In order to make use of this handler and connect to an Apache Phoenix Query Server in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE phoenix_datasource
WITH ENGINE = 'phoenix',
PARAMETERS = {
  "url": "http://127.0.0.1:8765",
  "autocommit": True
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM phoenix_datasource.example_tbl
~~~~

## Quickstart
To quickly spin up Apache Phoenix + HBase in Docker, the following repository can be used,
https://github.com/mrauhu/apache-hbase-phoenix

Close this repository locally,
~~~~bash
git clone https://github.com/mrauhu/apache-hbase-phoenix.git
~~~~

Run the containers in the background,
~~~~bash
docker-compose up -d
~~~~

Install MindsDB on your local Python environment,
~~~~bash
pip install mindsdb
~~~~

Launch the MindsDB SQL Editor,
~~~~bash
python -m mindsdb 
~~~~

Execute the following commands to create a data source and query the system table `SYSTEM.CATALOG` table as explained under the Usage section,

~~~~sql
CREATE DATABASE phoenix_datasource
WITH ENGINE = 'phoenix',
PARAMETERS = {
  "url": "http://127.0.0.1:8765",
  "autocommit": True
};

SELECT *
FROM phoenix_datasource.SYSTEM.CATALOG
~~~~
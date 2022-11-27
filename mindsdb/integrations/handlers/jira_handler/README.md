# Jira Handler

This is the implementation of the Jira handler for MindsDB.

## Jira
In short, Jira is a tool to track the progress of software defects,story and releases.
In this handler. python client of api is used and more information about this python client can be found (here)[https://pypi.org/project/atlassian-python-api/]


## Implementation
This handler was implemented using `duckdb`, a library that allows SQL queries to be executed on `pandas` DataFrames.

In essence, when querying a particular table, the entire table will first be pulled into a `pandas` DataFrame using the Jira API. Once this is done, SQL queries can be run on the DataFrame using `duckdb`.

Note: Since the entire table needs to be pulled into memory first (DataFrame), it is recommended to be somewhat careful when querying large tables so as not to overload your machine.

The required arguments to establish a connection are,
* `table_name`: Sample Jira table name.
* `jira_url`: Jira  url
* `user_id`: Jira User ID.
* `api_key`: API key for the Jira API.
* `jira_query`: Jira Search Query

## Usage
In order to make use of this handler and connect to an Access database in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE jira_source
WITH
engine='Jira',
parameters={
    "jira_url": "https://jira.linuxfoundation.org",
    "user_id": "balaceg",
    "api_key": "4Rhq&Ehd#KV4an!",
    "jira_query": "project = RELENG and status != Done"
};
~~~~

Now, you can use this established connection to query your table as follows,
~~~~sql
SELECT * FROM jira_source.example_tbl
~~~~

At the moment, only `SELECT` queries are allowed to be executed through `duckdb`. This, however, has no restriction on running machine learning algorithms against your data in Airtable using `CREATE PREDICTOR` statements.

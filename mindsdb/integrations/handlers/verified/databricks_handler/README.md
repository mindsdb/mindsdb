---
title: Databricks
sidebarTitle: Databricks
---
This documentation describes the integration of MindsDB with [Databricks](https://www.databricks.com/), the world's first data intelligence platform powered by generative AI.
The integration allows MindsDB to access data stored in a Databricks workspace and enhance it with AI capabilities.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).
2. To connect Databricks to MindsDB, install the required dependencies following [this instruction](/setup/self-hosted/docker#install-dependencies).

<Note>
If the Databricks cluster you are attempting to connect to is terminated, executing the queries given below will attempt to start the cluster and therefore, the first query may take a few minutes to execute.

To avoid any delays, ensure that the Databricks cluster is running before executing the queries.
</Note>

## Connection

Establish a connection to your Databricks workspace from MindsDB by executing the following SQL command:

```sql
CREATE DATABASE databricks_datasource
WITH
  engine = 'databricks',
  parameters = {
      "server_hostname": "adb-1234567890123456.7.azuredatabricks.net",
      "http_path": "sql/protocolv1/o/1234567890123456/1234-567890-test123",
      "access_token": "dapi1234567890ab1cde2f3ab456c7d89efa",
      "schema": "example_db"
  };
```

Required connection parameters include the following:

* `server_hostname`: The server hostname for the cluster or SQL warehouse.
* `http_path`: The HTTP path of the cluster or SQL warehouse.
* `access_token`: A Databricks personal access token for the workspace.

<Tip>
Refer the instructions given https://docs.databricks.com/en/integrations/compute-details.html and https://docs.databricks.com/en/dev-tools/python-sql-connector.html#authentication to find the connection parameters mentioned above for your compute resource.
</Tip>

Optional connection parameters include the following:

* `session_configuration`: Additional (key, value) pairs to set as Spark session configuration parameters. This should be provided as a JSON string.
* `http_headers`: Additional (key, value) pairs to set in HTTP headers on every RPC request the client makes. This should be provided as a JSON string.
* `catalog`: The catalog to use for the connection. Default is `hive_metastore`.
* `schema`: The schema (database) to use for the connection. Default is `default`.

## Usage

Retrieve data from a specified table by providing the integration name, catalog, schema, and table name:

```sql
SELECT *
FROM databricks_datasource.catalog_name.schema_name.table_name
LIMIT 10;
```

<Note>
The catalog and schema names only need to be provided if the table to be queried is not in the specified (or default) catalog and schema.
</Note>

Run Databricks SQL queries directly on the connected Databricks workspace:

```sql
SELECT * FROM databricks_datasource (

    --Native Query Goes Here
    SELECT
      city,
      car_model,
      RANK() OVER (PARTITION BY car_model ORDER BY quantity) AS rank
    FROM dealer
    QUALIFY rank = 1;
);

```

<Note>
The above examples utilize `databricks_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

## Troubleshooting Guide

<Warning>
`Database Connection Error`

* **Symptoms**: Failure to connect MindsDB with the Databricks workspace.
* **Checklist**:
    1. Make sure the Databricks workspace is active.
    2. Confirm that server hostname, HTTP path, access token are correctly provided. If the catalog and schema are provided, ensure they are correct as well.
    3. Ensure a stable network between MindsDB and Databricks workspace.
</Warning>

<Warning>
SQL statements running against tables (of reasonable size) are taking longer than expected.

* **Symptoms**: SQL queries taking longer than expected to execute.
* **Checklist**:
    1. Ensure the Databricks cluster is running before executing the queries.
    2. Check the network connection between MindsDB and Databricks workspace.
</Warning>

<Warning>
`SQL statement cannot be parsed by mindsdb_sql`

* **Symptoms**: SQL queries failing or not recognizing table names containing special characters.
* **Checklist**:
    1. Ensure table names with special characters are enclosed in backticks.
    2. Examples:
        * Incorrect: SELECT * FROM integration.travel-data
        * Incorrect: SELECT * FROM integration.'travel-data'
        * Correct: SELECT * FROM integration.\`travel-data\`
</Warning>
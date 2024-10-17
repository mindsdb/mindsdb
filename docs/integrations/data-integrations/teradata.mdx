---
title: Teradata
sidebarTitle: Teradata
---

This documentation describes the integration of MindsDB with [Teradata](https://www.teradata.com/why-teradata), the complete cloud analytics and data platform for Trusted AI.
The integration allows MindsDB to access data from Teradata and enhance Teradata with AI capabilities.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To connect Teradata to MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).

## Connection

Establish a connection to Teradata from MindsDB by executing the following SQL command and providing its [handler name](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/teradata_handler) as an engine.

```sql
CREATE DATABASE teradata_datasource
WITH
   ENGINE = 'teradata',
   PARAMETERS = {
      "host": "192.168.0.41",
      "user": "demo_user",
      "password": "demo_password",
      "database": "example_db"
   };
```

Required connection parameters include the following:

* `host`: The hostname, IP address, or URL of the Teradata server.
* `user`: The username for the Teradata database.
* `password`: The password for the Teradata database.

Optional connection parameters include the following:

* `database`: The name of the Teradata database to connect to. Defaults is the user's default database.

## Usage

Retrieve data from a specified table by providing the integration, database and table names:

```sql
SELECT *
FROM teradata_datasource.database_name.table_name
LIMIT 10;
```

Run Teradata SQL queries directly on the connected Teradata database:

```sql
SELECT * FROM teradata_datasource (

   --Native Query Goes Here
   SELECT emp_id, emp_name, job_duration AS tsp
   FROM employee
   EXPAND ON job_duration AS tsp BY INTERVAL '1' YEAR
      FOR PERIOD(DATE '2006-01-01', DATE '2008-01-01');

);
```

<Note>
The above examples utilize `teradata_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

## Troubleshooting

<Warning>
`Database Connection Error`

* **Symptoms**: Failure to connect MindsDB with the Teradata database.
* **Checklist**:
    1. Make sure the Teradata database is active.
    2. Confirm that host, user and password are correct. Try a direct connection using a client like DBeaver.
    3. Ensure a stable network between MindsDB and Teradata.
</Warning>

<Warning>
`SQL statement cannot be parsed by mindsdb_sql`

* **Symptoms**: SQL queries failing or not recognizing table names containing spaces or special characters.
* **Checklist**:
    1. Ensure table names with spaces or special characters are enclosed in backticks.
    2. Examples:
        * Incorrect: SELECT * FROM integration.travel-data
        * Incorrect: SELECT * FROM integration.'travel-data'
        * Correct: SELECT * FROM integration.\`travel-data\`
</Warning>

<Warning>
`Connection Timeout Error`

* **Symptoms**: Connection to the Teradata database times out or queries take too long to execute.
* **Checklist**:
    1. Ensure the Teradata server is running and accessible (if the server has been idle for a long time, it may have shut down automatically).

</Warning>
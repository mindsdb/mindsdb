---
title: Snowflake
sidebarTitle: Snowflake
---

This documentation describes the integration of MindsDB with [Snowflake](https://www.snowflake.com/en/), a cloud data warehouse used to store and analyze data.
The integration allows MindsDB to access data stored in the Snowflake database and enhance it with AI capabilities.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).
2. To connect Snowflake to MindsDB, install the required dependencies following [this instruction](/setup/self-hosted/docker#install-dependencies).

## Connection

Establish a connection to your Snowflake database from MindsDB by executing the following SQL command:

```sql
CREATE DATABASE snowflake_datasource
WITH
    ENGINE = 'snowflake',
    PARAMETERS = {
        "account": "tvuibdy-vm85921",
        "password": "password",
        "database": "test_db"
    };
```

Required connection parameters include the following:

* `account`: The Snowflake account identifier. This [guide](https://docs.snowflake.com/en/user-guide/admin-account-identifier) will help you find your account identifier.
* `user`: The username for the Snowflake account.
* `password`: The password for the Snowflake account.
* `database`: The name of the Snowflake database to connect to.

Optional connection parameters include the following:

* `warehouse`: The Snowflake warehouse to use for running queries.
* `schema`: The database schema to use within the Snowflake database. Default is `PUBLIC`.
* `role`: The Snowflake role to use.

## Usage

Retrieve data from a specified table by providing the integration name, schema, and table name:

```sql
SELECT *
FROM snowflake_datasource.schema_name.table_name
LIMIT 10;
```

Run Snowflake SQL queries directly on the connected Snowflake database:

```sql
SELECT * FROM snowflake_datasource (

    --Native Query Goes Here
    SELECT
        employee_table.* EXCLUDE department_id,
        department_table.* RENAME department_name AS department
    FROM employee_table INNER JOIN department_table
        ON employee_table.department_id = department_table.department_id
    ORDER BY department, last_name, first_name;

);
```

<Note>
The above examples utilize `snowflake_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

## Troubleshooting Guide

<Warning>
`Database Connection Error`

* **Symptoms**: Failure to connect MindsDB with the Snowflake account.
* **Checklist**:
    1. Make sure the Snowflake is active.
    2. Confirm that account, user, password and database are correct. Try a direct Snowflake connection using a client like DBeaver.
    3. Ensure a stable network between MindsDB and Snowflake.
</Warning>

<Warning>
`SQL statement cannot be parsed by mindsdb_sql`

* **Symptoms**: SQL queries failing or not recognizing table names containing spaces or special characters.
* **Checklist**:
    1. Ensure table names with spaces or special characters are enclosed in backticks.
    2. Examples:
        * Incorrect: SELECT * FROM integration.travel data
        * Incorrect: SELECT * FROM integration.'travel data'
        * Correct: SELECT * FROM integration.\`travel data\`
</Warning>

This [troubleshooting guide](https://community.snowflake.com/s/article/Snowflake-Client-Connectivity-Troubleshooting) provided by Snowflake might also be helpful.
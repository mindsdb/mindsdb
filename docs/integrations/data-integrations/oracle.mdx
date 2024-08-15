---
title: Oracle
sidebarTitle: Oracle
---

This documentation describes the integration of MindsDB with [Oracle](https://www.techopedia.com/definition/8711/oracle-database), one of the most trusted and widely used relational database engines for storing, organizing and retrieving data by type while still maintaining relationships between the various types.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).
2. To connect Oracle to MindsDB, install the required dependencies following [this instruction](/setup/self-hosted/docker#install-dependencies).

## Connection

Establish a connection to your Oracle database from MindsDB by executing the following SQL command:

```sql
CREATE DATABASE oracle_datasource
WITH
  ENGINE = 'oracle',
  PARAMETERS = {
      "host": "localhost",
      "service_name": "FREEPDB1",
      "user": "SYSTEM",
      "password": "password"
  };
```

Required connection parameters include the following:

* `user`: The username for the Oracle database.
* `password`: The password for the Oracle database.

* `dsn`: The data source name (DSN) for the Oracle database.
OR
* `host`: The hostname, IP address, or URL of the Oracle server.
AND
* `sid`: The system identifier (SID) of the Oracle database.
OR
* `service_name`: The service name of the Oracle database.

Optional connection parameters include the following:

* `port`: The port number for connecting to the Oracle database. Default is 1521.
* `disable_oob`: The boolean parameter to disable out-of-band breaks. Default is `false`.
* `auth_mode`: The authorization mode to use.

## Usage

Retrieve data from a specified table by providing the integration name, schema, and table name:

```sql
SELECT *
FROM oracle_datasource.schema_name.table_name
LIMIT 10;
```

Run PL/SQL queries directly on the connected Oracle database:

```sql
SELECT * FROM oracle_datasource (

    --Native Query Goes Here
    SELECT employee_id, first_name, last_name, email, hire_date
    FROM oracle_datasource.hr.employees
    WHERE department_id = 10
    ORDER BY hire_date DESC;

);
```

<Note>
The above examples utilize `oracle_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

## Troubleshooting Guide

<Warning>
`Database Connection Error`

* **Symptoms**: Failure to connect MindsDB with the Oracle database.
* **Checklist**:
    1. Make sure the Oracle database is active.
    2. Confirm that the connection parameters provided (DSN, host, SID, service_name) and the credentials (user, password) are correct.
    3. Ensure a stable network between MindsDB and Oracle.
</Warning>

This [troubleshooting guide](https://docs.oracle.com/en/database/oracle/oracle-database/19/ntqrf/database-connection-issues.html) provided by Oracle might also be helpful.
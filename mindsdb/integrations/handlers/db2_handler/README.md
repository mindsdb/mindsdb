---
title: IBM Db2
sidebarTitle: IBM Db2
---

This documentation describes the integration of MindsDB with [IBM Db2](https://www.ibm.com/db2), the cloud-native database built to power low-latency transactions, real-time analytics and AI applications at scale.
The integration allows MindsDB to access data stored in the IBM Db2 database and enhance it with AI capabilities.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).
2. To connect IBM Db2 to MindsDB, install the required dependencies following [this instruction](/setup/self-hosted/docker#install-dependencies).

## Connection

Establish a connection to your IBM Db2 database from MindsDB by executing the following SQL command:

```sql
CREATE DATABASE db2_datasource
WITH
    engine = 'db2',
    parameters = {
        "host": "127.0.0.1",
        "user": "db2inst1",
        "password": "password",
        "database": "example_db"
    };
```

Required connection parameters include the following:

* `host`: The hostname, IP address, or URL of the IBM Db2 database.
* `user`: The username for the IBM Db2 database.
* `password`: The password for the IBM Db2 database.
* `database`: The name of the IBM Db2 database to connect to.

Optional connection parameters include the following:

* `port`: The port number for connecting to the IBM Db2 database. Default is `50000`.
* `schema`: The database schema to use within the IBM Db2 database.

## Usage

Retrieve data from a specified table by providing the integration name, schema, and table name:

```sql
SELECT *
FROM db2_datasource.schema_name.table_name
LIMIT 10;
```

Run IBM Db2 native queries directly on the connected database:

```sql
SELECT * FROM db2_datasource (

    --Native Query Goes Here
    WITH
    DINFO (DEPTNO, AVGSALARY, EMPCOUNT) AS
        (SELECT OTHERS.WORKDEPT, AVG(OTHERS.SALARY), COUNT(*)
            FROM EMPLOYEE OTHERS
            GROUP BY OTHERS.WORKDEPT
        ),
    DINFOMAX AS
        (SELECT MAX(AVGSALARY) AS AVGMAX FROM DINFO)
    SELECT THIS_EMP.EMPNO, THIS_EMP.SALARY,
        DINFO.AVGSALARY, DINFO.EMPCOUNT, DINFOMAX.AVGMAX
    FROM EMPLOYEE THIS_EMP, DINFO, DINFOMAX
    WHERE THIS_EMP.JOB = 'SALESREP'
    AND THIS_EMP.WORKDEPT = DINFO.DEPTNO

);
```

<Note>
The above examples utilize `db2_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

## Troubleshooting Guide

<Warning>
`Database Connection Error`

* **Symptoms**: Failure to connect MindsDB with the IBM Db2 database.
* **Checklist**:
    1. Make sure the IBM Db2 database is active.
    2. Confirm that host, user, password and database are correct. Try a direct connection using a client like DBeaver.
    3. Ensure a stable network between MindsDB and the IBM Db2 database.
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

This [guide](https://www.ibm.com/docs/en/db2/11.5?topic=connect-common-db2-problems) of common connection Db2 connection issues provided by IBM might also be helpful.

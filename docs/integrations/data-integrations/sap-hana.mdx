---
title: SAP HANA
sidebarTitle: SAP HANA
---

This documentation describes the integration of MindsDB with [SAP HANA](https://www.sap.com/products/technology-platform/hana/what-is-sap-hana.html), a multi-model database with a column-oriented in-memory design that stores data in its memory instead of keeping it on a disk.
The integration allows MindsDB to access data from SAP HANA and enhance SAP HANA with AI capabilities.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To connect SAP HANA to MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).

## Connection

Establish a connection to SAP HANA from MindsDB by executing the following SQL command and providing its [handler name](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/hana_handler) as an engine.

```sql
CREATE DATABASE sap_hana_datasource
WITH
    ENGINE = 'hana',
    PARAMETERS = {
        "address": "123e4567-e89b-12d3-a456-426614174000.hana.trial-us10.hanacloud.ondemand.com",
        "port": "443",
        "user": "demo_user",
        "password": "demo_password",
        "encrypt": true
    };
```

Required connection parameters include the following:

* `address`: The hostname, IP address, or URL of the SAP HANA database.
* `port`: The port number for connecting to the SAP HANA database.
* `user`: The username for the SAP HANA database.
* `password`: The password for the SAP HANA database.

Optional connection parameters include the following:

* 'database': The name of the database to connect to. This parameter is not used for SAP HANA Cloud.
* `schema`: The database schema to use. Defaults to the user's default schema.
* `encrypt`: The setting to enable or disable encryption. Defaults to `True'

## Usage

Retrieve data from a specified table by providing the integration, schema and table names:

```sql
SELECT *
FROM sap_hana_datasource.schema_name.table_name
LIMIT 10;
```

Run Teradata SQL queries directly on the connected Teradata database:

```sql
SELECT * FROM sap_hana_datasource (

    --Native Query Goes Here
    SELECT customer, year, SUM(sales)
        FROM t1
        GROUP BY ROLLUP(customer, year);

    SELECT customer, year, SUM(sales)
        FROM t1
        GROUP BY GROUPING SETS
        (
        (customer, year),
        (customer)
        )
    UNION ALL
    SELECT NULL, NULL, SUM(sales)
        FROM t1;    

);
```

<Note>
The above examples utilize `sap_hana_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

## Troubleshooting

<Warning>
`Database Connection Error`

* **Symptoms**: Failure to connect MindsDB with the SAP HANA database.
* **Checklist**:
    1. Make sure the SAP HANA database is active.
    2. Confirm that address, port, user and password are correct. Try a direct connection using a client like DBeaver.
    3. Ensure a stable network between MindsDB and SAP HANA.
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
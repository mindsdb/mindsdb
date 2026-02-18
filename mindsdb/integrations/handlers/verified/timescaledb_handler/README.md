---
title: TimescaleDB
sidebarTitle: TimescaleDB
---

This documentation describes the integration of MindsDB with [TimescaleDB](https://docs.timescale.com).

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).
2. To connect TimescaleDB to MindsDB, install the required dependencies following [this instruction](/setup/self-hosted/docker#install-dependencies).

## Connection

Establish a connection to TimescaleDB from MindsDB by executing the following SQL command and providing its [handler name](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/timescaledb_handler) as an engine.

```sql
CREATE DATABASE timescaledb_datasource
WITH
    engine = 'timescaledb',
    parameters = {
        "host": "examplehost.timescaledb.com",
        "port": 5432,
        "user": "example_user",
        "password": "my_password",
        "database": "tsdb"
    };
```


Required connection parameters include the following:

*    `user`: The username for the TimescaleDB database.
*    `password`: The password for the TimescaleDB database.
*    `host`: The hostname, IP address, or URL of the TimescaleDB server.
*    `port`: The port number for connecting to the TimescaleDB server.
*    `database`: The name of the TimescaleDB database to connect to.

Optional connection parameters include the following:

*    `schema`: The database schema to use. Default is public.


## Usage

Before attempting to connect to a TimescaleDB server using MindsDB, ensure that it accepts incoming connections using [this guide](https://docs.timescale.com/latest/getting-started/setup/remote-connections/).

The following usage examples utilize the connection to TimescaleDB made via the `CREATE DATABASE` statement and named `timescaledb_datasource`.

Retrieve data from a specified table by providing the integration and table name.


You can use this established connection to query your table as follows,

```sql
SELECT *
FROM timescaledb_datasource.sensor;
```

Run PostgreSQL-native queries directly on the connected TimescaleDB database:

```sql
SELECT * FROM timescaledb_datasource (

    --Native Query Goes Here
     SELECT 
        model, 
        COUNT(*) OVER (PARTITION BY model, year) AS units_to_sell, 
        ROUND((CAST(tax AS decimal) / price), 3) AS tax_div_price
    FROM used_car_price

);
```

## Troubleshooting

<Warning>
`Database Connection Error`

* **Symptoms**: Failure to connect MindsDB with the TimescaleDB database.
* **Checklist**:
    1. Make sure the TimescaleDB server is active.
    2. Confirm that host, port, user, schema, and password are correct. Try a direct TimescaleDB connection.
    3. Ensure a stable network between MindsDB and TimescaleDB.
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
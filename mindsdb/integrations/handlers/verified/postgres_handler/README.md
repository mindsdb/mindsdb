---
title: PostgreSQL
sidebarTitle: PostgreSQL
---

This documentation describes the integration of MindsDB with [PostgreSQL](https://www.postgresql.org/), a powerful, open-source, object-relational database system. 
The integration allows MindsDB to access data stored in the PostgreSQL database and enhance PostgreSQL with AI capabilities.

### Prerequisites

Before proceeding, ensure the following prerequisites are met:

 1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
 2. To connect PostgreSQL to MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).

## Connection

Establish a connection to your PostgreSQL database from MindsDB by executing the following SQL command:

```sql
CREATE DATABASE postgresql_conn 
WITH ENGINE = 'postgres', 
PARAMETERS = {
    "host": "127.0.0.1",
    "port": 5432,
    "database": "postgres",
    "user": "postgres",
    "schema": "data",
    "password": "password"
};
```

Required connection parameters include the following:

*    `user`: The username for the PostgreSQL database.
*    `password`: The password for the PostgreSQL database.
*    `host`: The hostname, IP address, or URL of the PostgreSQL server.
*    `port`: The port number for connecting to the PostgreSQL server.
*    `database`: The name of the PostgreSQL database to connect to.

Optional connection parameters include the following:

*    `schema`: The database schema to use. Default is public.
*    `sslmode`: The SSL mode for the connection.
*    `connection_parameters`:  allows passing any PostgreSQL libpq parameters, such as:
    * SSL settings: sslrootcert, sslcert, sslkey, sslcrl, sslpassword
    * Network and reliability options: connect_timeout, keepalives, keepalives_idle, keepalives_interval, keepalives_count
    * Session options: application_name, options, client_encoding
    * Any other libpq-supported parameter

## Usage

The following usage examples utilize the connection to PostgreSQL made via the `CREATE DATABASE` statement and named `postgresql_conn`.

Retrieve data from a specified table by providing the integration name, schema, and table name:

```sql
SELECT *
FROM postgresql_conn.table_name
LIMIT 10;
```

Run PostgreSQL-native queries directly on the connected PostgreSQL database:

```sql
SELECT * FROM postgresql_conn (

    --Native Query Goes Here
     SELECT 
        model, 
        COUNT(*) OVER (PARTITION BY model, year) AS units_to_sell, 
        ROUND((CAST(tax AS decimal) / price), 3) AS tax_div_price
    FROM used_car_price

);
```

<Tip>
**Next Steps**

Follow [this tutorial](https://docs.mindsdb.com/use-cases/predictive_analytics/house-sales-forecasting) to see more use case examples.
</Tip>

## Troubleshooting

<Warning>
`Database Connection Error`

* **Symptoms**: Failure to connect MindsDB with the PostgreSQL database.
* **Checklist**:
    1. Make sure the PostgreSQL server is active.
    2. Confirm that host, port, user, schema, and password are correct. Try a direct PostgreSQL connection.
    3. Ensure a stable network between MindsDB and PostgreSQL.
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

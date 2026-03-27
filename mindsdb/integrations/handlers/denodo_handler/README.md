---
title: Denodo
sidebarTitle: Denodo
---

This documentation describes the integration of MindsDB with [Denodo](https://www.denodo.com/), a powerful data virtualization platform that enables real-time access and integration of multiple data sources.
The integration allows MindsDB to query Denodo views and enhance them with AI capabilities.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).

## Connection

Establish a connection to Denodo from MindsDB by executing the following SQL command and providing its [handler name](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/denodo_handler) as an engine.

```sql
CREATE DATABASE denodo_conn
WITH ENGINE = 'denodo',
PARAMETERS = {
    "host": "host-name",
    "port": 9996,
    "database": "db-name",
    "user": "user-name",
    "password": "password"
};
```

Required connection parameters include the following:

- `user`: The username for the Denodo database.
- `password`: The password for the Denodo database.
- `host`: The hostname, IP address, or URL of the Denodo server.
- `port`: The port number for connecting to the Denodo server (default is `9999`).
- `database`: The name of the Denodo virtual database to connect to.

## Usage

The following usage examples utilize the connection to Denodo made via the `CREATE DATABASE` statement and named `denodo_conn`.

Retrieve data from a specified Denodo view by providing the integration and view name.

```sql
SELECT *
FROM denodo_conn.view_name
LIMIT 10;
```

Running native SQL queries on Denodo views is also supported.

```sql
SELECT * FROM denodno_conn (
    DESC VIEW view_name
);
```
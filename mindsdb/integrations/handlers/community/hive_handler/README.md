---
title: Apache Hive
sidebarTitle: Apache Hive
---

This documentation describes the integration of MindsDB with [Apache Hive](https://hive.apache.org/), a data warehouse software project built on top of Apache Hadoop for providing data query and analysis. Hive gives an SQL-like interface to query data stored in various databases and file systems that integrate with Hadoop.
The integration allows MindsDB to access data from Apache Hive and enhance Apache Hive with AI capabilities.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To connect Apache Hive to MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).

## Connection

Establish a connection to Apache Hive from MindsDB by executing the following SQL command and providing its [handler name](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/hive_handler) as an engine.

```sql
CREATE DATABASE hive_datasource
WITH
  engine = 'hive',
  parameters = {
    "username": "demo_user",
    "password": "demo_password",
    "host": "127.0.0.1",
    "database": "default"
  };
```

Required connection parameters include the following:

* `host`: The hostname, IP address, or URL of the Apache Hive server.
* `database`: The name of the Apache Hive database to connect to.

Optional connection parameters include the following:

* `username`: The username for the Apache Hive database.
* `password`: The password for the Apache Hive database.
* `port`: The port number for connecting to the Apache Hive server. Default is `10000`.
* `auth`: The authentication mechanism to use. Default is `CUSTOM`. Other options are `NONE`, `NOSASL`, `KERBEROS` and `LDAP`.

## Usage

Retrieve data from a specified table by providing the integration and table names:

```sql
SELECT *
FROM hive_datasource.table_name
LIMIT 10;
```

Run HiveQL queries directly on the connected Apache Hive database:

```sql
SELECT * FROM hive_datasource (

    --Native Query Goes Here
    FROM (FROM (FROM src
                SELECT TRANSFORM(value)
                USING 'mapper'
                AS value, count) mapped
          SELECT cast(value as double) AS value, cast(count as int) AS count
          SORT BY value, count) sorted
    SELECT TRANSFORM(value, count)
    USING 'reducer'
    AS whatever

);
```

<Note>
The above examples utilize `hive_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

## Troubleshooting

<Warning>
`Database Connection Error`

* **Symptoms**: Failure to connect MindsDB with the Apache Hive database.
* **Checklist**:
    1. Ensure that the Apache Hive server is running and accessible
    2. Confirm that host, port, user, and password are correct. Try a direct Apache Hive connection using a client like DBeaver.
    3. Test the network connection between the MindsDB host and the Apache Hive server.
</Warning>

# `#!sql CREATE DATABASE` Statement

## Description

MindsDB enables connections to your favorite databases, data warehouses, data lakes, via the `#!sql CREATE DATABASE` syntax.

Our MindsDB SQL API supports creating a database connection by passing any credentials needed by each type of system that you are connecting to.

## Syntax

```sql
CREATE DATABASE [datasource_name]
WITH
    engine=[engine_string],
    parameters={
            "key":"value",
            ...
    };
```

On execution, you should get:

```sql
Query OK, 0 rows affected (x.xxx sec)
```

Where:

|                     | Description                                                      |
| ------------------- | ---------------------------------------------------------------- |
| `[datasource_name]` | Identifier for the datasource to be created                      |
| `[engine_string]`   | Engine to be selected depending on the database connection       |
| `parameters`   | `#!json {"key":"value"}` object with the connection parameters specific for each engine  |

## Example

Here is a concrete example on how to connect to a MySQL database.

```sql
CREATE DATABASE mysql_datasource
WITH
 engine='mariadb',
 parameters={
                "user":"root",
                "port": 3307,
                "password": "Mimzo3i-mxt@9CpThpBj",
                "host": "127.0.0.1",
                "database": "my_database"
        };
```

On execution:

```sql
Query OK, 0 rows affected (8.878 sec)
```

## Listing Linked DATABASES

You can list linked databases as follows:

```sql
SHOW DATABASES;
```

On execution:

```sql
+--------------------+
| Database           |
+--------------------+
| information_schema |
| mindsdb            |
| files              |
| views              |
| example_db         |
+--------------------+
```

## Getting Linked DATABASES Metadata

You can also get metadata about the linked databases in `mindsdb.datasources`:

```sql
SELECT * FROM mindsdb.datasources;
```

On execution:

```sql

+------------+---------------+--------------+------+-----------+
| name       | database_type | host         | port | user      |
+------------+---------------+--------------+------+-----------+
| example_db | postgres      | 3.220.66.106 | 5432 | demo_user |
+------------+---------------+--------------+------+-----------+
```

## Supported Integrations

### Snowflake

```sql
CREATE DATABASE snowflake_datasource
WITH
    engine='snowflake',
    parameters={
            "user":"user",
            "port": 443,
            "password": "Mimzo3i-mxt@9CpThpBj",
            "host": "127.0.0.1",
            "database": "snowflake",
            "account": "account",
            "schema": "public",
            "protocol": "https",
            "warehouse": "warehouse"
    };
```

### Singlestore

```sql
CREATE DATABASE singlestore_datasource
WITH
    engine='singlestore',
    parameters={
            "user":"root",
            "port": 3306,
            "password": "Mimzo3i-mxt@9CpThpBj",
            "host": "127.0.0.1",
            "database": "singlestore"
    };
```

### MySQL

```sql
CREATE DATABASE mysql_datasource
WITH
    engine='mysql',
    parameters={
            "user":"root",
            "port": 3306,
            "password": "Mimzo3i-mxt@9CpThpBj",
            "host": "127.0.0.1",
            "database": "mysql"
    };
```

### ClickHouse

```sql
CREATE DATABASE clickhouse_datasource
WITH
    engine='clickhouse',
    parameters={
            "user":"default",
            "port": 9000,
            "password": "Mimzo3i-mxt@9CpThpBj",
            "host": "127.0.0.1",
            "database": "default"
    };
```

### PostgreSQL

```sql
CREATE DATABASE psql_datasource
WITH
    engine='postgres',
    parameters={
            "user":"postgres",
            "port": 5432,
            "password": "Mimzo3i-mxt@9CpThpBj",
            "host": "127.0.0.1",
            "database": "postgres"
    };
```

### Cassandra

```sql
CREATE DATABASE psql_datasource
WITH
    engine='cassandra',
    parameters={
            "user":"cassandra",
            "port": 9042,
            "password": "cassandra",
            "host": "127.0.0.1",
            "database": "keyspace"
    };
```

### MariaDB

```sql
CREATE DATABASE maria_datasource
WITH
    engine='mariadb',
    parameters={
            "user":"root",
            "port": 3306,
            "password": "Mimzo3i-mxt@9CpThpBj",
            "host": "127.0.0.1",
            "database": "mariadb"
    };
```

### Microsoft SQL Server

```sql
CREATE DATABASE mssql_datasource
WITH
    engine='mssql',
    parameters={
            "user":"sa",
            "port": 1433,
            "password": "Mimzo3i-mxt@9CpThpBj",
            "host": "127.0.0.1",
            "database": "master"
    };
```

### Scylladb

```sql
CREATE DATABASE scylladb_datasource
WITH
    engine='scylladb',
    parameters={
            "user":"scylladb",
            "port": 9042,
            "password": "Mimzo3i-mxt@9CpThpBj",
            "host": "127.0.0.1",
            "database": "scylladb"
    };
```

### Trino

```sql
CREATE DATABASE trino_datasource
WITH
    engine='trinodb',
    parameters={
            "user":"trino",
            "port": 8080,
            "password": "Mimzo3i-mxt@9CpThpBj",
            "host": "127.0.0.1",
            "catalog": "default",
            "schema": "test"
    };
```

### QuestDB

```sql
CREATE DATABASE questdb_datasource
WITH
    engine='questdb',
    parameters={
            "user":"admin",
            "port": 8812,
            "password": "quest",
            "host": "127.0.0.1",
            "database": "qdb"
    };
```


## Connecting Through Ngrok

When connecting your local database to MindsDB Cloud, you need to expose the local database server to be publicly accessible using [Ngrok Tunnel](https://ngrok.com). The free tier offers all you need to get started.

The installation instructions are easy to follow, head over to the [downloads page](https://ngrok.com/download) and choose your operating system. Follow the instructions for installation.

Then [create a free account](https://dashboard.ngrok.com/signup) to get an auth token that you can use to config your ngrok instance.

Once installed and configured, run the following command to obtain the host and port number:

```bash
ngrok tcp [port-number]
```

Example:

```bash
ngrok tcp 5431  # assuming you are running a db on the port 5432, for example, postgres
```

At this point you will see a line saying something like this:
```bash
Session Status                online
Account                       myaccount (Plan: Free)
Version                       2.3.40
Region                        United States (us)
Web Interface                 http://127.0.0.1:4040
Forwarding                    tcp://4.tcp.ngrok.io:15093 -> localhost 5432
```

The forwarded address information will be required when connecting to MindsDB's GUI. We will make use of the `Forwarding` information, in this case it is tcp://4.tcp.ngrok.io:15093 where where tcp://4.tcp.ngrok.io will be used for the host parameter and 15093 as the port number.

Proceed to create a database connection in the MindsDB GUI. Once you have selected a database as a datasource, you can execute the syntax with the host and port number retrieved.

Example:

```sql
CREATE DATABASE psql_datasource
WITH
    engine='postgres',
    parameters={
            "user":"postgres",
            "port": 15093,
            "password": "Mimzo3i-mxt@9CpThpBj",
            "host": "4.tcp.ngrok.io", 
            "database": "postgres"
    };
```

Please note that when the tunnel loses connection(the ngrok tunnel is stopped or cancelled), you will have to reconnect your database again. In the free tier, Ngrok changes the url each time you launch the program, so if you need to reset the connection you will have to drop the datasource using the DROP DATABASE syntax:

```sql
DROP DATABASE example_db;
```

You can go ahead and set up the connection again. Your trained predictors won't be affected, however if you have to RETRAIN the predictors please ensure the database connection has the same name you used when creating the predictor to avoid it failing to retrain.


!!! info "Work in progress"
Note this feature is in beta version. If you have additional questions about other supported datasources or you experience some issues [reach out to us on Slack](https://join.slack.com/t/mindsdbcommunity/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ) or open GitHub issue.

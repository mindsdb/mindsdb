# `#!sql CREATE DATABASE` Statement

## Description

MindsDB enables connections to your favorite databases, data warehouses, data lakes, via the `#!sql CREATE DATABASE` syntax.

Our MindsDB SQL API supports creating a database connection by passing any credentials needed by each type of system that you are connecting to.

## Syntax

```sql
CREATE DATABASE [datasource_name]
WITH ENGINE=[engine_string],
PARAMETERS={
  "key":"value",
  ...
};
```

On execution, we get:

```sql
Query OK, 0 rows affected (x.xxx sec)
```

Where:

| Name                | Description                                                                              |
| ------------------- | ---------------------------------------------------------------------------------------- |
| `[datasource_name]` | Identifier for the datasource to be created                                              |
| `[engine_string]`   | Engine to be selected depending on the database connection                               |
| `parameters`        | `#!json {"key":"value"}` object with the connection parameters specific for each engine  |

## Example

Here is a concrete example on how to connect to a MySQL database.

```sql
CREATE DATABASE mysql_datasource
WITH ENGINE='mariadb',
PARAMETERS={
  "user":"root",
  "port": 3307,
  "password": "Mimzo3i-mxt@9CpThpBj",
  "host": "127.0.0.1",
  "database": "my_database"
};
```

On execution, we get:

```sql
Query OK, 0 rows affected (8.878 sec)
```

## Listing Linked DATABASES

You can list linked databases as follows:

```sql
SHOW DATABASES;
```

On execution, we get:

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
SELECT *
FROM mindsdb.datasources;
```

On execution, we get:

```sql
+------------+---------------+--------------+------+-----------+
| name       | database_type | host         | port | user      |
+------------+---------------+--------------+------+-----------+
| example_db | postgres      | 3.220.66.106 | 5432 | demo_user |
+------------+---------------+--------------+------+-----------+
```

## Supported Integrations

The list of databases supported by MindsDB keeps growing. Here are the currently supported integrations:

<p align="center">
  <img src="/assets/supported_integrations.png" />
</p>

Let's look at sample codes showing how to connect to each of the supported integrations. You can also have a look at the particular [databases' handler files here](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers) to see their connection arguments.

### Airtable

=== "Template"

    ```sql
    CREATE DATABASE airtable_datasource          --- display name for database
    WITH ENGINE='airtable',                      --- name of the mindsdb handler
    PARAMETERS={
      "base_id": " ",                            --- the Airtable base ID
      "table_name": " ",                         --- the Airtable table name
      "api_key": " "                             --- the API key for the Airtable API
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE airtable_datasource
    WITH ENGINE='airtable',
    PARAMETERS={
      "base_id": "appve10klsda2",
      "table_name": "my_table",
      "api_key": "KdJX2Q5km%5b$T$sQYm^gvN"
    };
    ```

### Amazon Redshift

```sql
CREATE DATABASE amazonredshift_datasource
WITH ENGINE='amazonredshift',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 5439,
  "database": "test",
  "user": "amazonredshift",
  "password": "amazonredshift"
};
```

### Amazon S3

```sql
CREATE DATABASE amazons3_datasource
WITH ENGINE = 's3',
PARAMETERS = {
  "aws_access_key_id": " ",
  "aws_secret_access_key": " ",
  "region_name": " ",
  "bucket": " ",
  "key": " ",
  "input_serialization": " "
};
```

### cassandra

```sql
CREATE DATABASE cassandra_datasource
WITH ENGINE='cassandra',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 9042,
  "user": "cassandra",
  "password": "cassandra",
  "protocol_version": ,
  "keyspace": " ",
  "secure_connect_bundle": {
    "path": " "
  }
};
```

The `secure_connect_bundle` parameter can be defined as a path or a URL.

```sql
CREATE DATABASE cassandra_datasource
WITH ENGINE='cassandra',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 9042,
  "user": "cassandra",
  "password": "cassandra",
  "protocol_version": ,
  "keyspace": " ",
  "secure_connect_bundle": {
    "url": " "
  }
};
```

### ckan

```sql
CREATE DATABASE ckan_datasource
WITH ENGINE = 'ckan',
PARAMETERS = {
  "url": "http://demo.ckan.org/api/3/action/",
  "apikey": " "
};
```

### ClickHouse

```sql
CREATE DATABASE clickhouse_datasource
WITH ENGINE='clickhouse',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 9000,
  "database": "default",
  "user": "default",
  "password": "Mimzo3i-mxt@9CpThpBj"
};
```

### Cockroach Labs

```sql
CREATE DATABASE cockroach_datasource
WITH ENGINE='cockroachdb',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 26257,
  "database": "cockroachdb",
  "user": "username",
  "password": "Mimzo3i-mxt@9CpThpBj"
};
```

### Couchbase

```sql
CREATE DATABASE couchbase_datasource
WITH ENGINE = 'couchbase',
PARAMETERS = {
  "host": "127.0.0.1",
  "user": "couchbase",
  "password": "Mimzo3i-mxt@9CpThpBj",
  "bucket": " ",
  "scope": " "
};
```

### CrateDB

```sql
CREATE DATABASE cratedb_datasource
WITH ENGINE='crate',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 4200,
  "user": "crate",
  "password": "Mimzo3i-mxt@9CpThpBj",
  "schema_name": " "
};
```

### databricks

```sql
CREATE DATABASE databricks_datasource
WITH ENGINE='databricks',
PARAMETERS={
  "server_hostname": " ",
  "http_path": " ",
  "access_token": " ",
  "session_configuration": " ",
  "http_headers": " ",
  "catalog": " ",
  "schema": " "
};
```

### DataStax

```sql
CREATE DATABASE datastax_datasource
WITH ENGINE='astra',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 7077,
  "user": "datastax",
  "password": "Mimzo3i-mxt@9CpThpBj",
  "protocol_version": ,
  "keyspace": " ",
  "secure_connection_bundle": {
    "path": " "
  }
};
```

The `secure_connection_bundle` parameter can be defined as a path or a URL.

```sql
CREATE DATABASE datastax_datasource
WITH ENGINE='astra',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 7077,
  "user": "datastax",
  "password": "Mimzo3i-mxt@9CpThpBj",
  "protocol_version": ,
  "keyspace": " ",
  "secure_connection_bundle": {
    "url": " "
  }
};
```

### druid

```sql
CREATE DATABASE druid_datasource
WITH ENGINE = 'druid',
PARAMETERS = {
  "host": "127.0.0.1",
  "port": 8888,
  "user": " ",
  "password": " ",
  "path": " ",
  "scheme": "http"
};
```

### DynamoDB

```sql
CREATE DATABASE dynamodb_datasource
WITH ENGINE='dynamodb',
PARAMETERS={
  "aws_access_key_id": " ",
  "aws_secret_access_key": " ",
  "region_name": " "
};
```

### d0lt

```sql
CREATE DATABASE d0lt_datasource
WITH ENGINE = 'd0lt',
PARAMETERS = {
  "host": "127.0.0.1",
  "port": 3306,
  "database": " ",
  "user": "d0lt",
  "password": "Mimzo3i-mxt@9CpThpBj",
  "ssl": True,
  "ssl_ca": {
    "path": " "
  },
  "ssl_cert": {
    "path": " "
  },
  "ssl_key": {
    "path": " "
  }
};
```

The `ssl` parameter value indicates whether SSL is enabled (`True`) or disabled (`False`). If it is enabled, then the `ssl_ca`, `ssl_cert`, and `ssl_key` paramaters can be defined as a path or a URL.

```sql
CREATE DATABASE d0lt_datasource
WITH ENGINE = 'd0lt',
PARAMETERS = {
  "host": "127.0.0.1",
  "port": 3306,
  "database": " ",
  "user": "d0lt",
  "password": "Mimzo3i-mxt@9CpThpBj",
  "ssl": True,
  "ssl_ca": {
    "url": " "
  },
  "ssl_cert": {
    "url": " "
  },
  "ssl_key": {
    "url": " "
  }
};
```

### elastic

```sql
CREATE DATABASE elastic_datasource
WITH ENGINE = 'elasticsearch',
PARAMETERS = {
  "hosts": "127.0.0.1",
  "username": " ",
  "password": " ",
  "cloud_id": " "
};
```

### Firebird

```sql
CREATE DATABASE firebird_datasource
WITH ENGINE='firebird',
PARAMETERS={
  "host": "127.0.0.1",
  "database": "test",
  "user": "firebird",
  "password": "firebird"
};
```

### Google Big Query

```sql
CREATE DATABASE bigquery_datasource
WITH ENGINE='bigquery',
PARAMETERS={
  "project_id": "badger-345908",
  "service_account_keys": {
    "path": "/home/Downloads/badger-345908.json"
  }
};
```

If you are using MindsDB Cloud, provide the `service_account_keys` parameter as a URL:

```sql
CREATE DATABASE bigquery_datasource
WITH ENGINE='bigquery',
PARAMETERS={
  "project_id": "badger-345908",
  "service_account_keys": {
    "url": "https://url/badger-345908.json"
  }
};
```

### IBM DB2

```sql
CREATE DATABASE db2_datasource
WITH ENGINE='DB2',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 25000,
  "database": " ",
  "user": "db2",
  "password": "Mimzo3i-mxt@9CpThpBj",
  "schema_name": " "
};
```

### Informix

```sql
CREATE DATABASE informix_datasource
WITH ENGINE = 'informix',
PARAMETERS = {
  "server": " ",
  "host": "127.0.0.1",
  "port": 9091,
  "database": " ",
  "user": "informix",
  "password": "Mimzo3i-mxt@9CpThpBj",
  "schema_name": " ",
  "logging_enabled": False
};
```

### MariaDB

=== "Template"

    ```sql
    CREATE DATABASE maria_datasource          --- display name for database
    WITH ENGINE='mariadb',                    --- name of the mindsdb handler
    PARAMETERS={
      "host": " ",                            --- host in the form of an ip address or a url
      "port": ,                               --- default port value is 3306
      "database": " ",                        --- optional, name of your database
      "user": " ",                            --- database user
      "password": " ",                        --- database password
      "ssl": True/False,                      --- optional, enabling/disabling SSL
      "ssl_ca": {                             --- optional, SSL Certificate Authority
        "path": " "                           --- either "path" or "url"
      },
      "ssl_cert": {                           --- optional, SSL certificates
        "path": " "                           --- either "path" or "url"
      },
      "ssl_key": {                            --- optional, SSL keys
        "path": " "                           --- either "path" or "url"
      }
    };
    ```

    The `ssl` parameter value indicates whether SSL is enabled (`True`) or disabled (`False`). If it is enabled, then the `ssl_ca`, `ssl_cert`, and `ssl_key` paramaters can be defined as a path or a URL.

=== "Example for MariaDB"

    ```sql
    CREATE DATABASE maria_datasource
    WITH ENGINE='mariadb',
    PARAMETERS={
      "host": "127.0.0.1",
      "port": 3306,
      "database": "mariadb",
      "user": "root",
      "password": "Mimzo3i-mxt@9CpThpBj"
    };
    ```

=== "Example for MariaDB Cloud (or SkySQL)"

    ```sql
    CREATE DATABASE skysql_datasource
    WITH ENGINE = 'mariadb',
    PARAMETERS = {
      "host": "mindsdbtest.mdb0002956.db1.skysql.net",
      "port": "5001",
      "database": "mindsdb_data",
      "user": "DB00007539",
      "password": "[DaS3I8g527n41637sFM|XtjjX",
      --- here, the SSL certificate is required
      "ssl-ca": {
        "url": "https://mindsdb-web-builds.s3.amazonaws.com/aws_skysql_chain.pem"
      }
    };
    ```

### Matrixone

```sql
CREATE DATABASE matrixone_datasource
WITH ENGINE = 'matrixone',
PARAMETERS = {
  "host": "127.0.0.1",
  "port": 6001,
  "database": " ",
  "user": "matrixone",
  "password": "Mimzo3i-mxt@9CpThpBj",
  "ssl": True,
  "ssl_ca": {
    "path": " "
  },
  "ssl_cert": {
    "path": " "
  },
  "ssl_key": {
    "path": " "
  }
};
```

The `ssl` parameter value indicates whether SSL is enabled (`True`) or disabled (`False`). If it is enabled, then the `ssl_ca`, `ssl_cert`, and `ssl_key` paramaters can be defined as a path or a URL.

```sql
CREATE DATABASE matrixone_datasource
WITH ENGINE = 'matrixone',
PARAMETERS = {
  "host": "127.0.0.1",
  "port": 6001,
  "database": " ",
  "user": "matrixone",
  "password": "Mimzo3i-mxt@9CpThpBj",
  "ssl": True,
  "ssl_ca": {
    "url": " "
  },
  "ssl_cert": {
    "url": " "
  },
  "ssl_key": {
    "url": " "
  }
};
```

### Microsoft Access

```sql
CREATE DATABASE access_datasource
WITH ENGINE = 'access',
PARAMETERS = {
  "db_file": " "
};
```

### Microsoft SQL Server

```sql
CREATE DATABASE mssql_datasource
WITH ENGINE='mssql',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 1433,
  "database": "master",
  "user": "sa",
  "password": "Mimzo3i-mxt@9CpThpBj"
};
```

### monetdb

```sql
CREATE DATABASE monetdb_datasource
WITH ENGINE = 'monetdb',
PARAMETERS = {
  "host": "127.0.0.1",
  "port": 50000,
  "database": " ",
  "user": "monetdb",
  "password": "Mimzo3i-mxt@9CpThpBj",
  "schema_name": " "
};
```

### mongoDB

```sql
CREATE DATABASE mongo_datasource
WITH ENGINE='mongo',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 27017,
  "user": "mongo",
  "password": "Mimzo3i-mxt@9CpThpBj"
};
```

Follow the [Mongo API documentation](/mongo/collection-structure/) for details.

### MySQL

```sql
CREATE DATABASE mysql_datasource
WITH ENGINE='mysql',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 3306,
  "database": "mysql",
  "user": "root",
  "password": "Mimzo3i-mxt@9CpThpBj",
  "ssl": True,
  "ssl_ca": {
    "path": " "
  },
  "ssl_cert": {
    "path": " "
  },
  "ssl_key": {
    "path": " "
  }
};
```

The `ssl` parameter value indicates whether SSL is enabled (`True`) or disabled (`False`). If it is enabled, then the `ssl_ca`, `ssl_cert`, and `ssl_key` paramaters can be defined as a path or a URL.

```sql
CREATE DATABASE mysql_datasource
WITH ENGINE='mysql',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 3306,
  "database": "mysql",
  "user": "root",
  "password": "Mimzo3i-mxt@9CpThpBj",
  "ssl": True,
  "ssl_ca": {
    "url": " "
  },
  "ssl_cert": {
    "url": " "
  },
  "ssl_key": {
    "url": " "
  }
};
```

### Oracle

```sql
CREATE DATABASE oracle_datasource
WITH ENGINE='oracle',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 1521,
  "sid": " ",
  "service_name": " ",
  "user": "sys",
  "password": "Mimzo3i-mxt@9CpThpBj"
};
```

### pinot

```sql
CREATE DATABASE pinot_datasource
WITH ENGINE='pinot',
PARAMETERS={
  "host": "127.0.0.1",
  "broker_port": 8000,
  "controller_port": 9000,
  "path": "/query/sql",
  "scheme": " ",
  "username": " ",
  "password": " ",
  "verify_ssl": " "
};
```

### PostgreSQL

```sql
CREATE DATABASE psql_datasource
WITH ENGINE='postgres',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 5432,
  "database": "postgres",
  "user": "postgres",
  "password": "Mimzo3i-mxt@9CpThpBj"
};
```

### QuestDB

```sql
CREATE DATABASE questdb_datasource
WITH ENGINE='questdb',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 8812,
  "database": "qdb",
  "user": "admin",
  "password": "quest"
};
```

### Scylla

```sql
CREATE DATABASE scylladb_datasource
WITH ENGINE='scylladb',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 7199,
  "user": "user@mindsdb.com",
  "password": "pass",
  "protocol_version": ,
  "keyspace": " ",
  "secure_connect_bundle": {
    "path": "/home/zoran/Downloads/secure-connect-mindsdb.zip"
  }
};
```

The `secure_connect_bundle` parameter can be defined as a path or a URL.

```sql
CREATE DATABASE scylladb_datasource
WITH ENGINE='scylladb',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 7199,
  "user": "user@mindsdb.com",
  "password": "pass",
  "protocol_version": ,
  "keyspace": " ",
  "secure_connect_bundle": {
    "url": " "
  }
};
```

### SingleStore

```sql
CREATE DATABASE singlestore_datasource
WITH ENGINE='mysql',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 3306,
  "database": "singlestore",
  "user": "root",
  "password": "Mimzo3i-mxt@9CpThpBj",
  "ssl": True,
  "ssl_ca": {
    "path": " "
  },
  "ssl_cert": {
    "path": " "
  },
  "ssl_key": {
    "path": " "
  }
};
```

The `ssl` parameter value indicates whether SSL is enabled (`True`) or disabled (`False`). If it is enabled, then the `ssl_ca`, `ssl_cert`, and `ssl_key` paramaters can be defined as a path or a URL.

```sql
CREATE DATABASE singlestore_datasource
WITH ENGINE='mysql',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 3306,
  "database": "singlestore",
  "user": "root",
  "password": "Mimzo3i-mxt@9CpThpBj",
  "ssl": True,
  "ssl_ca": {
    "url": " "
  },
  "ssl_cert": {
    "url": " "
  },
  "ssl_key": {
    "url": " "
  }
};
```

### snowflake

```sql
CREATE DATABASE snowflake_datasource
WITH ENGINE='snowflake',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 443,
  "database": "snowflake",
  "user": "user",
  "password": "Mimzo3i-mxt@9CpThpBj",
  "account": "account",
  "schema": "public",
  "protocol": "https",
  "warehouse": "warehouse"
};
```

### SQLite

```sql
CREATE DATABASE sqlite_datasource
WITH ENGINE='sqlite',
PARAMETERS={
  "db_file": " "
};
```

### supabase

```sql
CREATE DATABASE supabase_datasource
WITH ENGINE='supabase',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 54321,
  "database": "test",
  "user": "supabase",
  "password": "supabase"
};
```

### TiDB

```sql
CREATE DATABASE tidb_datasource
WITH ENGINE='tidb',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 4000,
  "database": "tidb",
  "user": "root",
  "password": "Mimzo3i-mxt@9CpThpBj"
};
```

### trino

```sql
CREATE DATABASE trino_datasource
WITH ENGINE='trinodb',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 8080,
  "user": "trino",
  "password": "Mimzo3i-mxt@9CpThpBj",
  "catalog": "default",
  "schema": "test"
};
```

### Vertica

```sql
CREATE DATABASE vertica_datasource
WITH ENGINE='vertica',
PARAMETERS={
  "host": "127.0.0.1",
  "port": 5433,
  "database": " ",
  "user": "vertica",
  "password": "Mimzo3i-mxt@9CpThpBj",
  "schema_name": " "
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
WITH ENGINE='postgres',
PARAMETERS={
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

# `#!sql CREATE DATABASE` Statement

## Description

MindsDB lets you connect to your favorite databases, data warehouses, data lakes, etc., via the `#!sql CREATE DATABASE` command.

The MindsDB SQL API supports creating connections to integrations by passing the connection parameters specific per integration. You can find more in the [Supported Integrations](#supported-integrations) chapter.

## Syntax

Let's review the syntax for the `#!sql CREATE DATABASE` command.

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
| `[datasource_name]` | Identifier for the data source to be created.                                            |
| `[engine_string]`   | Engine to be selected depending on the database connection.                              |
| `PARAMETERS`        | `#!json {"key":"value"}` object with the connection parameters specific for each engine. |

## Example

### Connecting a Data Source
Here is an example of how to connect to a MySQL database.

```sql
CREATE DATABASE mysql_datasource
WITH ENGINE='mariadb',
PARAMETERS={
  "user":"root",
  "port": 3307,
  "password": "password",
  "host": "127.0.0.1",
  "database": "my_database"
};
```

On execution, we get:

```sql
Query OK, 0 rows affected (8.878 sec)
```

### Listing Linked Databases

You can list all the linked databases using the command below.

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
| mysql_datasource   |
+--------------------+
```

### Getting Linked Databases Metadata

You can get metadata about the linked databases by querying the `mindsdb.datasources` table.

```sql
SELECT *
FROM mindsdb.datasources;
```

On execution, we get:

```sql
+------------------+---------------+--------------+------+-----------+
| name             | database_type | host         | port | user      |
+------------------+---------------+--------------+------+-----------+
| mysql_datasource | mysql         | 3.220.66.106 | 3306 | root      |
+------------------+---------------+--------------+------+-----------+
```

## Making your Local Database Available to MindsDB

When connecting your local database to MindsDB Cloud, you should expose the local database server to be publicly accessible. It is easy to accomplish using [Ngrok Tunnel](https://ngrok.com). The free tier offers all you need to get started.

The installation instructions are easy to follow. Head over to the [downloads page](https://ngrok.com/download) and choose your operating system. Follow the instructions for installation.

Then [create a free account at Ngrok](https://dashboard.ngrok.com/signup) to get an auth token that you can use to configure your Ngrok instance.

Once installed and configured, run the following command to obtain the host and port for your localhost at `[port-number]`.

```bash
ngrok tcp [port-number]
```

Here is an example. Assuming that you run a PostgreSQL database at `localhost:5432`, use the following command:

```bash
ngrok tcp 5432
```

On execution, we get:

```bash
Session Status                online
Account                       myaccount (Plan: Free)
Version                       2.3.40
Region                        United States (us)
Web Interface                 http://127.0.0.1:4040
Forwarding                    tcp://4.tcp.ngrok.io:15093 -> localhost 5432
```

Now you can access your local database at `4.tcp.ngrok.io:15093` instead of `localhost:5432`.

So to connect your local database to the MindsDB GUI, use the `Forwarding` information. The host is `4.tcp.ngrok.io`, and the port is `15093`.

Proceed to create a database connection in the MindsDB GUI by executing the `#!sql CREATE DATABASE` statement with the host and port number obtained from Ngrok.

```sql
CREATE DATABASE psql_datasource
WITH ENGINE='postgres',
PARAMETERS={
  "user":"postgres",
  "port": 15093,
  "password": "password",
  "host": "4.tcp.ngrok.io", 
  "database": "postgres"
};
```

Please note that the Ngrok tunnel loses connection when stopped or canceled. To reconnect your local database to MindsDB, you should create an Ngrok tunnel again. In the free tier, Ngrok changes the host and port values each time you launch the program, so you need to reconnect your database in the MindsDB Cloud by passing the new host and port values obtained from Ngrok.

Before resetting the database connection, drop the previously connected data source using the `#!sql DROP DATABASE` statement.

```sql
DROP DATABASE psql_datasource;
```

After dropping the data source and reconnecting your local database, you can use the predictors that you trained using the previously connected data source. However, if you have to `RETRAIN` your predictors, please ensure the database connection has the same name you used when creating the predictor to avoid failing to retrain.

!!! info "Work in progress"
    Please note that this feature is a beta version. If you have questions about the supported data sources or experience some issues, [reach out to us on Slack](https://join.slack.com/t/mindsdbcommunity/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ) or open a [GitHub issue](https://github.com/mindsdb/mindsdb/issues).

## Supported Integrations

The list of databases supported by MindsDB keeps growing. Here are the currently supported integrations:

<p align="center">
  <img src="/assets/supported_integrations.png" />
</p>

You can find particular [databases' handler files here](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers) to see their connection arguments. For example, to see the latest updates to the Oracle handler, check [Oracle's `readme.md` file here](https://github.com/mindsdb/mindsdb/blob/staging/mindsdb/integrations/handlers/oracle_handler/README.md).

Let's look at sample codes showing how to connect to each of the supported integrations.

### Airtable

=== "Template"

    ```sql
    CREATE DATABASE airtable_datasource          --- display name for the database
    WITH ENGINE='airtable',                      --- name of the MindsDB handler
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

=== "Template"

    ```sql
    CREATE DATABASE amazonredshift_datasource         --- display name for the database
    WITH ENGINE='amazonredshift',                     --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                                    --- host name or IP address of the Redshift cluster
      "port": ,                                       --- port used when connecting to the Redshift cluster
      "database": " ",                                --- database name used when connecting to the Redshift cluster
      "user": " ",                                    --- user to authenticate with the Redshift cluster
      "password": " "                                 --- password used to authenticate with the Redshift cluster
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE amazonredshift_datasource
    WITH ENGINE='amazonredshift',
    PARAMETERS={
      "host": "127.0.0.1",
      "port": 5439,
      "database": "test",
      "user": "amazonredshift",
      "password": "password"
    };
    ```

### Amazon S3

=== "Template"

    ```sql
    CREATE DATABASE amazons3_datasource     --- display name for the database
    WITH ENGINE='s3',                       --- name of the MindsDB handler
    PARAMETERS={
      "aws_access_key_id": " ",             --- the AWS access key
      "aws_secret_access_key": " ",         --- the AWS secret access key
      "region_name": " ",                   --- the AWS region
      "bucket": " ",                        --- name of the S3 bucket
      "key": " ",                           --- key of the object to be queried
      "input_serialization": " "            --- format of the data to be queried
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE amazons3_datasource
    WITH ENGINE='s3',
    PARAMETERS={
        "aws_access_key_id": "PCAQ2LJDOSWLNSQKOCPW",
        "aws_secret_access_key": "U/VjewPlNopsDmmwItl34r2neyC6WhZpUiip57i",
        "region_name": "us-east-1",
        "bucket": "mindsdb-bucket",
        "key": "iris.csv",
        "input_serialization": "{'CSV': {'FileHeaderInfo': 'NONE'}}"
    };
    ```

### Cassandra

=== "Template"

    ```sql
    CREATE DATABASE cassandra_datasource        --- display name for the database
    WITH ENGINE='cassandra',                    --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                              --- host name or IP address
      "port": ,                                 --- port used to make TCP/IP connection
      "user": " ",                              --- database user
      "password": " ",                          --- database password
      "keyspace": " ",                          --- database name
      "protocol_version": ,                     --- optional, protocol version (defaults to 4 if left blank)
      "secure_connect_bundle": {                --- optional, secure connect bundle file
        "path": " "                                 --- either "path" or "url"
      }
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE cassandra_datasource
    WITH ENGINE='cassandra',
    PARAMETERS={
      "host": "127.0.0.1",
      "port": 9043,
      "user": "user",
      "password": "password",
      "keyspace": "test_data",
      "protocol_version": 4
    };
    ```

### CKAN

=== "Template"

    ```sql
    CREATE DATABASE ckan_datasource          --- display name for the database
    WITH ENGINE = 'ckan',                    --- name of the MindsDB handler
    PARAMETERS = {
      "url": " ",                            --- host name or IP address
      "apikey": " "                          --- the API key used for authentication
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE ckan_datasource
    WITH ENGINE = 'ckan',
    PARAMETERS = {
      "url": "http://demo.ckan.org/api/3/action/",
      "apikey": "YOUR_API_KEY"
    };
    ```

### ClickHouse

=== "Template"

    ```sql
    CREATE DATABASE clickhouse_datasource       --- display name for the database
    WITH ENGINE='clickhouse',                   --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                              --- host name or IP address
      "port": ,                                 --- port used to make TCP/IP connection
      "database": " ",                          --- database name
      "user": " ",                              --- database user
      "password": " "                           --- database password
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE clickhouse_datasource
    WITH ENGINE='clickhouse',
    PARAMETERS={
      "host": "127.0.0.1",
      "port": 9000,
      "database": "test_data",
      "user": "root",
      "password": "password"
    };
    ```

### Cockroach Labs

=== "Template"

    ```sql
    CREATE DATABASE cockroach_datasource        --- display name for the database
    WITH ENGINE='cockroachdb',                  --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                              --- host name or IP address
      "port": ,                                 --- port used to make TCP/IP connection
      "database": " ",                          --- database name
      "user": " ",                              --- database user
      "password": " ",                          --- database password
      "publish": " "                            --- optional, publish
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE cockroach_datasource
    WITH ENGINE='cockroachdb',
    PARAMETERS={
      "host": "127.0.0.1",
      "port": 26257,
      "database": "cockroachdb",
      "user": "username",
      "password": "password"
    };
    ```

### Couchbase

=== "Template"

    ```sql
    CREATE DATABASE couchbase_datasource        --- display name for the database
    WITH ENGINE = 'couchbase',                  --- name of the MindsDB handler
    PARAMETERS = {
      "host": " ",                              --- host name or IP address of the Couchbase server
      "user": " ",                              --- user to authenticate with the Couchbase server
      "password": " ",                          --- password used to authenticate with the Couchbase server
      "bucket": " ",                            --- bucket name
      "scope": " "                              --- scope used to query (defaults to `_default` if left blank)
    };                                              --- a scope in Couchbase is equivalent to a schema in MySQL
    ```

=== "Example"

    ```sql
    CREATE DATABASE couchbase_datasource
    WITH ENGINE = 'couchbase',
    PARAMETERS = {
      "host": "127.0.0.1",
      "user": "couchbase",
      "password": "password",
      "bucket": "test-bucket"
    };
    ```

### CrateDB

=== "Template"

    ```sql
    CREATE DATABASE cratedb_datasource        --- display name for the database
    WITH ENGINE='crate',                      --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                            --- host name or IP address
      "port": ,                               --- port used to make TCP/IP connection
      "user": " ",                            --- database user
      "password": " ",                        --- database password
      "schema_name": " "                      --- database schema name (defaults to `doc` if left blank)
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE cratedb_datasource
    WITH ENGINE='crate',
    PARAMETERS={
      "host": "127.0.0.1",
      "port": 4200,
      "user": "crate",
      "password": "password",
      "schema_name": "doc"
    };
    ```

### Databricks

=== "Template"

    ```sql
    CREATE DATABASE databricks_datasource         --- display name for the database
    WITH ENGINE='databricks',                     --- name of the MindsDB handler
    PARAMETERS={
      "server_hostname": " ",                     --- server hostname of the cluster or SQL warehouse
      "http_path": " ",                           --- http path to the cluster or SQL warehouse
      "access_token": " ",                        --- personal Databricks access token
      "schema": " ",                              --- schema name (defaults to `default` if left blank)
      "session_configuration": " ",               --- optional, dictionary of Spark session configuration parameters
      "http_headers": " ",                        --- optional, additional (key, value) pairs to set in HTTP headers on every RPC request the client makes
      "catalog": " "                              --- catalog (defaults to `hive_metastore` if left blank)
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE databricks_datasource
    WITH ENGINE='databricks',
    PARAMETERS={
      "server_hostname": "adb-1234567890123456.7.azuredatabricks.net",
      "http_path": "sql/protocolv1/o/1234567890123456/1234-567890-test123",
      "access_token": "dapi1234567890ab1cde2f3ab456c7d89efa",
      "schema": "example_db"
    };
    ```

### DataStax

=== "Template"

    ```sql
    CREATE DATABASE datastax_datasource           --- display name for the database
    WITH ENGINE='astra',                          --- name of the MindsDB handler
    PARAMETERS={
      "user": " ",                                --- user to be authenticated
      "password": " ",                            --- password for authentication
      "secure_connection_bundle": {               --- secure connection bundle zip file
        "path": " "                                   --- either "path" or "url"
      },
      "host": " ",                                --- optional, host name or IP address
      "port": ,                                   --- optional, port used to make TCP/IP connection
      "protocol_version": ,                       --- optional, protocol version
      "keyspace": " "                             --- optional, keyspace
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE datastax_datasource
    WITH ENGINE='astra',
    PARAMETERS={
      "host": "127.0.0.1",
      "port": 7077,
      "user": "datastax",
      "password": "password",
      "secure_connection_bundle": {
        "path": "/home/Downloads/file.zip"
      }
    };
    ```

### Druid

=== "Template"

    ```sql
    CREATE DATABASE druid_datasource        --- display name for the database
    WITH ENGINE = 'druid',                  --- name of the MindsDB handler
    PARAMETERS = {
      "host": " ",                          --- host name or IP address of Apache Druid
      "port": ,                             --- port where Apache Druid runs
      "user": " ",                          --- optional, user to authenticate with Apache Druid
      "password": " ",                      --- optional, password used to authenticate with Apache Druid
      "path": " ",                          --- query path
      "scheme": " "                         --- the URI scheme (defaults to `http` if left blank)
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE druid_datasource
    WITH ENGINE = 'druid',
    PARAMETERS = {
      "host": "127.0.0.1",
      "port": 8888,
      "path": "/druid/v2/sql/",
      "scheme": "http"
    };
    ```

### DynamoDB

=== "Template"

    ```sql
    CREATE DATABASE dynamodb_datasource       --- display name for the database
    WITH ENGINE='dynamodb',                   --- name of the MindsDB handler
    PARAMETERS={
      "aws_access_key_id": " ",               --- the AWS access key
      "aws_secret_access_key": " ",           --- the AWS secret access key
      "region_name": " "                      --- the AWS region
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE dynamodb_datasource
    WITH ENGINE='dynamodb',
    PARAMETERS={
      "aws_access_key_id": "PCAQ2LJDOSWLNSQKOCPW",
      "aws_secret_access_key": "U/VjewPlNopsDmmwItl34r2neyC6WhZpUiip57i",
      "region_name": "us-east-1"
    };
    ```

### D0lt

=== "Template"

    ```sql
    CREATE DATABASE d0lt_datasource             --- display name for the database
    WITH ENGINE = 'd0lt',                       --- name of the MindsDB handler
    PARAMETERS = {
      "host": " ",                              --- host name or IP address
      "port": ,                                 --- port used to make TCP/IP connection
      "database": " ",                          --- database name
      "user": " ",                              --- database user
      "password": " ",                          --- database password
      "ssl": True/False,                        --- optional, the `ssl` parameter value indicates whether SSL is enabled (`True`) or disabled (`False`)
      "ssl_ca": {                               --- optional, SSL Certificate Authority
        "path": " "                                 --- either "path" or "url"
      },
      "ssl_cert": {                             --- optional, SSL certificates
        "url": " "                                  --- either "path" or "url"
      },
      "ssl_key": {                              --- optional, SSL keys
        "path": " "                                 --- either "path" or "url"
      }
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE d0lt_datasource
    WITH ENGINE='d0lt',
    PARAMETERS={
      "host": "127.0.0.1",
      "port": 3306,
      "database": "information_schema",
      "user": "root",
      "password": "password"
    };
    ```

### Elastic

=== "Template"

    ```sql
    CREATE DATABASE elastic_datasource      --- display name for the database
    WITH ENGINE = 'elasticsearch',          --- name of the MindsDB handler
    PARAMETERS = {
      "hosts": " ",                         --- one or more host names or IP addresses of the Elasticsearch server
      "username": " ",                      --- optional, username to authenticate with the Elasticsearch server
      "password": " ",                      --- optional, password used to authenticate with the Elasticsearch server
      "cloud_id": " "                       --- optional, unique ID of your hosted Elasticsearch cluster (must be provided when "hosts" is left blank)
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE elastic_datasource
    WITH ENGINE = 'elasticsearch',
    PARAMETERS = {
      "hosts": "localhost:9200"
    };
    ```

### Firebird

=== "Template"

    ```sql
    CREATE DATABASE firebird_datasource         --- display name for the database
    WITH ENGINE='firebird',                     --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                              --- host name or IP address of the Firebird server
      "database": " ",                          --- database name
      "user": " ",                              --- user to authenticate with the Firebird server
      "password": " "                           --- password used to authenticate with the Firebird server
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE firebird_datasource
    WITH ENGINE='firebird',
    PARAMETERS={
      "host": "127.0.0.1",
      "database": "test",
      "user": "firebird",
      "password": "password"
    };
    ```

### Google Big Query

=== "Template"

    ```sql
    CREATE DATABASE bigquery_datasource       --- display name for the database
    WITH ENGINE='bigquery',                   --- name of the MindsDB handler
    PARAMETERS={
      "project_id": " ",                      --- globally unique project identifier
      "service_account_keys": {               --- service account keys file
        "path": " "                               --- either "path" or "url"
      }
    };
    ```

=== "Example for Self-Hosted MindsDB"

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

=== "Example for MindsDB Cloud"

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

=== "Template"

    ```sql
    CREATE DATABASE db2_datasource        --- display name for the database
    WITH ENGINE='DB2',                    --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                        --- host name or IP address
      "port": ,                           --- port used to make TCP/IP connection
      "database": " ",                    --- database name
      "user": " ",                        --- database user
      "password": " ",                    --- database password
      "schema_name": " "                  --- database schema name
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE db2_datasource
    WITH ENGINE='DB2',
    PARAMETERS={
      "host": "127.0.0.1",
      "port": 25000,
      "database": "BOOKS",
      "user": "db2admin",
      "password": "password",
      "schema_name": "db2admin"
    };
    ```

### Informix

=== "Template"

    ```sql
    CREATE DATABASE informix_datasource       --- display name for the database
    WITH ENGINE = 'informix',                 --- name of the MindsDB handler
    PARAMETERS = {
      "server": " ",                          --- server name
      "host": " ",                            --- host name or IP address
      "port": ,                               --- port used to make TCP/IP connection
      "database": " ",                        --- database name
      "user": " ",                            --- database user
      "password": " ",                        --- database password
      "schema_name": " ",                     --- database schema name
      "logging_enabled": True/False           --- indicates whether logging is enabled (defaults to `True` if left blank)
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE informix_datasource
    WITH ENGINE = 'informix',
    PARAMETERS = {
      "server": "server",
      "host": "127.0.0.1",
      "port": 9091,
      "database": "stores_demo",
      "user": "informix",
      "password": "password",
      "schema_name": "demo_schema",
      "logging_enabled": False
    };
    ```

### MariaDB

=== "Template"

    ```sql
    CREATE DATABASE maria_datasource            --- display name for the database
    WITH ENGINE='mariadb',                      --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                              --- host IP address or URL
      "port": ,                                 --- port used to make TCP/IP connection
      "database": " ",                          --- database name
      "user": " ",                              --- database user
      "password": " ",                          --- database password
      "ssl": True/False,                        --- optional, the `ssl` parameter value indicates whether SSL is enabled (`True`) or disabled (`False`)
      "ssl_ca": {                               --- optional, SSL Certificate Authority
        "path": " "                                 --- either "path" or "url"
      },
      "ssl_cert": {                             --- optional, SSL certificates
        "url": " "                                  --- either "path" or "url"
      },
      "ssl_key": {                              --- optional, SSL keys
        "path": " "                                 --- either "path" or "url"
      }
    };
    ```

=== "Example for MariaDB"

    ```sql
    CREATE DATABASE maria_datasource
    WITH ENGINE='mariadb',
    PARAMETERS={
      "host": "127.0.0.1",
      "port": 3306,
      "database": "mariadb",
      "user": "root",
      "password": "password"
    };
    ```

=== "Example for MariaDB Cloud (or SkySQL)"

    ```sql
    CREATE DATABASE skysql_datasource
    WITH ENGINE = 'mariadb',
    PARAMETERS = {
      "host": "mindsdbtest.mdb0002956.db1.skysql.net",
      "port": 5001,
      "database": "mindsdb_data",
      "user": "DB00007539",
      "password": "password",
      --- here, the SSL certificate is required
      "ssl-ca": {
        "url": "https://mindsdb-web-builds.s3.amazonaws.com/aws_skysql_chain.pem"
      }
    };
    ```

### Matrixone

=== "Template"

    ```sql
    CREATE DATABASE matrixone_datasource        --- display name for the database
    WITH ENGINE='matrixone',                    --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                              --- host IP address or URL
      "port": ,                                 --- port used to make TCP/IP connection
      "database": " ",                          --- database name
      "user": " ",                              --- database user
      "password": " ",                          --- database password
      "ssl": True/False,                        --- optional, the `ssl` parameter value indicates whether SSL is enabled (`True`) or disabled (`False`)
      "ssl_ca": {                               --- optional, SSL Certificate Authority
        "path": " "                                 --- either "path" or "url"
      },
      "ssl_cert": {                             --- optional, SSL certificates
        "url": " "                                  --- either "path" or "url"
      },
      "ssl_key": {                              --- optional, SSL keys
        "path": " "                                 --- either "path" or "url"
      }
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE matrixone_datasource
    WITH ENGINE = 'matrixone',
    PARAMETERS = {
      "host": "127.0.0.1",
      "port": 6001,
      "database": "mo_catalog",
      "user": "matrixone",
      "password": "password"
    };
    ```

### Microsoft Access

=== "Template"

    ```sql
    CREATE DATABASE access_datasource       --- display name for the database
    WITH ENGINE = 'access',                 --- name of the MindsDB handler
    PARAMETERS = {
      "db_file": " "                        --- path to the database file to be used
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE access_datasource
    WITH ENGINE = 'access',
    PARAMETERS = {
      "db_file": "example_db.accdb"
    };
    ```

### Microsoft SQL Server

=== "Template"

    ```sql
    CREATE DATABASE mssql_datasource        --- display name for the database
    WITH ENGINE='mssql',                    --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                          --- host name or IP address
      "port": ,                             --- port used to make TCP/IP connection
      "database": " ",                      --- database name
      "user": " ",                          --- database user
      "password": " "                       --- database password
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE mssql_datasource
    WITH ENGINE='mssql',
    PARAMETERS={
      "host": "127.0.0.1",
      "port": 1433,
      "database": "master",
      "user": "sa",
      "password": "password"
    };
    ```

### MonetDB

=== "Template"

    ```sql
    CREATE DATABASE monetdb_datasource          --- display name for the database
    WITH ENGINE = 'monetdb',                    --- name of the MindsDB handler
    PARAMETERS = {
      "host": " ",                              --- host name or IP address
      "port": ,                                 --- port used to make TCP/IP connection
      "database": " ",                          --- database name
      "user": " ",                              --- database user
      "password": " ",                          --- database password
      "schema_name": " "                        --- database schema name (defaults to the current schema if left blank)
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE monetdb_datasource
    WITH ENGINE = 'monetdb',
    PARAMETERS = {
      "host": "127.0.0.1",
      "port": 50000,
      "database": "demo",
      "user": "monetdb",
      "password": "password",
      "schema_name": "sys"
    };
    ```

### MongoDB

=== "Template"

    ```sql
    CREATE DATABASE mongo_datasource          --- display name for the database
    WITH ENGINE='mongo',                      --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                            --- host name or IP address
      "port": ,                               --- port used to make TCP/IP connection
      "user": " ",                            --- database user
      "password": " "                         --- database password
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE mongo_datasource
    WITH ENGINE='mongo',
    PARAMETERS={
      "host": "127.0.0.1",
      "port": 27017,
      "user": "mongo",
      "password": "password"
    };
    ```

Follow the [Mongo API documentation](/mongo/collection-structure/) for details.

### MySQL

=== "Template"

    ```sql
    CREATE DATABASE mysql_datasource            --- display name for the database
    WITH ENGINE='mysql',                        --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                              --- host name or IP address
      "port": ,                                 --- port used to make TCP/IP connection
      "database": " ",                          --- database name
      "user": " ",                              --- database user
      "password": " ",                          --- database password
      "ssl": True/False,                        --- optional, the `ssl` parameter value indicates whether SSL is enabled (`True`) or disabled (`False`)
      "ssl_ca": {                               --- optional, SSL Certificate Authority
        "path": " "                                 --- either "path" or "url"
      },
      "ssl_cert": {                             --- optional, SSL certificates
        "url": " "                                  --- either "path" or "url"
      },
      "ssl_key": {                              --- optional, SSL keys
        "path": " "                                 --- either "path" or "url"
      }
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE mysql_datasource
    WITH ENGINE='mysql',
    PARAMETERS={
      "host": "127.0.0.1",
      "port": 3306,
      "database": "mysql",
      "user": "root",
      "password": "password"
    };
    ```

### Oracle

=== "Template"

    ```sql
    CREATE DATABASE oracle_datasource         --- display name for the database
    WITH ENGINE='oracle',                     --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                            --- host name or IP address
      "port": ,                               --- port used to make TCP/IP connection
      "sid": " ",                             --- unique identifier of the database instance
      "service_name": " ",                    --- optional, database service name (must be provided when "sid" is left blank)
      "user": " ",                            --- database user
      "password": " "                         --- database password
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE oracle_datasource
    WITH ENGINE='oracle',
    PARAMETERS={
      "host": "127.0.0.1",
      "port": 1521,
      "sid": "ORCL",
      "user": "sys",
      "password": "password"
    };
    ```

### Pinot

=== "Template"

    ```sql
    CREATE DATABASE pinot_datasource        --- display name for the database
    WITH ENGINE='pinot',                    --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                          --- host name or IP address of the Apache Pinot cluster
      "broker_port": ,                      --- port where the broker of the Apache Pinot cluster runs
      "controller_port": ,                  --- port where the controller of the Apache Pinot cluster runs
      "path": " ",                          --- query path
      "scheme": " ",                        --- scheme (defaults to `http` if left blank)
      "username": " ",                      --- optional, user
      "password": " ",                      --- optional, password
      "verify_ssl": " "                     --- optional, verify SSL
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE pinot_datasource
    WITH ENGINE='pinot',
    PARAMETERS={
      "host": "127.0.0.1",
      "broker_port": 8000,
      "controller_port": 9000,
      "path": "/query/sql",
      "scheme": "http"
    };
    ```

### PostgreSQL

=== "Template"

    ```sql
    CREATE DATABASE psql_datasource         --- display name for the database
    WITH ENGINE='postgres',                 --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                          --- host name or IP address
      "port": ,                             --- port used to make TCP/IP connection
      "database": " ",                      --- database name
      "user": " ",                          --- database user
      "password": " "                       --- database password
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE psql_datasource
    WITH ENGINE='postgres',
    PARAMETERS={
      "host": "127.0.0.1",
      "port": 5432,
      "database": "postgres",
      "user": "postgres",
      "password": "password"
    };
    ```

### QuestDB

=== "Template"

    ```sql
    CREATE DATABASE questdb_datasource      --- display name for the database
    WITH ENGINE='questdb',                  --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                          --- host name or IP address
      "port": ,                             --- port used to make TCP/IP connection
      "database": " ",                      --- database name
      "user": " ",                          --- database user
      "password": " ",                      --- database password
      "public": True/False                  --- public (defaults to `True` if left blank)
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE questdb_datasource
    WITH ENGINE='questdb',
    PARAMETERS={
      "host": "127.0.0.1",
      "port": 8812,
      "database": "qdb",
      "user": "admin",
      "password": "password"
    };
    ```

### SAP HANA

=== "Template"

    ```sql
    CREATE DATABASE sap_hana_datasource           --- display name for the database
    WITH ENGINE='hana',                           --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                                --- host name or IP address
      "port": ,                                   --- port used to make TCP/IP connection
      "user": " ",                                --- user
      "password": " ",                            --- password
      "schema": " ",                              --- database schema name (defaults to the current schema if left blank)
      "encrypt":                                  --- whether connection is encrypted (required for cloud usage)
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE sap_hana_datasource
    WITH ENGINE='hana',
    PARAMETERS={
      "host": "<uuid>.hana.trial-us10.hanacloud.ondemand.com",
      "port": "443",
      "user": "DBADMIN",
      "password": "password",
      "schema": "MINDSDB",
      "encrypt": true
    };
    ```

### Scylla

=== "Template"

    ```sql
    CREATE DATABASE scylladb_datasource           --- display name for the database
    WITH ENGINE='scylladb',                       --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                                --- host name or IP address
      "port": ,                                   --- port used to make TCP/IP connection
      "user": " ",                                --- user
      "password": " ",                            --- password
      "protocol_version": ,                       --- optional, protocol version (defaults to 4 if left blank)
      "keyspace": " ",                            --- keyspace name (it is the top level container for tables)
      "secure_connect_bundle": {                  --- secure connect bundle file
        "path": " "                                   --- either "path" or "url"
      }
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE scylladb_datasource
    WITH ENGINE='scylladb',
    PARAMETERS={
      "host": "127.0.0.1",
      "port": 7199,
      "user": "user@mindsdb.com",
      "password": "password",
      "protocol_version": 4,
      "keyspace": "keyspace_name",
      "secure_connect_bundle": {
        "path": "/home/zoran/Downloads/secure-connect-mindsdb.zip"
      }
    };
    ```

### SingleStore

=== "Template"

    ```sql
    CREATE DATABASE singlestore_datasource          --- display name for the database
    WITH ENGINE='mysql',                            --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                                  --- host name or IP address
      "port": ,                                     --- port used to make TCP/IP connection
      "database": " ",                              --- database name
      "user": " ",                                  --- database user
      "password": " ",                              --- database password
      "ssl": True/False,                            --- optional, the `ssl` parameter value indicates whether SSL is enabled (`True`) or disabled (`False`)
      "ssl_ca": {                                   --- optional, SSL Certificate Authority
        "path": " "                                     --- either "path" or "url"
      },
      "ssl_cert": {                                 --- optional, SSL certificates
        "url": " "                                      --- either "path" or "url"
      },
      "ssl_key": {                                  --- optional, SSL keys
        "path": " "                                     --- either "path" or "url"
      }
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE singlestore_datasource
    WITH ENGINE='mysql',
    PARAMETERS={
      "host": "127.0.0.1",
      "port": 3306,
      "database": "singlestore",
      "user": "root",
      "password": "password"
    };
    ```

### Snowflake

=== "Template"

    ```sql
    CREATE DATABASE snowflake_datasource              --- display name for the database
    WITH ENGINE='snowflake',                          --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                                    --- host name or IP address
      "port": ,                                       --- port used to make TCP/IP connection
      "database": " ",                                --- database name
      "user": " ",                                    --- database user
      "password": " ",                                --- database password
      "account": " ",                                 --- the Snowflake account
      "schema": " ",                                  --- schema name (defaults to `public` if left blank)
      "protocol": " ",                                --- protocol (defaults to `https` if left blank)
      "warehouse": " "                                --- the warehouse account
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE snowflake_datasource
    WITH ENGINE='snowflake',
    PARAMETERS={
      "host": "127.0.0.1",
      "port": 443,
      "database": "snowflake",
      "user": "user",
      "password": "password",
      "account": "account",
      "schema": "public",
      "protocol": "https",
      "warehouse": "warehouse"
    };
    ```

### SQLite

=== "Template"

    ```sql
    CREATE DATABASE sqlite_datasource         --- display name for the database
    WITH ENGINE='sqlite',                     --- name of the MindsDB handler
    PARAMETERS={
      "db_file": " "                          --- path to the database file to be used
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE sqlite_datasource
    WITH ENGINE='sqlite',
    PARAMETERS={
      "db_file": "example.db"
    };
    ```

### Supabase

=== "Template"

    ```sql
    CREATE DATABASE supabase_datasource             --- display name for the database
    WITH ENGINE='supabase',                         --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                                  --- host name or IP address
      "port": ,                                     --- port used to make TCP/IP connection
      "database": " ",                              --- database name
      "user": " ",                                  --- database user
      "password": " ",                              --- database password
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE supabase_datasource
    WITH ENGINE='supabase',
    PARAMETERS={
      "host": "127.0.0.1",
      "port": 54321,
      "database": "test",
      "user": "supabase",
      "password": "password"
    };
    ```

### TiDB

=== "Template"

    ```sql
    CREATE DATABASE tidb_datasource                 --- display name for the database
    WITH ENGINE='tidb',                             --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                                  --- host name or IP address
      "port": ,                                     --- port used to make TCP/IP connection
      "database": " ",                              --- database name
      "user": " ",                                  --- database user
      "password": " ",                              --- database password
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE tidb_datasource
    WITH ENGINE='tidb',
    PARAMETERS={
      "host": "127.0.0.1",
      "port": 4000,
      "database": "tidb",
      "user": "root",
      "password": "password"
    };
    ```

### Trino

=== "Template"

    ```sql
    CREATE DATABASE trino_datasource          --- display name for the database
    WITH ENGINE='trino',                      --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                            --- host name or IP address
      "port": ,                               --- port used to make TCP/IP connection
      "auth": " ",                            --- optional, authentication method, currently only `basic` is supported
      "http_scheme": " ",                     --- optional, `http`(default) or `https`
      "user": " ",                            --- database user
      "password": " ",                        --- database password
      "catalog": " ",                         --- optional, catalog
      "schema": " "                           --- optional, schema
      "with":                                 --- optional, default WITH-clause(properties) for ALL tables(*)
    };
    ```

(*): this parameter is experimental and might be changed or removed in future release

=== "Example"

    ```sql
    CREATE DATABASE trino_datasource
    WITH ENGINE='trino',
    PARAMETERS={
      "host": "127.0.0.1",
      "port": 8080,
      "user": "trino",
      "password": "password",
      "catalog": "default",
      "schema": "test"
    };
    ```
or

    ```sql
    CREATE DATABASE trino_datasource
    WITH ENGINE='trino',
    PARAMETERS={
      "host": "127.0.0.1",
      "port": 443,
      "auth": "basic",
      "http_scheme": "https",
      "user": "trino",
      "password": "password",
      "catalog": "default",
      "schema": "test",
      "with": "with (transactional = true)"
    };
    ```

### Vertica

=== "Template"

    ```sql
    CREATE DATABASE vertica_datasource        --- display name for the database
    WITH ENGINE='vertica',                    --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                            --- host name or IP address
      "port": ,                               --- port used to make TCP/IP connection
      "database": " ",                        --- database name
      "user": " ",                            --- database user
      "password": " ",                        --- database password
      "schema_name": " "                      --- database schema name
    };
    ```

=== "Example"

    ```sql
    CREATE DATABASE vertica_datasource
    WITH ENGINE='vertica',
    PARAMETERS={
      "host": "127.0.0.1",
      "port": 5433,
      "database": "VMart",
      "user": "vertica",
      "password": "password",
      "schema_name": "public"
    };
    ```

### SAP Hana Handler

=== "Template"

  ```sql
    CREATE DATABASE sap_hana_trial --- display name for the database
    WITH ENGINE = 'hana',   --- name of the MindsDB handler
    PARAMETERS = {
      "user": "",   --- user name
      "password": "", --- password
      "host": "",  --- host name or IP address
      "port": "", --- port used to make TCP/IP connection
      "schema": "", --- name of database schema
      "encrypt":   -- set to true or false
  };
  ```

=== "Example"

  ```sql
    CREATE DATABASE sap_hana_trial
    WITH ENGINE = 'hana',
    PARAMETERS = {
      "user": "DBADMIN",
      "password": "password",
      "host": "<uuid>.hana.trial-us10.hanacloud.ondemand.com",
      "port": "443", 
      "schema": "MINDSDB",
      "encrypt": true
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

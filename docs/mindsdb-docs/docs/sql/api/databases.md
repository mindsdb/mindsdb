# Create Database

MindsDB enables connections to your favorite databases, data warehouses, data lakes, etc in a simple way.

Our SQL API supports creating a database connection by passing any credentials needed by each type of system that you are connecting to. 

## Syntax

```sql
CREATE DATABASE datasource_name
WITH
	engine=engine_string, 
	parameters={"key":"value", ...};
```

### Example: MariaDB

Here is a concrete example to connect to a MySQL database.

```sql
CREATE DATABASE mysql_datasource 
WITH 
	engine='mariadb', 
	parameters={
                "user":"root",
                "port": 3307, 
                "password": "password", 
                "host": "127.0.0.1", 
                "database": "mariadb"
        };
```


### Create Database configurations

> Click on each section to expand the example query for connection.

<details class="success">
   <summary>Connect to Snowflake</summary> 
     ```sql
        CREATE DATABASE snowflake_datasource 
        WITH 
                engine='snowflake', 
                parameters={
                        "user":"user",
                        "port": 443, 
                        "password": "password", 
                        "host": "127.0.0.1", 
                        "database": "snowflake",
                        "account": "account",
                        "schema": "public",
                        "protocol": "https",
                        "warehouse": "warehouse"
                };
     ```
</details>

<details class="success">
   <summary>Connect to Singlestore</summary> 
     ```sql
        CREATE DATABASE singlestore_datasource 
        WITH 
                engine='singlestore', 
                parameters={
                        "user":"root",
                        "port": 3306, 
                        "password": "password", 
                        "host": "127.0.0.1", 
                        "database": "singlestore"
                };
     ```
</details>

<details class="success">
   <summary>Connect to MySQL</summary> 
     ```sql
        CREATE DATABASE mysql_datasource 
        WITH 
                engine='mysql', 
                parameters={
                        "user":"root",
                        "port": 3306, 
                        "password": "password", 
                        "host": "127.0.0.1", 
                        "database": "mysql"
                };
     ```
</details>

<details class="success">
   <summary>Connect to ClickHouse</summary> 
     ```sql
        CREATE DATABASE clickhouse_datasource 
        WITH 
                engine='clickhouse', 
                parameters={
                        "user":"default",
                        "port": 9000, 
                        "password": "password", 
                        "host": "127.0.0.1", 
                        "database": "default"
                };
     ```
</details>

<details class="success">
   <summary> Connect to PostgreSQL</summary> 
     ```sql
        CREATE DATABASE psql_datasource 
        WITH 
                engine='postgres', 
                parameters={
                        "user":"postgres",
                        "port": 5432, 
                        "password": "password", 
                        "host": "127.0.0.1", 
                        "database": "postgres"
                };
     ```
</details>

<details class="success">
   <summary> Connect to Cassandra</summary> 
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
</details>

<details class="success">
   <summary> Connect to MariaDB</summary> 
     ```sql
        CREATE DATABASE maria_datasource 
        WITH 
                engine='mariadb', 
                parameters={
                        "user":"root",
                        "port": 3306, 
                        "password": "password", 
                        "host": "127.0.0.1", 
                        "database": "mariadb"
                };
     ```
</details>

<details class="success">
   <summary> Connect to Microsoft SQL Server</summary> 
     ```sql
        CREATE DATABASE mssql_datasource 
        WITH 
                engine='mssql', 
                parameters={
                        "user":"sa",
                        "port": 1433, 
                        "password": "password", 
                        "host": "127.0.0.1", 
                        "database": "master"
                };
     ```
</details>

<details class="success">
   <summary>Connect to Scylladb</summary> 
     ```sql
        CREATE DATABASE scylladb_datasource 
        WITH 
                engine='scylladb', 
                parameters={
                        "user":"scylladb",
                        "port": 9042, 
                        "password": "password", 
                        "host": "127.0.0.1", 
                        "database": "scylladb"
                };
     ```
</details>

<details class="success">
   <summary>Connect to Trino</summary> 
     ```sql
        CREATE DATABASE trino_datasource 
        WITH 
                engine='trinodb', 
                parameters={
                        "user":"trino",
                        "port": 8080, 
                        "password": "password", 
                        "host": "127.0.0.1", 
                        "catalog": "default",
                        "schema": "test"
                };
     ```
</details>

<details class="success">
   <summary>Connect to QuestDB</summary> 
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
</details>

## Listing linked DATABASES 

You can list linked databases as follows:

```sql
SHOW DATABASES;
```

You can also get metadata about the linked databases in  in `mindsdb.datasources`:.

```sql
select * from mindsdb.datasources;
```

![Once a datasource has been correctly created, you will see it registered in `mindsdb.datasources`](/assets/sql/datasource_listing.png)

!!! info "Work in progress"
    Note this feature is in beta version. If you have additional questions about other supported datasources or you expirience some issues [reach out to us on Slack](https://join.slack.com/t/mindsdbcommunity/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ) or open GitHub issue.

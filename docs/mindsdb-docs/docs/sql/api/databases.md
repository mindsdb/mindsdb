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

## Listing linked DATABASES 

You can list linked databases as follows:

```sql
SHOW DATABASES;
```

You can also get metadata about the linked databases in  in `mindsdb.datasources`:.

```sql
select * from mindsdb.datasources;
```

![Once a datasource has been correctly created, you will see it registered in `mindsdb.datasources`](../../assets/sql/datasource_listing.png)

!!! info "Work in progress"
    Note this feature is in beta version. If you have additional questions about other supported datasources or you expirience some issues [reach out to us on Slack](https://join.slack.com/t/mindsdbcommunity/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ) or open GitHub issue.

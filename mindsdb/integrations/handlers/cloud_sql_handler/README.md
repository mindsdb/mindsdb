# Google Cloud SQL Handler

This is the implementation of the Google Cloud SQL handler for MindsDB.

## Google Cloud SQL
Fully managed relational database service for MySQL, PostgreSQL, and SQL Server with rich extension collections, configuration flags, and developer ecosystems.
<br>
https://cloud.google.com/sql

## Implementation
This handler was implemented using the existing MindsDB handlers for MySQL, PostgreSQL and SQL Server.

The required arguments to establish a connection are,
* `host`: the host name or IP address of the Google Cloud SQL instance.
* `port`: the TCP/IP port of the Google Cloud SQL instance.
* `user`: the username used to authenticate with the Google Cloud SQL instance.
* `password`: the password to authenticate the user with the Google Cloud SQL instance.
* `database`: the database name to use when connecting with the Google Cloud SQL instance.
* `db_engine`: the database engine of the Google Cloud SQL instance. This can take one of three values: 'mysql', 'postgresql' or 'mssql'.

## Usage
In order to make use of this handler and connect to a Google Cloud SQL MySQL instance in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE cloud_sql_mysql_datasource
WITH ENGINE = 'cloud_sql',
PARAMETERS = {
    "db_engine": "mysql",
    "host": "53.170.61.16",
    "port": 3306,
    "user": "admin",
    "password": "password",
    "database": "example_db"
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM cloud_sql_mysql_datasource.example_tbl
~~~~

Similar commands can be used to establish a connection and query Google Cloud SQL PostgreSQL and SQL Server instances,
~~~~sql
CREATE DATABASE cloud_sql_postgres_datasource
WITH ENGINE = 'cloud_sql',
PARAMETERS = {
    "db_engine": "postgresql",
    "host": "53.170.61.17",
    "port": 5432,
    "user": "postgres",
    "password": "password",
    "database": "example_db "
};

SELECT * FROM cloud_sql_postgres_datasource.example_tbl
~~~~

~~~~sql
CREATE DATABASE cloud_sql_mssql_datasource
WITH ENGINE = 'cloud_sql',
PARAMETERS = {
    "db_engine": "mssql",
    "host": "53.170.61.18",
    "port": 1433,
    "user": "postgres",
    "password": "password",
    "database": "example_db "
};

SELECT * FROM cloud_sql_mssql_datasource.example_tbl
~~~~
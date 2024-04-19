---
title: MySQL
sidebarTitle: MySQL
---

This documentation describes the integration of MindsDB with MySQL, a fast, reliable, and scalable open-source database. 
The integration allows MindsDB to access data stored in the MySQL database and enhance MySQL with AI capabilities.

### Prerequisites

Before proceeding, ensure the following prerequisites are met:

 1. Install MindsDB and MySQL on your system or obtain access to cloud options.
 2. To use MySQL with MindsDB, install the required dependencies by running `pip install mindsdb[mysql]`.

## Connection

Establish a connection to your MySQL database from MindsDB by executing the following SQL command:

```sql
CREATE DATABASE mysql_datasource 
WITH ENGINE = 'mysql', 
PARAMETERS = {
    "host": "127.0.0.1",
    "port": 3306,
    "database": "mysql",
    "user": "root",
    "password": "password"
};
```

Or, using the `url` parameter:

```sql
CREATE DATABASE mysql_datasource
WITH
  ENGINE = 'mysql',
  PARAMETERS = {
    "url": "mysql://user@127.0.0.1:3306"
  };
```

Required connection parameters include the following:

*    `user`: The username for the MySQL database.
*    `password`: The password for the MySQL database.
*    `host`: The hostname, IP address, or URL of the MySQL server.
*    `port`: The port number for connecting to the MySQL server.
*    `database`: The name of the MySQL database to connect to.
*    `url`: You can specify a connection to MySQL Server using a URI-like string, if the above params are not provided.

Optional connection parameters include the following:

 * `ssl`: Boolean parameter that indicates whether SSL encryption is enabled for the connection. Set to True to enable SSL and enhance connection security, or set to False to use the default non-encrypted connection. 
 * `ssl_ca`:  Specifies the path to the Certificate Authority (CA) file in PEM format. 
 * `ssl_cert`:  Specifies the path to the SSL certificate file. This certificate should be signed by a trusted CA specified in the `ssl_ca` file or be a self-signed certificate trusted by the server.
 * `ssl_key`: Specifies the path to the private key file (in PEM format).

## Usage

Retrieve data from a specified table by providing the integration and table name:

```sql
SELECT *
FROM mysql_datasource.table_name
LIMIT 10;
```

<Note>
The above examples utilize `mysql_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

<Tip>
**Next Steps**

 Check out the [Forecast Monthly Expenditures tutorial](https://docs.mindsdb.com/sql/tutorials/expenditures-statsforecast), which uses the data from MySQL database to MindsDB.
</Tip>

## Troubleshooting Guide

<Warning>
`Database Connection Error`

* **Symptoms**: Failure to connect MindsDB with the MySQL database.
* **Checklist**:
    1. Ensure that the MySQL server is running and accessible
    2. Confirm that host, port, user, and password are correct. Try a direct MySQL connection.
    3. Test the network connection between the MindsDB host and the MySQL server.
</Warning>

<Warning>
`SQL statement cannot be parsed by mindsdb_sql`

* **Symptoms**: SQL queries failing or not recognizing table names containing spaces, reserved words or special characters.
* **Checklist**:
    1. Ensure table names with spaces or special characters are enclosed in backticks.
    2. Examples:
        * Incorrect: SELECT * FROM integration.travel data
        * Incorrect: SELECT * FROM integration.'travel data'
        * Correct: SELECT * FROM integration.\`travel data\`
</Warning>

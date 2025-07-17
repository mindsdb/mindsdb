---
title: MySQL
sidebarTitle: MySQL
---

This documentation describes the integration of MindsDB with [MySQL](https://www.mysql.com/), a fast, reliable, and scalable open-source database.
The integration allows MindsDB to access data from MySQL and enhance MySQL with AI capabilities.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To connect MySQL to MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).

## Connection

Establish a connection to MySQL from MindsDB by executing the following SQL command and providing its [handler name](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/mysql_handler) as an engine.

```sql
CREATE DATABASE mysql_conn
WITH ENGINE = 'mysql', 
PARAMETERS = {
    "host": "host-name",
    "port": 3306,
    "database": "db-name",
    "user": "user-name",
    "password": "password"
};
```

Or:

```sql
CREATE DATABASE mysql_datasource
WITH
  ENGINE = 'mysql',
  PARAMETERS = {
    "url": "mysql://user-name@host-name:3306"
  };
```

Required connection parameters include the following:

*    `user`: The username for the MySQL database.
*    `password`: The password for the MySQL database.
*    `host`: The hostname, IP address, or URL of the MySQL server.
*    `port`: The port number for connecting to the MySQL server.
*    `database`: The name of the MySQL database to connect to.

Or:

*    `url`: You can specify a connection to MySQL Server using a URI-like string, as an alternative connection option.

Optional connection parameters include the following:

 * `ssl`: Boolean parameter that indicates whether SSL encryption is enabled for the connection. Set to True to enable SSL and enhance connection security, or set to False to use the default non-encrypted connection. 
 * `ssl_ca`: Specifies the path to the Certificate Authority (CA) file in PEM format. 
 * `ssl_cert`: Specifies the path to the SSL certificate file. This certificate should be signed by a trusted CA specified in the `ssl_ca` file or be a self-signed certificate trusted by the server.
 * `ssl_key`: Specifies the path to the private key file (in PEM format).
 * `use_pure` (`True` by default): Whether to use pure Python or C Extension. If `use_pure=False` and the C Extension is not available, then Connector/Python will automatically fall back to the pure Python implementation.

## Usage

The following usage examples utilize the connection to MySQL made via the `CREATE DATABASE` statement and named `mysql_conn`.

Retrieve data from a specified table by providing the integration and table name.

```sql
SELECT *
FROM mysql_conn.table_name
LIMIT 10;
```

<Tip>
**Next Steps**

Follow [this tutorial](https://docs.mindsdb.com/use-cases/data_enrichment/text-summarization-inside-mysql-with-openai) to see more use case examples.
</Tip>

## Troubleshooting

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

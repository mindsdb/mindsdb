---
title: Microsoft SQL Server
sidebarTitle: Microsoft SQL Server
---

This documentation describes the integration of MindsDB with Microsoft SQL Server, a relational database management system developed by Microsoft.
The integration allows for advanced SQL functionalities, extending Microsoft SQL Server's capabilities with MindsDB's features.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB [locally via Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or use [MindsDB Cloud](https://cloud.mindsdb.com/).
2. To connect Microsoft SQL Server to MindsDB, install the required dependencies following [this instruction](/setup/self-hosted/docker#install-dependencies).

### Installation

The MSSQL handler supports two connection methods:

#### Option 1: Standard Connection (pymssql - Recommended)

```bash
pip install mindsdb[mssql]
```

This installs `pymssql`, which provides native FreeTDS-based connections. Works on all platforms.

#### Option 2: ODBC Connection (pyodbc)

```bash
pip install mindsdb[mssql-odbc]
```

This installs both `pymssql` and `pyodbc` for ODBC driver support.

**Additional requirements for ODBC:**
- **System ODBC libraries**: On Linux, install `unixodbc` and `unixodbc-dev`
  ```bash
  sudo apt-get install unixodbc unixodbc-dev
  ```
- **Microsoft ODBC Driver for SQL Server**: 
  - **Linux**: 
    ```bash
    # Add Microsoft repository
    curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc
    curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
    
    # Install ODBC Driver 18
    sudo apt-get update
    sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
    ```
  - **macOS**: `brew install msodbcsql18`
  - **Windows**: Download from [Microsoft](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

To verify installed drivers:

```bash
odbcinst -q -d
```

## Connection

Establish a connection to your Microsoft SQL Server database from MindsDB by executing the following SQL command:

```sql
CREATE DATABASE mssql_datasource 
WITH ENGINE = 'mssql', 
PARAMETERS = {
    "host": "127.0.0.1",
    "port": 1433,
    "user": "sa",
    "password": "password",
    "database": "master"
};
```

Required connection parameters include the following:

* `user`: The username for the Microsoft SQL Server.
* `password`: The password for the Microsoft SQL Server.
* `host` The hostname, IP address, or URL of the Microsoft SQL Server.
* `database` The name of the Microsoft SQL Server database to connect to.

Optional connection parameters include the following:

* `port`: The port number for connecting to the Microsoft SQL Server. Default is 1433.
* `server`: The server name to connect to. Typically only used with named instances or Azure SQL Database.
* `schema`: The schema in which objects are searched first. If specified, all table references without an explicit schema will be automatically qualified with this schema.

### ODBC Connection

The handler also supports ODBC connections via `pyodbc` for advanced scenarios like Windows Authentication or specific driver requirements.


#### Setup

1. Install: `pip install mindsdb[mssql-odbc]`
2. Install system ODBC driver (see Installation section above)

Basic ODBC Connection:

```sql
CREATE DATABASE mssql_odbc_datasource 
WITH ENGINE = 'mssql', 
PARAMETERS = {
    "host": "127.0.0.1",
    "port": 1433,
    "user": "sa",
    "password": "password",
    "database": "master",
    "driver": "ODBC Driver 18 for SQL Server"  -- Specifying driver enables ODBC
};
```
ODBC-specific Parameters:

* `driver`: The ODBC driver name (e.g., "ODBC Driver 18 for SQL Server"). When specified, enables ODBC mode.
* `use_odbc`: Set to `true` to explicitly use ODBC. Optional if `driver` is specified.
* `encrypt`: Connection encryption: `"yes"` or `"no"`. Driver 18 defaults to `"yes"`.
* `trust_server_certificate`: Whether to trust self-signed certificates: `"yes"` or `"no"`.
* `connection_string_args`: Additional connection string arguments.

#### Example: Azure SQL Database with Encryption:

```sql
CREATE DATABASE azure_sql_datasource 
WITH ENGINE = 'mssql', 
PARAMETERS = {
    "host": "myserver.database.windows.net",
    "port": 1433,
    "user": "adminuser",
    "password": "SecurePass123!",
    "database": "mydb",
    "driver": "ODBC Driver 18 for SQL Server",
    "encrypt": "yes",
    "trust_server_certificate": "no"
};
```

#### Example: Local Development (Self-Signed Certificate):

```sql
CREATE DATABASE local_mssql 
WITH ENGINE = 'mssql', 
PARAMETERS = {
    "host": "localhost",
    "port": 1433,
    "user": "sa",
    "password": "YourStrong@Passw0rd",
    "database": "testdb",
    "driver": "ODBC Driver 18 for SQL Server",
    "encrypt": "yes",
    "trust_server_certificate": "yes"  -- Allow self-signed certs
};
```

### ODBC Connection

The handler also supports ODBC connections via `pyodbc` for advanced scenarios like Windows Authentication or specific driver requirements.


#### Setup

1. Install: `pip install mindsdb[mssql-odbc]`
2. Install system ODBC driver (see Installation section above)

Basic ODBC Connection:

```sql
CREATE DATABASE mssql_odbc_datasource 
WITH ENGINE = 'mssql', 
PARAMETERS = {
    "host": "127.0.0.1",
    "port": 1433,
    "user": "sa",
    "password": "password",
    "database": "master",
    "driver": "ODBC Driver 18 for SQL Server"  -- Specifying driver enables ODBC
};
```
ODBC-specific Parameters:

* `driver`: The ODBC driver name (e.g., "ODBC Driver 18 for SQL Server"). When specified, enables ODBC mode.
* `use_odbc`: Set to `true` to explicitly use ODBC. Optional if `driver` is specified.
* `encrypt`: Connection encryption: `"yes"` or `"no"`. Driver 18 defaults to `"yes"`.
* `trust_server_certificate`: Whether to trust self-signed certificates: `"yes"` or `"no"`.
* `connection_string_args`: Additional connection string arguments.

#### Example: Azure SQL Database with Encryption:

```sql
CREATE DATABASE azure_sql_datasource 
WITH ENGINE = 'mssql', 
PARAMETERS = {
    "host": "myserver.database.windows.net",
    "port": 1433,
    "user": "adminuser",
    "password": "SecurePass123!",
    "database": "mydb",
    "driver": "ODBC Driver 18 for SQL Server",
    "encrypt": "yes",
    "trust_server_certificate": "no"
};
```

#### Example: Local Development (Self-Signed Certificate):

```sql
CREATE DATABASE local_mssql 
WITH ENGINE = 'mssql', 
PARAMETERS = {
    "host": "localhost",
    "port": 1433,
    "user": "sa",
    "password": "YourStrong@Passw0rd",
    "database": "testdb",
    "driver": "ODBC Driver 18 for SQL Server",
    "encrypt": "yes",
    "trust_server_certificate": "yes"  -- Allow self-signed certs
};
```

## Usage

Retrieve data from a specified table by providing the integration name, schema, and table name:

```sql
SELECT *
FROM mssql_datasource.schema_name.table_name
LIMIT 10;
```

Run T-SQL queries directly on the connected Microsoft SQL Server database:

```sql
SELECT * FROM mssql_datasource (

    --Native Query Goes Here
    SELECT 
      SUM(orderqty) total
    FROM Product p JOIN SalesOrderDetail sd ON p.productid = sd.productid
    JOIN SalesOrderHeader sh ON sd.salesorderid = sh.salesorderid
    JOIN Customer c ON sh.customerid = c.customerid
    WHERE (Name = 'Racing Socks, L') AND (companyname = 'Riding Cycles');

);
```

<Note>
The above examples utilize `mssql_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

### Performance Optimization for Large Datasets

The handler is optimized for efficient data processing, but for very large result sets (millions of rows):

1. **Use SQL Server's filtering**: Apply `WHERE` clauses to filter data on the server side
2. **Use pagination**: Use `TOP`/`OFFSET-FETCH` in SQL Server or `LIMIT` in MindsDB queries
3. **Aggregate when possible**: Use `GROUP BY`, `COUNT()`, `AVG()`, etc. to reduce data volume
4. **Index your tables**: Ensure proper indexes on SQL Server for query performance

**Example - Paginated Query:**
```sql
SELECT * FROM mssql_datasource (
    SELECT TOP 100000 *
    FROM large_table
    ORDER BY id
    OFFSET 0 ROWS
);
```

## Troubleshooting Guide

<Warning>
`Database Connection Error`

* **Symptoms**: Failure to connect MindsDB with the Microsoft SQL Server database.
* **Checklist**:
    1. Make sure the Microsoft SQL Server is active.
    2. Confirm that host, port, user, and password are correct. Try a direct Microsoft SQL Server connection using a client like SQL Server Management Studio or DBeaver.
    3. Ensure a stable network between MindsDB and Microsoft SQL Server.
</Warning>

<Warning>
`SQL statement cannot be parsed by mindsdb_sql`

* **Symptoms**: SQL queries failing or not recognizing table names containing spaces or special characters.
* **Checklist**:
    1. Ensure table names with spaces or special characters are enclosed in backticks.
    2. Examples:
        * Incorrect: SELECT * FROM integration.travel data
        * Incorrect: SELECT * FROM integration.'travel data'
        * Correct: SELECT * FROM integration.\`travel data\`
</Warning>

<Warning>
`ODBC Driver Connection Error`

* **Symptoms**: Errors like "Driver not found", "Can't open lib 'ODBC Driver 17 for SQL Server'", or "pyodbc is not installed".
* **Checklist**:
    1. **Verify pyodbc is installed**: `pip list | grep pyodbc`
    2. **Check system ODBC libraries**: `ldconfig -p | grep odbc` (Linux) should show libodbc.so
    3. **Verify ODBC drivers**: Run `odbcinst -q -d` to list installed drivers
    4. **Match driver name exactly**: Use the exact name from `odbcinst -q -d` (case-sensitive)
    5. **For Driver 18 encryption errors**: Add `"encrypt": "yes", "trust_server_certificate": "yes"` for local/dev servers
    6. **Test connection manually**: 
       ```python
       import pyodbc
       print(pyodbc.drivers())  # Should list available drivers
       ```
</Warning>
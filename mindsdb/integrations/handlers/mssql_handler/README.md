# Microsoft SQL Server Integration

This documentation describes the integration of MindsDB with Microsoft SQL Server, a relational database management system developed by Microsoft. The integration allows for advanced SQL functionalities, extending PostgreSQL's capabilities with MindsDB's features.

## Getting Started

### Prerequisites

   1. Ensure that MindsDB and Microsoft SQL Server are installed on your system or you have access to cloud options.
   2. If running locally install the dependencies as `pip install mindsdb[mssql]`.

### Connection

Use the following syntax to create a connection to the Microsoft SQL Server database in MindsDB:

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

Required Parameters:

* `user`: The username for the Microsoft SQL Server database.
* `password`: The password for the Microsoft SQL Server database.
* `host` The hostname, IP address, or URL of the Microsoft SQL Server server.
* `database` The name of the Microsoft SQL Server database to connect to.

Optional Parameters:

* `port`: The port number for connecting to the Microsoft SQL Server. Default is 1433.
* `server`: The server name to connect to. Typically only used with named instances or Azure SQL Database.

### Example Usage

Querying a Table:

 ```sql
    SELECT *
    FROM mssql_datasource.schema_name.table_name
    LIMIT 10;
```

Running T-SQL queries by wrapping them inside the mssql integration SELECT:

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
> Note: In the above examples we are using `mssql_datasource` name, which was created with CREATE DATABASE query.
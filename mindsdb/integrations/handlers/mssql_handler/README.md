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
        "user": "mssql",
        "password": "password",
        "database": "mssql"
    };
```

Required Parameters:

* `user`: The username for the Microsoft SQL Server database.
* `password`: The password for the Microsoft SQL Server database.
* `host` The hostname, IP address, or URL of the Microsoft SQL Server server.
* `port` The port number for connecting to the Microsoft SQL Server server.
* `database` The name of the Microsoft SQL Server database to connect to.

### Example Usage

Querying a Table:

 ```sql

```

Running native queries by wrapping them inside the postgresql integration SELECT:

```sql

```
> Note: In the above examples we are using `mssql_datasource` name, which was created with CREATE DATABASE query.
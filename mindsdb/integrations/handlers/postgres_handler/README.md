# PostgreSQL Integration

This documentation describes the integration of MindsDB with PostgreSQL, a powerful, open-source, object-relational database system. The integration allows for advanced SQL functionalities, extending PostgreSQL's capabilities with MindsDB's features.

## Getting Started

### Prerequisites

   1. Ensure that MindsDB and PostgreSQL are installed on your system or you have access to cloud options.
   2. If running locally install the dependencies as `pip install mindsdb[postgres]`.
        

### Connection

Use the following syntax to create a connection to the PostgreSQL database in MindsDB:

```sql
    CREATE DATABASE psql_datasource 
    WITH ENGINE = 'postgres', 
    PARAMETERS = {
        "host": "127.0.0.1",
        "port": 5432,
        "database": "postgres",
        "user": "postgres",
        "schema": "data",
        "password": "password"
    };
```

Required Parameters:

* `user`: The username for the PostgreSQL database.
* `password`: The password for the PostgreSQL database.
* `host`: The hostname, IP address, or URL of the PostgreSQL server.
* `port`: The port number for connecting to the PostgreSQL server.
* `database`: The name of the PostgreSQL database to connect to.

Optional Parameters:

* `schema`: The database schema to use. Default is `public`.
* `sslmode`: The SSL mode for the connection.

### Example Usage

Querying a Table:

 ```sql
    SELECT * FROM psql_datasource.demo_data.used_car_price LIMIT 10;
```

Running native queries by wrapping them inside the postgresql integration SELECT:

```sql
SELECT * FROM psql_datasource (
    --Native Query Goes Here
     SELECT 
        model, 
        COUNT(*) OVER (PARTITION BY model, year) AS units_to_sell, 
        ROUND((CAST(tax AS decimal) / price), 3) AS tax_div_price
    FROM demo_data.used_car_price
);
```
> Note: In the above examples we are using `psql_datasource` name, which was created with CREATE DATABASE query.
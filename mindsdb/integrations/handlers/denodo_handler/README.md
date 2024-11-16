# Denodo Handler

This is the implementation of the Denodo handler for MindsDB.

## Denodo
Denodo is a data virtualization platform that provides real-time access to various data sources, including databases, cloud services, and big data systems. It allows users to create a unified view of their data without the need for data replication or movement.

## Implementation
This handler was implemented using the `pyodbc` library, which allows you to use Python code to run SQL commands on Denodo.

The required arguments to establish a connection are:
* `driver`: The driver to use for the Denodo connection.
* `url`: The URL for the Denodo connection.
* `user`: The user for the Denodo connection.
* `password`: The password for the Denodo connection.

## Usage
In order to make use of this handler and connect to Denodo in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE denodo_db
WITH
    ENGINE = 'denodo',
    PARAMETERS = {
        "driver": "com.denodo.vdp.jdbc.Driver",
        "url": "jdbc:vdb://<hostname>:<port>/<database_name>",
        "user": "admin",
        "password": "password"
    };
```

Now, you can use this established connection to query your database as follows:

```sql
SELECT * FROM denodo_db.<table_name>;
```

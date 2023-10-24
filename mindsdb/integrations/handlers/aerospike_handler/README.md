## Implementation

This is the implementation of the Aereospike for MindsDB.

This handler was implemented using `duckdb`, a library that allows SQL queries to be executed on `pandas` DataFrames.

The required arguments to establish a connection are as follows:

-   `user` is the database user.
-   `password` is the database password.
-   `host` is the host IP address or URL.
-   `port` is the port used to make TCP/IP connection.
-   `namespace` is the aerospike namespace.

Other optional parameters are not supported as of now.

## Usage

In order to make use of this handler and connect to the Aereospike database in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE aerospike_db
WITH ENGINE = "aerospike",
PARAMETERS = {
    "user": "test",
    "password": "password",
    "host": "localhost",
    "port": 3000,
    "namespace": "test"
    };
```

You can use this established connection to query your table as follows.

```sql
SELECT * FROM aerospike_db.house_rentals;
```

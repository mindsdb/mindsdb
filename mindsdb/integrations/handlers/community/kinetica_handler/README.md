## Implementation

This handler is implemented by extending the PostgresHandler.

The required arguments to establish a connection are as follows:

-   `user` is the database user.
-   `password` is the database password.
-   `host` is the host IP address or URL.
-   `port` is the port used to make TCP/IP connection.
-   `database` is the database name.

There are several optional arguments that can be used as well.

-   `sslmode` ssl modes (`disable, allow, prefer, require, verify-ca, verify-full`).

## Usage

In order to make use of this handler and connect to the MariaDB database in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE kinetica
WITH ENGINE = "kinetica",
PARAMETERS = {
    "user": "xxxxxx",
    "password": "xxxxx",
    "host": "abc.abc.abc.com",
    "port": 5432,
    "database": "test"
    };
```

You can use this established connection to query your table as follows.

```sql
SELECT * FROM kinetica.home_rentals_new;
```

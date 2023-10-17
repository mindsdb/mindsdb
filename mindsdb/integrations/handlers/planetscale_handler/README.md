## Implementation

This handler is implemented by extending the `MySQLHandler` that uses `mysql-connector-python` library. 
The required arguments to establish a connection are as follows:

* `user` is the database user.
* `password` is the database password.
* `host` is the host name, IP address, or URL.
* `port` is the port used to make TCP/IP connection.
* `database` is the database name.


## Usage

In order to make use of this handler and connect to the PlanetScale, the following syntax can be used:

```sql
CREATE DATABASE ps_datasource
WITH
  ENGINE = 'planet_scale',
  PARAMETERS = {
    "host": "127.0.0.1",
    "port": 3306,
    "database": "mysql",
    "user": "root",
    "password": "password"
  };
```

You can use this established connection to query your table as follows:

```sql
SELECT *
FROM ps_datasource.example_table;
```

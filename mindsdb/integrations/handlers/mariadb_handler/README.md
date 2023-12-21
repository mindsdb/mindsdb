## Implementation

This handler is implemented by extending the MySQLHandler.

**Option 1:**
Connect MariaDB to MindsDB by providing the URL parameter. Learn more [here](https://dev.mysql.com/doc/refman/8.0/en/connecting-using-uri-or-key-value-pairs.html).

**Option 2:**
Connect MariaDB to MindsDB by providing the following parameters:

* `user` is the database user.
* `password` is the database password.
* `host` is the host name, IP address, or URL.
* `port` is the port used to make TCP/IP connection. Default: 3306.
* `database` is the database name.

There are several optional parameters that can be used as well.

* `ssl` is the `ssl` parameter value that indicates whether SSL is enabled (`True`) or disabled (`False`).
* `ssl_ca` is the SSL Certificate Authority.
* `ssl_cert` stores SSL certificates.
* `ssl_key` stores SSL keys.

## Usage

In order to make use of this handler and connect to the MariaDB database in MindsDB, the following syntax can be used:

**Option 1:**
```sql
CREATE DATABASE maria_datasource
WITH
  ENGINE = 'mariadb',
  PARAMETERS = {
    "url": "mysql://user@127.0.0.1:3306"
  };
```

**Option 2:**
```sql
CREATE DATABASE maria_datasource
WITH
  engine = 'mariadb',
  parameters = {
    "host": "127.0.0.1",
    "port": 3306,
    "database": "mariadb",
    "user": "root",
    "password": "password"
  };
```

You can use this established connection to query your table as follows.

```sql
SELECT *
FROM maria_datasource.example_table;
```

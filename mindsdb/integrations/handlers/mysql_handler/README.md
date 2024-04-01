## Implementation

This handler is implemented using the `mysql-connector-python` library. It's a Python client for MySQL that doesn't depend on any MySQL C libraries.

**Option 1:**
Connect MySQL to MindsDB by providing the URL parameter. Learn more [here](https://dev.mysql.com/doc/refman/8.0/en/connecting-using-uri-or-key-value-pairs.html).

**Option 2:**
Connect MySQL to MindsDB by providing the following parameters:

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

In order to make use of this handler and connect to the MySQL database in MindsDB, the following syntax can be used:

**Option 1:**
```sql
CREATE DATABASE mysql_datasource
WITH
  ENGINE = 'mysql',
  PARAMETERS = {
    "url": "mysql://user@127.0.0.1:3306"
  };
```

**Option 2:**
```sql
CREATE DATABASE mysql_datasource
WITH
  ENGINE = 'mysql',
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
FROM mysql_datasource.example_table;
```

### Testing

To run tests:

```
env PYTHONPATH=./ pytest -v mindsdb/integrations/handlers/mysql_handler/tests/test_mysql_handler.py
```
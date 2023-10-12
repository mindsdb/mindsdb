# Oracle Handler

This is the implementation of the Oracle handler for MindsDB.

## Oracle

Oracle Database is a multi-model database management system produced and marketed by Oracle Corporation.
https://www.oracle.com/database/

## Implementation
This handler is implemented using the [`oracledb` library](https://oracle.github.io/python-oracledb/).
Please install it before using this handler:

```
pip install oracledb
```

## Usage

The following syntax can be used, in order to connect to the Oracle DB:
```sql
CREATE DATABASE oracle_db 
WITH ENGINE = "oracle", 
PARAMETERS = {
    "host": "127.0.0.1", 
    "port": "1521",
    "sid": "ORCL",
    "user": "admin",
    "password": "pass",
    "disable_oob": true, -- optional, disable out-of-band breaks, sometimes may help with connection see https://python-oracledb.readthedocs.io/en/latest/api_manual/module.html
    "auth_mode": "SYSDBA" -- optional, to connect with database privilege: SYSDBA, SYSOPER or others
};
```

The connection accepts either `sid` or `service_name` arguments to target to the right DB instance.

Now, you can use this established connection to query your database as follows:

```sql
SELECT * FROM oracle_db.my_table
```

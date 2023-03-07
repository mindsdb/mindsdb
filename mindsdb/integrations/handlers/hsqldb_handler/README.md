# HSQLDB handler

This is the implementation of the HSQLDB handler for MindsDB.

## HyperSQLDB

HSQLDB (HyperSQL DataBase) is the leading SQL relational database system written in Java. It offers a small, fast multithreaded and transactional database engine with in-memory and disk-based tables and supports embedded and server modes. It includes a powerful command line SQL tool and simple GUI query tools.

HSQLDB supports the widest range of SQL Standard features seen in any open source database engine: SQL:2016 core language features and an extensive list of SQL:2016 optional features. It supports full Advanced ANSI-92 SQL with only two exceptions. Many extensions to the Standard, including syntax compatibility modes and features of other popular database engines, are also supported.

HyperSQL is fully multithreaded and supports high performance 2PL and MVCC (multiversion concurrency control) transaction control models.

https://hsqldb.org/

## Implementation

This handler was implemented using `pyodbc`, the Python ODBC bridge.

## Usage

In order to make use of this handler and connect to an Access database in MindsDB, the following syntax can be used,

```sql
CREATE DATABASE access_datasource
WITH
engine='hsqldb',
parameters={
    "server": "3.220.66.106",
    "port":  "5432",
    "database": "demo",
    "username": "demo_user",
    "password": "demo_password"
};
```

Now, you can use this established connection to query your database as follows,

```sql
SELECT * FROM [your table];
```

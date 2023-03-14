# HSQLDB handler

This is the implementation of the HSQLDB handler for MindsDB.
To know more about how was this implemented [follow this link](http://hsqldb.org/doc/2.0/guide/guide.html#odbc-chapt). There is also a python code sample for you to [check](http://hsqldb.org/doc/2.0/verbatim/sample/sample.py)

## HyperSQLDB

HSQLDB (HyperSQL DataBase) is the leading SQL relational database system written in Java. It offers a small, fast multithreaded and transactional database engine with in-memory and disk-based tables and supports embedded and server modes. It includes a powerful command line SQL tool and simple GUI query tools.

HSQLDB supports the widest range of SQL Standard features seen in any open source database engine: SQL:2016 core language features and an extensive list of SQL:2016 optional features. It supports full Advanced ANSI-92 SQL with only two exceptions. Many extensions to the Standard, including syntax compatibility modes and features of other popular database engines, are also supported.

HyperSQL is fully multithreaded and supports high performance 2PL and MVCC (multiversion concurrency control) transaction control models.

https://hsqldb.org/

## Implementation

This handler was implemented using [pyodbc](https://pypi.org/project/pyodbc/), the Python ODBC bridge and the [Postgres ODBC Driver](https://www.postgresql.org/ftp/odbc/versions/).

## Usage

In order to make use of this handler and connect to a HyperSQL database in MindsDB, you must install [unixODBC](https://www.unixodbc.org/) along with [Postgres ODBC Driver](https://www.postgresql.org/ftp/odbc/versions/). There are [several guides](https://www.ibm.com/docs/en/db2/11.1?topic=managers-installing-unixodbc-driver-manager) for you to follow. The postgres odbc driver must be added in your unixODBC `odbcinst.ini` file as it follows:

```
[PostgreSQL Unicode]
Description     = PostgreSQL ODBC driver (Unicode version)
Driver          = psqlodbcw.so
Debug           = 0
CommLog         = 1
UsageCount      = 1
```

then, in mindsDB, the following syntax can be used to acces your database,

```sql
CREATE DATABASE exampledb
WITH
engine='hsqldb',
parameters={
    "server_name": "3.220.66.106",
    "port":  "5432",
    "database_name": "demo",
    "username": "demo_user",
    "password": "demo_password"
};
```

Now, you can make queries to your database as follows,

```sql
SELECT * FROM [your table];
```

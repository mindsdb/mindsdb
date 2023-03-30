# Ingres Handler

This is the implementation of the Ingres handler for MindsDB.

## Ingres

Ingres is an open-source relational database management system (DBMS) designed for large-scale commercial and government
applications. Actian Corporation currently oversees the development of the database while providing certified binaries
and support. It is designed to run on a wide range of platforms, including Unix, Linux, Windows, and mainframe systems,
and is known for its scalability, reliability, and security.

## Implementation

This handler was implemented using [pyodbc](https://pypi.org/project/pyodbc/)
and [ingres_sa_dialect](https://pypi.org/project/ingres-sa-dialect/) for the implementation of the Ingres dialect for
SQLAlchemy.

The required arguments to establish a connection are:

* `server`: Server to be connected
* `database`: Database name to be connected

The optional arguments are:

* `user`: username associated with database *(optional)*
* `password`: password to authenticate your access *(optional)*
* `servertype`: Server type to be connected *(optional)* (default: `ingres`)

## Usage

Install the Ingres ODBC driver for your platform. You can find the appropriate driver on the Ingres website.

In order to make use of this handler and connect to Ingres in MindsDB, the following syntax can be used:

~~~~sql
CREATE DATABASE ingres_db
WITH engine='ingres',
parameters={
    "user": "admin",
    "password": "password",
    "server": "myserver.example.com",
    "database": "test_db",
    "servertype": "ingres"
};
~~~~

Now, you can use this established connection to query your database as follows:

~~~~sql
SELECT *
FROM test_db.test;
~~~~


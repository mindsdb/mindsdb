# Ingres Handler

This is the implementation of the Ingres handler for MindsDB.

## Ingres

Ingres is an open-source relational database management system (DBMS) designed for large-scale commercial and government
applications. Actian Corporation currently oversees the development of the database while providing certified binaries
and support. It is designed to run on a wide range of platforms, including Unix, Linux, Windows, and mainframe systems,
and is known for its scalability, reliability, and security.

## Implementation

This handler was implemented using [pyodbc](https://pypi.org/project/pyodbc/)
and [ingres_sa_dialect](https://pypi.org/project/ingres-sa-dialect/) for the implementation of the Ingres dialect from
SQLAlchemy.

The required arguments to establish a connection are:

* `user`: username associated with database
* `password`: password to authenticate your access
* `server`: Server to be connected
* `database`: Database name to be connected

The optional arguments are:

* `servertype`: Server type to be connected *(optional)* (default: `ingres`)

## Usage

Install the Ingres ODBC driver for your platform. You can find the appropriate driver on the Ingres website.

Information about connecting to Ingres 11.2 using ODBC can be
found [here](https://docs.actian.com/ingres/11.2/index.html#page/QuickStart_Linux/Connecting_to_Ingres_Using_ODBC.htm#ww306952).

**Important**:

Before you run the Ingres Handler you first need to execute the following commands
in order to install the newest version Ingres dialect for SQLAlchemy:

~~~~shell
python -m pip install pyodbc sqlalchemy
cd mindsdb/integrations/handlers/ingres_handler
git clone https://github.com/ActianCorp/ingres_sa_dialect.git
cd ingres_sa_dialect
python -m pip install -e .
~~~~

In order to make use of this handler and connect to Ingres in MindsDB, the following syntax can be used:

~~~~sql
CREATE
DATABASE ingres_db
WITH engine='ingres',
parameters={
    "user": "admin",
    "password": "password",
    "server": "myserver.example.com",
    "database": "test_db"
};
~~~~

Now, you can use this established connection to query your database as follows:

~~~~sql
SELECT *
FROM test_db.test;
~~~~


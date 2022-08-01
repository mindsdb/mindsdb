# Microsoft Access Handler

This is the implementation of the Microsoft Access handler for MindsDB.

## Microsoft Access
Microsoft Access is a pseudo-relational database engine from Microsoft. It is part of the Microsoft Office suite of applications that also includes Word, Outlook and Excel, among others. Access is also available for purchase as a stand-alone product. Access uses the Jet Database Engine for data storage.
https://www.techopedia.com/definition/1218/microsoft-access

## Implementation
This handler was implemented using `pyodbc`, the Python ODBC bridge.

The only required argument to establish a connection is `db_file`. This points to the database file that the connection is to be made to.

## Usage
In order to make use of this handler and connect to an Access database in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE access_datasource
WITH
engine='access',
parameters={
    "db_file":"C:\\Users\\minurap\\Documents\\example_db.accdb"
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM access_datasource.example_tbl
~~~~
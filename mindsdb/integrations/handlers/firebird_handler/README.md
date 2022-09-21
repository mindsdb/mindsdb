# Firebird Handler

This is the implementation of the Firebird handler for MindsDB.

## Firebird
Firebird is a relational database offering many ANSI SQL standard features that runs on Linux, Windows, and a variety of Unix platforms. Firebird offers excellent concurrency, high performance, and powerful language support for stored procedures and triggers. It has been used in production systems, under a variety of names, since 1981.
<br>
https://firebirdsql.org/en/about-firebird/#:~:text=Firebird%20is%20a%20relational%20database,for%20stored%20procedures%20and%20triggers.

## Implementation
This handler was implemented using the `fdb` library, the Python driver for Firebird.

The required arguments to establish a connection are,
* `host`: the host name or IP address of the Firebird server
* `database`: the port to use when connecting with the Firebird server
* `user`: the user to authenticate the user with the Firebird server
* `password`: the password to authenticate the user with the Firebird server

## Usage
In order to make use of this handler and connect to a Firebird server in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE firebird_datasource
WITH
engine='firebird',
parameters={
    "host": "localhost",
    "database": r"C:\Users\minura\Documents\mindsdb\example.fdb",
    "user": "sysdba",
    "password": "password"
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM firebird_datasource.example_tbl
~~~~
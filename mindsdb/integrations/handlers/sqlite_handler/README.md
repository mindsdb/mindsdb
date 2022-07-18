# SQLite Handler

This is the implementation of the SQLite handler for MindsDB.

## SQLite
SQLite is an in-process library that implements a self-contained, serverless, zero-configuration, transactional SQL database engine. The code for SQLite is in the public domain and is thus free for use for any purpose, commercial or private. SQLite is the most widely deployed database in the world with more applications than we can count, including several high-profile projects.
https://www.sqlite.org/about.html

## Implementation
This handler was implemented using the standard `sqlite3` library that comes with Python.

The only required argument to establish a connection is `db_file`. This points to the database file that the connection is to be made to.

Optionally, this may also be set to `:memory:`, which will create an in-memory database.

## Usage
In order to make use of this handler and connect to a SQLite database in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE sqlite_datasource
WITH
engine='sqlite',
parameters={
    "db_file":"example.db"
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM sqlite_datasource.example_tbl
~~~~
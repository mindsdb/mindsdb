# SAP SQL Anywhere Handler

This is the implementation of the SAP SQL Anywhere handler for MindsDB.

## SAP SQL Anywhere


SAP SQL Anywhere Embedded Database for Application Software
enables secure, reliable data management for servers where no DBA is available and synchronization for tens of thousands of mobile devices, Internet of Things (IoT) systems, and remote environments. [Read more](https://www.sap.com/products/technology-platform/sql-anywhere.html).

## Implementation

This handler was implemented using `sqlanydb` - the Python driver for SAP SQL Anywhere.

The required arguments to establish a connection are,

* `host`: the host name or IP address of the SAP SQL Anywhere instance
* `port`: the port number of the SAP SQL Anywhere instance
* `user`: specifies the user name
* `password`: specifies the password for the user
* `database`: sets the current database
* `server`: sets the current server

## Usage

Based on the current connected database we have a table called `TEST` that was created using
the following SQL statements:

~~~~sql
CREATE TABLE TEST
(
    ID          INTEGER NOT NULL,
    NAME        NVARCHAR(1),
    DESCRIPTION NVARCHAR(1)
);

CREATE UNIQUE INDEX TEST_ID_INDEX
    ON TEST (ID);

ALTER TABLE TEST
    ADD CONSTRAINT TEST_PK
        PRIMARY KEY (ID);

INSERT INTO TEST
VALUES (1, 'h', 'w');
~~~~

In order to make use of this handler and connect to the SAP SQL Anywhere database in MindsDB, the following syntax can be used:

~~~~sql
CREATE DATABASE sap_sqlany_trial
WITH ENGINE = 'sqlany', 
PARAMETERS = {
    "user": "DBADMIN",
    "password": "password",
    "host": "localhost",
    "port": "55505",
    "server": "TestMe",
    "database": "MINDSDB"
};
~~~~

**Note**: The above example assumes usage of SAP SQL Anywhere Cloud, which requires the `encrypt` parameter to be set to `true` and uses port `443`.

Now, you can use this established connection to query your database as follows:

~~~~sql
SELECT * FROM sap_sqlany_trial.test
~~~~

| ID | NAME | DESCRIPTION |
|----|------|-------------|
| 1  | h    | w           |

![MindsDB using SAP SQL Anywhere Integration](https://imgur.com/a/sE9uQoL)

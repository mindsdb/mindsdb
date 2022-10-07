# SAP HANA Handler

This is the implementation of the SAP HANA handler for MindsDB.

## SAP HANA

SAP HANA (High-performance ANalytic Appliance) is a multi-model database that stores data in its memory instead of keeping it on a disk. The column-oriented in-memory database design used by SAP HANA allows users to run advanced analytics alongside high-speed transactions in a single system. [Read more](https://www.sap.com/products/technology-platform/hana/what-is-sap-hana.html).

## Implementation

This handler was implemented using `hdbcli` - the Python driver for SAP HANA.

The required arguments to establish a connection are,

* `host`: the host name or IP address of the SAP HANA instance
* `port`: the port number of the SAP HANA instance
* `user`: specifies the user name
* `password`: specifies the password for the user
* `schema`: sets the current schema, which is used for identifiers without a schema

## Usage

Assuming you created a schema in SAP HANA called `MINDSDB` and you have a table called `TEST` that was created using
the following SQL statements:

~~~~sql
CREATE SCHEMA MINDSDB;

CREATE TABLE MINDSDB.TEST
(
    ID          INTEGER NOT NULL,
    NAME        NVARCHAR(1),
    DESCRIPTION NVARCHAR(1)
);

CREATE UNIQUE INDEX MINDSDB.TEST_ID_INDEX
    ON MINDSDB.TEST (ID);

ALTER TABLE MINDSDB.TEST
    ADD CONSTRAINT TEST_PK
        PRIMARY KEY (ID);

INSERT INTO MINDSDB.TEST
VALUES (1, 'h', 'w');
~~~~

In order to make use of this handler and connect to the SAP HANA database in MindsDB, the following syntax can be used:

~~~~sql
CREATE DATABASE sap_hana_trial
WITH ENGINE = 'hana', 
PARAMETERS = {
    "user": "DBADMIN",
    "password": "password",
    "host": "<uuid>.hana.trial-us10.hanacloud.ondemand.com",
    "port": "443",
    "schema": "MINDSDB",
    "encrypt": true
};
~~~~

**Note**: The above example assumes usage of SAP HANA Cloud, which requires the `encrypt` parameter to be set to `true` and uses port `443`.

Now, you can use this established connection to query your database as follows:

~~~~sql
SELECT * FROM sap_hana_trial.test
~~~~

| ID | NAME | DESCRIPTION |
|----|------|-------------|
| 1  | h    | w           |

![MindsDB using SAP HANA Integration](https://i.imgur.com/okXNhoc.jpg)

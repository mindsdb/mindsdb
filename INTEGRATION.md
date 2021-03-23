## Parameters

A list of the parameters needed to add and query an integration.

Note: Query parameters should come as top level parameters in the application/json request made to the datasources endpoints (they are accessed via `request.json` inside mindsdb)


### Clickhouse

#### Config parameters (when adding a new integration)
* user            [str]
* password        [str]
* host            [str]
* port            [int]

#### Query parameters (when generating a dataset)
* query           [str]


### Mariadb, Postgres, Mysql, Microsfot SQL Server

#### Config parameters (when adding a new integration)
* user            [str]
* password        [str]
* host            [str]
* port            [int]
* database   [optional - str]

#### Query parameters (when generating a dataset)
* query           [str]
* database        [optional - str]


### Snowflake

#### Config parameters (when adding a new integration)
* user            [str]
* password        [str]
* host            [str]
* account         [str]

#### Query parameters (when generating a dataset)
* query           [str]
* database        [str]
* warehouse       [str]
* schema          [str]


### Mongodb

#### Config parameters (when adding a new integration)
* user            [str]
* password        [str]
* host            [str]
* port            [int]

#### Query parameters (when generating a dataset)
* find            [str]
* collection        [str]
* database        [str]

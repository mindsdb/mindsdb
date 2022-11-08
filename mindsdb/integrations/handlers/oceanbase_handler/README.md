# OceanBase Handler

This is the implementation of the OceanBase Handler for MindsDB.

## OceanBase
OceanBase Database is a distributed relational database. It has been supporting the Double 11 Shopping Festival for 9 years and is also the only distributed database in the world that has broken both TPC-C and TPC-H records. It has set forth a new standard of city-level disaster recovery solutions with five IDCs across three sites. OceanBase Database adopts an independently developed integrated architecture, which encompasses both the scalability of a distributed architecture and the performance advantage of a centralized architecture. OceanBase Database supports hybrid transaction/analytical processing (HTAP) with one engine. With features such as strong data consistency, high availability, high performance, online scalability, high compatibility with SQL and mainstream relational databases, transparency to applications, and a high cost/performance ratio, OceanBase Database has helped over 400 customers across industries upgrade their core systems.

OceanBase Database features high business continuity, ease of use, low costs, and low risks.

## Implementation

This handler was implemented by extending mysql connector.

The required arguments to establish a connection are:

* `host`: the host name of the OceanBase connection 
* `port`: the port to use when connecting 
* `user`: the user to authenticate 
* `password`: the password to authenticate the user
* `database`: database name

## Usage

In order to make use of this handler and connect to a OceanBase server in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE oceanbase_datasource
WITH ENGINE = "oceanbase",
PARAMETERS = { 
  "user": "root",
  "password": "<L0v3>",
  "host": "localhost",
  "port": 9030,
  "database": "test"
}
```

Now, you can use this established connection to query your database as follows:

```sql
SELECT * FROM oceanbase_datasource.LoveU LIMIT 10;
```
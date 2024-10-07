---
title: MongoDB
sidebarTitle: MongoDB
---

This documentation describes the integration of MindsDB with [MongoDB](https://www.mongodb.com/company/what-is-mongodb), a document database with the scalability and flexibility that you want with the querying and indexing that you need.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).

## Connection

Establish a connection to MongoDB from MindsDB by executing the following SQL command:

```sql
CREATE DATABASE mongodb_datasource
WITH
  ENGINE = 'mongodb',
  PARAMETERS = {
    "host": "mongodb+srv://admin:admin@demo.mongodb.net/public"
  };
```

Required connection parameters include the following:

* `host`: The host name, IP address or connection string of the MongoDB server.

Optional connection parameters include the following:

* `username`: The username associated with the database.
* `password`: The password to authenticate your access.
* `port`: The port through which TCP/IP connection is to be made.
* `database`: The database name to be connected. This will be required if the connection string is missing the `/database` path.

## Usage

Retrieve data from a specified collection by providing the integration name and collection name:

```sql
SELECT *
FROM mongodb_datasource.my_collection
LIMIT 10;
```

<Note>
The above examples utilize `mongodb_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

<Tip>
At the moment, this integration only supports `SELECT` and `UPDATE` queries.
</Tip>

<Warning>
**For this connection, we strongly suggest using the Mongo API instead of the SQL API.**

MindsDB has a dedicated [Mongo API](/sdks/mongo/mindsdb-mongo-ql-overview) that allows you to use the full power of the MindsDB platform.
Using the Mongo API feels more natural for MongoDB users and allows you to use all the features of MindsDB.

You can find the instructions on how to connect MindsDB to [MongoDB Compass](/connect/mongo-compass) or [MongoDB Shell](/connect/mongo-shell) and proceed with the [Mongo API documentation](/sdks/mongo/mindsdb-mongo-ql-overview) for further details.
</Warning>

<Tip>
Once you connected MindsDB to MongoDB Compass or MongoDB Shell, you can run this command to connect your database to MindsDB:

```sql
test> use mindsdb
mindsdb> db.databases.insertOne({
              name: "mongo_datasource",
              engine: "mongodb",
              connection_args: {
                      "host": "mongodb+srv://user:pass@db.xxxyyy.mongodb.net/"
              }
          });
```

Then you can query your data, like this:

```sql
mindsdb> use mongo_datasource
mongo_datasource> db.demo.find({}).limit(3)
```
</Tip>

## Troubleshooting Guide

<Warning>
`Database Connection Error`

* **Symptoms**: Failure to connect MindsDB with the MongoDB server.
* **Checklist**:
    1. Make sure the MongoDB server is active.
    2. Confirm that host and credentials provided are correct. Try a direct MongoDB connection using a client like MongoDB Compass.
    3. Ensure a stable network between MindsDB and MongoDB. For example, if you are using MongoDB Atlas, ensure that the IP address of the machine running MindsDB is whitelisted.
</Warning>

<Warning>
`Unknown statement`

* **Symptoms**: Errors related to the issuing of unsupported queries to MongoDB via the integration.
* **Checklist**:
    1. Ensure the query is a `SELECT` or `UPDATE` query.

</Warning>

<Warning>
`SQL statement cannot be parsed by mindsdb_sql`

* **Symptoms**: SQL queries failing or not recognizing collection names containing special characters.
* **Checklist**:
    1. Ensure table names with special characters are enclosed in backticks.
    2. Examples:
        * Incorrect: SELECT * FROM integration.travel-data
        * Incorrect: SELECT * FROM integration.'travel-data'
        * Correct: SELECT * FROM integration.\`travel-data\`
</Warning>
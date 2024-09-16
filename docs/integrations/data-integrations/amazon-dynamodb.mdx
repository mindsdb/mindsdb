---
title: Amazon DynamoDB
sidebarTitle: Amazon DynamoDB
---

This documentation describes the integration of MindsDB with [Amazon DynamoDB](https://aws.amazon.com/dynamodb/), a serverless, NoSQL database service that enables you to develop modern applications at any scale.

## Prerequisites

Before proceeding, ensure that MindsDB is installed locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).

## Connection

Establish a connection to your Amazon DynamoDB from MindsDB by executing the following SQL command:

```sql
CREATE DATABASE dynamodb_datasource
WITH
    engine = 'dynamodb',
    parameters = {
      "aws_access_key_id": "PCAQ2LJDOSWLNSQKOCPW",
      "aws_secret_access_key": "U/VjewPlNopsDmmwItl34r2neyC6WhZpUiip57i",
      "region_name": "us-east-1"
    };
```

Required connection parameters include the following:

* `aws_access_key_id`: The AWS access key that identifies the user or IAM role.
* `aws_secret_access_key`: The AWS secret access key that identifies the user or IAM role.
* `region_name`: The AWS region to connect to.

Optional connection parameters include the following:

* `aws_session_token`: The AWS session token that identifies the user or IAM role. This becomes necessary when using temporary security credentials.

## Usage

Retrieve data from a specified table by providing the integration name and the table name:

```sql
SELECT *
FROM dynamodb_datasource.table_name
LIMIT 10;
```

Indexes can also be queried by adding a third-level namespace:

```sql
SELECT *
FROM dynamodb_datasource.table_name.index_name
LIMIT 10;
```

<Tip>
The queries issued to Amazon DynamoDB are in PartiQL, a SQL-compatible query language for Amazon DynamoDB. For more information, refer to the [PartiQL documentation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.html).

There are a few limitations to keep in mind when querying data from Amazon DynamoDB (some of which are specific to PartiQL):
- The `LIMIT`, `GROUP BY` and `HAVING` clauses are not supported in PartiQL `SELECT` statements. Furthermore, subqueries and joins are not supported either. Refer to the [PartiQL documentation for SELECT statements](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.select.html) for more information.
- `INSERT` statements are not supported by this integration. However, this can be overcome by issuing a 'native query' via an established connection. An example of this is provided below.
</Tip>

Run PartiQL queries directly on Amazon DynamoDB:

```sql
SELECT * FROM dynamodb_datasource (

    --Native Query Goes Here
    INSERT INTO "Music" value {'Artist' : 'Acme Band1','SongTitle' : 'PartiQL Rocks'}

);
```

<Note>
The above examples utilize `dynamodb_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

## Troubleshooting Guide

<Warning>
`Database Connection Error`

* **Symptoms**: Failure to connect MindsDB with the Amazon S3  DynamoDB.
* **Checklist**:
    1. Confirm that provided AWS credentials are correct. Try making a direct connection to the Amazon DynamoDB using the AWS CLI.
    2. Ensure a stable network between MindsDB and AWS.
</Warning>

<Warning>
`SQL statement cannot be parsed by mindsdb_sql`

* **Symptoms**: SQL queries failing or not recognizing table names containing special characters.
* **Checklist**:
    1. Ensure table names with special characters are enclosed in backticks.
    2. Examples:
        * Incorrect: SELECT * FROM integration.travel-data
        * Incorrect: SELECT * FROM integration.'travel-data'
        * Correct: SELECT * FROM integration.\`travel-data\`
</Warning>
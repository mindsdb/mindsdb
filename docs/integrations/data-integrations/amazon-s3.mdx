---
title: Amazon S3
sidebarTitle: Amazon S3
---

This documentation describes the integration of MindsDB with [Amazon S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html), an object storage service that offers industry-leading scalability, data availability, security, and performance.

## Prerequisites

Before proceeding, ensure that MindsDB is installed locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).

## Connection

Establish a connection to your Amazon S3 bucket from MindsDB by executing the following SQL command:

```sql
CREATE DATABASE s3_datasource
WITH
    engine = 's3',
    parameters = {
      "aws_access_key_id": "AQAXEQK89OX07YS34OP"
      "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
      "region_name": "us-east-2",
      "bucket": "my-bucket",
    };
```

Required connection parameters include the following:

* `aws_access_key_id`: The AWS access key that identifies the user or IAM role.
* `aws_secret_access_key`: The AWS secret access key that identifies the user or IAM role.
* `bucket`: The name of the Amazon S3 bucket.

Optional connection parameters include the following:

* `aws_session_token`: The AWS session token that identifies the user or IAM role. This becomes necessary when using temporary security credentials.
* `region_name`: The AWS region to connect to. Default is `us-east-1`.

## Usage

Retrieve data from a specified object (file) in the S3 bucket by providing the integration name and the object key:

```sql
SELECT *
FROM s3_datasource.`my-file.csv`;
LIMIT 10;
```

<Tip>
Wrap the object key in backticks (\`) to avoid any issues parsing the SQL statements provided. This is especially important when the object key contains spaces, special characters or prefixes, such as `my-folder/my-file.csv`.

At the moment, the supported file formats are CSV, TSV, JSON, and Parquet. 
</Tip>

<Note>
The above examples utilize `s3_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

## Troubleshooting Guide

<Warning>
`Database Connection Error`

* **Symptoms**: Failure to connect MindsDB with the Amazon S3 bucket.
* **Checklist**:
    1. Make sure the Amazon S3 bucket exists.
    2. Confirm that provided AWS credentials are correct. Try making a direct connection to the S3 bucket using the AWS CLI.
    3. Ensure a stable network between MindsDB and AWS.
</Warning>

<Warning>
`SQL statement cannot be parsed by mindsdb_sql`

* **Symptoms**: SQL queries failing or not recognizing object names containing spaces, special characters or prefixes.
* **Checklist**:
    1. Ensure object names with spaces, special characters or prefixes are enclosed in backticks.
    2. Examples:
        * Incorrect: SELECT * FROM integration.travel/travel_data.csv
        * Incorrect: SELECT * FROM integration.'travel/travel_data.csv'
        * Correct: SELECT * FROM integration.\`travel/travel_data.csv\`
</Warning>
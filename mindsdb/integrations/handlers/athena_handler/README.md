# Amazon Athena Handler

This is the implementation of the Athena handler for MindsDB.

## Athena
Amazon Athena is an interactive query service that makes it easy to analyze data in Amazon S3 using standard SQL. 
Athena is serverless, so there is no infrastructure to manage, and you pay only for the queries that you run.

<br>
https://aws.amazon.com/athena/

## Implementation
This handler was implemented using the `boto3`, the AWS SDK for Python.

The required arguments to establish a connection are,
* `aws_access_key_id`: the AWS access key
* `aws_secret_access_key`: the AWS secret access key
* `region_name`: the AWS region
* `database`: the Athena database name
* `results_output_location`: the S3 bucket location to store the query results

## Usage
To use this handler, you need to have an AWS account and an S3 bucket to store the query results.

```sql
CREATE DATABASE athena_datasource
WITH
engine='athena',
parameters={
    'aws_access_key_id': 'YOUR_AWS_ACCESS_KEY_ID',
    'aws_secret_access_key': 'YOUR_AWS_SECRET_ACCESS',
    'region_name': 'YOUR_AWS_REGION',
    'database': 'YOUR_ATHENA_DATABASE',
    'results_output_location': 's3://YOUR_S3_BUCKET_NAME/'
};
```

Now, you can use this established connection to query Athena as follows,
```sql
SELECT * FROM dynamodb_datasource.example_tbl
```

# S3 Handler

This is the implementation of the S3 handler for MindsDB.

## Amazon S3
Amazon Simple Storage Service (Amazon S3) is an object storage service that offers industry-leading scalability, data availability, security, and performance. Customers of all sizes and industries can use Amazon S3 to store and protect any amount of data for a range of use cases, such as data lakes, websites, mobile applications, backup and restore, archive, enterprise applications, IoT devices, and big data analytics. Amazon S3 provides management features so that you can optimize, organize, and configure access to your data to meet your specific business, organizational, and compliance requirements.<br>
https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html

## Implementation
This handler was implemented using the `boto3`, the AWS SDK for Python.

The required arguments to establish a connection are,
* `aws_access_key_id`: the AWS access key
* `aws_secret_access_key`: the AWS secret access key
* `region_name`: the AWS region
* `bucket`: the name of the S3 bucket
* `key`: the key of the object to be queried
* `input_serialization`: the format of the data in the object that is to be queried

## Usage
In order to make use of this handler and connect to an object in a S3 bucket through MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE s3_datasource
WITH
engine='s3',
parameters={
    "aws_access_key_id": "PCAQ2LJDOSWLNSQKOCPW",
    "aws_secret_access_key": "U/VjewPlNopsDmmwItl34r2neyC6WhZpUiip57i",
    "region_name": "us-east-1",
    "bucket": "mindsdb-bucket",
    "key": "iris.csv",
    "input_serialization": "{'CSV': {'FileHeaderInfo': 'NONE'}}"
};
~~~~

Now, you can use this established connection to query your object as follows,
~~~~sql
SELECT * FROM s3_datasource.S3Object
~~~~

Queries to objects in S3 are issued using S3 Select,
https://aws.amazon.com/blogs/aws/s3-glacier-select/

This feature is used in `boto3` as described here,
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.select_object_content
<br>
The required format of the `InputSerialization` parameter described here (which translates to the `input_serialization` parameter of the handler) will be of special importance. This describes how to specify the format of the data in the object that is to be queried.

At the moment, S3 Select does not allow multiple files to be queried. Therefore, queries can be issued only to the object that is passed in as a paramter (`key`). This object should always be referred to as `S3Object` when writing queries as shown in the example given above.
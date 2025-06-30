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
      "aws_access_key_id": "AQAXEQK89OX07YS34OP",
      "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
      "bucket": "my-bucket"
    };
```

<Note>
Note that sample parameter values are provided here for reference, and you should replace them with your connection parameters.
</Note>

Required connection parameters include the following:

* `aws_access_key_id`: The AWS access key that identifies the user or IAM role.
* `aws_secret_access_key`: The AWS secret access key that identifies the user or IAM role.

Optional connection parameters include the following:

* `aws_session_token`: The AWS session token that identifies the user or IAM role. This becomes necessary when using temporary security credentials.
* `bucket`: The name of the Amazon S3 bucket. If not provided, all available buckets can be queried, however, this can affect performance, especially when listing all of the available objects.

## Usage

Retrieve data from a specified object (file) in a S3 bucket by providing the integration name and the object key:

```sql
SELECT *
FROM s3_datasource.`my-file.csv`;
LIMIT 10;
```

<Tip>
If a bucket name is provided in the `CREATE DATABASE` command, querying will be limited to that bucket and the bucket name can be ommitted from the object key as shown in the example above. However, if the bucket name is not provided, the object key must include the bucket name, such as `s3_datasource.`my-bucket/my-folder/my-file.csv`.

Wrap the object key in backticks (\`) to avoid any issues parsing the SQL statements provided. This is especially important when the object key contains spaces, special characters or prefixes, such as `my-folder/my-file.csv`.

At the moment, the supported file formats are CSV, TSV, JSON, and Parquet. 
</Tip>

<Note>
The above examples utilize `s3_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

The special `files` table can be used to list all objects available in the specified bucket or all buckets if the bucket name is not provided:

```sql
SELECT *
FROM s3_datasource.files LIMIT 10
```

The content of files can also be retrieved by explicitly requesting the `content` column. This column is empty by default to avoid unnecessary data transfer:

```sql
SELECT path, content
FROM s3_datasource.files LIMIT 10
```

<Tip>
This table will return all objects regardless of the file format, however, only the supported file formats mentioned above can be queried.
</Tip>

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

# S3 Handler

This handler allows MindsDB to work with Amazon S3 and S3-compatible storage services.

## Features
- Read and write files from/to S3 buckets
- Support for AWS S3 and S3-compatible services (like MinIO)
- Custom endpoint URL support
- File format support: CSV, JSON, Parquet

## Configuration

### AWS S3
```json
{
    "aws_access_key_id": "your_access_key",
    "aws_secret_access_key": "your_secret_key",
    "region_name": "us-east-1",
    "bucket": "your_bucket"
}
```

### S3-Compatible Services (e.g., MinIO)
```json
{
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin",
    "bucket": "your_bucket",
    "endpoint_url": "http://localhost:9000"
}
```

## Custom Endpoint Support

The S3 handler now supports custom endpoints, allowing you to connect to any S3-compatible service like MinIO, Ceph, or Wasabi.

### Configuration

To use a custom endpoint, add the `endpoint_url` parameter to your configuration:

```json
{
    "aws_access_key_id": "your_access_key",
    "aws_secret_access_key": "your_secret_key",
    "bucket": "your_bucket",
    "endpoint_url": "http://your-endpoint:9000"
}
```

### Features

- Connect to any S3-compatible service
- Use HTTP or HTTPS endpoints
- Automatic path-style addressing for custom endpoints
- Compatible with MinIO and other S3-compatible services
- Same interface as AWS S3

### Example: MinIO Setup

1. Start MinIO server:
```bash
docker run -p 9000:9000 -p 9001:9001 \
  --name minio \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  -v minio_data:/data \
  minio/minio server /data --console-address ":9001"
```

2. Create a bucket in MinIO
3. Connect using the handler:
```sql
CREATE DATABASE minio_datasource
WITH
    engine = 's3',
    parameters = {
      "aws_access_key_id": "minioadmin",
      "aws_secret_access_key": "minioadmin",
      "bucket": "test-bucket",
      "endpoint_url": "http://localhost:9000"
    };
```

### Usage with Custom Endpoints

```sql
-- Read from custom endpoint
SELECT * FROM minio_datasource.`test.csv`;

-- Write to custom endpoint
INSERT INTO minio_datasource.`output.csv`
SELECT * FROM source_table;

-- List files in custom endpoint
SELECT * FROM minio_datasource.files;
```

### Troubleshooting Custom Endpoints

- **Connection Issues**:
  - Verify the endpoint URL is correct
  - Check if the service is running
  - Ensure proper credentials are used

- **File Operation Issues**:
  - Verify bucket exists
  - Check file permissions
  - Ensure proper path formatting

- **Performance**:
  - Use appropriate timeout settings
  - Consider network latency
  - Optimize file sizes

### Testing Custom Endpoints

The handler includes comprehensive tests for custom endpoints. To run the tests:

```bash
python -m unittest mindsdb/integrations/handlers/s3_handler/tests/test_s3_endpoints.py
```

These tests verify:
- Connection to custom endpoints
- File operations (read/write)
- Error handling
- Timeout scenarios
- Cleanup procedures

## Testing

### Prerequisites
1. Python 3.7+
2. Docker (for MinIO testing)

### Setup
1. For AWS S3 testing:
   - Create a `.env` file in the handler directory
   - Add your AWS credentials:
     ```
     AWS_ACCESS_KEY_ID=your_access_key
     AWS_SECRET_ACCESS_KEY=your_secret_key
     ```

2. For MinIO testing:
   - Start MinIO server:
     ```bash
     docker run -p 9000:9000 -p 9001:9001 minio/minio server /data --console-address ":9001"
     ```
   - Access MinIO console at http://localhost:9001
   - Create a bucket named 'test-bucket'

### Running Tests
```bash
# Run all tests
python -m unittest discover

# Run specific test file
python -m unittest mindsdb/integrations/handlers/s3_handler/tests/test_s3_endpoints.py
```

## Usage Examples

### Reading a File
```sql
SELECT * FROM s3.files
WHERE path = 'data.csv';
```

### Writing Data
```sql
INSERT INTO s3.files (path, data)
VALUES ('output.csv', 'col1,col2\n1,2\n3,4');
```
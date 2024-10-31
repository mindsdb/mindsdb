---
title: IBM COS
sidebarTitle: IBM Cloud Object Storage
---

This documentation describes the integration of MindsDB with [IBM COS](https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-getting-started-cloud-object-storage), an object storage service that offers industry-leading scalability, data availability, security, and performance.

## Prerequisites

Before proceeding, ensure that MindsDB is installed locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).

## Connection

Establish a connection to your IBM COS buckets from MindsDB by executing the following SQL command:

```sql
CREATE DATABASE ibm_datasource
WITH ENGINE = 'ibm_cos',
    PARAMETERS = {
        'cos_hmac_access_key_id': 'your-access-key-id',
        'cos_hmac_secret_access_key': 'your-secret-access-key',
        'cos_endpoint_url': 'https://s3.eu-gb.cloud-object-storage.appdomain.cloud',
        'bucket': 'your-bucket-name' -- Not required
    };
```

<Note>
Note that sample parameter values are provided here for reference, and you should replace them with your connection parameters.
</Note>

Required connection parameters include the following:

- `cos_hmac_access_key_id`: The IBM COS access key that identifies the user or IAM role.
- `cos_hmac_secret_access_key`: The IBM COS secret access key that identifies the user or IAM role.
- `cos_endpoint_url`: The IBM COS resource ID for your cloud Object Storage.

Optional connection parameters include the following:

- `bucket`: The name of the IBM COS bucket. If not provided, all available buckets can be queried, however, this can affect performance, especially when listing all of the available objects.

## Usage

Retrieve data from a specified object (file) in a IBM COS bucket by providing the integration name and the object key:

```sql
SELECT *
FROM ibm_datasource.`my-file.csv`;
LIMIT 10;
```

<Tip>
If a bucket name is provided in the `CREATE DATABASE` command, querying will be limited to that bucket and the bucket name can be ommitted from the object key as shown in the example above. However, if the bucket name is not provided, the object key must include the bucket name, such as `ibm_datasource.`my-bucket/my-folder/my-file.csv`.

Wrap the object key in backticks (\`) to avoid any issues parsing the SQL statements provided. This is especially important when the object key contains spaces, special characters or prefixes, such as `my-folder/my-file.csv`.

At the moment, the supported file formats are CSV, TSV, JSON, and Parquet.
</Tip>

<Note>
The above examples utilize `ibm_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

The special `files` table can be used to list all objects available in the specified bucket or all buckets if the bucket name is not provided:

```sql
SELECT *
FROM ibm_datasource.files LIMIT 10
```

The content of files can also be retrieved by explicitly requesting the `content` column. This column is empty by default to avoid unnecessary data transfer:

```sql
SELECT path, content
FROM ibm_datasource.files LIMIT 10
```

<Tip>
This table will return all objects regardless of the file format, however, only the supported file formats mentioned above can be queried.
</Tip>

## Troubleshooting Guide

<Warning>
`Database Connection Error`

- **Symptoms**: Failure to connect MindsDB with the Amazon S3 bucket.
- **Checklist**: 1. Make sure the IBM COS bucket exists. 2. Confirm that provided IBM COS credentials are correct. Try making a direct connection to the IBM COS bucket using the IBM CLI. 3. Ensure a stable network between MindsDB and IBM COS.
  </Warning>

<Warning>
`SQL statement cannot be parsed by mindsdb_sql`

- **Symptoms**: SQL queries failing or not recognizing object names containing spaces, special characters or prefixes.
- **Checklist**: 1. Ensure object names with spaces, special characters or prefixes are enclosed in backticks. 2. Examples:
  _ Incorrect: SELECT _ FROM integration.travel/travel_data.csv
  - Incorrect: SELECT _ FROM integration.'travel/travel_data.csv'
    _ Correct: SELECT \_ FROM integration.\`travel/travel_data.csv\`
    </Warning>

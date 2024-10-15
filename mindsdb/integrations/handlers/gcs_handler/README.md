---
title: Google Cloud Storage
sidebarTitle: Google Cloud Storage
---

This documentation describes the integration of MindsDB with [Google Cloud Storage](https://cloud.google.com/storage), an object storage service that offers industry-leading scalability, data availability, security, and performance.

## Prerequisites

1. Before proceeding, ensure that MindsDB is installed locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).
2. Install the dependencies from the `requirements.txt` file. Run the following command

```bash
pip install requirements.txt
```

## Connection

Establish a connection to your GCS bucket from MindsDB by executing the following SQL command:

```sql
CREATE DATABASE gcs_datasource
WITH
    engine = 'gcs',
    parameters = {
      "bucket": "<bucket-name>",
      "project_id": "<project-id>",
      "service_account_json_file_path": "/Users/sam/Downloads/svc.json"
    };
```

`project_id` is optional. Connection can be established without `project_id` as well.

```sql
CREATE DATABASE gcs_datasource
WITH
    engine = 'gcs',
    parameters = {
      "bucket": "<bucket-name>",
      "service_account_json_file_path": "/Users/sam/Downloads/svc.json"
    };
```

Required connection parameters include the following:

* `service_account_json_file_path`: The path to the service account JSON file
* `bucket`: The name of the GCS bucket.

Optional connection parameters include the following:

* `project_id`: 'The GCP project id.

## Usage

Retrieve data from a specified object (file) in the GCS bucket by providing the integration name and the object key:

```sql
SELECT *
FROM gcs_datasource.`my-file.csv`;
LIMIT 10;
```

<Tip>
Wrap the object key in backticks (\`) to avoid any issues parsing the SQL statements provided. This is especially important when the object key contains spaces, special characters or prefixes, such as `my-folder/my-file.csv`.

At the moment, the supported file formats are CSV, TSV, JSON, and Parquet. 
</Tip>

<Note>
The above examples utilize `gcs_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

## Troubleshooting Guide

<Warning>
`Database Connection Error`

* **Symptoms**: Failure to connect MindsDB with the GCS bucket.
* **Checklist**:
    1. Make sure the GCS bucket exists.
    2. Confirm that provided service account credentials are correct. Try making a direct connection to the GCS bucket using the gcloud CLI.
    3. Ensure a stable network between MindsDB and GCP.
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
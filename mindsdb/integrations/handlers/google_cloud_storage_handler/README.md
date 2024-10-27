---
title: Google Cloud Storage
sidebarTitle: Google Cloud Storage
---

This documentation describes the integration of MindsDB with [Google CLoud Storage](https://cloud.google.com/storage), 
an object storage service that offers industry-leading scalability, data availability, security, and performance.



## Table of Contents
1. [About](#about)
2. [Prerequisites](#prerequisites)
3. [Implementation](#implementation)
4. [Usage](#usage)
5. [Limitations](#limitations)
6. [TODO](#todo)
7. [Troubleshooting Guide](#troubleshooting-guide)


## About
**The purpose** of this integration is to simplify data movement from GCS to MindsDB, enabling users to leverage predictive analytics and machine learning models on their GCS-stored objects (files). 

**Key features** include automated handling of data imports, compatibility with MindsDB’s model training workflows, and support for structured data in various formats.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).
2. To connect Google Cloud Storage to MindsDB, install the required dependencies following [this instruction](/setup/self-hosted/docker#install-dependencies).

## Implementation

### Setup
1. Generate Google HMAC key id and secret from [here](https://console.cloud.google.com/storage/settings;tab=interoperability).
2. Create a [service account](https://console.cloud.google.com/iam-admin/serviceaccounts) with **Storage Admin** Role, and download the json key file.

### Connection
Establish a connection to your Google Cloud Storage bucket from MindsDB by executing the following SQL command:
```sql
CREATE DATABASE gcs_datasource
WITH
    engine = 'gcs',
    parameters = {
       "gcs_access_key_id": 'AQAXEQK89OX07YS34OP',
       "gcs_secret_access_key": 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
       "service_account_keys": '/path/to/service_account.json',
       "bucket": 'my-bucket',
       "prefix": 'al',
       "file_type": 'parquet'
    };
```

### Required Parameters
* `gcs_access_key_id`: The GCS HMAC access key that identifies the user or service account.
* `gcs_secret_access_key`: The GCS HMAC secret access key that identifies the user or service account.
* `bucket`: The name of the Google Cloud Storage bucket.

### Optional Parameters
* `service_account_keys`: The full path to the service account key file.
* `service_account_json`: The content of a JSON file defined by the `service_account_keys` parameter.
* `prefix`: A string to filter your objects by name.
* `file_type`: The type of files you want to include, e.g. `parquet`


## Usage
Retrieve data from a specified object (file) in the GCS bucket by providing the integration name and the object key:

```sql
SELECT *
FROM gcs_datasource.`my-file.csv`;
LIMIT 10;
```

<Tip>
Wrap the object key in backticks (\`) to avoid any issues parsing the SQL statements provided. This is especially important when the object key contains spaces, special characters or prefixes, such as `my-folder/my-file.csv`.
</Tip>

<Note>
The above examples utilize `gcs_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>


## Limitations
* This integration supports only `SELECT` query statement.
* At the moment, the supported file formats are CSV, TSV, JSON, and Parquet.


## TODO
- Avoid tables creation for SELECT queries.
- Avoid writing the table to a file after each query.
- Introduce more efficient way to get the column details.


## Troubleshooting Guide

<Warning>
`Database Connection Error`

* **Symptoms**: Failure to connect MindsDB with the Google Cloud Storage bucket.
* **Checklist**:
    1. Make sure the GCS bucket exists.
    2. Confirm that provided GCP HMAC credentials are correct. Try making a direct connection to the GCS bucket using the gcloud CLI.
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

---
title: Azure Blob Storage
sidebarTitle: Azure Blob Storage
---

This documentation describes the integration of MindsDB with [Azure Blob Storage]

## Prerequisites

Before proceeding, ensure that MindsDB is installed locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).

## Connection

Establish a connection to your Azure Blob Storage from MindsDB by executing the following SQL command:

```sql
CREATE DATABASE azureblob_datasource
WITH
    engine = 'azureblob',
    parameters = {
      "storage_account_name": "",
      "account_access_key": "",
      "container_name":""
    };
```

Required connection parameters include the following:

* `storage_account_name`: The Storage Account Name.
* `account_access_key`: The account access key.
* `container_name`: The container name.


## Usage

Retrieve data from a specified object (file) in the Azure Blob Storage by providing the integration name and the object key:

```sql
SELECT *
FROM azureblob_datasource.`my-file.csv`;
LIMIT 10;
```

<Tip>
Wrap the object key in backticks (\`) to avoid any issues parsing the SQL statements provided. This is especially important when the object key contains spaces, special characters or prefixes, such as `my-folder/my-file.csv`.

At the moment, the supported file formats are CSV, TSV, JSON, and Parquet. 
</Tip>

<Note>
The above examples utilize `azureblob_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>
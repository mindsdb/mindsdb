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
      "container_name":"",
      "connection_string":""
    };
```

Required connection parameters include the following:

* `storage_account_name`: The name of your storage account.
* `account_access_key`: The account access key, you can found it after entering storage account interface, under "Security & Networking" menu >> "Access Keys" >> pick one from 2 available key pairs.
* `container_name`: The name of your container.
* `connection_string`: The connection string of your account, you can found it after entering storage account interface, under "Security & Networking" menu >> "Access Keys" >> pick the connection string that under the same key as your account_access_key


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

## Known Issue
<Warning>
`Problem with the SSL CA cert` (you most likely get this error if your current OS is ubuntu / other linux distro)

* **Symptoms**: Error: Invalid Error: Fail to get a new connection for: https://⟨storage account name⟩.blob.core.windows.net/. Problem with the SSL CA cert (path? access rights?))

* **Solution**:
Current workaround is executing the following 2 statements as root:
mkdir -p /etc/pki/tls/certs
ln -s /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt

References:
https://duckdb.org/docs/extensions/azure.html#authentication
https://medium.com/datamindedbe/quacking-queries-in-the-azure-cloud-with-duckdb-14be50f6e141
</Warning>

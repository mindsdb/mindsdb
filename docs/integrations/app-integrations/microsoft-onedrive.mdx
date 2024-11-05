---
title: Microsoft One Drive
sidebarTitle: Microsoft One Drive
---

This documentation describes the integration of MindsDB with [Microsoft OneDrive](https://www.microsoft.com/en-us/microsoft-365/onedrive/online-cloud-storage), a cloud storage service that lets you back up, access, edit, share, and sync your files from any device.

## Prerequisites

1. Before proceeding, ensure that MindsDB is installed locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).
2. Register an application in the [Azure portal](https://portal.azure.com/).
    - Navigate to the [Azure Portal](https://portal.azure.com/#home) and sign in with your Microsoft account.
    - Locate the **Microsoft Entra ID** service and click on it.
    - Click on **App registrations** and then click on **New registration**.
    - Enter a name for your application and select the `Accounts in this organizational directory only` option for the **Supported account types** field.
    - Keep the **Redirect URI** field empty and click on **Register**.
    - Click on **API permissions** and then click on **Add a permission**.
    - Select **Microsoft Graph** and then click on **Delegated permissions**.
    - Search for the `Files.Read` permission and select it.
    - Click on **Add permissions**.
    - Request an administrator to grant consent for the above permissions. If you are the administrator, click on **Grant admin consent for [your organization]** and then click on **Yes**.
    - Copy the **Application (client) ID** and record it as the `client_id` parameter, and copy the **Directory (tenant) ID** and record it as the `tenant_id` parameter.
    - Click on **Certificates & secrets** and then click on **New client secret**.
    - Enter a description for your client secret and select an expiration period.
    - Click on **Add** and copy the generated client secret and record it as the `client_secret` parameter.
    - Click on **Authentication** and then click on **Add a platform**.
    - Select **Web** and enter URL where MindsDB has been deployed followed by `/verify-auth` in the **Redirect URIs** field. For example, if you are running MindsDB locally (on `http://localhost:47334`), enter `http://localhost:47334/verify-auth` in the **Redirect URIs** field.

## Connection

Establish a connection to Microsoft OneDrive from MindsDB by executing the following SQL command:

```sql
CREATE DATABASE one_drive_datasource
WITH
    engine = 'one_drive',
    parameters = {
        "client_id": "12345678-90ab-cdef-1234-567890abcdef",
        "client_secret": "abcd1234efgh5678ijkl9012mnop3456qrst7890uvwx",
        "tenant_id": "abcdef12-3456-7890-abcd-ef1234567890",
        "email": "user@example.com"
    };
```

<Note>
Note that sample parameter values are provided here for reference, and you should replace them with your connection parameters.
</Note>

Required connection parameters include the following:

* `client_id`: The client ID of the registered application.
* `client_secret`: The client secret of the registered application.
* `tenant_id`: The tenant ID of the registered application.
* `email`: The email address of the user account.

## Usage

Retrieve data from a specified file in Microsoft OneDrive by providing the integration name and the file name:

```sql
SELECT *
FROM one_drive_datasource.`my-file.csv`;
LIMIT 10;
```

<Tip>
Wrap the object key in backticks (\`) to avoid any issues parsing the SQL statements provided. This is especially important when the file name contains spaces, special characters or prefixes, such as `my-folder/my-file.csv`.

At the moment, the supported file formats are CSV, TSV, JSON, and Parquet. 
</Tip>

<Note>
The above examples utilize `one_drive_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

The special `files` table can be used to list the files available in Microsoft OneDrive:

```sql
SELECT *
FROM one_drive_datasource.files LIMIT 10
```

The content of files can also be retrieved by explicitly requesting the `content` column. This column is empty by default to avoid unnecessary data transfer:

```sql
SELECT path, content
FROM one_drive_datasource.files LIMIT 10
```

<Tip>
This table will return all objects regardless of the file format, however, only the supported file formats mentioned above can be queried.
</Tip>

## Troubleshooting Guide

<Warning>
`Database Connection Error`

* **Symptoms**: Failure to connect MindsDB with Microsoft OneDrive.
* **Checklist**:
    1. Ensure the `client_id`, `client_secret`, `tenant_id`, and `email` parameters are correctly provided.
    2. Ensure the registered application has the required permissions.
    3. Ensure the generated client secret is not expired.
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
---
title: Box
sidebarTitle: Box
---

This documentation describes the integration of MindsDB with [Box](https://www.box.com/), a cloud content management and file sharing service.

## Prerequisites

Before proceeding, ensure that MindsDB is installed locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).

## Connection

Establish a connection to your Box account from MindsDB by executing the following SQL command:

```sql
CREATE DATABASE box_datasource
WITH
    engine = 'box',
    parameters = {
      "client_id": "YOUR_CLIENT_ID",
      "client_secret": "YOUR_CLIENT_SECRET",
      "access_token": "YOUR_ACCESS_TOKEN",
      "refresh_token": "YOUR_REFRESH_TOKEN"
    };
```

<Note>
Note that sample parameter values are provided here for reference, and you should replace them with your connection parameters.
</Note>

Required connection parameters include the following:

* `client_id`: The client ID for the Box application.
* `client_secret`: The client secret for the Box application.

Optional connection parameters include the following:

* `access_token`: The access token for the Box application.
* `refresh_token`: The refresh token for the Box application.

## Usage

Retrieve data from a specified file in Box by providing the integration name and the file ID:

```sql
SELECT *
FROM box_datasource.`file_id`
LIMIT 10;
```

<Tip>
Wrap the file ID in backticks (\`) to avoid any issues parsing the SQL statements provided. This is especially important when the file ID contains spaces, special characters or prefixes.
</Tip>

<Note>
The above examples utilize `box_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

The special `files` table can be used to list all files available in the Box account:

```sql
SELECT *
FROM box_datasource.files LIMIT 10
```

The content of files can also be retrieved by explicitly requesting the `content` column. This column is empty by default to avoid unnecessary data transfer:

```sql
SELECT path, content
FROM box_datasource.files LIMIT 10
```

<Tip>
This table will return all files regardless of the file format, however, only the supported file formats mentioned above can be queried.
</Tip>

## Troubleshooting Guide

<Warning>
`Database Connection Error`

* **Symptoms**: Failure to connect MindsDB with the Box account.
* **Checklist**:
    1. Make sure the Box account exists.
    2. Confirm that provided Box credentials are correct. Try making a direct connection to the Box account using the Box SDK.
    3. Ensure a stable network between MindsDB and Box.
</Warning>

<Warning>
`SQL statement cannot be parsed by mindsdb_sql`

* **Symptoms**: SQL queries failing or not recognizing file IDs containing spaces, special characters or prefixes.
* **Checklist**:
    1. Ensure file IDs with spaces, special characters or prefixes are enclosed in backticks.
    2. Examples:
        * Incorrect: SELECT * FROM integration.file_id
        * Incorrect: SELECT * FROM integration.'file_id'
        * Correct: SELECT * FROM integration.\`file_id\`
</Warning>

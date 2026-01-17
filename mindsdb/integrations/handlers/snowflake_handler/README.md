---
title: Snowflake
sidebarTitle: Snowflake
---

This documentation describes the integration of MindsDB with [Snowflake](https://www.snowflake.com/en/), a cloud data warehouse used to store and analyze data.
The integration allows MindsDB to access data stored in the Snowflake database and enhance it with AI capabilities.

<Warning>
**Important!**

When querying data from Snowflake, MindsDB automatically converts column names to lower-case. To prevent this, users can provide an alias name as shown below.

**This update is introduced with the MindsDB version 25.3.4.1. It is not backward-compatible and has the following implications:**

1. Queries to Snowflake will return column names in lower-case from now on.
2. The models created with Snowflake as a data source must be recreated.

**How it works**

The below query presents how Snowflake columns are output when queried from MindsDB.

```sql
SELECT
       CC_NAME,                 -- converted to lower-case
       CC_CLASS AS `CC_CLASS`,  -- provided alias name in upper-case
       CC_EMPLOYEES,
       cc_employees
FROM snowflake_data.TPCDS_SF100TCL.CALL_CENTER;
```

Here is the output:

```sql
+--------------+----------+--------------+--------------+
| cc_name      | CC_CLASS | cc_employees | cc_employees |
+--------------+----------+--------------+--------------+
| NY Metro     | large    | 597159671    | 597159671    |
| Mid Atlantic | medium   | 944879074    | 944879074    |
+--------------+----------+--------------+--------------+
```

</Warning>

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).
2. To connect Snowflake to MindsDB, install the required dependencies following [this instruction](/setup/self-hosted/docker#install-dependencies).

## Connection

The Snowflake handler supports two authentication methods:

### 1. Password Authentication (Legacy)

Establish a connection using username and password:

```sql
CREATE DATABASE snowflake_datasource
WITH
    ENGINE = 'snowflake',
    PARAMETERS = {
        "account": "tvuibdy-vm85921",
        "user": "your_username",
        "password": "your_password",
        "database": "test_db",
        "auth_type": "password"
    };
```

### 2. Key Pair Authentication (Recommended)

Key pair authentication is more secure and is the recommended method by Snowflake:

```sql
CREATE DATABASE snowflake_datasource
WITH
    ENGINE = 'snowflake',
    PARAMETERS = {
        "account": "tvuibdy-vm85921",
        "user": "your_username",
        "private_key_path": "/path/to/your/private_key.pem",
        "database": "test_db",
        "auth_type": "key_pair"
    };
```

If the private key cannot be accesed from disk (for example when running MindsDB on Cloud), provide the PEM content directly:

```sql
CREATE DATABASE snowflake_datasource
WITH
    ENGINE = 'snowflake',
    PARAMETERS = {
        "account": "tvuibdy-vm85921",
        "user": "your_username",
        "private_key": "-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----",
        "database": "test_db",
        "auth_type": "key_pair"
    };
```

With encrypted private key (passphrase protected):

```sql
CREATE DATABASE snowflake_datasource
WITH
    ENGINE = 'snowflake',
    PARAMETERS = {
        "account": "tvuibdy-vm85921",
        "user": "your_username",
        "private_key_path": "/path/to/your/private_key.pem",
        "private_key_passphrase": "your_passphrase",
        "database": "test_db",
        "auth_type": "key_pair"
    };
```

### Connection Parameters

Required parameters:

* `account`: The Snowflake account identifier. This [guide](https://docs.snowflake.com/en/user-guide/admin-account-identifier) will help you find your account identifier.
* `user`: The username for the Snowflake account.
* `database`: The name of the Snowflake database to connect to.
* `auth_type`: The authentication type to use. Options: `"password"` or `"key_pair"`.

Authentication parameters (one method required):

* `password`: The password for the Snowflake account (password authentication).
* `private_key_path`: Path to the private key file for key pair authentication.
* `private_key`: PEM-formatted private key content for key pair authentication. Use when the key cannot be stored on disk.
* `private_key_passphrase`: Optional passphrase for encrypted private key (key pair authentication).

Optional parameters:

* `warehouse`: The Snowflake warehouse to use for running queries.
* `schema`: The database schema to use within the Snowflake database. Default is `PUBLIC`.
* `role`: The Snowflake role to use.

<Note>
For detailed instructions on setting up key pair authentication, please refer to [AUTHENTICATION.md](AUTHENTICATION.md) or the [Snowflake Key Pair Authentication documentation](https://docs.snowflake.com/en/user-guide/key-pair-auth.html).
</Note>

## Usage

Retrieve data from a specified table by providing the integration name, schema, and table name:

```sql
SELECT *
FROM snowflake_datasource.schema_name.table_name
LIMIT 10;
```

Run Snowflake SQL queries directly on the connected Snowflake database:

```sql
SELECT * FROM snowflake_datasource (

    --Native Query Goes Here
    SELECT
        employee_table.* EXCLUDE department_id,
        department_table.* RENAME department_name AS department
    FROM employee_table INNER JOIN department_table
        ON employee_table.department_id = department_table.department_id
    ORDER BY department, last_name, first_name;

);
```

<Note>
The above examples utilize `snowflake_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

## Troubleshooting Guide

<Warning>
`Database Connection Error`

* **Symptoms**: Failure to connect MindsDB with the Snowflake account.
* **Checklist**:
    1. Make sure the Snowflake is active.
    2. Confirm that account, user, password and database are correct. Try a direct Snowflake connection using a client like DBeaver.
    3. Ensure a stable network between MindsDB and Snowflake.
</Warning>

<Warning>
`SQL statement cannot be parsed by mindsdb_sql`

* **Symptoms**: SQL queries failing or not recognizing table names containing spaces or special characters.
* **Checklist**:
    1. Ensure table names with spaces or special characters are enclosed in backticks.
    2. Examples:
        * Incorrect: SELECT * FROM integration.travel data
        * Incorrect: SELECT * FROM integration.'travel data'
        * Correct: SELECT * FROM integration.\`travel data\`
</Warning>

This [troubleshooting guide](https://community.snowflake.com/s/article/Snowflake-Client-Connectivity-Troubleshooting) provided by Snowflake might also be helpful.

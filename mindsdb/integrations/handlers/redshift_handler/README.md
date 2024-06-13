---
title: Amazon Redshift
sidebarTitle: Amazon Redshift
---

This documentation describes the integration of MindsDB with [Amazon Redshift](https://docs.aws.amazon.com/redshift/latest/mgmt/welcome.html), a fully managed, petabyte-scale data warehouse service in the cloud. You can start with just a few hundred gigabytes of data and scale to a petabyte or more, enabling you to use your data to acquire new insights for your business and customers.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).
2. To connect Redshift to MindsDB, install the required dependencies following [this instruction](/setup/self-hosted/docker#install-dependencies).

## Connection

Establish a connection to your Redshift database from MindsDB by executing the following SQL command:

```sql
CREATE DATABASE redshift_datasource
WITH
  engine = 'redshift',
  parameters = {
    "host": "examplecluster.abc123xyz789.us-west-1.redshift.amazonaws.com",
    "port": 5439,
    "database": "example_db",
    "user": "awsuser",
    "password": "my_password"
  };
```

Required connection parameters include the following:

* `host`: The host name or IP address of the Redshift cluster.
* `port`: The port to use when connecting with the Redshift cluster.
* `database`: The database name to use when connecting with the Redshift cluster.
* `user`: The username to authenticate the user with the Redshift cluster.
* `password`: The password to authenticate the user with the Redshift cluster.

Optional connection parameters include the following:

* `schema`: The database schema to use. Default is public.
* `sslmode`: The SSL mode for the connection.

## Usage

Retrieve data from a specified table by providing the integration name, schema, and table name:

```sql
SELECT *
FROM redshift_datasource.schema_name.table_name
LIMIT 10;
```

Run Amazon Redshift SQL queries directly on the connected Redshift database:

```sql
SELECT * FROM redshift_datasource (

    --Native Query Goes Here
    WITH VENUECOPY AS (SELECT * FROM VENUE)
      SELECT * FROM VENUECOPY ORDER BY 1 LIMIT 10;

);
```

<Note>
The above examples utilize `redshift_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>
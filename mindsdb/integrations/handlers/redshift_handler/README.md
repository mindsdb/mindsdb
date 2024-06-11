---
title: Amazon Redshift
sidebarTitle: Amazon Redshift
---

This documentation describes the integration of MindsDB with [Amazon Redshift](https://docs.aws.amazon.com/redshift/latest/mgmt/welcome.html), a fully managed, petabyte-scale data warehouse service in the cloud. You can start with just a few hundred gigabytes of data and scale to a petabyte or more, enabling you to use your data to acquire new insights for your business and customers.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).
2. To connect Redshift to MindsDB, install the required dependencies following [this instruction](/setup/self-hosted/docker#install-dependencies).

<Tip>
Please note that, if you are using Docker to run MindsDB, before installing the dependencies for this integration as per the instructions given above, it is currently necessary to install Git in the container. To do this, run the following commands:

Start an interactive shell in the container:
```bash
docker exec -it mindsdb_container sh
```
If you haven't specified a name when spinning up the MindsDB container with `docker run`, you can find it by running `docker ps`.

Install Git:
```bash
apt-get -y update
apt-get -y install git
``` 

The need to perform this step will be removed in future versions of MindsDB.
</Tip>

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
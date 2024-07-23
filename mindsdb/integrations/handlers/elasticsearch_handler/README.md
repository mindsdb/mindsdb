---
title: Elasticsearch
sidebarTitle: Elasticsearch
---

This documentation describes the integration of MindsDB with [Elasticsearch](https://www.elastic.co/what-is/elasticsearch), a distributed, free and open search and analytics engine for all types of data, including textual, numerical, geospatial, structured, and unstructured.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).
2. To connect Elasticsearch to MindsDB, install the required dependencies following [this instruction](/setup/self-hosted/docker#install-dependencies).

## Connection

Establish a connection to Elasticsearch from MindsDB by executing the following SQL command:

```sql
CREATE DATABASE elasticsearch_datasource
WITH
  engine = 'elasticsearch',
  parameters = {
    "hosts": "127.0.0.1:9200",
    "user": "user",
    "password": "password"
  };
```

Required connection parameters include the following (at least one of these parameters should be provided):

- `hosts`: The host name(s) or IP address(es) of the Elasticsearch server(s). If multiple host name(s) or IP address(es) exist, they should be separated by commas, e.g., `host1:port1, host2:port2`. If this parameter is not provided, `cloud_id` should be.
- `cloud_id`: The unique ID to your hosted Elasticsearch deployment on Elastic Cloud. If this parameter is not provided, `hosts` should be.

Optional connection parameters include the following:

- `user`: The username to connect to the Elasticsearch server with.
- `password`: The password to authenticate the user with the Elasticsearch server.
- `api_key`: The API key for authentication with the Elasticsearch server.

## Usage

Retrieve data from a specified index by providing the integration name and index name:

```sql
SELECT *
FROM elasticsearch_datasource.my_index
LIMIT 10;
```

<Note>
The above examples utilize `elasticsearch_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

<Tip>
At the moment, the Elasticsearch SQL API has certain limitations that have an impact on the queries that can be issued via MindsDB. The most notable of these limitations are listed below:
1. Only `SELECT` queries are supported at the moment.
2. Array fields are not supported.
3. Nested fields cannot be queried directly. However, they can be accessed using the `.` operator.

For a detailed guide on the limitations of the Elasticsearch SQL API, refer to the [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-limitations.html).
</Tip>

## Troubleshooting Guide

<Warning>
`Database Connection Error`

- **Symptoms**: Failure to connect MindsDB with the Elasticsearch server.
- **Checklist**: 1. Make sure the Elasticsearch server is active. 2. Confirm that server, cloud ID and credentials are correct. 3. Ensure a stable network between MindsDB and Elasticsearch.
  </Warning>

<Warning>
`Transport Error` or `Request Error`

- **Symptoms**: Errors related to the issuing of unsupported queries to Elasticsearch.
- **Checklist**: 1. Ensure the query is a `SELECT` query. 2. Avoid querying array fields. 3. Access nested fields using the `.` operator. 4. Refer to the [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-limitations.html) for more information if needed.
  </Warning>

<Warning>
`SQL statement cannot be parsed by mindsdb_sql`

- **Symptoms**: SQL queries failing or not recognizing index names containing special characters.
- **Checklist**: 1. Ensure table names with special characters are enclosed in backticks. 2. Examples:
  _ Incorrect: SELECT _ FROM integration.travel-data
  _ Incorrect: SELECT _ FROM integration.'travel-data'
  _ Correct: SELECT _ FROM integration.\`travel-data\`
  </Warning>

This [troubleshooting guide](https://www.elastic.co/guide/en/elasticsearch/reference/current/troubleshooting.html) provided by Elasticsearch might also be helpful.

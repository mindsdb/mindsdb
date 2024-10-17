---
title: ElasticSearch
sidebarTitle: ElasticSearch
---

This documentation describes the integration of MindsDB with [ElasticSearch](https://www.elastic.co/), a distributed, multitenant-capable full-text search engine with an HTTP web interface and schema-free JSON documents..
The integration allows MindsDB to access data from ElasticSearch and enhance ElasticSearch with AI capabilities.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To connect ElasticSearch to MindsDB, install the required dependencies following [this instruction](/setup/self-hosted/docker#install-dependencies).
3. Install or ensure access to ElasticSearch.

## Connection

Establish a connection to ElasticSearch from MindsDB by executing the following SQL command and providing its [handler name](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/elasticsearch_handler) as an engine.

```sql
CREATE DATABASE elasticsearch_datasource
WITH ENGINE = 'elasticsearch',
PARAMETERS={
   'cloud_id': 'xyz',                               -- optional, if hosts are provided
   'hosts': 'https://xyz.xyz.gcp.cloud.es.io:123',  -- optional, if cloud_id is provided
   'api_key': 'xyz',                                -- optional, if user and password are provided
   'user': 'elastic',                               -- optional, if api_key is provided
   'password': 'xyz'                                -- optional, if api_key is provided
};
```

The connection parameters include the following:

* `cloud_id`: The Cloud ID provided with the ElasticSearch deployment. Required only when `hosts` is not provided.
* `hosts`: The ElasticSearch endpoint provided with the ElasticSearch deployment. Required only when `cloud_id` is not provided.
* `api_key`: The API key that you generated for the ElasticSearch deployment. Required only when `user` and `password` are not provided.
* `user` and `password`: The user and password used to authenticate. Required only when `api_key` is not provided.

<Tip>
If you want to connect to the local instance of ElasticSearch, use the below statement:

```sql
CREATE DATABASE elasticsearch_datasource
WITH ENGINE = 'elasticsearch',
PARAMETERS = {
   "hosts": "127.0.0.1:9200",
   "user": "user",
   "password": "password"
};
```

Required connection parameters include the following (at least one of these parameters should be provided):

* `hosts`: The IP address and port where ElasticSearch is deployed.
* `user`: The user used to autheticate access.
* `password`: The password used to autheticate access.
</Tip>

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

* **Symptoms**: Failure to connect MindsDB with the Elasticsearch server.
* **Checklist**:
    1. Make sure the Elasticsearch server is active.
    2. Confirm that server, cloud ID and credentials are correct.
    3. Ensure a stable network between MindsDB and Elasticsearch.
</Warning>

<Warning>
`Transport Error` or `Request Error`

* **Symptoms**: Errors related to the issuing of unsupported queries to Elasticsearch.
* **Checklist**:
    1. Ensure the query is a `SELECT` query.
    2. Avoid querying array fields.
    3. Access nested fields using the `.` operator.
    4. Refer to the [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-limitations.html) for more information if needed.
</Warning>

<Warning>
`SQL statement cannot be parsed by mindsdb_sql`

* **Symptoms**: SQL queries failing or not recognizing index names containing special characters.
* **Checklist**:
    1. Ensure table names with special characters are enclosed in backticks.
    2. Examples:
        * Incorrect: SELECT * FROM integration.travel-data
        * Incorrect: SELECT * FROM integration.'travel-data'
        * Correct: SELECT * FROM integration.\`travel-data\`
</Warning>

This [troubleshooting guide](https://www.elastic.co/guide/en/elasticsearch/reference/current/troubleshooting.html) provided by Elasticsearch might also be helpful.

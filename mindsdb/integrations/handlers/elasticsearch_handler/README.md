# Elasticsearch Handler

This is the implementation of the Elasticsearch handler for MindsDB.

## Elasticsearch
Elasticsearch is a distributed, free and open search and analytics engine for all types of data, including textual, numerical, geospatial, structured, and unstructured. Elasticsearch is built on Apache Lucene and was first released in 2010 by Elasticsearch N.V. (now known as Elastic).
<br>
https://www.elastic.co/what-is/elasticsearch

## Implementation
This handler was implemented using the `elasticsearch` library, the Python Elasticsearch client.

The required arguments to establish a connection are,
* `hosts`: the host name(s) or IP address(es) of the Elasticsearch server(s). If multiple host name(s) or IP address(es) exist, they should be separated by commas. This parameter is optional, but it should be provided if cloud_id is not.
* `cloud_id`: the unique ID to your hosted Elasticsearch cluster on Elasticsearch Service. This parameter is optional, but it should be provided if hosts is not.
* `username`: the username used to authenticate with the Elasticsearch server. This parameter is optional.
* `password`: the password to authenticate the user with the Elasticsearch server. This parameter is optional.

## Usage
In order to make use of this handler and connect to an Elasticsearch server in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE elasticsearch_datasource
WITH
engine='elasticsearch',
parameters={
    "hosts": "localhost:9200"
};
~~~~

Now, you can use this established connection to query your index as follows,
~~~~sql
SELECT * FROM elasticsearch_datasource.example_index
~~~~

There are certain limitations that need to be taken into account when issuing queries to Elasticsearch. Given below is a detailed guide on what these limitations are,
https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-limitations.html
# Airtable Handler

This is the implementation of the Airtable handler for MindsDB.

## Airtable

In short, Airtable is a platform that makes it easy to build powerful, custom applications. These tools can streamline just about any process, workflow, or project—and best of all, you can build them without ever learning to write a single line of code. (Spoiler alert: that’s what low-code/no-code is all about.) Our customers use Airtable to do everything from tracking job interviews to managing large-scale video production, and thousands of companies use Airtable to run their most important business processes every day.
https://www.airtable.com/guides/start/what-is-airtable

## Implementation

This handler was implemented using the Airtable Python SDK (`pyairtable`).

The documentation for the Airtable API is available here,
<br>
https://pyairtable.readthedocs.io/en/stable/

The required arguments to establish a connection are,

- `base_id`: the Airtable base ID
- `table_name`: the Airtable table name
- `access_token`: the API key for the Airtable API

## Usage

In order to make use of this handler and connect to an Access database in MindsDB, the following syntax can be used,

```sql
CREATE DATABASE airtable_datasource
WITH
engine='airtable',
parameters={
    "base_id": "dqweqweqrwwqq",
    "table_name": "iris",
    "access_token": "knlsndlknslk"
};
```

Now, you can use this established connection to query your table as follows,

```sql
SELECT * FROM airtable_datasource.example_tbl
```

### Example

```sql

CREATE DATABASE airtable_mindsdb_features_table
WITH ENGINE='airtable',
parameters={
  "base_id": "appE1gKsjXspnE0ssds",
  "table_name": "mindsdb_features",
  "access_token": "patcPF6PshamQshji.86aea51b6cdaa33e0f0d0782d1d461e6d7f3s35d8946f0d5bab741e560"
};

SELECT Feature, Description from airtable_mindsdb_features_table.mindsdb_features

INSERT INTO airtable_mindsdb_features_table.mindsdb_features
    (SELECT * from airtable_mindsdb_features_table.mindsdb_features LIMIT 2);

```

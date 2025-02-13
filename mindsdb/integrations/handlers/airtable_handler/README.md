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

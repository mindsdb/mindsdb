# BigQuery Handler

This is the implementation of the BigQuery handler for MindsDB.

## Google BigQuery

BigQuery is a fully-managed, serverless data warehouse that enables scalable analysis over petabytes of data. It is a Platform as a Service that supports querying using ANSI SQL. For more info check https://cloud.google.com/bigquery/.

## Implementation

This handler was implemented using the python `google-cloud-bigquery` library.

The required arguments to establish a connection are:

* `project_id`:  A globally unique identifier for your project
* `service_account_keys`: Full path to the service account key file

For more info about creating and managing the service account key visit  https://cloud.google.com/iam/docs/creating-managing-service-account-keys.

## Usage

In order to make use of this handler and connect to a BigQuery use the following syntax:

```sql
CREATE DATABASE bqdataset
WITH ENGINE = "bigquery",
PARAMETERS = {
   "project_id": "tough-future-332513",
   "service_account_keys": "/home/user/MyProjects/tough-future-332513.json"
   }
```

Now, you can use this established connection to query your dataset as follows:

```sql
SELECT * FROM bgdataset.dataset.table LIMIT 10;
```

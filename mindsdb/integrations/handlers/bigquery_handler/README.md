# BigQuery Handler

This is the implementation of the BigQuery handler for MindsDB.

## Google BigQuery

BigQuery is a fully-managed, serverless data warehouse that enables scalable analysis over petabytes of data. It is a Platform as a Service that supports querying using ANSI SQL. For more info check https://cloud.google.com/bigquery/.

## Implementation

This handler was implemented using the python `google-cloud-bigquery` library.

The required arguments to establish a connection are:

* `project_id`: required, a globally unique identifier for your project
* `dataset`: required, selected dataset in for your project
* `service_account_keys`: optional, full path to the service account key file
* `service_account_json`: optional, service account key content as json
One of service_account_keys or service_account_json has to be chosen.

For more info about creating and managing the service account key visit  https://cloud.google.com/iam/docs/creating-managing-service-account-keys.

## Usage

In order to make use of this handler and connect to a BigQuery use the following syntax:

```sql
CREATE DATABASE bqdataset
WITH ENGINE = "bigquery",
PARAMETERS = {
   "project_id": "tough-future-332513",
   "dataset": "mydataset",
   "service_account_keys": "/home/user/MyProjects/tough-future-332513.json"
   }
```

Or using service_account_json
```sql
CREATE DATABASE bqdataset
WITH ENGINE = "bigquery",
PARAMETERS = {
   "project_id": "tough-future-332513",
   "dataset": "mydataset",
   "service_account_json": {
        "type": "service_account",
        "project_id": "bgtest-1111",
        "private_key_id": "aaaaaaaaaa",
        "private_key": "---------BIG STRING WITH KEY-------\n",
        "client_email": "testbigquery@bgtest-11111.iam.gserviceaccount.com",
        "client_id": "1111111111111",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/testbigquery%40bgtest-11111.iam.gserviceaccount.com"
        }
   }
```


Now, you can use this established connection to query your dataset as follows:

```sql
SELECT * FROM bgdataset.table LIMIT 10;
```

Or selecting from other dataset in the same project:

```sql
SELECT * FROM bgdataset.dataset.table LIMIT 10;
```

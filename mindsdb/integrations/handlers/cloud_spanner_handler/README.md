# Cloud SpannerHandler
This is the implementation of the Cloud Spanner handler for MindsDB.

## Cloud Spanner
Cloud Spanner is a fully managed, mission-critical, relational database service that offers transactional consistency at global scale, automatic, synchronous replication for high availability.
Cloud spanner supports two SQL dialects: GoogleSQL (ANSI 2011 with extensions) and PostgreSQL.

## Implementation
This handler was implemented using the `google-cloud-spanner` python client library.

The arguments to establish a connection are:

* `instance_id`: the instance identifier.
* `database_id`: the datbase identifier.
* `project`: the identifier of the project that owns the resources.
* `credentials`: a stringified GCP service account key JSON.


## Usage
In order to make use of this handler and connect to a Cloud Spanner database in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE cloud_spanner_datasource
WITH
engine='cloud_spanner',
parameters={
    "instance_id":"my-instance",
    "database_id":"example-id",
    "project":"my-project",
    "credentials":"{...}"
};
```

Now, you can use this established connection to query your database as follows:
```sql
SELECT * FROM cloud_spanner_datasource.my_table;
```
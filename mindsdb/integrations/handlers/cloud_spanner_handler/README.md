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

> **NOTE** : Cloud Spanner supports PostgreSQL syntax and also Google's GoogleSQL dialect. But, not all PostgresSQL dialect features are supported. Find the list of such features below.
> - Change streams
> - GoogleSQL `JSON` type (PostgreSQL-dialect databases support the PostgreSQL JSONB type.)
> - `SELECT DISTINCT` (`DISTINCT` is supported in aggregate functions.)
> - `FULL JOIN` with `USING`
> - Query optimizer versioning
> - Optimizer statistics package versioning
> - `ORDER BY`, `LIMIT`, and `OFFSET` in `UNION`,`EXCEPT`, or `DISTINCT` statements
> - The following columns in `SPANNER_SYS` statistics tables:
>      - Transaction statistics: `TOTAL_LATENCY_DISTRIBUTION` and `OPERATIONS_BY_TABLE`
>      - Query statistics: `LATENCY_DISTRIBUTION`
>      - Lock Statistics: `SAMPLE_LOCK_REQUESTS`

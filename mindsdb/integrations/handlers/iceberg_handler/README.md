# Apache Iceberg Handler

This is the implementation of the Apache Iceberg handler for MindsDB.

## Iceberg
Iceberg is a high-performance format for huge analytic tables. Iceberg brings the reliability and simplicity of SQL tables to big data, while making it possible for engines like Spark, Trino, Flink, Presto, Hive and Impala to safely work with the same tables, at the same time. Iceberg tables support ACID transactions, schema evolution, and many other features.

## Implementation
This handler was implemented using the `pyiceberg` library, which allows you to access Iceberg tables from Python, without the use of a JVM.

The required arguments to establish a connection are,
* `name`: The name of the catalog
* `password`: The password of the PostgreSQL database where the metadata is stored
* `database`: The name of the PostgreSQL database where the metadata is stored
* `table`: The name of the table you wish to access
* `user`: The username of the PostgreSQL database where the metadata is stored
* `namespace`: The namespace of the table catalog.

You may need to establish a connection to an S3 bucket where the `metadata.json` files for the catalog are located. If the PostgreSQL database that you are connecting to already contains iceberg schemas, then this is not necessary. More information can be found in the [pyiceberg documentation](https://py.iceberg.apache.org/api/).

## Usage
In order to make use of this handler and connect to Iceberg in MindsDB, the following syntax can be used,
```sql
CREATE DATABASE iceberg_datasource
WITH
engine='iceberg',
parameters={
    'name': 'iceberg_catalog',
    'password': 'password',
    'database': 'iceberg_db',
    'namespace': 'iceberg_namespace',
    'user': 'postgres',
    'table': 'iceberg_table'
}
```

Now, you can use this established connection to query your database as follows,
```sql
SELECT * FROM iceberg_datasource.iceberg_table;
```

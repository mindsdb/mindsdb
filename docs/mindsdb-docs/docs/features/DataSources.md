---
id: data-sources
title: Data sources
---
Our goal is to make it very simple to ingest and prepare data that can be feed into MindsDB as **DataSources**. Here are the basics:

mindsdb.learn **from_data** argument can be any of the following:
 
 * **file**: can be a path in the local system, stringio object or a url to a file, supported types are (csv, json, xlsx, xls).
 * **data frame**: A [pandas DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html), pandas is one of the most useful data preparation libraries out there, so it makes sense that we support this.
 * **MindsDB data source**: MindsDB has a special class for datasources which lends itself for simple data ingestion and preparation, as well as to combine various datasources to learn from.

*Note: By default, only the FileDS data source is available, to make sure the other data sources work, install mindsdb via `pip install mindsdb[extra_data_sources]`, or replace `mindsdb` to `mindsdb[extra_data_sources]` in whatever install instructions are given for your platform*

## MindsDB data source:

MindsDB datasource is an enriched version of a pandas dataframe so all methods in the pandas dataframe also apply to MindsDB,
However, the DataSource class provides a way to implement data loading transformations and cleaning of data.

Some special implementations of datasources that already do cleaning and various tasks:

You can learn about them in [mindsdb repository](https://github.com/mindsdb/mindsdb_native/tree/stable/mindsdb_native/libs/data_sources).

### FileDS

An important one is the one MindsDB uses to work with files.

```python
from mindsdb import FileDS

ds = FileDS(file, clean_rows = True, custom_parser = None)


# Now you can pass this DataSource to MindsDB
mdb.learn(
    from_data=ds,
    predict='<what column you want to predict>', # the column we want to learn to predict given all the data in the file
    model_name='<the name you want to give to this model>' # the name of this model
)

```
FileDS arguments are:

* ***file*** can be a path in the local system, stringio object or a url to a file, supported types are (csv, json, xlsx, xls).
* ***clean_header*** (default=True) clean column names, so that they don't have special characters or white spaces.
* ***clean_rows*** (default=True) Goes row by row making sure that nulls are nulls and that no corrupt data exists in them, it also cleans empty rows
* ***custom_parser*** (default=None) special function to extract the actual table structure from the file in case you need special transformations.


### S3DS

Use an [s3](https://aws.amazon.com/s3/) object as the input.

```python
from mindsdb import Predictor, S3DS

s3_ds = S3DS(bucket_name='mindsdb-example-data', file_path='home_rentals.csv')

Predictor(name='test').learn(from_data=s3_ds, to_predict='target')
```

This data source also takes the optional initialization/constructor arguments:

* access_key -- The AWS access key [string]
* secret_key -- The AWS secret key [string]
* use_default_credentails -- Whether or not to use the default credentials on your machine (e.g. the credentials inside `~/.aws` or the role of the machine), defaults to `False` [boolean]


### MySqlDS

Used to select data from [MySQL](https://www.mysql.com/) or [MariaDB](https://mariadb.org/).

```python
from mindsdb import Predictor, MySqlDS

mysql_ds = MySqlDS(query="SELECT COUNT(*), SUM(spend), SUM(is_click), website FROM advertising_data", user="my_user", password="my very secret password", database="main_db", table='advertising_data', port=3306)

Predictor(name='test').learn(from_data=mysql_ds, to_predict='target')
```

This data source also takes the optional initialization/constructor arguments:

* query -- Query which extracts the data, mutually exclusive with `table` [string]
* host -- The host of the database (e.g. localhost or some ip) [string]
* user -- User for the database [string]
* password -- Password for the user [string]
* database -- Database to use [string]
* port -- Port of the database [integer]
* table -- Table from which to select all the data, mutually exclusive with `query` [string]

### PostgresDS

Used to select data from [PostgreSQL](https://www.postgresql.org/).

```python
from mindsdb import Predictor, PostgresDS

pg_ds = PostgresDS(query="SELECT COUNT(*), SUM(spend), SUM(is_click), website FROM advertising_data", user="my_user", password="my very secret password", database="main_db")

Predictor(name='test').learn(from_data=pg_ds, to_predict='target')
```

This data source also takes the optional initialization/constructor arguments:

* query -- Query which extracts the data, mutually exclusive with `table` [string]
* host -- The host of the database (e.g. localhost or some ip) [string]
* user -- User for the database [string]
* password -- Password for the user [string]
* database -- Database to use [string]
* port -- Port of the database [integer]
* table -- Table from which to select all the data, mutually exclusive with `query` [string]


### ClickhouseDS

Used to select data from [ClickHouse](https://clickhouse.tech/).

```python
from mindsdb import Predictor, ClickhouseDS

ch_ds = ClickhouseDS(query="SELECT COUNT(*), SUM(spend), SUM(is_click), website FROM default.advertising_data",
                     user="my_user", password="my very secret password")

Predictor(name='test').learn(from_data=ch_ds, to_predict='target')
```

This data source also take the optional initialization/constructor arguments:

* query -- Query with which to extract the data, mutually exclusive with `table` [string]
* host -- The host of the database (e.g. localhost or some ip) [string]
* user -- User for the database [string]
* password -- Password for the user [string]
* port -- Port of the database [integer]
* table -- Table from which to select all the data, mutually exclusive with `query` [string]

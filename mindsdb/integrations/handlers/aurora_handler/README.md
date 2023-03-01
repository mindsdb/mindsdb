# Amazon Aurora Handler

This is the implementation of the Amazon Aurora handler for MindsDB.

## Amazon Aurora
Amazon Aurora (Aurora) is a fully managed relational database engine that's compatible with MySQL and PostgreSQL. 
https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/CHAP_AuroraOverview.html

## Implementation
This handler was implemented using the existing MindsDB handlers for MySQL and PostgreSQL.

The required arguments to establish a connection are,
* `host`: the host name or IP address of the Amazon Aurora DB cluster.
* `port`: the TCP/IP port of the Amazon Aurora DB cluster.
* `user`: the username used to authenticate with the Amazon Aurora DB cluster.
* `password`: the password to authenticate the user with the Amazon Aurora DB cluster.
* `database`: the database name to use when connecting with the Amazon Aurora DB cluster.

There are several optional arguments that can be used as well,
* `db_engine`: the database engine of the Amazon Aurora DB cluster. This can take one of two values: 'mysql' or 'postgresql'. This parameter is optional, but if it is not provided, `aws_access_key_id` and `aws_secret_access_key` parameters must be provided.
* `aws_access_key_id`: the access key for the AWS account. This parameter is optional and is only required to be provided if the `db_engine` parameter is not provided.
* `aws_secret_access_key`: the secret key for the AWS account. This parameter is optional and is only required to be provided if the `db_engine` parameter is not provided.

## Usage
In order to make use of this handler and connect to an Amazon Aurora MySQL DB Cluster in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE aurora_mysql_datasource
WITH ENGINE = 'aurora',
PARAMETERS = {
    "db_engine": "mysql",
    "host": "mysqlcluster.cluster-123456789012.us-east-1.rds.amazonaws.com",
    "port": 3306,
    "user": "admin",
    "password": "password",
    "database": "example_db"
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM aurora_mysql_datasource.example_tbl
~~~~

Similar commands can be used to establish a connection and query Amazon Aurora PostgreSQL DB Cluster,
~~~~sql
CREATE DATABASE aurora_postgres_datasource
WITH ENGINE = 'aurora',
PARAMETERS = {
    "db_engine": "postgresql",
    "host": "postgresmycluster.cluster-123456789012.us-east-1.rds.amazonaws.com",
    "port": 5432,
    "user": "postgres",
    "password": "password",
    "database": "example_db "
};

SELECT * FROM aurora_postgres_datasource.example_tbl
~~~~
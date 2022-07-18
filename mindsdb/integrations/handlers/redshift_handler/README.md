# Redshift Handler

This is the implementation of the Redshift handler for MindsDB.

## Redshift
Amazon Redshift is a fully managed, petabyte-scale data warehouse service in the cloud. You can start with just a few hundred gigabytes of data and scale to a petabyte or more. This enables you to use your data to acquire new insights for your business and customers.
https://docs.aws.amazon.com/redshift/latest/mgmt/welcome.html

## Implementation
This handler was implemented using the `redshift_connector` library that is provided by Amazon Web Services.

The required arguments to establish a connection are,
* `host`: the host name or IP address of the Redshift cluster
* `port`: the port to use when connecting with the Redshift cluster
* `database`: the database name to use when connecting with the Redshift cluster
* `user`: the user to authenticate the user with the Redshift cluster
* `password`: the password to authenticate the user with the Redshift cluster

## Usage
Before attempting to connect to a Redshift cluster using MindsDB, ensure that it accepts incoming connections. The following can be used as a guideline to accomplish this,
<br>
https://aws.amazon.com/premiumsupport/knowledge-center/cannot-connect-redshift-cluster/

In order to make use of this handler and connect to a Redshift cluster in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE redshift_datasource
WITH
engine='redshift',
parameters={
    "host": "examplecluster.abc123xyz789.us-west-1.redshift.amazonaws.com",
    "port": 5439,
    "database": "example_db",
    "user": "awsuser",
    "password": "my_password"
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM sqlite_datasource.example_tbl
~~~~
# Replicate Handler

This is the implementation of the Replicate handler for MindsDB.

## Replicate
Replicate is a platform and tool that aims to make it easier to work with machine learning models. It provides a library of open-source machine learning models that can be run in the cloud. Replicate allows users to deploy their own machine learning models at scale by providing infrastructure and automatic generation of API servers.


## Implementation
This handler was implemented using the `replicate` library that is provided by Amazon Web Services.

The required arguments to establish a connection are,
* `host`: the host name or IP address of the Replicate cluster
* `port`: the port to use when connecting with the Replicate cluster
* `database`: the database name to use when connecting with the Replicate cluster
* `user`: the user to authenticate the user with the Replicate cluster
* `password`: the password to authenticate the user with the Replicate cluster

## Usage
Before attempting to connect to a Replicate cluster using MindsDB, ensure that it accepts incoming connections. The following can be used as a guideline to accomplish this,
<br>
https://aws.amazon.com/premiumsupport/knowledge-center/cannot-connect-Replicate-cluster/

In order to make use of this handler and connect to a Replicate cluster in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE Replicate_datasource
WITH
engine='Replicate',
parameters={
    "host": "examplecluster.abc123xyz789.us-west-1.Replicate.amazonaws.com",
    "port": 5439,
    "database": "example_db",
    "user": "awsuser",
    "password": "my_password"
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM Replicate_datasource.example_tbl
~~~~
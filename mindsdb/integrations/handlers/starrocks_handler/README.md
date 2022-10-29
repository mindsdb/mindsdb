# StarRocks Handler

This is the implementation of the StarRocks Handler for MindsDB.

## StarRocks
StarRocks is the next-generation data platform designed to make data-intensive real-time analytics fast and easy. It delivers query speeds 5 to 10 times faster than other popular solutions. StarRocks can perform real-time analytics well while updating historical records. It can also enhance real-time analytics with historical data from data lakes easily. With StarRocks, you can get rid of the de-normalized tables and get the best performance and flexibility.


## Implementation

This handler was implemented by extending mysql connector.

The required arguments to establish a connection are:

* `host`: the host name of the StarRocks connection 
* `port`: the port to use when connecting 
* `user`: the user to authenticate 
* `password`: the password to authenticate the user
* `database`: database name

## Usage

In order to make use of this handler and connect to a StarRocks server in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE starrocks_datasource
WITH ENGINE = "starrocks",
PARAMETERS = { 
  "user": "root",
  "password": "<p455W0rd>",
  "host": "localhost",
  "port": 9030,
  "database": "starrocks  "
}
```

Now, you can use this established connection to query your database as follows:

```sql
SELECT * FROM starrocks_datasource.loveU LIMIT 10;
```
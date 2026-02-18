# Lindorm Handler

This is the implementation of the Lindrom for MindsDB.

## Lindorm
Lindorm is Alibaba Cloud's data governance platform that helps users protect sensitive data and ensure regulatory compliance. It provides discovery, classification, access control, and monitoring capabilities to help customers find, understand, and control their data across cloud services and on-premises systems.

## Implementation

This handler uses `phoenixdb` python library connect to a Lindorm database instance. The handler is implemented in `lindorm_handler.py` and the tests are in `test_lindorm_handler.py`.

The required arguments to establish a connection are:

* `url`: the url of database while connecting 
* `autocommit`: Whether or not to autocommit changes
* `lindorm_user`: Username for authentication
* `lindorm_password`: Password for authentication


## Usage

replace your lindorm database url, username and password in the following command


```sql
CREATE DATABASE lindorm_datasource
WITH ENGINE = 'lindorm',
PARAMETERS = {
  "url": "<lindormdb_url>",
  "autocommit": True,
  "lindorm_user":"root" , 
  "lindorm_password": "UWtx4ebU"
};
```
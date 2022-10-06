
# Connecting to MariaDB SkySQL Service

## Steps on [MariaDB SkySQL](https://cloud.MariaDB.com/)

### 1. Safelisting MindsDB on MariaDB SkySQL Service

Once you have identified the service to be enabled with MindsDB, make sure to allowlist the following IP addresses, do this by clicking on the cog icon (service settings) and navigating to Security Access, then input as prompted, one by one, the following IPs.

```
18.220.205.95
3.19.152.46
52.14.91.162
```

### 2. Downloading your .pem from MariaDB SkySQL

Under the same service, click on the world icon (connect to service) and after on the Download link under Login Credentials, Certificate authority chain; This will download on your machine an `aws_skysql_chain.pem` file; We recommend storing it securely as you will need it on a working public URL or local path, later, on the MindsDB configuration.

## Steps on [MindsDB](https://cloud.mindsdb.com/)

### 1. Navigate and log into [cloud.mindsdb.com](https://cloud.mindsdb.com/)

Navigate to [cloud.mindsdb.com](https://cloud.mindsdb.com/) to get access to MindsDB. Make sure you have an account, if you are unsure how to log in or sign in, please check the following doc.

### 2. Use the [`#!sql CREATE DATABASE`](/sql/create/databases/) to connect to your MariaDB SkySQL Service

Once logged into the MindsDB Graphical User Interface, use the following template to fill and execute a query connecting your MariaDB SkySQL instance with your MindsDB unlocking the ML capabilities.  

=== "Template"

    ```sql
    CREATE DATABASE maria_datasource            --- display name for the database
    WITH ENGINE='MariaDB',                      --- name of the MindsDB handler
    PARAMETERS={
      "host": " ",                              --- host IP address or URL
      "port": ,                                 --- port used to make TCP/IP connection
      "database": " ",                          --- database name
      "user": " ",                              --- database user
      "password": " ",                          --- database password
      "ssl": True/False,                        --- optional, the `ssl` parameter value indicates whether SSL is enabled (`True`) or disabled (`False`)
      "ssl_ca": {                               --- optional, SSL Certificate Authority
        "path": " "                                 --- either "path" or "url"
      },
      "ssl_cert": {                             --- optional, SSL certificates
        "url": " "                                  --- either "path" or "url"
      },
      "ssl_key": {                              --- optional, SSL keys
        "path": " "                                 --- either "path" or "url"
      }
    };
    ```

=== "Example for MariaDB SkySQL Service"

    ```sql
    CREATE DATABASE skysql_datasource
    WITH ENGINE = 'MariaDB',
    PARAMETERS = {
      "host": "mindsdbtest.mdb0002956.db1.skysql.net",
      "port": 5001,
      "database": "mindsdb_data",
      "user": "DB00007539",
      "password": "password",
      --- here, the SSL certificate is required
      "ssl-ca": {
        "url": "https://mindsdb-web-builds.s3.amazonaws.com/aws_skysql_chain.pem"
      }
    };
    ```

## What's Next?

Now that you are all set, we recommend you check out our **Tutorials** and **Community Tutorials** sections, where you'll find various examples of regression, classification, and time series predictions with MindsDB.

To learn more about MindsDB itself, follow the guide on [MindsDB database structure](/sql/table-structure/). Also, don't miss out on the remaining pages from the **SQL API** section, as they explain a common SQL syntax with examples.

Have fun!

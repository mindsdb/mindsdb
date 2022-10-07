
# [MariaDB SkySQL](https://cloud.MariaDB.com/) Setup Guide

## 1. Select your service for MindsDB

If you haven’t already, identify the service to be enabled with MindsDB and make sure it is running. Otherwise skip to step 2.

![type:video](https://youtube.com/embed/TgzK4WpUzwE)

## 2. Add MindsDB to your service Allowlist

Access to MariaDB SkySQL services is [restricted on a per-service basis](https://mariadb.com/products/skysql/docs/security/firewalls/ip-allowlist-services/).
Add the following IP addresses to allow MindsDB to connect to your MariaDB service, do this by clicking on the cog icon and navigating to Security Access. In the dialog, input as prompted – one by one – the following IPs:

```
18.220.205.95
3.19.152.46
52.14.91.162
```

## 3. Download your service .pem file

A [certificate authority chain](https://mariadb.com/products/skysql/docs/connect/connection-parameters-portal/#certificate-authority-chain) (.pem file) must be provided for proper TLS certificate validation.

From your selected service, click on the world globe icon (Connect to service). In the Login Credentials section, click Download. The `aws_skysql_chain.pem` file will download onto your machine.

## 4. Publically Expose your service .pem File

Select secure storage for the `aws_skysql_chain.pem` file that allows a working public URL or localpath.

## 5. Link [MindsDB](https://cloud.mindsdb.com) to your MariaDB SkySQL Service

To print the query template, select Add Data in either the top or side navigation and choose MariaDB SkySQL from the list. Fill in the values and run query to complete the setup.

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

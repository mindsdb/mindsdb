# Connect MindsDB MySQL API to MySQL CLI

MindsDB provides a powerful MySQL API that allows users to connect to it using the [MySQL Command-Line Client](https://dev.mysql.com/doc/refman/8.0/en/mysql.html). Connecting to MySQL API is the same as connecting to a MySQL database:

## How to Connect

```bash
mysql -h [hostname] --port [TCP/IP port number] -u [user] -p [password]
```

You can either connect locally or to a MindsDB Cloud instance, depending on the case; open your terminal and run:

=== "Self-Hosted Local Deployment"

    ```bash
    mysql -h 127.0.0.1 --port 47335 -u mindsdb
    ```

=== "MindsDB Cloud"

    ```bash
    mysql -h cloud.mindsdb.com --port 3307 -u [mindsdb_cloud_email] -p [mindsdb_cloud_password]
    ```

On execution, we get:

```bash
Welcome to the MariaDB monitor.  Commands end with ; or \g.
Server version: 5.7.1-MindsDB-1.0 (MindsDB)

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

MySQL [(none)]>
```

## Example

``` bash
mysql -h cloud.mindsdb.com --port 3306 -u zoran@mindsdb.com -p
```

```bash
Enter password:

Welcome to the MariaDB monitor.  Commands end with ; or \g.
Server version: 5.7.1-MindsDB-1.0 (MindsDB)

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

MySQL [(none)]>
```

## What's Next?

Now that you are all set, we recommend you check out our **Tutorials** and **Community Tutorials** sections, where you'll find various examples of regression, classification, and time series predictions with MindsDB.

To learn more about MindsDB itself, follow the guide on [MindsDB database structure](/sql/table-structure/). Also, don't miss out on the remaining pages from the **SQL API** section, as they explain a common SQL syntax with examples.

Have fun!

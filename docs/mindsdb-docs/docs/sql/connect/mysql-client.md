# MindsDB as a SQL Database

MindsDB provides a powerful MySQL API that allows users to connect to it using the [MySQL Command-Line Client](https://dev.mysql.com/doc/refman/8.0/en/mysql.html).

## Connecting MySQL Command-Line Client to the MindsDB MySQL API

Connecting to MySQL API is the same as connecting to a MySQL database:

```bash
mysql -h [hostname] --port [TCP/IP port number] -u [user] -p [password]
```

You can either connect locally or to a MindsDB Cloud instance, depending on the case; open your terminal and run:

=== "Local Deployment"

    ```bash
      mysql -h 127.0.0.1 --port 47335 -u mindsdb
    ```

=== "MindsDB Cloud"

    ```bash
      mysql -h cloud.mindsdb.com --port 3307 -u [mindsdb_cloud_email] -p [mindsdb_cloud_password]
    ```

On execution, you should get:

```bash
Welcome to the MariaDB monitor.  Commands end with ; or \g.
Server version: 5.7.1-MindsDB-1.0 (MindsDB)

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

MySQL [(none)]>
```

??? example "Example"

    ``` bash
    ~$ mysql -h cloud.mindsdb.com --port 3306 -u zoran@mindsdb.com -p
    ```

    ```bash
    Enter password:

    Welcome to the MariaDB monitor.  Commands end with ; or \g.
    Server version: 5.7.1-MindsDB-1.0 (MindsDB)

    Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

    MySQL [(none)]>
    ```

## The MindsDB Database

### General Structure

On startup the mindsdb database will contain 2 tables: `predictors` and `datasources`

```sql

MySQL [mindsdb]> show tables;
+---------------------------+
| Tables_in_mindsdb         |
+---------------------------+
| predictors                |
| datasources               |
+---------------------------+
3 rows in set (0.14 sec)

```

### The predictors TABLE

All of the newly trained machine learning models will be visible as a new record inside the `predictors` table.
The `predictors` columns contains information about each model as:

| Column name         | Description                                  |
| ------------------- | -------------------------------------------- |
| `name`              | The name of the model.                       |
| `status`            | Training status(training, complete, error).  |
| `predict`           | The name of the target variable column.      |
| `accuracy`          | The model accuracy.                          |
| `update_status`     | Trainig update status(up_to_date, updating). |
| `mindsdb_version`   | The mindsdb version used.                    |
| `error`             | Error message info in case of an errror.     |
| `select_data_query` | SQL select query to create the datasource.   |
| `training options`  | Additional training parameters.              |

### The datasource TABLE

### The `[integration_name]` TABLE

## Troubleshooting

!!! warning "Connecting to localhost"
    Make sure you always use `127.0.0.1` locally instead of `localhost` as a hostname.

!!! tip "Passing no parameter after -p"
    You can omit writing your password as a parameter; the MySQL Command-Line Client will ask you prompt you to input the password directly on the console:

    ```bash
    mysql -h cloud.mindsdb.com --port 3306 -u zoran@mindsdb.com -p
    Enter password:
    ```

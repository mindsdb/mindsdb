# MindsDB as a SQL Database

MindsDB provides a powerful MySQL API that allows users to connect to it using the [MySQL Command-Line Client](https://dev.mysql.com/doc/refman/8.0/en/mysql.html) or [DBeaver](https://dbeaver.io/). By default, MindsDB Server will start the HTTP and MySQL APIs. If you want to run only the MySQL API you can provide that as a parameter on the server start:

```
python3 -m mindsdb --api=http,mysql
```

This will start MySQL API on a `127.0.0.1:47335` with `mindsdb` as default user and create a `mindsdb` database. To change the default parameters you need to extend the MindsDBs `config.json` or create another config and send it as a parameter to the serve start command as:

```
python3 -m mindsdb --api=http,mysql --config=config.json
```

In case you are using Docker, visit the [Docker extend config docs](/deployment/docker/#extend-configjson).
To read more about available config.json options check the [configuration docs](/datasources/configuration/#extending-default-configuration).

## Connect

!!! tip "Connecting to the localhost"
    Make sure you always use 127.0.0.1 locally instead of localhost as a hostname.

Connecting to MySQL API is the same as connecting to a MySQL database. You can use one of the below clients to connect:

* [MySQL Command-Line Client](https://dev.mysql.com/doc/refman/8.0/en/mysql.html) 
* [DBeaver](https://dbeaver.io/)


## MySQL client

Open mysql client and run:

```
mysql -h 127.0.0.1 --port 47335 -u mindsdb -p 
```

The required parameters are:

* -h: Host name of mindsdbs mysql api (127.0.0.1).
* --port: TCP/IP port number for connection(47335).
* -u: MySQL user name to use when connecting(default mindsdb).
* -p:  Password to use when connecting(default no password).

![Connect](/assets/sql/mysql-client.gif)


## Dbeaver

If you are using Dbeaver make sure to select Driver for MySQL 8 or later. If the driver is missing you can [download it](https://dev.mysql.com/downloads/connector/j/) and add it from the [database-drivers section](https://dbeaver.com/docs/wiki/Database-drivers/).

1. From the navigation menu, click Connect to database.
2. Search `MySQL 8+`.

    ![Connect mysql 8](/assets/sql/dbeaver8.png)

3. Select the `MySQL 8+` or `MySQL`.
4. Click on `Next`.
5. Add the Hostname (127.0.0.1).
6. Add the Database name (leave empty).
7. Add Port (47335).
8. Add the database user (default mindsdb).
9. Add Password for the user (default empty).
10. Click on `Finish`.

![Connect](/assets/sql/dbeaver-local.png)


## MindsDB Database

On startup the mindsdb database will contain 2 tables `predictors` and `commands`. 

![Connect](/assets/sql/show.png)

All of the newly trained machine learning models will be visible as a new record inside the `predictors` table. The `predictors` columns contains information about each model as:

* name - The name of the model.
* status - Training status(training, complete, error).
* accuracy - The model accuracy.
* predict - The name of the target variable.
* select_data_query - SQL select query to create the datasource.
* training options - Additional training parameters. The full list can be found at [Predictor Interface docs](/PredictorInterface/#learn).

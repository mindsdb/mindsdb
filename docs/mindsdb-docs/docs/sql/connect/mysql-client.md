# MindsDB as a SQL Database

MindsDB provides a powerful MySQL API that allows users to connect to it using the [MySQL Command-Line Client](https://dev.mysql.com/doc/refman/8.0/en/mysql.html).

## Connect

!!! tip "Connecting to the localhost"
    Make sure you always use 127.0.0.1 locally instead of localhost as a hostname.

Connecting to MySQL API is the same as connecting to a MySQL database. Open mysql client and run:

```
mysql -h 127.0.0.1 --port 47335 -u mindsdb -p 
```

The required parameters are:

* -h: Host name of MindsDB SQL Server
    * cloud.mindsdb.com - for MindsDB cloud
    * 127.0.0.1 - for local deployment
* --port: TCP/IP port number for connection
    * 3307 - for MindsDB cloud
    * 47335 - for local deployment
* -u: user name to use when connecting
    * your MindsDB cloud name
    * mindsdb - for local deployment
* -p:  Password to use when connecting
    * your MindsDB cloud password
    * no password for local deployment

![Connect](/assets/sql/mysql-client.gif)


## MindsDB Database

On startup the mindsdb database will contain 3 tables as  `predictors`, `commands` and `datasources`. 
    <div id="create-datasource">
      <style>
        #create-datasource code { background-color: #353535; color: #f5f5f5 }
      </style>
    ```
    MySQL [(none)]> use mindsdb;
    Database changed
    MySQL [mindsdb]> show tables;
    +---------------------------+
    | Tables_in_mindsdb         |
    +---------------------------+
    | predictors                |
    | commands                  |
    | datasources               |
    +---------------------------+
    3 rows in set (0.14 sec)

    MySQL [mindsdb]> 
    ```
    </div>


All of the newly trained machine learning models will be visible as a new record inside the `predictors` table. The `predictors` columns contains information about each model as:

* name - The name of the model.
* status - Training status(training, complete, error).
* accuracy - The model accuracy.
* predict - The name of the target variable.
* select_data_query - SQL select query to create the datasource.
* training options - Additional training parameters.

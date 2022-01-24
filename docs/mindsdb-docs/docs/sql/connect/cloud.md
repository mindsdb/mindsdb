# MindsDB Cloud as a SQL Database

MindsDB Cloud provides a powerful MySQL API that allows cloud users to connect to it. The first step to connect is to use the MindsDB Cloud user. If you haven't signup to the MindsDB Cloud follow the steps explained [here](/deployment/cloud). After that you can use one of the below db clients:

* [MySQL Command-Line Client](https://dev.mysql.com/doc/refman/8.0/en/mysql.html) 
* [DBeaver](https://dbeaver.io/)

## MySQL client

Open mysql client and run:

```
mysql -h cloud.mindsdb.com --port 3306 -u cloudusername@mail.com -p
```
The required parameters are:

* -h: Host name of mindsdbs mysql api (by default takes cloud.mindsdb.com if not specified)	
* --port: TCP/IP port number for connection(3306)	
* -u: MindsDB Cloud username
* -p:  MindsDB Cloud password

![Connect](/assets/sql/mysql-client.gif)


## Dbeaver

If you are using Dbeaver make sure to select Driver for MySQL 8 or later. If the driver is missing you can [download it](https://dev.mysql.com/downloads/connector/j/) and add it from the [database-drivers section](https://dbeaver.com/docs/wiki/Database-drivers/).

1. From the navigation menu, click Connect to database.
2. Search `MySQL 8+`.
    ![Connect mysql 8](/assets/sql/dbeaver8.png)

3. Select the `MySQL 8+` or `MySQL`.
4. Click on `Next`.
5. Add the Hostname (cloud-mysql.mindsdb.com).
6. Add the Database name (leave empty).
7. Add Port (3306).
8. Add the database user (your MindsDB Cloud username).
9. Add Password for the user (your MindsDB Cloud password).
10. Click on `Finish`.

![Connect](/assets/sql/connectcloud.png)

## MindsDB Database

At startup mindsdb database will contain 2 tables `predictors` and `commands`. 

![Connect](/assets/sql/show.png)

All of the newly trained machine learning models will be visible as a new record inside the `predictors` table. The `predictors` columns contains information about each model as:

* name - The name of the model.
* status - Training status(training, complete, error).
* accuracy - The model accuracy.
* predict - The name of the target variable.
* select_data_query - SQL select query to create the datasource.
* training options - Additional training parameters. The full list can be found at [Predictor Interface docs](/PredictorInterface/#learn).


!!! tip "Whitelist MindsDB Cloud IP address"
    If you need to whitelist MindsDB Cloud IP address to have access to your database, reach out to MindsDB team so we can share the Cloud static IP with you.

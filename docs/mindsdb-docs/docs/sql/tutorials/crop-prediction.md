# Best crop type prediction

Modern agriculture is becoming very dependant on technology. From advanced machinery to specially selected crops. All the technology produces a lot of data that can be used for better adjustment of the farming process. One use case of machine learning in agriculture could be the selection of the best crop for a specific field to maximize the potential yield. Such problems are often called *Classification Problems* in machine learning. With **MindsDB** you can easily use your existing database to create prediction models that help you make better business decisions.

In this tutorial you will learn how to predict the best crop type based on field parameters using **MindsDB** and **MariaDB**.

## Pre-requisites

Before you start make sure you have:

1. Access to MindsDB. Check out the installation guide for [Docker](https://docs.mindsdb.com/deployment/docker/) or [PyPi](https://docs.mindsdb.com/deployment/pypi/). You can also use [MindsDB Cloud](https://docs.mindsdb.com/deployment/cloud/).
2. Access to a MariaDB database. You can install it [locally](https://mariadb.org/download/) or through [Docker](https://hub.docker.com/_/mariadb).
3. Downloaded the dataset. You can get it from [Kaggle](https://www.kaggle.com/atharvaingle/crop-recommendation-dataset).
4. Access to mysql-client. You can probably get it from your systems package manager. For Debian/Ubuntu check [here](https://packages.ubuntu.com/search?keywords=mysql-client).
4. Optional: Access to ngrok. You can check the installation details at the [ngrok website](https://ngrok.com/).

## Setup the database

In this section you will create a MariaDB database and a table into which you will then load the dataset.

First connect to your MariaDB instance. You can use the CLI based `mysql` or any manager like [DBeaver](https://dbeaver.io/).

If you have MariaDB running locally, you can use the following line to connect.

> Remember to change the username if you have a different one set up in MariaDB.

```bash
mysql -u root -p -h 127.0.0.1
```

After connecting you can create a database for the project. You can skip this step if you already have a database you want to use.

```sql
CREATE DATABASE agriculture;
```
You can check that the database was created with the following query.

```sql
SHOW DATABASES;
```
The output will be similar to the one below.

```console
+--------------------+
| Database           |
+--------------------+
| agriculture        |
| information_schema |
| mysql              |
| performance_schema |
| sys                |
+--------------------+
5 rows in set (0.01 sec)
```
Next, you need to create a table for the dataset. To do so, first switch to the database you want to use.

```sql
USE agriculture;
```

Now you can create the table with the following schema.

```sql
CREATE TABLE crops (
    N INT,
    P INT,
    K INT,
    temperature INT,
    humidity DECIMAL(10, 2),
    ph DECIMAL(10, 2),
    rainfall DECIMAL(10, 2),
    label VARCHAR(50)
);
```

You can check if the table was created with the `SHOW TABLES;` query. You should see a similar output.

```console
+-----------------------+
| Tables_in_agriculture |
+-----------------------+
| crops                 |
+-----------------------+
1 row in set (0.00 sec)
```

When the table is created you can load the dataset into it.

To load the CSV file into the table use the following query.
> Remember to change the path to the dataset to match the file location on your system.

```sql
LOAD DATA INFILE '/Crop_recommendation.csv'
INTO TABLE crops 
FIELDS TERMINATED BY ','
IGNORE 1 LINE;
```

To verify that the data has been loaded, you can make a simple `SELECT` query.

```sql
SELECT * FROM crops LIMIT 5;
```

You should see a similar output.

```console
+------+------+------+-------------+----------+------+----------+-------+
| N    | P    | K    | temperature | humidity | ph   | rainfall | label |
+------+------+------+-------------+----------+------+----------+-------+
|   90 |   42 |   43 |          21 |    82.00 | 6.50 |   202.94 | rice
|   85 |   58 |   41 |          22 |    80.32 | 7.04 |   226.66 | rice
|   60 |   55 |   44 |          23 |    82.32 | 7.84 |   263.96 | rice
|   74 |   35 |   40 |          26 |    80.16 | 6.98 |   242.86 | rice
|   78 |   42 |   42 |          20 |    81.60 | 7.63 |   262.72 | rice
+------+------+------+-------------+----------+------+----------+-------+
5 rows in set (0.00 sec)
```

You have now finished setting up the MariaDB database!

## Connect MindsDB to your database

In this section, you will connect your database to MindsDB.

The recommended way of connecting a database to MindsDB is through MindsDB GUI. In this tutorial we will use the GUI at MindsDB Cloud.

Since our MariaDB instance is local we will use `ngrok` to make it available to MindsDB Cloud. If you are using a MariaDB instance that already has a public address or you have installed MindsDB locally you can skip this step.

First you need to setup an ngrok tunnel with the following command.
> If you have used a diffrent port for your MariaDB installation, remember to change it here.

```bash
ngrok tcp 3306
```

You should see a similar output:

```console
Session Status                online
Account                       myaccount (Plan: Free)
Version                       2.3.40
Region                        United States (us)
Web Interface                 http://127.0.0.1:4040
Forwarding                    tcp://x.tcp.ngrok.io:12345 -> localhost:5432
```
Now you can copy the forwarded address from the above output. You are interested in the `x.tcp.ngrok.io:12345` part.

With the address copied, head over to MindsDB GUI.

In the main screen, select `ADD DATABASE`. Then add your integration details.

![MindsDB MariaDB integration details](/docs/mindsdb-docs/docs/assets/sql/tutorials/crop-prediction/database-integration-mariadb.png)

Click `Connect`, you should now see your MariaDB database connection in the main screen.

You are now done with connecting MindsDB to your database! 🚀

## Create a predictor

In this section you will connect to MindsDB with the MySQL API and create a predictor.

First you need to connect to MindsDB through the MySQL API. To do so, use the following command.
> Remember to change the username for the connection

```bash
mysql -h cloud.mindsdb.com --port 3306 -u cloudusername@mail.com -p
```
After that switch to the `mindsdb` database.

```sql
USE mindsdb;
```

Use the following query to create a predictor that will predict the `label` (*crop type*) for the specific field parameters.

```sql
CREATE PREDICTOR crop_predictor
FROM crops_integration (
    SELECT * FROM crops
) PREDICT label as crop_type;
```

After creating the predictor you should se a similar output:

```console
Query OK, 0 rows affected (11.66 sec)
```

Now the predictor will begin training. You can check the status of the predictor with the following query.

```sql
SELECT * FROM mindsdb.predictors WHERE name='crop_predictor';
```

After the predictor has finished training, you will see a similar output.

```console
+-----------------+----------+--------------------+---------+---------------+-----------------+-------+-------------------+---------------------+------------------+
| name            | status   | accuracy           | predict | update_status | mindsdb_version | error | select_data_query | external_datasource | training_options |
+-----------------+----------+--------------------+---------+---------------+-----------------+-------+-------------------+---------------------+------------------+
| crop_predictor | complete | 0.9954545454545454 | label   | up_to_date    | 2.55.2          |       |                   |                     |                  |
+-----------------+----------+--------------------+---------+---------------+-----------------+-------+-------------------+---------------------+------------------+
1 row in set (0.29 sec)

```

You are now done with creating the predictor! ✨

## Make predictions

In this section you will learn how to make predictions using your trained model.

To run a prediction against new or existing data, you can use the following query.

```sql
SELECT label
FROM mindsdb.crop_predictor
WHERE when_data='{"N": 77, "P": 52, "K": 17, "temperature": 24, "humidity": 20.74, "ph": 5.71, "rainfall": 75.82}'\G
```

```console
label: maize
1 row in set (0.32 sec)
```

As we have used a real data point from our dataset we can verify the prediction.
```text
N,  P,  K,  temperature,  humidity,   ph,           rainfall,     label
77, 52, 17, 24.86374934,  65.7420046, 5.714799723,  75.82270467,  maize
```
 
As you can see, the model correctly predicted the most appropriate crop type for our field.

You are now done with the tutorial! 🎉

For more check out other [tutorials and MindsDB documentation](https://docs.mindsdb.com/).